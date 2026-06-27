# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A small FastAPI web app + dashboard that backs up entire Hugging Face Hub repositories
(models, datasets, spaces) to a local folder, with bounded concurrency, live progress, and
automatic resume. Single-host, no auth.

## Environment & commands

**There is no system `pip` or `python` on the PATH — always use the venv interpreter.** `pip`
was bootstrapped into `.venv` via `get-pip.py`.

```bash
.venv/bin/python -m pip install -r requirements.txt   # install deps
.venv/bin/python -m app.main                          # run (binds 0.0.0.0:8000 by default)
HOST=127.0.0.1 PORT=9000 .venv/bin/python -m app.main # restrict to localhost / change port
.venv/bin/uvicorn app.main:build_default_app --factory --reload   # dev autoreload

.venv/bin/python -m pytest                                        # unit tests (Hub mocked)
.venv/bin/python -m pytest tests/test_backup_helpers.py::test_name # single test
.venv/bin/python -m pytest -m integration                         # end-to-end vs real Hub (network)
```

Integration tests are **excluded by default** (`pytest.ini` sets `-m "not integration"`); pass
`-m integration` to opt in. There is no linter configured.

`.env` holds secrets (HF token) — **do not read it**; reference variables by name. Required:
`HUGGINGFACE_ACCESS_KEY`, `BACKUP_DIR`. Optional: `MAX_CONCURRENT_JOBS` (default 2), `MAX_WORKERS`
(default 8), `DB_PATH` (default `jobs.db`). See `.env.example`.

## Architecture

The app is built around **dependency injection so tests never hit the network**. `create_app()`,
`JobRunner`, and `run_backup_job()` all accept injected collaborators (`api`, `launcher`,
`detect`); `build_default_app()` is the only place that wires the real `HfApi` /
`snapshot_download`. Tests pass fakes — that's why the default suite runs offline.

Request/data flow across the four modules:

- **`config.py`** — `load_settings()` reads env into a frozen `Settings` dataclass; validates the
  token, creates/writes-checks `BACKUP_DIR`, raises `ConfigError` on bad input.
- **`db.py`** — `JobStore` wraps **one** SQLite connection shared across threads (a `threading.Lock`
  guards every call; `check_same_thread=False`). `Job` has a computed `percent`. Status lifecycle:
  `queued → running → completed | failed | paused | retrying`. A **transient** failure (DNS,
  connection drop, timeout, 5xx/429 — classified by `app/retry.py` against httpx) auto-retries up
  to 5× with backoff `30s/60s/2m/4m/8m` (status `retrying`, `next_retry_at`); **permanent** errors
  (404/gated/bad slug/disk-full) go straight to `failed`. Paused jobs are excluded from startup
  re-queuing and resumed manually. Cancelling removes the row for queued/paused/retrying jobs and
  also terminates the child process for running ones, discarding files. `__init__` **migrates a
  pre-existing `jobs` table** (adds `retry_count`/`next_retry_at`) and seeds the `app_state` table,
  which holds the persistent global Pause/Play **valve** (`paused_all`). `UNIQUE(repo_type, slug)`
  means one row per repo+type.
- **`backup.py`** — the worker engine. A **central dispatcher** loop (`JobRunner.start()`) is the
  only thing that starts downloads: while the valve is open and a slot is free (`running_count <
  max_concurrent_jobs`) it claims the lowest-id eligible job (`queued`, or `retrying` whose backoff
  elapsed) and submits it to a `ThreadPoolExecutor`. Endpoints/lifespan only write DB state.
  `run_backup_job()` handles **one** repo: size it, run a **pre-flight disk-space check**, then run
  `snapshot_download(..., max_workers=max_workers)` in a **terminable child process**
  (`app/launcher.py`, `SubprocessLauncher`, spawn) so it can be paused/cancelled near-instantly even
  mid-file. A `RunningRegistry` maps `job_id → handle`+intent (`pause`/`cancel`/`requeue`); the
  worker performs the single terminal transition (`completed`/`paused`/`failed`/`retrying`, delete,
  or requeue) after the child exits. Global pause (`pause_all`) closes the valve and requeues every
  running job; `resume_all` reopens it.
- **`main.py`** — HTTP API + serves the `static/` dashboard. The FastAPI **lifespan hook resets
  orphaned `running` jobs to `queued` and starts the dispatcher** (the auto-resume mechanism, valve
  permitting). Endpoints: `POST/GET /api/jobs`, `GET /api/storage` (includes `planned` and
  `paused_all`), `POST /api/jobs/{id}/retry|pause|resume|cancel|delete`, and `POST
  /api/pause-all|resume-all` (the global valve).

### Two things that are easy to get wrong

1. **Progress is measured from disk, not the downloader.** A daemon poller thread samples
   `directory_size(local_dir)` every `POLL_INTERVAL` (1.5s) and writes it to the DB. The download
   completing is decided by `snapshot_download` returning, *not* by the byte count. `directory_size`
   counts completed files **plus `*.incomplete` Xet staging** under `.cache/huggingface/download/`
   (other `.cache` content is excluded) — without that, progress freezes for minutes under Xet then
   jumps, because Xet stages large files there before renaming them into place.

2. **`MAX_CONCURRENT_JOBS` × `MAX_WORKERS` multiply into memory pressure** (repos-in-parallel ×
   files-per-repo). Both are the dials for the speed/RAM tradeoff.

## Deployment (systemd) & the Xet memory gotcha

Runs as the systemd unit **`hug-face-rip`**. The repo copy `deploy/hug-face-rip.service` is the source of
truth; it is **installed to `/etc/systemd/system/`**. To change service config:

```bash
sudo cp deploy/hug-face-rip.service /etc/systemd/system/ && sudo systemctl daemon-reload \
  && sudo systemctl restart hug-face-rip
```

`Environment=` vars and `MemoryMax` only take effect **on restart** — the running process keeps its
old env until then. The service runs from the working tree, so a restart also picks up code changes.

**hf-xet downloader memory:** `hf-xet` (used by `snapshot_download` for Xet-backed repos) has
adaptive concurrency that can balloon download buffers into the gigabytes and OOM the box. On a
small host, set `HF_HUB_DISABLE_XET=1` (plain HTTP, ~near-constant memory). On a host with RAM
headroom, leave Xet enabled but bound it with `HF_XET_NUM_CONCURRENT_RANGE_GETS` (and keep
`MemoryMax` as a cgroup backstop). **Do not** set `HF_XET_HIGH_PERFORMANCE` — it saturates network
and all cores and is the most memory-hungry mode.

**Subprocess memory model (post pause/resume):** Each concurrent download now runs in its own
spawned Python child process, so `MAX_CONCURRENT_JOBS` multiplies whole-process baseline memory
(including `hf_xet` buffer pools), not just per-thread buffers — plan headroom accordingly.
`MemoryMax` still bounds the total because child processes inherit the parent's systemd cgroup.
Importantly, an OOM kill now most likely terminates a single download child rather than the whole
server — that job lands in `failed` (retryable, partial files kept) and the dashboard stays up.

Binding to `0.0.0.0` (the default) exposes an **unauthenticated** dashboard that downloads using
your HF token — only run on a trusted network, or set `HOST=127.0.0.1`.

## Git workflow

History uses short-lived feature branches merged into `master` with `--no-ff` (see `git log`).
Match that: branch, commit focused changes, `git merge --no-ff`, then push.
