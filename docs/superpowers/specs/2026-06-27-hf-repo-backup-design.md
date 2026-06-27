# HF Repo Backup Dashboard — Design

**Date:** 2026-06-27
**Status:** Approved

## Summary

A small self-hosted web app that backs up entire Hugging Face Hub
repositories (models, datasets, or spaces) to a local folder. The user
pastes a repo slug into a dashboard; the app downloads the full repo to a
configured backup directory, with bounded concurrency, live progress, and
automatic resume across both transient failures and server restarts.

## Configuration (`.env`)

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `HUGGINGFACE_ACCESS_KEY` | yes | — | HF access token, used to authenticate downloads (incl. private/gated repos). |
| `BACKUP_DIR` | yes | — | Destination root folder. Each repo is backed up to `BACKUP_DIR/<repo_type>s/<owner>/<name>` (e.g. `BACKUP_DIR/datasets/bigcode/the-stack`), mirroring the Hub's URL layout. |
| `MAX_CONCURRENT_JOBS` | no | `2` | How many repos download simultaneously. |
| `MAX_WORKERS` | no | `8` | Parallel file-download threads per repo (passed to `snapshot_download`). |
| `DB_PATH` | no | `jobs.db` | SQLite job-store location. |

`config.py` loads these via `python-dotenv`, validates that the two required
vars are present and that `BACKUP_DIR` exists (creating it if needed) and is
writable. On failure it raises a clear error at startup — the server does not
start in a half-configured state.

## Architecture

A FastAPI app with a background worker pool:

- **Web layer** — serves the dashboard and a small JSON API for creating and
  listing jobs.
- **Worker layer** — a bounded pool that runs downloads via
  `huggingface_hub.snapshot_download`. A global semaphore caps concurrent
  repos at `MAX_CONCURRENT_JOBS`; each download uses `MAX_WORKERS` threads for
  parallel files. This is the "graceful concurrent thread management."
- **Persistence** — SQLite, so jobs and their progress survive a restart.

### Components

- **`config.py`** — load + validate `.env`. Exposes a typed settings object.
- **`db.py`** — SQLite job store. Single `jobs` table:
  `id, slug, repo_type, status, total_bytes, downloaded_bytes, error,
  created_at, updated_at`. A repo's identity is the tuple
  **`(repo_type, slug)`** — that is the unique key (a model and a dataset that
  share `owner/name` are two distinct backups). A `UNIQUE(repo_type, slug)`
  index prevents duplicate active jobs for the same repo; re-submitting an
  existing repo resumes/retries it rather than creating a duplicate. Statuses:
  `queued → running → completed | failed`, plus `cancelled` for queued jobs
  cancelled before they start. All access is thread-safe (short-lived
  connections / serialized writes).
- **`backup.py`** — repo-type detection plus the download worker.
  - **`detect_repo_types(slug) -> list[str]`** — probes `model`, `dataset`,
    and `space` via `HfApi.repo_info` and returns *every* type that resolves
    (a slug can validly exist in more than one namespace). Called by the API at
    job-creation time so each matching type becomes its own job. Empty list ⇒
    "repo not found or not accessible".
  - **Worker** — given a job's known `(repo_type, slug)`:
    1. **Size the repo** — sum sibling file sizes from
       `repo_info(files_metadata=True)` to get `total_bytes`.
    2. **Download** — `snapshot_download(repo_id, repo_type,
       local_dir=BACKUP_DIR/<repo_type>s/<owner>/<name>, token=...,
       max_workers=MAX_WORKERS)`. `local_dir` produces real files (not cache
       symlinks), which is what a backup should be.
    3. **Progress poller** — a lightweight loop sums bytes-on-disk in the
       target dir every ~1.5s and writes `downloaded_bytes`, giving a live
       percentage without hooking the library's internals.
    4. Concurrency gated by a `threading.Semaphore(MAX_CONCURRENT_JOBS)`.
- **`main.py`** — FastAPI routes + static file serving + startup resume hook.
- **`static/index.html` (+ inline JS/CSS)** — paste-slug form and a live job
  table that polls the API.

## Data Flow

1. User pastes a slug → `POST /api/jobs`.
2. The API calls `detect_repo_types(slug)`. For each matching type it inserts a
   `queued` row keyed on `(repo_type, slug)` and submits it to the worker pool;
   the response lists the created jobs. Zero matches → `404` with
   "repo not found or not accessible". A type already present resumes/retries
   instead of duplicating.
3. Each worker acquires the semaphore, sizes its repo, downloads to its
   `BACKUP_DIR/<repo_type>s/<owner>/<name>` folder, and updates progress as it
   goes.
4. Frontend polls `GET /api/jobs` every ~1.5s and re-renders the table
   (status, progress bar, downloaded/total, error text).

## Auto-Resume (two layers)

1. **Within a download** — `snapshot_download` skips files already complete on
   disk and resumes partially-downloaded files automatically.
2. **Across restarts** — on startup, any job left in `queued` or `running` is
   re-queued and re-run. Because finished files are already on disk, the
   download effectively continues where it stopped. Failed jobs expose a
   **Retry** button that resumes the same way.

## Error Handling

Every failure mode marks the job `failed` with a human-readable `error`
message surfaced in the table; the server stays up:

- **Repo not found / not accessible** — all three type probes miss.
- **Auth failure** — bad/insufficient token for a private or gated repo.
- **Disk full / permission** — raised by `snapshot_download`; captured per job.
- **Network errors** — `snapshot_download` retries internally; if it still
  fails, the job is marked failed and can be retried (resuming).

## API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/` | Dashboard HTML. |
| `POST` | `/api/jobs` | Body `{ "slug": "owner/name" }`. Detects matching repo type(s) and creates one queued job per match (resuming any that already exist); returns the created/affected jobs. `404` if the slug matches no type. |
| `GET` | `/api/jobs` | List all jobs with live progress (polling endpoint). |
| `POST` | `/api/jobs/{id}/retry` | Re-queue a `failed` job (resumes). |
| `POST` | `/api/jobs/{id}/cancel` | Cancel a `queued` job (no-op if already running). |

## Out of Scope (v1)

- Cancelling a download that is already in flight (only queued jobs cancel).
- Multi-user auth / accounts — single-user, local tool.
- Scheduling / periodic re-sync.
- Deleting or pruning backups from the UI.

## Testing (TDD)

- **Unit:** config validation (missing vars, unwritable dir); DB job lifecycle
  transitions; repo-type detection with a mocked `HfApi`; progress/percent math
  and the byte-sizing helper.
- **Integration:** back up a tiny real public repo end-to-end and assert files
  land in `BACKUP_DIR` and the job reaches `completed`.
- HF network calls are mocked in unit tests; the single integration test is the
  only one that touches the real Hub.
