# Background Delete with a `deleting` Status + Action Logging — Design

**Date:** 2026-09-13
**Status:** Approved (user: "i want that and fix the logging")

## Problem

Deleting `bigscience/bloom` (~330 GB) looked stuck. The `delete` endpoint runs
`shutil.rmtree` **synchronously** and only then drops the DB row, so for the
~2 minutes the unlink took the dashboard kept re-rendering an unchanged
`completed` card. The browser dropped the connection before the response came
back, and uvicorn skips the access-log line for a disconnected client, so the
journal has **no record that a delete ever happened** — only the app's own
WARNING lines reach the journal today (the `app` logger has no INFO handler).

The same synchronous-rmtree shape exists in `cancel` (queued/paused/retrying
jobs with partial files) and `redownload` (corrupted jobs), and the worker's
`cancel` branch holds a concurrency slot while it rmtrees.

## Goal

1. Every "discard files" action returns immediately, the job shows a
   **`deleting`** status with a **draining progress bar** until its files are
   gone, and the row then disappears (or, for re-download, returns to `queued`).
2. Every state-changing request and every deletion outcome leaves a line in
   `journalctl`, regardless of whether the client waited for the response.

## Scope

- **In:** `deleting` status; a background deleter used by `delete`, `cancel`
  (non-running), `redownload`, and the worker's running-`cancel` branch; startup
  resume of orphaned `deleting` jobs; dashboard rendering; INFO logging for the
  `app` package; a request-arrival log line for mutating API calls; tests; docs.
- **Out (YAGNI):** parallel/serialized deletion queues, cancelling a deletion,
  deleting `failed` jobs (pre-existing gap, separate concern).

## State machine

One new persisted status, `deleting`.

```
completed ──(delete)────────> deleting ──> [row removed]
queued|paused|retrying ──(cancel)──> deleting ──> [row removed]
running ──(cancel, child exits)────> deleting ──> [row removed]
completed+corrupted ──(redownload)─> deleting ──> queued (bytes 0, unverified)
deleting ──(rmtree error)──────────> failed  (error = "could not delete files: …")
```

`deleting` is **not** counted by `running_count()` (it holds no download slot),
is not runnable, not in `pending_bytes()`, and is left alone by the startup
resets for `running`/`verifying`. Re-adding a repo whose job is `deleting` is
treated like any in-progress job (returned unchanged, not requeued).

## Mechanism

`app/backup.py`:

- `run_delete_job(job_id, store, settings, requeue=False)` — the deleter. Sets
  `deleting`, starts the same kind of daemon poller the download uses (samples
  `directory_size` every `POLL_INTERVAL` into `downloaded_bytes`, so the bar
  drains), calls `delete_backup_files`, stops the poller, then either
  `delete_job` or (requeue) `requeue` + `reset_retry` + `set_verify_status
  ("unverified")` + `update_progress(0)`. Any exception lands the job at
  `failed` with `error="could not delete files: <exc>"` and `verify_status`
  `unverified`, logged with traceback. (Partial deletion leaves a broken tree;
  `failed` → Retry re-downloads the missing files, which is the right recovery.)
- `start_delete(job_id, store, settings, requeue=False) -> Thread` — marks
  `deleting` synchronously (so the very next poll shows it) and runs the deleter
  on a **daemon thread**. Not the download executor: with
  `MAX_CONCURRENT_JOBS=1` a delete there would wait behind a multi-hour
  download and vice versa. Daemon so a systemd stop is not delayed by an
  in-flight rmtree; the job stays `deleting` in the DB and startup resumes it
  (rmtree of a half-removed tree is idempotent, a missing dir is a no-op).
- `JobRunner.delete(job_id, requeue=False)` wraps `start_delete`.
- The worker's `intent == "cancel"` branch calls `start_delete` instead of
  deleting inline, freeing the slot immediately.

`app/db.py`: `DELETING` constant; `deleting_jobs()`.

`app/main.py`: `delete`, `cancel` (non-running), `redownload` call
`runner.delete(...)` and all return `{"deleting": id}`. Lifespan re-submits
every `deleting_jobs()` on startup as a plain delete — the requeue intent is not
persisted, so a re-download interrupted by a restart mid-rmtree ends as a
delete (re-add the repo to download it again). Rare enough not to warrant a
column.

## Logging

- `configure_logging()` in `app/main.py`, called from `__main__` before
  `uvicorn.run`: attaches a stderr handler at INFO to the `app` logger,
  formatted like uvicorn's lines (`INFO:     …`), `propagate=False`. Root and
  third-party loggers are untouched (no httpx/hub chatter).
- A Starlette middleware logs every non-GET request **on arrival**:
  `POST /api/jobs/8/delete from 10.0.0.114` — independent of whether the client
  waits for the response.
- The deleter logs `deleting <slug> (<type>) job <id>: <size> at <path>`,
  `deleted <slug> (<type>) job <id> in <n>s, freed <size>[, requeued]`, and
  `ERROR could not delete files for <slug>…` with traceback.

## Dashboard

`deleting` rows: label `deleting…`, amber like `paused`/`retrying`, **no action
buttons**, bar drains via `percent` as the poller reports shrinking bytes.
Cancel/Delete confirm dialogs are unchanged.

## Testing (offline, injected fakes)

- db: `deleting_jobs()`; `running_count()` ignores `deleting`.
- backup: deleter removes files + row; progress reported downward while
  deleting; `requeue=True` lands at `queued`/0 bytes/`unverified`; rmtree error →
  `failed` + error + ERROR log; `start_delete` marks `deleting` before returning;
  worker cancel frees the slot and still removes files + row; runner shutdown
  does not block on a deleter thread.
- api: `delete`/`cancel`/`redownload` hand off to `runner.delete` and return
  immediately with the job in `deleting`; startup re-submits orphaned
  `deleting` jobs; mutating requests are logged on arrival (caplog); re-adding a
  `deleting` repo does not requeue it.
- static: `.st.deleting` rule and `deleting` label present.
- main: `configure_logging()` installs one INFO handler on `app`, idempotent.
