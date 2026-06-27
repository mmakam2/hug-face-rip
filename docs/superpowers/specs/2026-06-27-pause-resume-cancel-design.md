# Pause / Resume / Cancel for In-Progress Downloads — Design

**Date:** 2026-06-27
**Status:** Approved (pending spec review)

## Goal

Let a user **pause**, **resume**, and **cancel** a download that is actively
running (status `running`), plus extend **cancel** to be the universal "stop and
discard" action for any not-yet-completed job.

- **Pause** stops the download but keeps the bytes on disk, so it can be resumed
  later. Status becomes `paused`.
- **Resume** restarts a `paused` job; Hugging Face's automatic resume reuses the
  already-downloaded files and `.incomplete` staging, so no progress is lost.
- **Cancel** stops whatever is happening, **deletes the partial files**, and
  removes the job row — disk is freed immediately, re-adding the repo later
  starts from scratch.

Stopping must take effect **near-instantly, even mid-file** (a multi-GB
safetensors must not have to finish first). That requirement drives the core
mechanism: each download runs in a **terminable child process**.

## Scope

- **In scope:** `paused` status; `pause`/`resume`/`cancel` endpoints; per-download
  child process with a terminate-based stop; dashboard buttons; tests.
- **Out of scope (YAGNI):** pause-all / resume-all, job priority / reordering,
  rate limiting, and removing partial files for `failed` jobs (a pre-existing gap,
  separate concern).

## State machine

One new persisted status, `paused`, is added. The existing `CANCELLED` constant
stays unused (cancel deletes the row, matching today's queued-cancel behavior).

```
queued ──> running ──> completed
   │          │  │
   │          │  └────> failed ──(retry)──> queued
   │          │
   │          ├──(pause)──> paused ──(resume)──> queued
   │          └──(cancel)─> [row + files deleted]
   │
   └──(cancel)─> [row + files deleted]
paused ──(cancel)─> [row + files deleted]
```

Interactions with existing machinery — all **no-change** because they key off
`running`/`queued`, and `paused` is neither:

- **Auto-resume on restart** (`unfinished_jobs()` selects `queued`/`running`): a
  `paused` job is excluded, so it **stays paused across a systemd restart**.
- **`pending_bytes()`** ("+planned" storage bar): excludes `paused`; paused bytes
  sit on disk at their last-polled percentage and are not counted as planned.
- **Client speed indicator:** keys off `running`; a paused job drops out of the
  rate naturally.

## Core mechanism: per-download child process

Today the worker thread calls `snapshot_download(...)` inline and blocks; Python
cannot interrupt that mid-file from another thread. To kill it instantly, the
download runs in a **child process** the worker can `terminate()` (SIGTERM). HF's
resume reuses the `.incomplete` staging on the next run, so a terminated download
loses no real progress.

### The launcher seam (replaces the `downloader` injection)

The injected `downloader` callable on `run_backup_job` / `JobRunner` is replaced
by an injected **launcher**:

- **`SubprocessLauncher`** (production default): `start(**kwargs) -> Handle`.
  Spawns a `multiprocessing` process using the **spawn** start method, running a
  module-level `_download_entry(kwargs, queue)` that calls the real
  `snapshot_download` and reports its outcome (`ok` / `error: <msg>`) back over a
  `multiprocessing.Queue`. The returned `Handle` wraps the process + queue and
  exposes `.join(timeout)`, `.terminate()`, and `.outcome`.
  - **Spawn, not fork:** forking a multithreaded server process (uvicorn + the
    poller thread + the sqlite lock) risks deadlock in the child; spawn avoids it.
  - **Picklability:** the spawned target is a module-level function called with
    plain kwargs (strings/ints), so nothing un-picklable crosses the boundary.
  - **HF env vars** (`HF_HUB_DISABLE_XET`, `HF_XET_*`, token) propagate via the
    child's inherited environment, exactly as they reach the in-process call today.
  - **The child never touches the DB.** It only downloads and reports its outcome
    over the queue. All DB writes stay in the parent (poller + worker thread), so
    there is no cross-process sqlite access.

- **In-thread fake launcher** (tests): runs the existing fake downloader in a
  thread; `.terminate()` flips an `Event` the fake honors. Keeps the whole default
  suite offline and fast — no real processes are spawned in unit tests.

### The running registry

`JobRunner` holds a thread-safe **registry**: `job_id -> Handle`, plus a per-job
**intent** flag (`pause` or `cancel`). The worker registers its handle
immediately after `start()` and unregisters in a `finally`. Endpoints reach a live
download only through `runner.pause(id)` / `runner.cancel(id)`, which look up the
handle, set the intent, and call `handle.terminate()`.

A pause/cancel request that lands in the microsecond window after `running` is set
but before the handle is registered still takes effect: it sets the intent flag,
and the worker honors the intent after its `handle.join()` returns regardless of
registration timing.

Unregistering in the `finally` **clears the intent flag** as well as the handle.
This matters for resume: a paused job keeps the same `job_id`, so when it is
requeued and run again, the worker must not see the previous `pause` intent and
immediately stop. The intent lives only for one run.

## Who writes the terminal status (race-free)

**All terminal DB transitions happen in one place — the worker thread**, after
`handle.join()` returns. Endpoints only set intent + terminate, then return
optimistically; the dashboard reflects the change on its next 1.5s poll.

After the child exits, the worker decides:

1. **Download completed successfully** (`outcome == ok`) → `completed`, **even if
   a pause/cancel intent was set**. The download finished before the signal
   landed; the files are all present, so completion wins.
2. Otherwise, **honor the intent**:
   - **pause** → stop poller, `set_status(paused)`, keep files.
   - **cancel** → stop poller, `delete_backup_files(...)` then `delete_job(...)`.
3. **No intent** → existing behavior: `error` → `failed`, with the
   `_stopping`-during-shutdown guard still leaving the job `running` for the
   startup re-queue to resume.

Because the worker `join()`s the (reaped) child before any `rmtree`, cancel never
deletes files out from under a live writer. Stopping the poller before the
`rmtree`/`delete_job` likewise prevents a stray progress write racing the delete.

### Shutdown

`runner.shutdown()` sets `_stopping`, then **terminates all registered child
handles** and shuts down the thread pool. The `_stopping` guard makes workers
leave their jobs `running` (not `failed`/`paused`) so the next startup resumes
them. This is cleaner than today's teardown, which relies on the
"cannot schedule new futures after interpreter shutdown" exception path; that
guard is preserved for safety.

## Endpoints (`main.py`)

| Endpoint | Allowed from | Action |
|---|---|---|
| `POST /api/jobs/{id}/pause` | `running` | set intent=pause + terminate → worker sets `paused` |
| `POST /api/jobs/{id}/resume` | `paused` | `requeue()` + `runner.submit()` (same path as retry) |
| `POST /api/jobs/{id}/cancel` | `queued`, `running`, `paused` | stop if running; delete partial files + row |

- **`cancel`** is the behavior change: today it is queued-only and returns 409 on
  a running job. It becomes the universal "stop and discard":
  - `queued` / `paused` (no live process): delete partial files (if any) +
    delete row directly.
  - `running`: set intent=cancel + terminate; the worker deletes files + row.
  - Cancelling a `paused` job at, say, 90% **discards those bytes** — consistent
    with cancel always meaning "nothing left on disk."
- **`pause`** rejects (409) any non-`running` job; **`resume`** rejects any
  non-`paused` job.
- **`retry`** (failed-only) and **`delete`** (completed-only) are unchanged.

File deletion reuses the existing `delete_backup_files(backup_dir, repo_type,
slug)`, which already refuses to touch anything outside `BACKUP_DIR`.

## Dashboard (`static/index.html`)

Per-row actions become status-driven:

- `running` → **Pause** + **Cancel**
- `paused` → **Resume** + **Cancel**
- `queued` → **Cancel** (unchanged)
- `failed` → **Retry** (unchanged) · `completed` → **Delete** (unchanged)

- Add a `.st.paused` status color (muted amber).
- **Cancel** on a `running` or `paused` job shows a confirm dialog
  ("Stop and discard the N downloaded? This frees disk and cannot be undone."),
  since it now destroys data. Cancel on a `queued` job (nothing on disk) and
  **Pause** need no confirm.

## Testing

Default suite stays **offline** (no real subprocess); only one opt-in integration
test spawns a real process.

- **Worker (`tests/test_backup_worker.py`):** introduce the in-thread fake
  launcher; mechanically migrate existing tests from `downloader=fake` to
  `launcher=InThreadLauncher(fake)`. New cases:
  - pause → status `paused`, files kept.
  - cancel → partial files and row gone.
  - "completed wins over a late pause/cancel intent" (outcome ok despite intent).
  - shutdown with `_stopping` set still leaves the job `running`.
- **API (`tests/test_api.py`):** extend `FakeRunner` with `pause`/`cancel`/
  `resume`. Update `test_cancel_running_job_is_rejected` (a running cancel now
  stops it instead of 409). Add: pause endpoint (running→intent, 409 otherwise),
  resume endpoint (paused→requeue+submit, 409 otherwise), cancel-from-paused
  deletes files+row, and restart keeps a `paused` job paused.
- **DB (`tests/test_db.py`):** assert `paused` is excluded from
  `unfinished_jobs()` and `pending_bytes()`.
- **Integration (`-m integration`, opt-in):** real `SubprocessLauncher` — start a
  download, terminate mid-flight, assert resume completes it. The only place the
  real process path runs.
