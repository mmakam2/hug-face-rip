# Auto-Retry with Backoff + Global Pause/Play — Design

**Date:** 2026-06-27
**Status:** Approved (pending spec review)

## Goal

Two related robustness/control features, built on one new piece of shared
machinery (a central dispatcher):

1. **Auto-retry transient failures** — when a download fails with a *transient*
   error (DNS hiccup, connection reset, timeout, 5xx, 429), automatically retry
   it up to **5 times** with backoff **30s, 60s, 2m, 4m, 8m**. *Permanent*
   errors (404, gated/auth, invalid slug, disk-full) fail immediately without
   burning retries. Manual **Retry** still works on anything.
2. **Global Pause/Play** — a master valve that stops *all* active downloads and
   holds the queue; Play runs the held work in priority order. It is **separate
   from per-job pause**: jobs you paused individually stay paused and are not
   resumed by Play. The valve state **persists across restart**.

This was prompted by a transient DNS blip permanently failing 8 resumed jobs at
once — the app had no retry resilience and no way to hold everything at once.

## The shared core: a central dispatcher

Today jobs are submitted straight to a `ThreadPoolExecutor` the moment they are
created/retried/resumed (`runner.submit`). Nothing can hold the queue, delay a
job, or impose an order. Both features need an authority that decides *what runs
next*, so we introduce one and route all starts through it.

**Dispatcher** (a daemon loop owned by `JobRunner`, ~1s tick):

- Read the persisted **valve**. If closed → idle this tick.
- Count currently-`running` jobs. While a slot is free (`running < max_concurrent_jobs`):
  - Pick the **lowest-id eligible** job, where eligible =
    `status = 'queued'` (and `next_retry_at` is null or already past), **or**
    `status = 'retrying'` with `next_retry_at <= now`.
  - **Atomically claim** it: `UPDATE jobs SET status='running' WHERE id=? AND
    status IN ('queued','retrying')`. If 0 rows changed (it finished/changed under
    us), skip. This claim is what prevents double-dispatch.
  - Submit `run_backup_job` for the claimed job to the executor.
  - If no eligible job remains, stop filling slots.
- Sleep one tick.

**Consequence:** endpoints and the startup hook no longer call `runner.submit`;
they only **write DB state** (insert `queued`, requeue, toggle the valve). The
dispatcher is the single starter. This is the clean home for valve gating,
backoff timing, and priority order, with no second dispatch path to race.

`run_backup_job` no longer sets its own `running` status (the dispatcher's claim
did). Everything else in the worker (preflight, poller, child process,
registry, terminal transition) is unchanged except the failure branch (below).

## Status lifecycle

One new status, **`retrying`**:

```
queued ──(dispatcher claims)──> running ──> completed
   ▲                               │  │
   │                               │  ├─(transient fail, retries left)─> retrying ─(backoff elapsed, dispatcher)─> running
   │                               │  ├─(transient fail, retries exhausted)─> failed
   │                               │  ├─(permanent fail)──────────────────────> failed
   │                               │  ├─(pause)──> paused      ├─(cancel)──> [deleted]
   │                               │  └─(global pause: requeue intent)─┐
   └───────────────────────────────────────────────────────────────────┘
failed ──(manual retry)──> queued (retry_count reset to 0)
paused ──(per-job resume)──> queued
```

- A `retrying` job carries its **last error** (visible), an incremented
  `retry_count`, and a `next_retry_at` timestamp.
- `retry_count` resets to 0 on **successful completion** and on **manual Retry**.
  It does **not** reset on partial download progress (5 attempts total — YAGNI).
- Per-job `paused` jobs are never auto-started by the dispatcher.

## Data model (`db.py`)

`jobs` table gains:
- `retry_count INTEGER NOT NULL DEFAULT 0`
- `next_retry_at TEXT` (nullable; SQLite datetime string)

New tiny key/value table for persistent app state:
- `app_state(key TEXT PRIMARY KEY, value TEXT NOT NULL)`, seeded with
  `('paused_all', '0')`.

**Migration matters — there is a live `jobs.db`.** `JobStore.__init__` must add
the two columns to a pre-existing table if absent (check `PRAGMA table_info` /
`ALTER TABLE ADD COLUMN`; both new columns have safe defaults), and create +
seed `app_state` if absent. `CREATE TABLE IF NOT EXISTS` alone won't add columns
to an existing table.

New `JobStore` methods (names indicative):
- `next_runnable_job() -> Optional[Job]` — lowest-id eligible (the SELECT above).
- `claim(job_id) -> bool` — the atomic claim UPDATE; returns whether it won.
- `running_count() -> int`.
- `schedule_retry(job_id, error, delay_seconds)` — sets `status='retrying'`,
  `error=?`, `retry_count = retry_count + 1`,
  `next_retry_at = datetime('now', '+N seconds')`.
- `reset_retry(job_id)` — `retry_count=0, next_retry_at=NULL` (used by `requeue`
  on manual retry and by completion).
- `get_flag(key, default)` / `set_flag(key, value)` for the valve.
- `pending_bytes()` extended to also count `retrying` (still pending work).
- The startup reset sets orphaned `running` → `queued`.

`Job` / `to_dict` expose `retry_count` and `next_retry_at` for the UI.

## Transient vs permanent classification

A pure helper, `app/retry.py`:
- `BACKOFF_SECONDS = [30, 60, 120, 240, 480]`, `MAX_RETRIES = 5`.
- `is_retryable(exc) -> bool`:
  - **Retryable:** `socket.gaierror`, `requests.exceptions.ConnectionError`,
    `requests.exceptions.Timeout`, `urllib3` connection/timeout errors,
    `huggingface_hub.utils.HfHubHTTPError` whose status ∈ {429, 500, 502, 503,
    504}, and connection-related `OSError` (e.g. `ECONNRESET`, `ECONNREFUSED`).
  - **Permanent (everything else):** `RepositoryNotFoundError`,
    `GatedRepoError`, `EntryNotFoundError`, HTTP 401/403/404, `ValueError`
    (bad slug), and our own disk-space `RuntimeError`.

This helper is importable in both the download **child** (`_download_entry`
classifies the snapshot_download exception) and the **parent** worker (classifies
preflight failures — a sizing call that fails on a network error is retryable; a
disk-space failure is not).

`Outcome` (in `launcher.py`) gains `retryable: bool = False`. `_download_entry`
reports `("error", msg, is_retryable(exc))` over the queue; `ProcessHandle.wait`
parses the third field. The in-thread fake launcher used by tests mirrors this.

## Worker terminal decision (updated failure branch)

After the child exits (`handle.wait()`), the decision becomes:

1. `outcome.ok` → `completed` (and `reset_retry`).
2. intent `pause` → `paused`; intent `cancel` → delete files + row.
3. **intent `requeue` (new, from global pause)** → `status='queued'`, keep
   files, leave `retry_count`/`next_retry_at` untouched.
4. process-wide `stopping` (shutdown) → leave `running` (startup will reset → queued).
5. else a real failure — let `retryable = outcome.retryable` (or, for a `None`
   outcome = unexpected child exit/OOM, treat as retryable):
   - if `retryable and retry_count < MAX_RETRIES` →
     `schedule_retry(job_id, msg, BACKOFF_SECONDS[retry_count])` (status `retrying`).
   - else → `failed` (final), `next_retry_at = NULL`.

The same retryable-or-final logic applies in the parent-side `except` block for
preflight failures (sizing/disk), using `is_retryable(exc)`.

## Global valve

A new third terminate-intent, **`requeue`**, joins `pause`/`cancel` in the
`RunningRegistry`. Endpoints:

- `POST /api/pause-all` — `set_flag('paused_all','1')`, then signal every running
  job with the `requeue` intent (the worker stops each child and sets it
  `queued`, keeping files). Individually-`paused` jobs are untouched. Returns the
  new state.
- `POST /api/resume-all` — `set_flag('paused_all','0')`. The dispatcher resumes
  `queued`/due-`retrying` jobs by priority on its next tick. Per-job `paused`
  jobs stay paused. Returns the new state.

`JobRunner` gains `pause_all()` / `resume_all()` (set flag + the requeue signal),
and `RunningRegistry` gains `request_all('requeue')`.

## Other endpoint changes (`main.py`)

- **No endpoint calls `runner.submit` anymore.** `create_jobs`, `retry`, and
  per-job `resume` just write DB state (`queued`); the dispatcher starts them.
- `retry` additionally `reset_retry` (fresh budget). Still failed-only.
- `cancel` now also accepts `retrying` (delete files + row directly; no live
  process during backoff). Allowed from `queued`/`running`/`paused`/`retrying`.
- `GET /api/storage` adds `paused_all` (bool) so the dashboard reflects the valve.
- Lifespan: on startup, reset orphaned `running` → `queued`, then start the
  dispatcher (it honors the persisted valve). On shutdown, stop the dispatcher
  and terminate children (jobs left `running`/resumable as today).

## Dashboard (`static/index.html`)

- A **Pause-all / Resume-all** toggle in the header/storage bar, reflecting
  `paused_all`. Closed valve shows a subtle "Downloads paused" affordance.
- `retrying` rows: a distinct status color; the size/progress area shows
  "retrying · `retry_count`/5 · next in `Xs`" (computed client-side from
  `next_retry_at`), with the last error and a **Cancel** action.
- While the valve is closed, `queued` rows read "held".
- Speed indicator / planned bytes: `retrying` counts as planned; the client
  speed sampler still keys off `running` only (unchanged).

## Testing

Default suite stays offline.

- **`retry.py`:** `is_retryable` truth table across the retryable and permanent
  exception types; backoff list/length.
- **`db.py`:** new columns + migration (open an old-schema DB, confirm columns
  added); `app_state` seed + get/set flag; `next_runnable_job` eligibility
  (valve-independent SELECT — backoff gating + priority order); `claim`
  atomicity (two claims, one wins); `running_count`; `pending_bytes` includes
  `retrying`; `schedule_retry`/`reset_retry`.
- **`launcher.py`:** `Outcome.retryable` plumbed through the queue; child reports
  the third field.
- **worker:** transient failure → `retrying` + incremented count + future
  `next_retry_at`; exhausted (`retry_count==5`) → `failed`; permanent → `failed`
  immediately; `requeue` intent → `queued` keeping files; completion resets
  `retry_count`; preflight disk-space failure → `failed` (not retried).
- **dispatcher:** respects the valve (closed → starts nothing); starts due
  `retrying` but not future ones; priority (lowest id first); concurrency cap;
  the atomic claim prevents two dispatchers/ticks double-running one job.
- **api:** `pause-all`/`resume-all` toggle the flag and requeue running jobs;
  `resume-all` doesn't touch `paused`; `cancel` from `retrying`; `/storage`
  reports `paused_all`; `retry` resets `retry_count`; creating a job no longer
  needs a `submit` call (it becomes `queued` and the dispatcher would run it).
- **static:** pause-all/resume-all control present; `retrying` rendering present.
- **integration (`-m integration`, opt-in):** a fault-injected transient failure
  (a launcher that fails retryably once then succeeds) drives one real
  `retrying → completed` cycle through the dispatcher.

## Out of scope (YAGNI)

No per-job retry-count override; no resetting the retry budget on partial
progress; no "retry now" button on a `retrying` job; no priority **reordering**
UI (priority stays created/id order); no exponential-backoff tuning knobs.
