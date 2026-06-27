# Auto-Retry with Backoff + Global Pause/Play — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Auto-retry transient download failures with backoff, and add a persistent global Pause/Play valve — both built on a new central dispatcher that becomes the single authority for starting downloads.

**Architecture:** A daemon dispatcher loop in `JobRunner` claims and starts the lowest-id eligible job whenever the valve is open and a concurrency slot is free. Endpoints and startup only write DB state; the dispatcher starts work. Transient failures (classified by exception type) become a new `retrying` status with a backoff timer; permanent ones fail immediately.

**Tech Stack:** Python 3, FastAPI, `huggingface_hub`, `multiprocessing` (spawn), `threading`, SQLite, pytest.

## Global Constraints

- **No system `pip`/`python`** — use `.venv/bin/python` and `.venv/bin/python -m pytest`.
- **Never read `.env`.**
- **Default suite stays offline** — only `-m integration` touches the network.
- **Backoff schedule:** `BACKOFF_SECONDS = [30, 60, 120, 240, 480]`, `MAX_RETRIES = 5`. Retry `i` (0-based) waits `BACKOFF_SECONDS[i]`; after the 5th failure the job is final-`failed`.
- **Retryable** = `socket.gaierror`, `requests.exceptions.ConnectionError`, `requests.exceptions.Timeout`, `huggingface_hub.utils.HfHubHTTPError` with status ∈ {429,500,502,503,504}, connection-related `OSError`. **Permanent** = everything else (404/gated/auth/`ValueError`/disk-space `RuntimeError`). An unexpected child exit (`None` outcome) is treated as **retryable**.
- **`retry_count` resets to 0** on successful completion and on manual Retry; it does NOT reset on partial progress.
- **Valve is persistent** (`app_state.paused_all`), survives restart. Global pause is **separate** from per-job pause: per-job `paused` jobs are never auto-started.
- **Status lifecycle:** `queued → running → completed | failed | paused | retrying`; `retrying → (backoff) → running`; global pause requeues running → `queued` via a new `requeue` intent. `CANCELLED` stays defined-but-unset.
- **Git:** already on branch `feature/auto-retry-global-pause`. Commit after every green step.

---

### Task 1: `app/retry.py` — backoff schedule + transient/permanent classifier

A pure module, importable in both the download child and the parent worker.

**Files:**
- Create: `app/retry.py`
- Test: `tests/test_retry.py`

**Interfaces:**
- Produces: `app.retry.BACKOFF_SECONDS` (list[int]), `app.retry.MAX_RETRIES` (int), `app.retry.is_retryable(exc: BaseException) -> bool`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_retry.py`:

```python
import socket
import pytest
import requests
from huggingface_hub.utils import HfHubHTTPError, RepositoryNotFoundError
from app.retry import is_retryable, BACKOFF_SECONDS, MAX_RETRIES


def test_backoff_schedule_is_the_agreed_five():
    assert BACKOFF_SECONDS == [30, 60, 120, 240, 480]
    assert MAX_RETRIES == 5


@pytest.mark.parametrize("exc", [
    socket.gaierror(-3, "Temporary failure in name resolution"),
    requests.exceptions.ConnectionError("conn reset"),
    requests.exceptions.Timeout("read timed out"),
    OSError(104, "Connection reset by peer"),     # ECONNRESET
])
def test_network_errors_are_retryable(exc):
    assert is_retryable(exc) is True


def test_5xx_and_429_http_errors_are_retryable():
    for status in (429, 500, 502, 503, 504):
        resp = requests.Response()
        resp.status_code = status
        assert is_retryable(HfHubHTTPError("boom", response=resp)) is True


@pytest.mark.parametrize("status", [401, 403, 404])
def test_auth_and_notfound_http_errors_are_permanent(status):
    resp = requests.Response()
    resp.status_code = status
    assert is_retryable(HfHubHTTPError("nope", response=resp)) is False


@pytest.mark.parametrize("exc", [
    RepositoryNotFoundError("missing"),
    ValueError("bad slug"),
    RuntimeError("not enough disk space for x/y"),
])
def test_permanent_errors_are_not_retryable(exc):
    assert is_retryable(exc) is False
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_retry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.retry'`.

- [ ] **Step 3: Write the module**

Create `app/retry.py`:

```python
"""Backoff schedule and transient-vs-permanent error classification.

Pure and import-light so it is usable in both the download child process and
the parent worker without side effects.
"""
import errno
import socket

BACKOFF_SECONDS = [30, 60, 120, 240, 480]
MAX_RETRIES = 5

_RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}
# OSError errno values that indicate a transient network condition. (DNS
# EAI_AGAIN is handled separately via socket.gaierror below.)
_RETRYABLE_OS_ERRNO = {errno.ECONNRESET, errno.ECONNREFUSED, errno.ECONNABORTED,
                       errno.ETIMEDOUT, errno.EHOSTUNREACH, errno.ENETUNREACH}


def is_retryable(exc: BaseException) -> bool:
    """True if the failure looks transient (worth an automatic retry)."""
    # DNS resolution failures (e.g. EAI_AGAIN "Temporary failure in name resolution")
    if isinstance(exc, socket.gaierror):
        return True

    try:
        import requests
        if isinstance(exc, (requests.exceptions.ConnectionError,
                            requests.exceptions.Timeout)):
            return True
    except Exception:  # noqa: BLE001 - requests should be present, but never crash here
        pass

    # Hub HTTP errors: retry only server-side / rate-limit statuses
    try:
        from huggingface_hub.utils import HfHubHTTPError
        if isinstance(exc, HfHubHTTPError):
            resp = getattr(exc, "response", None)
            status = getattr(resp, "status_code", None)
            return status in _RETRYABLE_HTTP_STATUS
    except Exception:  # noqa: BLE001
        pass

    # Connection-related OSErrors (ConnectionError is a subclass of OSError)
    if isinstance(exc, OSError) and exc.errno in _RETRYABLE_OS_ERRNO:
        return True

    return False
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_retry.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/retry.py tests/test_retry.py
git commit -m "feat: retry classifier and backoff schedule (transient vs permanent)"
```

---

### Task 2: `db.py` — schema migration, new columns, app_state, dispatcher/retry methods

**Files:**
- Modify: `app/db.py`
- Test: `tests/test_db.py`

**Interfaces:**
- Produces:
  - constant `app.db.RETRYING == "retrying"`.
  - `Job.retry_count: int`, `Job.next_retry_at: Optional[str]` (defaults 0/None).
  - `JobStore.next_runnable_job() -> Optional[Job]` — lowest-id eligible (`queued` with no/elapsed `next_retry_at`, or `retrying` with elapsed `next_retry_at`).
  - `JobStore.claim(job_id) -> bool` — atomic `→running`; True if it won.
  - `JobStore.running_count() -> int`.
  - `JobStore.schedule_retry(job_id, error, delay_seconds)` — `→retrying`, `retry_count+1`, `next_retry_at = now + delay`.
  - `JobStore.reset_retry(job_id)` — `retry_count=0, next_retry_at=NULL`.
  - `JobStore.reset_running_to_queued()` — startup orphan reset.
  - `JobStore.get_flag(key, default) -> str` / `set_flag(key, value)`.
  - `pending_bytes()` includes `retrying`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_db.py` (update line 3 import to include `RETRYING`):

```python
from app.db import JobStore, Job, QUEUED, RUNNING, COMPLETED, FAILED, PAUSED, RETRYING


def test_app_state_flag_defaults_and_roundtrips(store):
    assert store.get_flag("paused_all", "0") == "0"     # seeded default
    store.set_flag("paused_all", "1")
    assert store.get_flag("paused_all", "0") == "1"
    assert store.get_flag("missing", "x") == "x"


def test_running_count(store):
    a = store.create_job("a/b", "model"); store.set_status(a.id, RUNNING)
    b = store.create_job("c/d", "model"); store.set_status(b.id, RUNNING)
    store.create_job("e/f", "model")     # queued
    assert store.running_count() == 2


def test_claim_is_atomic_only_one_wins(store):
    j = store.create_job("a/b", "model")        # queued
    assert store.claim(j.id) is True
    assert store.get_job(j.id).status == RUNNING
    assert store.claim(j.id) is False           # already running -> second claim loses


def test_next_runnable_prefers_lowest_id_and_skips_future_retries(store):
    a = store.create_job("a/b", "model")        # queued, id smallest
    b = store.create_job("c/d", "model")
    store.schedule_retry(b.id, "blip", 9999)    # retrying, far future -> not yet runnable
    nxt = store.next_runnable_job()
    assert nxt.id == a.id                        # a (queued) is eligible; b is not


def test_due_retry_becomes_runnable(store):
    j = store.create_job("a/b", "model")
    store.schedule_retry(j.id, "blip", 0)        # next_retry_at = now -> immediately due
    nxt = store.next_runnable_job()
    assert nxt is not None and nxt.id == j.id
    assert nxt.status == RETRYING and nxt.retry_count == 1


def test_schedule_retry_increments_then_reset_clears(store):
    j = store.create_job("a/b", "model")
    store.schedule_retry(j.id, "e1", 30)
    store.schedule_retry(j.id, "e2", 60)
    mid = store.get_job(j.id)
    assert mid.retry_count == 2 and mid.next_retry_at is not None and mid.error == "e2"
    store.reset_retry(j.id)
    after = store.get_job(j.id)
    assert after.retry_count == 0 and after.next_retry_at is None


def test_reset_running_to_queued(store):
    a = store.create_job("a/b", "model"); store.set_status(a.id, RUNNING)
    p = store.create_job("c/d", "model"); store.set_status(p.id, PAUSED)
    store.reset_running_to_queued()
    assert store.get_job(a.id).status == QUEUED
    assert store.get_job(p.id).status == PAUSED          # paused untouched


def test_pending_bytes_includes_retrying(store):
    r = store.create_job("a/b", "model")
    store.update_progress(r.id, 20, 100)
    store.schedule_retry(r.id, "blip", 30)               # retrying, 80 remaining
    assert store.pending_bytes() == 80


def test_migration_adds_columns_to_old_db(tmp_path):
    # An old-schema jobs table (no retry columns, no app_state) must be upgraded
    # in place by JobStore.__init__ without losing rows.
    import sqlite3
    dbp = tmp_path / "old.db"
    con = sqlite3.connect(dbp)
    con.executescript(
        "CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, slug TEXT NOT NULL,"
        " repo_type TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',"
        " total_bytes INTEGER NOT NULL DEFAULT 0, downloaded_bytes INTEGER NOT NULL DEFAULT 0,"
        " error TEXT, created_at TEXT NOT NULL DEFAULT (datetime('now')),"
        " updated_at TEXT NOT NULL DEFAULT (datetime('now')), UNIQUE(repo_type, slug));"
    )
    con.execute("INSERT INTO jobs (slug, repo_type, status) VALUES ('keep/me','model','failed')")
    con.commit(); con.close()

    store = JobStore(dbp)                      # must migrate in place
    job = store.get_job_by_repo("model", "keep/me")
    assert job is not None and job.retry_count == 0 and job.next_retry_at is None
    assert store.get_flag("paused_all", "0") == "0"
    store.close()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_db.py -v`
Expected: FAIL — `ImportError: cannot import name 'RETRYING'` and missing methods.

- [ ] **Step 3: Implement the db changes**

In `app/db.py`:

Add the constant (after `PAUSED`, line 10):

```python
RETRYING = "retrying"
```

Extend `_SCHEMA` to include the new columns (for fresh DBs) and the `app_state` table:

```python
_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL,
    repo_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    total_bytes INTEGER NOT NULL DEFAULT 0,
    downloaded_bytes INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(repo_type, slug)
);
CREATE TABLE IF NOT EXISTS app_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""
```

Add the two fields to the `Job` dataclass (after `updated_at`, with defaults so existing positional uses still work):

```python
@dataclass
class Job:
    id: int
    slug: str
    repo_type: str
    status: str
    total_bytes: int
    downloaded_bytes: int
    error: Optional[str]
    created_at: str
    updated_at: str
    retry_count: int = 0
    next_retry_at: Optional[str] = None
```

In `JobStore.__init__`, after `executescript(_SCHEMA)` and before commit, add the in-place migration + valve seed:

```python
    def __init__(self, db_path) -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        # Migrate a pre-existing jobs table that predates the retry columns.
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(jobs)")}
        if "retry_count" not in cols:
            self._conn.execute("ALTER TABLE jobs ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0")
        if "next_retry_at" not in cols:
            self._conn.execute("ALTER TABLE jobs ADD COLUMN next_retry_at TEXT")
        self._conn.execute("INSERT OR IGNORE INTO app_state (key, value) VALUES ('paused_all', '0')")
        self._conn.commit()
```

Change `pending_bytes` to include `retrying` (the `IN (...)` list):

```python
                "SELECT COALESCE(SUM(MAX(total_bytes - downloaded_bytes, 0)), 0) "
                "FROM jobs WHERE status IN ('running', 'queued', 'retrying')"
```

Add the new methods (anywhere among the other methods, e.g. after `requeue`):

```python
    def running_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE status = 'running'"
            ).fetchone()
        return row[0]

    def next_runnable_job(self) -> Optional[Job]:
        """Lowest-id job eligible to start now: a queued job (no pending retry
        delay, or its delay has elapsed), or a retrying job whose backoff is up."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE "
                "(status = 'queued' AND (next_retry_at IS NULL OR next_retry_at <= datetime('now'))) "
                "OR (status = 'retrying' AND next_retry_at <= datetime('now')) "
                "ORDER BY id ASC LIMIT 1"
            ).fetchone()
        return self._to_job(row) if row else None

    def claim(self, job_id: int) -> bool:
        """Atomically move a queued/retrying job to running. Returns True if this
        call is the one that moved it (prevents double-dispatch)."""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE jobs SET status = 'running', updated_at = datetime('now') "
                "WHERE id = ? AND status IN ('queued', 'retrying')",
                (job_id,),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def schedule_retry(self, job_id: int, error: Optional[str], delay_seconds: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status = 'retrying', error = ?, "
                "retry_count = retry_count + 1, "
                "next_retry_at = datetime('now', '+' || ? || ' seconds'), "
                "updated_at = datetime('now') WHERE id = ?",
                (error, int(delay_seconds), job_id),
            )
            self._conn.commit()

    def reset_retry(self, job_id: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET retry_count = 0, next_retry_at = NULL, "
                "updated_at = datetime('now') WHERE id = ?",
                (job_id,),
            )
            self._conn.commit()

    def reset_running_to_queued(self) -> None:
        """On startup, orphaned 'running' jobs (their processes died with the old
        interpreter) go back to 'queued' for the dispatcher to pick up."""
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status = 'queued', updated_at = datetime('now') "
                "WHERE status = 'running'"
            )
            self._conn.commit()

    def get_flag(self, key: str, default: str) -> str:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM app_state WHERE key = ?", (key,)
            ).fetchone()
        return row["value"] if row else default

    def set_flag(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO app_state (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )
            self._conn.commit()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_db.py -v`
Expected: PASS (all db tests).

- [ ] **Step 5: Commit**

```bash
git add app/db.py tests/test_db.py
git commit -m "feat(db): retry columns + migration, app_state valve, dispatcher/retry queries"
```

---

### Task 3: `launcher.py` — carry `retryable` on the Outcome

**Files:**
- Modify: `app/launcher.py`
- Test: `tests/test_launcher.py`, `tests/_proc_targets.py`

**Interfaces:**
- Consumes: `app.retry.is_retryable`.
- Produces: `Outcome.retryable: bool = False`; `_download_entry` reports `("error", msg, retryable)`; `ProcessHandle.wait` parses the optional third field (tolerant of 2- or 3-tuples).

- [ ] **Step 1: Write the failing tests**

Add a 3-tuple target to `tests/_proc_targets.py`:

```python
def retryable_error_target(queue, kwargs):
    queue.put(("error", "dns blip", True))
```

Add to `tests/test_launcher.py`:

```python
def test_outcome_carries_retryable_flag():
    from app.launcher import SubprocessLauncher, Outcome
    from tests import _proc_targets
    launcher = SubprocessLauncher(entry=_proc_targets.retryable_error_target)
    handle = launcher.start(repo_id="o/n", repo_type="model",
                            local_dir="/tmp/x", token=None, max_workers=2)
    outcome = handle.wait(timeout=10)
    assert outcome == Outcome(ok=False, error="dns blip", retryable=True)


def test_two_tuple_outcome_defaults_retryable_false():
    # The existing ok/error targets still put 2-tuples; wait() must tolerate them.
    from app.launcher import SubprocessLauncher, Outcome
    from tests import _proc_targets
    launcher = SubprocessLauncher(entry=_proc_targets.error_target)
    handle = launcher.start(repo_id="o/n", repo_type="model",
                            local_dir="/tmp/x", token=None, max_workers=2)
    assert handle.wait(timeout=10) == Outcome(ok=False, error="boom", retryable=False)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_launcher.py -v`
Expected: FAIL — `Outcome.__init__` has no `retryable`.

- [ ] **Step 3: Implement**

In `app/launcher.py`, extend `Outcome`:

```python
@dataclass
class Outcome:
    ok: bool
    error: Optional[str] = None
    retryable: bool = False
```

Update `_download_entry` to classify and report a 3-tuple:

```python
def _download_entry(queue, kwargs):
    """Child entry point. Runs the real snapshot_download and reports the result.

    Reports every failure (including unusual ones) as an error string plus a
    retryable flag (so the worker can decide whether to auto-retry). Never
    imported with side effects, so it is spawn-safe.
    """
    try:
        from huggingface_hub import snapshot_download
        snapshot_download(**kwargs)
        queue.put(("ok", None, False))
    except BaseException as exc:  # noqa: BLE001 - surface anything as a job error
        from .retry import is_retryable
        queue.put(("error", str(exc)[:500], is_retryable(exc)))
```

Make `ProcessHandle.wait` tolerant of 2- or 3-tuples:

```python
    def wait(self, timeout=None) -> Optional[Outcome]:
        """Block until the child exits, then return the Outcome it reported.

        Returns None if the child exited without reporting one (i.e. it was
        terminated mid-download). Tolerates a 2-tuple (legacy/no-retryable) or a
        3-tuple (tag, error, retryable).
        """
        self._process.join(timeout)
        try:
            tag, error, *rest = self._queue.get_nowait()
        except _queue.Empty:
            return None
        retryable = bool(rest[0]) if rest else False
        return Outcome(ok=(tag == "ok"), error=error, retryable=retryable)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_launcher.py -v`
Expected: PASS (including the existing launcher tests — `ok_target`/`error_target` still work via the 2-tuple tolerance).

- [ ] **Step 5: Commit**

```bash
git add app/launcher.py tests/test_launcher.py tests/_proc_targets.py
git commit -m "feat(launcher): carry retryable flag on the download Outcome"
```

---

### Task 4: worker — auto-retry transient failures, `requeue` intent, reset-on-complete

**Files:**
- Modify: `app/backup.py` (`run_backup_job`, lines 141-228)
- Test: `tests/test_backup_worker.py`

**Interfaces:**
- Consumes: `app.retry.is_retryable`, `BACKOFF_SECONDS`, `MAX_RETRIES`; `JobStore.schedule_retry`, `reset_retry`; `Outcome.retryable`.
- Produces: updated terminal decision in `run_backup_job`:
  - completion → `completed` **and** `reset_retry`.
  - intent `requeue` → `queued` (keep files).
  - failure (outcome not-ok, or `None`=unexpected exit→retryable): if retryable and `retry_count < MAX_RETRIES` → `schedule_retry(..., BACKOFF_SECONDS[retry_count])`; else final `failed`.
  - parent-side `except` (preflight) → same retry-or-fail using `is_retryable(exc)`.

- [ ] **Step 1: Write the failing tests**

In `tests/test_backup_worker.py`, first update the in-thread fake so its Outcome carries `retryable` (add the `is_retryable` import near the top and change the `InThreadLauncher.run` body). Update the import line and the `run()` closure inside `InThreadLauncher.start`:

```python
from app.retry import is_retryable, BACKOFF_SECONDS, MAX_RETRIES
from app.db import JobStore, COMPLETED, FAILED, CANCELLED, RUNNING, PAUSED, QUEUED, RETRYING
```

```python
        def run():
            try:
                self._fn(stop=stop, **kwargs)
                result["outcome"] = Outcome(ok=True)
            except BaseException as exc:  # noqa: BLE001
                result["outcome"] = Outcome(ok=False, error=str(exc),
                                            retryable=is_retryable(exc))
```

Update the existing `test_worker_marks_failed_on_unexpected_process_exit` (the `None`-outcome OOM test): `None` is now treated as **retryable**, so an unexhausted job goes to `retrying`, and only an exhausted one fails. Replace that test with two:

```python
def test_worker_unexpected_exit_schedules_retry_when_budget_remains(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")

    class _OomHandle:
        exitcode = -9
        def terminate(self): pass
        def wait(self, timeout=None): return None   # killed, reported nothing

    class _OomLauncher:
        def start(self, **kwargs):
            Path(kwargs["local_dir"]).mkdir(parents=True, exist_ok=True)
            return _OomHandle()

    run_backup_job(job.id, store, settings, api=FakeApi(10), launcher=_OomLauncher(),
                   registry=None)
    j = store.get_job(job.id)
    assert j.status == RETRYING and j.retry_count == 1 and j.next_retry_at is not None


def test_worker_unexpected_exit_fails_when_retries_exhausted(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    for _ in range(MAX_RETRIES):
        store.schedule_retry(job.id, "x", 0)         # retry_count -> 5
    assert store.get_job(job.id).retry_count == MAX_RETRIES

    class _OomHandle:
        exitcode = -9
        def terminate(self): pass
        def wait(self, timeout=None): return None

    class _OomLauncher:
        def start(self, **kwargs):
            Path(kwargs["local_dir"]).mkdir(parents=True, exist_ok=True)
            return _OomHandle()

    run_backup_job(job.id, store, settings, api=FakeApi(10), launcher=_OomLauncher(),
                   registry=None)
    assert store.get_job(job.id).status == FAILED
    store.close()
```

Append new behavior tests:

```python
import socket


def transient_downloader(*, local_dir, stop=None, **_):
    raise socket.gaierror(-3, "Temporary failure in name resolution")


def test_worker_retries_transient_download_failure(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    run_backup_job(job.id, store, settings, api=FakeApi(10),
                   launcher=InThreadLauncher(transient_downloader), registry=None)
    j = store.get_job(job.id)
    assert j.status == RETRYING and j.retry_count == 1
    assert j.next_retry_at is not None
    assert "name resolution" in (j.error or "")
    store.close()


def test_worker_permanent_failure_does_not_retry(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")

    def boom(*, local_dir, stop=None, **_):
        raise RuntimeError("403 gated")

    run_backup_job(job.id, store, settings, api=FakeApi(10),
                   launcher=InThreadLauncher(boom), registry=None)
    j = store.get_job(job.id)
    assert j.status == FAILED and j.retry_count == 0
    store.close()


def test_worker_completion_resets_retry_count(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    store.schedule_retry(job.id, "earlier blip", 0)      # retry_count -> 1
    run_backup_job(job.id, store, settings, api=FakeApi(11),
                   launcher=InThreadLauncher(fake_downloader_factory(b"y" * 11)),
                   registry=None)
    j = store.get_job(job.id)
    assert j.status == COMPLETED and j.retry_count == 0 and j.next_retry_at is None
    store.close()


def test_worker_requeue_intent_returns_job_to_queued_keeping_files(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    registry = RunningRegistry()
    started = threading.Event()
    t = threading.Thread(target=run_backup_job, kwargs=dict(
        job_id=job.id, store=store, settings=settings, api=FakeApi(1000),
        launcher=InThreadLauncher(blocking_downloader_factory(started)),
        registry=registry))
    t.start()
    assert started.wait(3)
    registry.request(job.id, "requeue")
    t.join(5)
    j = store.get_job(job.id)
    assert j.status == QUEUED
    assert (tmp_path / "backups" / "models" / "o" / "n" / "partial.bin").exists()
    store.close()


def test_worker_preflight_disk_failure_is_permanent(tmp_path, monkeypatch):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    monkeypatch.setattr("app.backup.free_disk_bytes", lambda p: 1000)
    run_backup_job(job.id, store, settings, api=FakeApi(1_000_000_000),
                   launcher=InThreadLauncher(fake_downloader_factory(b"x")), registry=None)
    j = store.get_job(job.id)
    assert j.status == FAILED and j.retry_count == 0   # disk-full is not retried
    store.close()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_backup_worker.py -v`
Expected: FAIL — the worker doesn't yet schedule retries or honor `requeue`.

- [ ] **Step 3: Implement the worker changes**

In `app/backup.py`, add imports (top, near `from .db import PAUSED`):

```python
from .db import PAUSED
from .retry import is_retryable, BACKOFF_SECONDS, MAX_RETRIES
```

Add a small helper above `run_backup_job`:

```python
def _record_failure(store, job_id, retry_count, message, retryable) -> None:
    """Either schedule an auto-retry (transient + budget remaining) or mark the
    job permanently failed."""
    msg = str(message)[:500]
    if retryable and retry_count < MAX_RETRIES:
        store.schedule_retry(job_id, msg, BACKOFF_SECONDS[retry_count])
    else:
        store.set_status(job_id, "failed", error=msg)
```

Replace the terminal-decision block (the `if outcome ... else` ladder, lines 201-218) with:

```python
        if outcome is not None and outcome.ok:
            # Finished before any stop signal landed -> completion wins.
            final = directory_size(local_dir)
            store.update_progress(job_id, total if total else final, total_bytes=total)
            store.set_status(job_id, "completed")
            store.reset_retry(job_id)
        elif intent == "pause":
            store.set_status(job_id, PAUSED)
        elif intent == "cancel":
            delete_backup_files(settings.backup_dir, job.repo_type, job.slug)
            store.delete_job(job_id)
        elif intent == "requeue":
            # Global pause: stop and return to the queue (keep files), no retry change.
            store.set_status(job_id, "queued")
        elif stopping is not None and stopping.is_set():
            # Process-wide shutdown terminated the child; leave 'running' so the
            # startup reset re-queues it instead of failing it.
            return
        else:
            if outcome is not None:
                message, retryable = outcome.error, outcome.retryable
            else:
                message = f"download process exited unexpectedly (code {handle.exitcode})"
                retryable = True   # unexpected child exit (e.g. OOM) is worth a retry
            _record_failure(store, job_id, job.retry_count, message, retryable)
```

Replace the parent-side `except` block (lines 219-225) so preflight failures also classify:

```python
    except Exception as exc:  # noqa: BLE001 - surface any pre-download failure
        stop.set()
        if poller.is_alive():
            poller.join(timeout=2)
        if stopping is not None and stopping.is_set():
            return
        _record_failure(store, job_id, job.retry_count, str(exc), is_retryable(exc))
```

(Leave the top-of-function `store.set_status(job_id, "running")` as-is; the dispatcher's claim in Task 5 also sets running, and a redundant set is harmless.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_backup_worker.py -v`
Expected: PASS (entire file — existing pause/cancel/completion tests plus the new retry/requeue ones).

- [ ] **Step 5: Commit**

```bash
git add app/backup.py tests/test_backup_worker.py
git commit -m "feat(worker): auto-retry transient failures, requeue intent, reset on complete"
```

---

### Task 5: dispatcher in `JobRunner` (added but not yet wired in production)

Add the dispatcher loop, the global-valve methods, and `request_all` to the registry. Production still uses `submit`/lifespan until Task 6, so the default suite stays green; the dispatcher is exercised by new tests that call `runner.start()` explicitly.

**Files:**
- Modify: `app/backup.py` (`RunningRegistry`, `JobRunner`)
- Test: `tests/test_backup_worker.py`

**Interfaces:**
- Consumes: `JobStore.next_runnable_job`, `claim`, `running_count`, `get_flag`, `set_flag` (Task 2).
- Produces:
  - `RunningRegistry.request_all(intent)` — set intent + terminate every live handle.
  - `JobRunner(store, settings, api=None, launcher=None, dispatch_interval=1.0)`.
  - `JobRunner.start()` — launch the dispatcher daemon thread (idempotent).
  - `JobRunner.pause_all()` — `set_flag('paused_all','1')` + `request_all('requeue')`.
  - `JobRunner.resume_all()` — `set_flag('paused_all','0')`.
  - `JobRunner.shutdown(wait=False)` — stops the dispatcher too.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_backup_worker.py`:

```python
def test_dispatcher_runs_queued_jobs_in_priority_order(tmp_path):
    settings = make_settings(tmp_path, max_jobs=1)
    store = JobStore(settings.db_path)
    a = store.create_job("a/b", "model")
    b = store.create_job("c/d", "model")
    runner = JobRunner(store, settings, api=FakeApi(5),
                       launcher=InThreadLauncher(fake_downloader_factory(b"x" * 5)),
                       dispatch_interval=0.02)
    runner.start()
    assert wait_until(lambda: store.get_job(a.id).status == COMPLETED
                      and store.get_job(b.id).status == COMPLETED, timeout=5)
    runner.shutdown()
    store.close()


def test_dispatcher_respects_closed_valve(tmp_path):
    settings = make_settings(tmp_path, max_jobs=2)
    store = JobStore(settings.db_path)
    job = store.create_job("a/b", "model")
    store.set_flag("paused_all", "1")                 # valve closed before start
    runner = JobRunner(store, settings, api=FakeApi(5),
                       launcher=InThreadLauncher(fake_downloader_factory(b"x" * 5)),
                       dispatch_interval=0.02)
    runner.start()
    assert not wait_until(lambda: store.get_job(job.id).status == COMPLETED, timeout=1)
    assert store.get_job(job.id).status == QUEUED     # held while paused
    runner.resume_all()
    assert wait_until(lambda: store.get_job(job.id).status == COMPLETED, timeout=5)
    runner.shutdown()
    store.close()


def test_dispatcher_honors_concurrency_cap(tmp_path):
    settings = make_settings(tmp_path, max_jobs=1)
    store = JobStore(settings.db_path)
    store.create_job("a/b", "model")
    store.create_job("c/d", "model")
    started = threading.Event()
    runner = JobRunner(store, settings, api=FakeApi(1000),
                       launcher=InThreadLauncher(blocking_downloader_factory(started)),
                       dispatch_interval=0.02)
    runner.start()
    assert started.wait(3)
    import time as _t; _t.sleep(0.3)                  # give the loop time to (not) over-dispatch
    assert store.running_count() == 1                 # cap of 1 honored
    runner.shutdown()
    store.close()


def test_pause_all_requeues_running_and_sets_flag(tmp_path):
    settings = make_settings(tmp_path, max_jobs=2)
    store = JobStore(settings.db_path)
    job = store.create_job("a/b", "model")
    started = threading.Event()
    runner = JobRunner(store, settings, api=FakeApi(1000),
                       launcher=InThreadLauncher(blocking_downloader_factory(started)),
                       dispatch_interval=0.02)
    runner.start()
    assert started.wait(3)
    runner.pause_all()
    assert store.get_flag("paused_all", "0") == "1"
    assert wait_until(lambda: store.get_job(job.id).status == QUEUED, timeout=5)
    runner.shutdown()
    store.close()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_backup_worker.py -k "dispatcher or pause_all" -v`
Expected: FAIL — `JobRunner` has no `start`/`pause_all`/`resume_all` and rejects `dispatch_interval`.

- [ ] **Step 3: Implement**

In `app/backup.py`, add `request_all` to `RunningRegistry` (after `terminate_all`):

```python
    def request_all(self, intent) -> None:
        """Record an intent for every running job and terminate each handle."""
        with self._lock:
            items = list(self._handles.items())
            for job_id, _ in items:
                self._intents[job_id] = intent
        for _, handle in items:
            handle.terminate()
```

Replace the `JobRunner` class with the dispatcher version:

```python
DISPATCH_INTERVAL = 1.0


class JobRunner:
    def __init__(self, store, settings, api=None, launcher=None,
                 dispatch_interval: float = DISPATCH_INTERVAL) -> None:
        self._store = store
        self._settings = settings
        self._api = api
        self._launcher = launcher
        self._interval = dispatch_interval
        self._stopping = threading.Event()
        self._registry = RunningRegistry()
        self._executor = ThreadPoolExecutor(max_workers=settings.max_concurrent_jobs)
        self._dispatcher = None

    def start(self) -> None:
        """Start the dispatcher daemon (idempotent). It is the only thing that
        starts downloads: while the valve is open and a slot is free it claims and
        runs the lowest-id eligible job."""
        if self._dispatcher is not None:
            return
        self._dispatcher = threading.Thread(target=self._dispatch_loop, daemon=True)
        self._dispatcher.start()

    def _dispatch_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                if self._store.get_flag("paused_all", "0") != "1":   # valve open
                    while self._store.running_count() < self._settings.max_concurrent_jobs:
                        job = self._store.next_runnable_job()
                        if job is None:
                            break
                        if self._store.claim(job.id):
                            self._submit(job.id)
            except Exception:  # noqa: BLE001 - the loop must never die
                pass
            self._stopping.wait(self._interval)

    def _submit(self, job_id) -> None:
        self._executor.submit(
            run_backup_job, job_id, self._store, self._settings,
            self._api, self._launcher, self._stopping, self._registry,
        )

    def pause(self, job_id) -> None:
        """Stop a running download but keep its files (worker sets it 'paused')."""
        self._registry.request(job_id, "pause")

    def cancel(self, job_id) -> bool:
        """Stop a running download; the worker deletes its files + row once the
        child dies. Returns True if a live download was terminated."""
        return self._registry.request(job_id, "cancel")

    def pause_all(self) -> None:
        """Close the global valve and return every running download to 'queued'
        (keeping files). Per-job 'paused' jobs are untouched."""
        self._store.set_flag("paused_all", "1")
        self._registry.request_all("requeue")

    def resume_all(self) -> None:
        """Open the global valve; the dispatcher resumes held work by priority."""
        self._store.set_flag("paused_all", "0")

    def shutdown(self, wait: bool = False) -> None:
        """Stop the dispatcher and runner. Default (wait=False) is for process
        shutdown: signal in-flight workers (so they leave jobs resumable),
        terminate their child processes, and stop accepting new work. wait=True
        drains running jobs to completion."""
        self._stopping.set()
        if self._dispatcher is not None:
            self._dispatcher.join(timeout=2)
        self._registry.terminate_all()
        self._executor.shutdown(wait=wait, cancel_futures=not wait)
```

Note: `submit` is renamed to the internal `_submit` (only the dispatcher calls it). The existing `test_runner_runs_job_to_completion` / `test_runner_shutdown_sets_stopping_flag` tests call `runner.submit(...)`; update those two to drive through the dispatcher instead — replace their `runner.submit(job.id)` + `runner.shutdown(wait=True)` body with `runner.start()` + `wait_until(... COMPLETED ...)` + `runner.shutdown()` (mirroring `test_dispatcher_runs_queued_jobs_in_priority_order`), since `submit` is no longer public.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_backup_worker.py -v`
Expected: PASS (whole file, including the migrated runner tests and the new dispatcher/pause_all tests).

- [ ] **Step 5: Commit**

```bash
git add app/backup.py tests/test_backup_worker.py
git commit -m "feat: central dispatcher + global pause/resume in JobRunner (not yet wired)"
```

---

### Task 6: wire `main.py` to the dispatcher; add global-pause + retry endpoints

Flip production from submit-driven to dispatcher-driven, and add the new endpoints.

**Files:**
- Modify: `app/main.py`
- Test: `tests/test_api.py`

**Interfaces:**
- Consumes: `JobRunner.start/pause_all/resume_all/shutdown`; `JobStore.reset_running_to_queued/requeue/reset_retry/get_flag`.
- Produces: lifespan resets running→queued and starts the dispatcher; `create_jobs`/`retry`/`resume` write DB state only (no submit); `cancel` accepts `retrying`; `POST /api/pause-all`, `POST /api/resume-all`; `/api/storage` includes `paused_all`.

- [ ] **Step 1: Write the failing tests**

In `tests/test_api.py`: extend `FakeRunner` and the import, and rewrite the assertions that referenced `runner.submitted` to assert DB state instead. Replace `FakeRunner` (lines 8-17) with:

```python
from app.db import JobStore, FAILED, QUEUED, PAUSED, RUNNING, RETRYING


class FakeRunner:
    def __init__(self):
        self.started = False
        self.paused = []
        self.cancelled = []
        self.paused_all = 0
        self.resumed_all = 0
        self.shutdowns = 0

    def start(self):
        self.started = True

    def pause(self, job_id):
        self.paused.append(job_id)

    def cancel(self, job_id):
        self.cancelled.append(job_id)
        return True

    def pause_all(self):
        self.paused_all += 1

    def resume_all(self):
        self.resumed_all += 1

    def shutdown(self, wait=False):
        self.shutdowns += 1
```

Update the existing tests whose only assertion was `submitted` (they now assert the job is `queued`, which is what the dispatcher would pick up):
- `test_create_jobs_makes_one_per_detected_type`: drop the `len(runner.submitted) == 2` line; keep the status==QUEUED assertions.
- `test_size_lookup_failure_still_queues_job_at_zero`: replace `job.id in runner.submitted` with `job.status == QUEUED`.
- `test_retry_only_failed`: replace `job.id in runner.submitted` with `store.get_job(job.id).status == QUEUED`.
- `test_resume_only_paused`: replace `job.id in runner.submitted` with `resp.json()["status"] == QUEUED`.
- `test_create_unknown_slug_404`, `test_create_blank_slug_400`, `test_create_malformed_slug_returns_400`: replace `runner.submitted == []` assertions by asserting no jobs were created (`store.list_jobs() == []`).
- `test_resubmit_running_job_does_not_double_run`: drop the `runner.submitted == []` line; keep "no duplicate jobs".
- `test_startup_resumes_unfinished_jobs`: rewrite to assert the dispatcher path — a leftover `running` job is reset to `queued` on startup and the dispatcher is started:

```python
def test_startup_resets_running_and_starts_dispatcher(tmp_path):
    settings = make_settings(tmp_path)
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)
    leftover = store.create_job("resume/me", "model")
    store.set_status(leftover.id, RUNNING)             # orphaned from a prior run
    runner = FakeRunner()
    app = create_app(settings, store, runner, detect=lambda s, t: [])
    with TestClient(app):
        pass
    assert runner.started is True
    assert store.get_job(leftover.id).status == QUEUED  # reset for the dispatcher
    store.close()
```

Add new endpoint tests:

```python
def test_pause_all_and_resume_all(ctx):
    client, store, runner = ctx
    assert client.post("/api/pause-all").status_code == 200
    assert runner.paused_all == 1
    assert client.post("/api/resume-all").status_code == 200
    assert runner.resumed_all == 1


def test_storage_reports_paused_all_flag(ctx):
    client, store, runner = ctx
    assert client.get("/api/storage").json()["paused_all"] is False
    store.set_flag("paused_all", "1")
    assert client.get("/api/storage").json()["paused_all"] is True


def test_cancel_retrying_deletes_files_and_row(ctx, tmp_path):
    client, store, runner = ctx
    from app.backup import local_dir_for
    backup = tmp_path / "backups"
    job = store.create_job("o/n", "model")
    d = local_dir_for(backup, "model", "o/n"); d.mkdir(parents=True)
    (d / "partial.bin").write_bytes(b"x" * 10)
    store.schedule_retry(job.id, "blip", 30)            # status retrying
    resp = client.post(f"/api/jobs/{job.id}/cancel")
    assert resp.status_code == 200
    assert store.get_job(job.id) is None
    assert not d.exists()
    assert runner.cancelled == []                        # no live process during backoff


def test_retry_resets_retry_count(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    store.schedule_retry(job.id, "blip", 30)            # retry_count -> 1, status retrying
    store.set_status(job.id, FAILED)                     # exhausted -> failed
    resp = client.post(f"/api/jobs/{job.id}/retry")
    assert resp.status_code == 200
    j = store.get_job(job.id)
    assert j.status == QUEUED and j.retry_count == 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_api.py -v`
Expected: FAIL — endpoints still call `runner.submit`; pause-all/resume-all 404; storage lacks `paused_all`.

- [ ] **Step 3: Implement the rewire**

In `app/main.py`, update the import (line 13):

```python
from .db import COMPLETED, FAILED, JobStore, PAUSED, QUEUED, RETRYING, RUNNING
```

Replace the lifespan body:

```python
    @asynccontextmanager
    async def lifespan(app):
        # Orphaned 'running' jobs (their processes died with the old interpreter)
        # go back to 'queued'; the dispatcher then drives everything per the valve.
        store.reset_running_to_queued()
        runner.start()
        yield
        runner.shutdown()
```

In `create_jobs`, remove both `runner.submit(...)` calls and reset the retry budget when re-queuing a finished job. The loop body becomes:

```python
        for repo_type in types:
            existing = store.get_job_by_repo(repo_type, slug)
            if existing is None:
                job = store.create_job(slug, repo_type)
                try:
                    total = sizer(slug, repo_type, settings.hf_token)
                except Exception:  # noqa: BLE001 - sizing must not block queuing
                    total = 0
                if total:
                    store.update_progress(job.id, 0, total_bytes=total)
                    job = store.get_job(job.id)
                # left 'queued'; the dispatcher will start it.
            elif existing.status in (RUNNING, QUEUED, RETRYING, PAUSED):
                # in progress / pending / paused -> don't disturb.
                job = existing
            else:
                # completed / failed -> requeue with a fresh retry budget.
                store.requeue(existing.id)
                store.reset_retry(existing.id)
                job = store.get_job(existing.id)
            created.append(job.to_dict())
```

Update `retry` (remove submit, reset budget):

```python
    @app.post("/api/jobs/{job_id}/retry")
    def retry(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != FAILED:
            raise HTTPException(status_code=409, detail="only failed jobs can be retried")
        store.requeue(job_id)
        store.reset_retry(job_id)
        return store.get_job(job_id).to_dict()
```

Update `resume` (remove submit):

```python
    @app.post("/api/jobs/{job_id}/resume")
    def resume(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != PAUSED:
            raise HTTPException(status_code=409, detail="only paused downloads can be resumed")
        store.requeue(job_id)
        return store.get_job(job_id).to_dict()
```

Update `cancel` to accept `RETRYING` (direct-delete branch):

```python
    @app.post("/api/jobs/{job_id}/cancel")
    def cancel(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status not in (QUEUED, RUNNING, PAUSED, RETRYING):
            raise HTTPException(
                status_code=409,
                detail="only queued, running, paused, or retrying jobs can be cancelled",
            )
        if job.status == RUNNING:
            runner.cancel(job_id)
            return {"cancelling": job_id}
        # queued / paused / retrying: no live process — discard files + row directly.
        delete_backup_files(settings.backup_dir, job.repo_type, job.slug)
        store.delete_job(job_id)
        return {"deleted": job_id}
```

Add the two global-valve endpoints (next to `cancel`):

```python
    @app.post("/api/pause-all")
    def pause_all():
        runner.pause_all()
        return {"paused_all": True}

    @app.post("/api/resume-all")
    def resume_all():
        runner.resume_all()
        return {"paused_all": False}
```

Add `paused_all` to `storage`:

```python
    @app.get("/api/storage")
    def storage():
        usage = shutil.disk_usage(settings.backup_dir)
        return {
            "path": str(settings.backup_dir),
            "total": usage.total,
            "used": usage.used,
            "free": usage.free,
            "planned": store.pending_bytes(),
            "paused_all": store.get_flag("paused_all", "0") == "1",
        }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_api.py tests/test_static.py -v`
Expected: PASS. Then the full suite: `.venv/bin/python -m pytest` → all green.

- [ ] **Step 5: Commit**

```bash
git add app/main.py tests/test_api.py
git commit -m "feat(api): dispatcher-driven endpoints, global pause/resume, cancel retrying"
```

---

### Task 7: dashboard — global Pause/Play, retrying display, held label

**Files:**
- Modify: `app/static/index.html`
- Test: `tests/test_static.py`

**Interfaces:**
- Consumes: `/api/pause-all`, `/api/resume-all`, `/api/storage.paused_all`, job `status='retrying'`, `retry_count`, `next_retry_at`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_static.py`:

```python
def test_global_pause_and_retrying_ui_present(client):
    page = client.get("/").text
    assert "pause-all" in page          # global pause endpoint wired
    assert "resume-all" in page         # global resume endpoint wired
    assert ".st.retrying" in page       # retrying status color rule
    assert "retryText" in page          # helper that renders "N/5 · next in …"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_static.py::test_global_pause_and_retrying_ui_present -v`
Expected: FAIL.

- [ ] **Step 3: Update the dashboard**

In `app/static/index.html`:

Add a status color for `retrying` (extend the `.st....` rule, line 33):

```css
.st.completed{color:var(--ok)}.st.failed{color:var(--err)}.st.running{color:var(--accent)}.st.queued,.st.cancelled{color:var(--muted)}.st.paused{color:#d2a24c}.st.retrying{color:#d2a24c}
```

Add a global pause/play button to the header — put it in the `<h1>` row. Replace the title line (line 52) with a flex header carrying the button:

```html
  <div style="display:flex;align-items:center;justify-content:space-between;gap:12px">
    <h1>Hugging Face Rip</h1>
    <button id="pauseAll" class="ghost" onclick="togglePauseAll()">Pause all</button>
  </div>
```

Add the toggle + retry-text helpers and a module var, near `confirmCancel` (after line 143):

```javascript
let pausedAll = false;
async function togglePauseAll() {
  try {
    await api(pausedAll ? "/api/resume-all" : "/api/pause-all", { method: "POST" });
    refresh();
  } catch (e) { alert(e.message); }
}
window.togglePauseAll = togglePauseAll;
function retryText(j) {
  let s = `retrying · ${j.retry_count}/5`;
  if (j.next_retry_at) {
    const due = new Date(j.next_retry_at.replace(" ", "T") + "Z").getTime();
    const secs = Math.round((due - Date.now()) / 1000);
    s += secs > 0 ? ` · next in ${secs}s` : " · due";
  }
  return s;
}
```

Update `row(j)` so `retrying` gets a Cancel action and shows the retry text, and `queued` reads "held" while globally paused. Replace the `actions` ternary and the status/size cells (lines 145-164) with:

```javascript
  const actions =
      j.status === "running"
    ? `<button class="ghost" onclick="act(${j.id},'pause')">Pause</button>`
      + `<button class="ghost" onclick="confirmCancel(${j.id},'${esc(j.slug)}',${j.downloaded_bytes})">Cancel</button>`
    : j.status === "paused"
    ? `<button class="ghost" onclick="act(${j.id},'resume')">Resume</button>`
      + `<button class="ghost" onclick="confirmCancel(${j.id},'${esc(j.slug)}',${j.downloaded_bytes})">Cancel</button>`
    : j.status === "retrying"
    ? `<button class="ghost" onclick="confirmCancel(${j.id},'${esc(j.slug)}',${j.downloaded_bytes})">Cancel</button>`
    : j.status === "failed"
    ? `<button class="ghost" onclick="act(${j.id},'retry')">Retry</button>`
    : j.status === "queued"
    ? `<button class="ghost" onclick="act(${j.id},'cancel')">Cancel</button>`
    : j.status === "completed"
    ? `<button class="ghost" onclick="confirmDelete(${j.id},'${esc(j.slug)}',${j.total_bytes})">Delete</button>`
    : "";
  const label = j.status === "queued" && pausedAll ? "held"
              : j.status === "retrying" ? retryText(j)
              : j.status;
  return `<tr>
    <td><div class="repo">${esc(j.slug)}</div><span class="badge ${esc(j.repo_type)}">${esc(j.repo_type)}</span>${j.error ? `<div class="err">${esc(j.error)}</div>` : ""}</td>
    <td><span class="st ${esc(j.status)}">${esc(label)}</span></td>
    <td><div class="bar"><span style="width:${j.percent}%"></span></div><div class="type">${j.percent}%</div></td>
    <td class="size">${fmt(j.downloaded_bytes)} / ${fmt(j.total_bytes)}</td>
    <td>${actions}</td></tr>`;
```

In `loadStorage`, set the module var and the button label from `s.paused_all` (add after `const planned = s.planned || 0;`, line 170):

```javascript
    pausedAll = !!s.paused_all;
    const pa = document.getElementById("pauseAll");
    if (pa) pa.textContent = pausedAll ? "Resume all" : "Pause all";
```

- [ ] **Step 4: Run the static tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_static.py -v`
Expected: PASS (including the existing badge/storage/confirm-delete tests).

Then sanity-check the JS parses (no harness in repo): extract `<script>` and `node --check` it if `node` is available; otherwise re-read the edited `row`, `retryText`, and `togglePauseAll` for balanced template literals.

- [ ] **Step 5: Commit**

```bash
git add app/static/index.html tests/test_static.py
git commit -m "feat(ui): global pause/play toggle, retrying countdown, held label"
```

---

### Task 8: integration test + docs

**Files:**
- Modify: `tests/test_integration.py`, `CLAUDE.md`

**Interfaces:**
- Consumes: `JobRunner` (dispatcher), `JobStore`, the worker retry path.

- [ ] **Step 1: Write the integration test**

Append to `tests/test_integration.py` (a fault-injected launcher that fails retryably once, then succeeds — drives one real `retrying → completed` cycle through the dispatcher; no network needed for the retry logic, but marked integration since it exercises the full runner):

```python
import socket
import threading
import time
from app.db import COMPLETED, RETRYING
from app.backup import JobRunner


class _FlakyHandle:
    def __init__(self, fail):
        self._fail = fail
    def terminate(self): pass
    @property
    def exitcode(self): return 0
    def wait(self, timeout=None):
        from app.launcher import Outcome
        if self._fail:
            return Outcome(ok=False, error="Temporary failure in name resolution",
                           retryable=True)
        return Outcome(ok=True)


class _FlakyLauncher:
    """Fails retryably on the first start, succeeds on the second."""
    def __init__(self):
        self._starts = 0
    def start(self, **kwargs):
        self._starts += 1
        from pathlib import Path
        Path(kwargs["local_dir"]).mkdir(parents=True, exist_ok=True)
        return _FlakyHandle(fail=(self._starts == 1))


def _wait_until(pred, timeout=60.0, interval=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


@pytest.mark.integration
def test_transient_failure_auto_retries_to_completion(tmp_path, monkeypatch):
    settings = Settings(
        hf_token="hf_test", backup_dir=tmp_path / "backups",
        max_concurrent_jobs=1, max_workers=2, db_path=tmp_path / "jobs.db",
    )
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    # Make the backoff instant so the test doesn't wait 30s.
    monkeypatch.setattr("app.backup.BACKOFF_SECONDS", [0, 0, 0, 0, 0])

    class _Sib:  # noqa
        size = 10
    class _Info:  # noqa
        siblings = [_Sib()]
    class _Api:  # noqa
        def repo_info(self, **k): return _Info()

    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    runner = JobRunner(store, settings, api=_Api(), launcher=_FlakyLauncher(),
                       dispatch_interval=0.05)
    runner.start()
    # First attempt fails retryably -> retrying, then the dispatcher re-runs it.
    assert _wait_until(lambda: store.get_job(job.id).status == COMPLETED, timeout=10), \
        store.get_job(job.id).status
    assert store.get_job(job.id).retry_count == 0   # reset on completion
    runner.shutdown()
    store.close()
```

- [ ] **Step 2: Run it**

Run: `.venv/bin/python -m pytest tests/test_integration.py -m integration -v`
Expected: PASS (both the pre-existing end-to-end test, network permitting, and the new fault-injected retry test).

Also confirm the default suite still excludes them and is green:
Run: `.venv/bin/python -m pytest`
Expected: PASS, integration deselected.

- [ ] **Step 3: Update `CLAUDE.md`**

In `CLAUDE.md`:
- In the Architecture section, note the new dispatcher: "A **central dispatcher** loop (`JobRunner.start`) is the only thing that starts downloads — endpoints and the lifespan only write DB state (`queued`, the `paused_all` valve, retry schedules); the dispatcher claims the lowest-id eligible job when the valve is open and a slot is free."
- Add to the status-lifecycle description: `retrying` (a transient failure auto-retries up to 5× with backoff `30s/60s/2m/4m/8m`; permanent errors fail immediately; `app/retry.py` classifies). Note the persistent global Pause/Play valve (`app_state.paused_all`) is separate from per-job pause.
- Note the SQLite migration: `JobStore.__init__` adds `retry_count`/`next_retry_at` to a pre-existing `jobs` table and seeds the `app_state` valve.

- [ ] **Step 4: Run the full default suite**

Run: `.venv/bin/python -m pytest`
Expected: PASS (all non-integration tests).

- [ ] **Step 5: Commit**

```bash
git add tests/test_integration.py CLAUDE.md
git commit -m "test: integration auto-retry cycle; docs: dispatcher, retry, global valve"
```

---

## Self-Review

**1. Spec coverage:**
- Central dispatcher (valve + backoff + priority + concurrency) → Task 5 (+ wired Task 6). ✓
- `retrying` status, backoff `[30,60,120,240,480]`, max 5, reset on complete/manual → Tasks 2, 4. ✓
- Transient vs permanent classification (by type) → Task 1; plumbed via Outcome Task 3; applied Task 4. ✓
- Persistent valve `app_state.paused_all`, `pause-all`/`resume-all`, separate from per-job pause, requeue intent → Tasks 2, 5, 6. ✓
- Live-DB migration (ALTER + seed) → Task 2 (`test_migration_adds_columns_to_old_db`). ✓
- Endpoints stop calling submit; `retry` resets budget; `cancel` accepts retrying; storage `paused_all` → Task 6. ✓
- Startup reset running→queued + start dispatcher; shutdown stops it → Tasks 2, 5, 6. ✓
- Dashboard pause/play, retrying countdown, held label → Task 7. ✓
- `pending_bytes` includes retrying → Task 2. ✓
- Integration auto-retry cycle → Task 8. ✓

**2. Placeholder scan:** No TBD/TODO; every code step shows full code; test-migration steps name each test and the exact assertion change.

**3. Type consistency:** `next_runnable_job`/`claim`/`running_count`/`schedule_retry`/`reset_retry`/`reset_running_to_queued`/`get_flag`/`set_flag` are defined in Task 2 and consumed with the same signatures in Tasks 5-6. `Outcome(ok, error, retryable)` consistent across Tasks 3-4 and the Task 8 fake. `request_all(intent)`, `pause_all`/`resume_all`/`start`/`_submit`/`dispatch_interval` consistent Tasks 5-6. `_record_failure(store, job_id, retry_count, message, retryable)` defined and used only in Task 4. Endpoint/flag names (`paused_all`, `pause-all`, `resume-all`) consistent across Tasks 2/5/6/7.
