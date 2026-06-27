# Pause / Resume / Cancel for In-Progress Downloads — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a user pause, resume, and cancel a download that is actively running, with stop taking effect near-instantly even mid-file.

**Architecture:** Each download runs in a terminable child process (`SubprocessLauncher`, spawn start method). A thread-safe registry on `JobRunner` maps `job_id → handle` plus a per-job stop *intent* (`pause`/`cancel`). Endpoints set intent + terminate; the worker thread, after the child exits, performs the single terminal DB transition (`paused`, or delete-on-cancel, or `completed`/`failed`). Resume re-queues a `paused` job — Hugging Face's automatic resume reuses the on-disk bytes.

**Tech Stack:** Python 3, FastAPI, `huggingface_hub.snapshot_download`, `multiprocessing` (spawn), `threading`, SQLite, pytest. Static dashboard is a single HTML file (no JS test harness).

## Global Constraints

- **No system `pip`/`python`** — always use `.venv/bin/python` and `.venv/bin/python -m pytest`.
- **Never read `.env`** — reference env vars by name only.
- **Default test suite must stay offline** — no network, no real Hub. Only `-m integration` tests touch the network. (Spawning trivial local subprocesses in unit tests is allowed and expected for the launcher.)
- **DI for testability** — production wiring lives only in `build_default_app()`; everything else takes injected collaborators.
- **Git workflow** — already on branch `feature/pause-resume-cancel`. Commit after every green step. (Final merge into `master` with `--no-ff` happens after the plan is complete; not part of any task.)
- **Status lifecycle** — `queued → running → completed | failed | paused`; `paused → queued` (resume); cancel deletes the row + partial files. The `CANCELLED` constant stays defined but unset.

---

### Task 1: `paused` status constant + DB exclusion coverage

`paused` is a new persisted status. No query changes are needed (`unfinished_jobs()` and `pending_bytes()` already enumerate only `queued`/`running`), so this task adds the constant and locks the exclusion behavior with tests.

**Files:**
- Modify: `app/db.py` (add the `PAUSED` constant near the other status constants, lines 6-10)
- Test: `tests/test_db.py`

**Interfaces:**
- Produces: `app.db.PAUSED == "paused"` (string constant), imported by `app/backup.py` and `app/main.py` in later tasks.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_db.py` (update the import on line 3 to include `PAUSED`):

```python
from app.db import JobStore, Job, QUEUED, RUNNING, COMPLETED, FAILED, PAUSED


def test_paused_excluded_from_unfinished_jobs(store):
    a = store.create_job("a/b", "model")              # queued -> resumable on restart
    p = store.create_job("c/d", "dataset")
    store.set_status(p.id, PAUSED)                     # paused -> must NOT auto-resume
    ids = {j.id for j in store.unfinished_jobs()}
    assert ids == {a.id}


def test_paused_excluded_from_pending_bytes(store):
    p = store.create_job("c/d", "model")
    store.set_status(p.id, PAUSED)
    store.update_progress(p.id, 30, 100)              # 70 remaining, but paused
    assert store.pending_bytes() == 0                 # paused bytes are not "planned"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_db.py::test_paused_excluded_from_unfinished_jobs tests/test_db.py::test_paused_excluded_from_pending_bytes -v`
Expected: FAIL with `ImportError: cannot import name 'PAUSED'`.

- [ ] **Step 3: Add the constant**

In `app/db.py`, add to the status constants block (currently lines 6-10):

```python
QUEUED = "queued"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"
PAUSED = "paused"
CANCELLED = "cancelled"
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_db.py -v`
Expected: PASS (all db tests).

- [ ] **Step 5: Commit**

```bash
git add app/db.py tests/test_db.py
git commit -m "feat: add 'paused' status constant; cover exclusion from auto-resume and planned bytes"
```

---

### Task 2: `SubprocessLauncher` — terminable child process per download

A new module owning everything about running one download in a child process: the picklable child entry point, the process handle (`wait`/`terminate`/`exitcode`), and the launcher. The entry function is injectable so the launcher's mechanics can be unit-tested with trivial local processes — no Hub, no network.

**Files:**
- Create: `app/launcher.py`
- Create: `tests/_proc_targets.py` (plain importable module — **not** a `test_` file — so the spawned child can import its targets)
- Test: `tests/test_launcher.py`

**Interfaces:**
- Produces:
  - `app.launcher.Outcome` — dataclass `Outcome(ok: bool, error: Optional[str] = None)`.
  - `app.launcher.ProcessHandle` — `.wait(timeout=None) -> Optional[Outcome]` (joins child, drains its result; `None` if terminated before it reported one), `.terminate() -> None`, `.exitcode -> Optional[int]`.
  - `app.launcher.SubprocessLauncher(ctx=None, entry=_download_entry)` — `.start(**kwargs) -> ProcessHandle`. `kwargs` are forwarded to `snapshot_download` in the child: `repo_id, repo_type, local_dir, token, max_workers`.
  - `app.launcher._download_entry(queue, kwargs)` — module-level child entry; calls the real `snapshot_download` and `queue.put(("ok", None))` or `queue.put(("error", "<msg>"))`.

- [ ] **Step 1: Write the failing tests**

Create `tests/_proc_targets.py`:

```python
"""Picklable child-process entry points for launcher unit tests.

Lives in a plain (non-test_) module so the spawn-method child can import it by
qualified name. Each mirrors the (queue, kwargs) contract of _download_entry.
"""
import time


def ok_target(queue, kwargs):
    queue.put(("ok", None))


def error_target(queue, kwargs):
    queue.put(("error", "boom"))


def sleep_target(queue, kwargs):
    time.sleep(30)            # long enough that the test always terminates it first
    queue.put(("ok", None))   # unreached when terminated
```

Create `tests/test_launcher.py`:

```python
from app.launcher import SubprocessLauncher, Outcome
from tests import _proc_targets


def test_successful_download_reports_ok():
    launcher = SubprocessLauncher(entry=_proc_targets.ok_target)
    handle = launcher.start(repo_id="o/n", repo_type="model",
                            local_dir="/tmp/x", token=None, max_workers=2)
    outcome = handle.wait(timeout=10)
    assert outcome == Outcome(ok=True, error=None)


def test_failed_download_reports_error():
    launcher = SubprocessLauncher(entry=_proc_targets.error_target)
    handle = launcher.start(repo_id="o/n", repo_type="model",
                            local_dir="/tmp/x", token=None, max_workers=2)
    outcome = handle.wait(timeout=10)
    assert outcome is not None and outcome.ok is False
    assert outcome.error == "boom"


def test_terminate_stops_a_running_download_and_reports_no_outcome():
    launcher = SubprocessLauncher(entry=_proc_targets.sleep_target)
    handle = launcher.start(repo_id="o/n", repo_type="model",
                            local_dir="/tmp/x", token=None, max_workers=2)
    handle.terminate()
    outcome = handle.wait(timeout=10)
    assert outcome is None                # killed before it put anything on the queue
    assert handle.exitcode not in (0, None)   # exited via signal (negative on POSIX)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_launcher.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.launcher'`.

- [ ] **Step 3: Write the launcher module**

Create `app/launcher.py`:

```python
"""Run one Hugging Face download in a terminable child process.

The download runs in a separate process (spawn start method) so the worker can
terminate it mid-file. The child only downloads and reports its outcome over a
queue; it never touches the DB. Spawn (not fork) avoids deadlocking a forked
copy of the multithreaded server process.
"""
import multiprocessing as mp
import queue as _queue
from dataclasses import dataclass
from typing import Optional


@dataclass
class Outcome:
    ok: bool
    error: Optional[str] = None


def _download_entry(queue, kwargs):
    """Child entry point. Runs the real snapshot_download and reports the result.

    Reports every failure (including unusual ones) as an error string rather than
    crashing silently. Never imported with side effects, so it is spawn-safe.
    """
    try:
        from huggingface_hub import snapshot_download
        snapshot_download(**kwargs)
        queue.put(("ok", None))
    except BaseException as exc:  # noqa: BLE001 - surface anything as a job error
        queue.put(("error", str(exc)[:500]))


class ProcessHandle:
    def __init__(self, process, queue) -> None:
        self._process = process
        self._queue = queue

    def terminate(self) -> None:
        """SIGTERM the child if it is still alive. A no-op once it has exited."""
        if self._process.is_alive():
            self._process.terminate()

    @property
    def exitcode(self) -> Optional[int]:
        return self._process.exitcode

    def wait(self, timeout=None) -> Optional[Outcome]:
        """Block until the child exits, then return the Outcome it reported.

        Returns None if the child exited without reporting one (i.e. it was
        terminated mid-download). The reported payload is tiny, so reading it
        after join carries no risk of the feeder-thread deadlock that large
        Queue items can cause.
        """
        self._process.join(timeout)
        try:
            tag, error = self._queue.get_nowait()
        except _queue.Empty:
            return None
        return Outcome(ok=(tag == "ok"), error=error)


class SubprocessLauncher:
    def __init__(self, ctx=None, entry=_download_entry) -> None:
        self._ctx = ctx or mp.get_context("spawn")
        self._entry = entry

    def start(self, **kwargs) -> ProcessHandle:
        queue = self._ctx.Queue()
        process = self._ctx.Process(
            target=self._entry, args=(queue, kwargs), daemon=True
        )
        process.start()
        return ProcessHandle(process, queue)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_launcher.py -v`
Expected: PASS (3 tests). If the spawned child cannot import `tests._proc_targets`, confirm pytest is run from the repo root (it is by default).

- [ ] **Step 5: Commit**

```bash
git add app/launcher.py tests/_proc_targets.py tests/test_launcher.py
git commit -m "feat: SubprocessLauncher — run a download in a terminable child process"
```

---

### Task 3: Worker rewrite — registry, launcher seam, intent-based transitions

Replace the inline `snapshot_download` call (and the `downloader=` injection) in `run_backup_job` with the launcher + a `RunningRegistry`. All terminal DB transitions move into the worker thread, decided by the child's outcome and the recorded intent.

**Files:**
- Modify: `app/backup.py` (add `RunningRegistry`; rewrite `run_backup_job`, lines 88-150; update `JobRunner.submit`/`shutdown` signature to pass `launcher`+`registry` — full `JobRunner` rewrite happens in Task 4, so here only `run_backup_job` and the new class change)
- Test: `tests/test_backup_worker.py`

**Interfaces:**
- Consumes: `app.launcher.SubprocessLauncher`, `app.launcher.Outcome`; `app.db.PAUSED`.
- Produces:
  - `app.backup.RunningRegistry` — `.register(job_id, handle)`, `.unregister(job_id)` (drops handle **and** intent), `.intent(job_id) -> Optional[str]`, `.request(job_id, intent) -> bool` (records intent, terminates the live handle if present, returns whether one was terminated), `.terminate_all()`.
  - `app.backup.run_backup_job(job_id, store, settings, api=None, launcher=None, stopping=None, registry=None)` — `launcher` replaces `downloader`; defaults to `SubprocessLauncher()` when `None`.

- [ ] **Step 1: Write the failing tests**

Replace the top of `tests/test_backup_worker.py` (imports + helpers) so the suite drives the worker through an in-thread fake launcher that mirrors `ProcessHandle` semantics. Update the import on line 5 and the helpers:

```python
import threading
import time
from pathlib import Path
import pytest
from app.config import Settings
from app.db import JobStore, COMPLETED, FAILED, CANCELLED, RUNNING, PAUSED
from app.backup import run_backup_job, JobRunner, RunningRegistry
from app.launcher import Outcome


# --- in-thread fake launcher: mirrors ProcessHandle without a real process ---
class _FakeHandle:
    def __init__(self, thread, result, stop, terminated):
        self._thread = thread
        self._result = result          # {"outcome": Outcome} once the fn returns
        self._stop = stop              # set by terminate(); the fake fn may wait on it
        self._terminated = terminated  # {"v": bool}

    def terminate(self):
        self._terminated["v"] = True
        self._stop.set()

    @property
    def exitcode(self):
        return -15 if self._terminated["v"] else 0

    def wait(self, timeout=None):
        self._thread.join(timeout)
        if self._terminated["v"]:
            return None                # mirror a SIGTERM'd child: reported nothing
        return self._result.get("outcome")


class InThreadLauncher:
    """Runs the injected download fn in a thread. Always passes a `stop` Event so
    blocking fakes can simulate an interruptible download."""
    def __init__(self, fn):
        self._fn = fn

    def start(self, **kwargs):
        stop = threading.Event()
        result = {}
        terminated = {"v": False}

        def run():
            try:
                self._fn(stop=stop, **kwargs)
                result["outcome"] = Outcome(ok=True)
            except BaseException as exc:  # noqa: BLE001
                result["outcome"] = Outcome(ok=False, error=str(exc))

        t = threading.Thread(target=run)
        t.start()
        return _FakeHandle(t, result, stop, terminated)


def wait_until(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class _Sibling:
    def __init__(self, size):
        self.size = size


class _Info:
    def __init__(self, siblings):
        self.siblings = siblings


class FakeApi:
    def __init__(self, total):
        self._total = total

    def repo_info(self, repo_id, repo_type, token=None, files_metadata=False):
        return _Info([_Sibling(self._total)])


def make_settings(tmp_path, max_jobs=2):
    return Settings(
        hf_token="hf_test",
        backup_dir=tmp_path / "backups",
        max_concurrent_jobs=max_jobs,
        max_workers=4,
        db_path=tmp_path / "jobs.db",
    )


def fake_downloader_factory(payload=b"hello-world"):
    def _download(*, local_dir, stop=None, **_):
        target = Path(local_dir)
        target.mkdir(parents=True, exist_ok=True)
        (target / "model.bin").write_bytes(payload)
    return _download


def blocking_downloader_factory(started):
    """Writes a partial file, signals `started`, then blocks until terminated."""
    def _download(*, local_dir, stop=None, **_):
        target = Path(local_dir)
        target.mkdir(parents=True, exist_ok=True)
        (target / "partial.bin").write_bytes(b"x" * 100)
        started.set()
        stop.wait(5)
    return _download
```

Now migrate every existing test in the file from `downloader=X` to `launcher=InThreadLauncher(X)`. Concretely:
- `test_worker_completes_and_writes_files`: `downloader=fake_downloader_factory(payload)` → `launcher=InThreadLauncher(fake_downloader_factory(payload))`.
- `test_worker_marks_failed_on_download_error`: `downloader=boom` → `launcher=InThreadLauncher(boom)`.
- `test_worker_leaves_job_resumable_on_shutdown`: `downloader=boom, stopping=stopping` → `launcher=InThreadLauncher(boom), stopping=stopping`.
- `test_worker_marks_failed_when_not_shutting_down`: same `boom` swap.
- `test_worker_skips_cancelled_job`: `downloader=downloader` → `launcher=InThreadLauncher(downloader)`.
- `test_worker_marks_failed_when_sizing_fails`: `downloader=downloader` → `launcher=InThreadLauncher(downloader)`.
- `test_worker_marks_failed_when_local_dir_cannot_be_created`: `downloader=fake_downloader_factory(b"x")` → `launcher=InThreadLauncher(fake_downloader_factory(b"x"))`.
- `test_runner_runs_job_to_completion`, `test_runner_shutdown_sets_stopping_flag`: change `downloader=...` → `launcher=InThreadLauncher(...)` (the `JobRunner` signature change lands in Task 4; these two will go green there — for now they may error on the kwarg, which is expected until Task 4).
- `test_worker_fails_when_insufficient_disk`: `downloader=downloader` → `launcher=InThreadLauncher(downloader)`.

Keep the existing `boom` definitions inside their tests (they already take `**kwargs`, which absorbs `stop`). Update the module-level `fake_downloader_factory` to the new signature shown above. Then append the new behavior tests:

```python
def test_worker_pause_keeps_files_and_sets_paused(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    registry = RunningRegistry()
    started = threading.Event()

    t = threading.Thread(target=run_backup_job, kwargs=dict(
        job_id=job.id, store=store, settings=settings, api=FakeApi(1000),
        launcher=InThreadLauncher(blocking_downloader_factory(started)),
        registry=registry,
    ))
    t.start()
    assert started.wait(3)                       # partial file written, download blocking
    registry.request(job.id, "pause")            # terminate -> worker honors pause
    t.join(5)

    paused = store.get_job(job.id)
    assert paused.status == PAUSED
    assert (tmp_path / "backups" / "models" / "o" / "n" / "partial.bin").exists()
    store.close()


def test_worker_cancel_deletes_files_and_row(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    registry = RunningRegistry()
    started = threading.Event()

    t = threading.Thread(target=run_backup_job, kwargs=dict(
        job_id=job.id, store=store, settings=settings, api=FakeApi(1000),
        launcher=InThreadLauncher(blocking_downloader_factory(started)),
        registry=registry,
    ))
    t.start()
    assert started.wait(3)
    registry.request(job.id, "cancel")
    t.join(5)

    assert store.get_job(job.id) is None                                   # row gone
    assert not (tmp_path / "backups" / "models" / "o" / "n").exists()      # files gone
    store.close()


def test_worker_completion_wins_over_a_late_pause_intent(tmp_path):
    # The download finishes successfully; a pause intent recorded after the fact
    # must not override completion (the files are all present).
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    registry = RunningRegistry()
    registry._intents[job.id] = "pause"          # intent present, but download will succeed

    run_backup_job(job.id, store, settings, api=FakeApi(11),
                   launcher=InThreadLauncher(fake_downloader_factory(b"y" * 11)),
                   registry=registry)

    assert store.get_job(job.id).status == COMPLETED
    store.close()


def test_worker_self_terminates_when_intent_set_before_registration(tmp_path):
    # A cancel that lands before the handle is registered must still take effect:
    # the worker checks intent right after registering and self-terminates.
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    registry = RunningRegistry()
    registry._intents[job.id] = "cancel"         # pre-set, no handle yet
    started = threading.Event()

    run_backup_job(job.id, store, settings, api=FakeApi(1000),
                   launcher=InThreadLauncher(blocking_downloader_factory(started)),
                   registry=registry)

    assert store.get_job(job.id) is None          # cancelled despite early intent
    store.close()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_backup_worker.py -v`
Expected: FAIL — `ImportError: cannot import name 'RunningRegistry'` (and `run_backup_job()` rejecting the `launcher=`/`registry=` kwargs).

- [ ] **Step 3: Add `RunningRegistry` and rewrite `run_backup_job`**

In `app/backup.py`, update the imports at the top to add `PAUSED`:

```python
from .db import PAUSED
```

Add the `RunningRegistry` class (place it just above `run_backup_job`):

```python
class RunningRegistry:
    """Tracks the live download handle and stop intent for each running job.

    Endpoints reach a running download only through here. The worker registers
    its handle after start and unregisters in a finally; unregister clears the
    intent too, so a paused-then-resumed job (same job_id) never inherits a stale
    'pause' from its previous run.
    """
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._handles = {}
        self._intents = {}

    def register(self, job_id, handle) -> None:
        with self._lock:
            self._handles[job_id] = handle

    def unregister(self, job_id) -> None:
        with self._lock:
            self._handles.pop(job_id, None)
            self._intents.pop(job_id, None)

    def intent(self, job_id):
        with self._lock:
            return self._intents.get(job_id)

    def request(self, job_id, intent) -> bool:
        """Record a stop intent; terminate the live handle if one is registered.
        Returns True if a handle was terminated (False if none was running yet —
        the worker will honor the recorded intent once it registers)."""
        with self._lock:
            self._intents[job_id] = intent
            handle = self._handles.get(job_id)
        if handle is not None:
            handle.terminate()
            return True
        return False

    def terminate_all(self) -> None:
        with self._lock:
            handles = list(self._handles.values())
        for handle in handles:
            handle.terminate()
```

Rewrite `run_backup_job` (replace the whole function, current lines 88-150):

```python
def run_backup_job(job_id, store, settings, api=None, launcher=None,
                   stopping=None, registry=None) -> None:
    if launcher is None:
        from .launcher import SubprocessLauncher
        launcher = SubprocessLauncher()

    job = store.get_job(job_id)
    if job is None or job.status == "cancelled":
        return

    store.set_status(job_id, "running")
    local_dir = local_dir_for(settings.backup_dir, job.repo_type, job.slug)

    stop = threading.Event()

    def _poll():
        while not stop.is_set():
            store.update_progress(job_id, directory_size(local_dir))
            stop.wait(POLL_INTERVAL)

    poller = threading.Thread(target=_poll, daemon=True)
    try:
        backup_root = settings.backup_dir.resolve()
        if not local_dir.resolve().is_relative_to(backup_root):
            raise ValueError(f"refusing to write outside backup dir: {local_dir}")
        local_dir.mkdir(parents=True, exist_ok=True)
        total = repo_total_bytes(job.slug, job.repo_type, settings.hf_token, api=api)
        already = directory_size(local_dir)
        store.update_progress(job_id, already, total_bytes=total)

        # Pre-flight: refuse a download that cannot physically fit, instead of
        # filling the disk / exhausting memory and getting OOM-killed mid-run.
        free = free_disk_bytes(settings.backup_dir)
        remaining = total - already
        if total and remaining > free:
            raise RuntimeError(
                f"not enough disk space for {job.slug}: needs ~{remaining / 1e9:.1f} GB "
                f"more, only {free / 1e9:.1f} GB free in {settings.backup_dir}"
            )

        poller.start()
        handle = launcher.start(
            repo_id=job.slug,
            repo_type=job.repo_type,
            local_dir=str(local_dir),
            token=settings.hf_token or None,
            max_workers=settings.max_workers,
        )
        if registry is not None:
            registry.register(job_id, handle)
            # Close the race where pause/cancel landed before registration.
            if registry.intent(job_id) is not None:
                handle.terminate()

        outcome = handle.wait()
        stop.set()
        poller.join(timeout=2)

        intent = registry.intent(job_id) if registry is not None else None

        if outcome is not None and outcome.ok:
            # Finished before any stop signal landed -> completion wins.
            final = directory_size(local_dir)
            store.update_progress(job_id, total if total else final, total_bytes=total)
            store.set_status(job_id, "completed")
        elif intent == "pause":
            store.set_status(job_id, PAUSED)
        elif intent == "cancel":
            delete_backup_files(settings.backup_dir, job.repo_type, job.slug)
            store.delete_job(job_id)
        elif stopping is not None and stopping.is_set():
            # Process-wide shutdown terminated the child; leave the job 'running'
            # so the startup re-queue resumes it instead of failing it.
            return
        else:
            err = outcome.error if outcome is not None else \
                f"download process exited unexpectedly (code {handle.exitcode})"
            store.set_status(job_id, "failed", error=str(err)[:500])
    except Exception as exc:  # noqa: BLE001 - surface any pre-download failure
        stop.set()
        if poller.is_alive():
            poller.join(timeout=2)
        if stopping is not None and stopping.is_set():
            return
        store.set_status(job_id, "failed", error=str(exc)[:500])
    finally:
        if registry is not None:
            registry.unregister(job_id)
```

- [ ] **Step 4: Run the worker tests to verify the worker-level ones pass**

Run: `.venv/bin/python -m pytest tests/test_backup_worker.py -v`
Expected: PASS for every test **except** `test_runner_runs_job_to_completion` and `test_runner_shutdown_sets_stopping_flag`, which still construct `JobRunner(..., launcher=...)` — those go green in Task 4. If they fail with a `JobRunner` kwarg/launcher error, that is expected at this step.

- [ ] **Step 5: Commit**

```bash
git add app/backup.py tests/test_backup_worker.py
git commit -m "feat: worker runs downloads via launcher + registry; intent-based pause/cancel transitions"
```

---

### Task 4: `JobRunner` — registry-backed pause/cancel and terminate-on-shutdown

Wire the `JobRunner` to own a `RunningRegistry`, take a `launcher` instead of a `downloader`, expose `pause`/`cancel`, and terminate all live children on shutdown.

**Files:**
- Modify: `app/backup.py` (`JobRunner`, current lines 153-176)
- Test: `tests/test_backup_worker.py` (the two runner tests already migrated in Task 3 now pass; add two concurrent ones)

**Interfaces:**
- Consumes: `RunningRegistry`, `run_backup_job` (Task 3).
- Produces:
  - `JobRunner(store, settings, api=None, launcher=None)` — `.submit(job_id)`, `.pause(job_id)`, `.cancel(job_id) -> bool`, `.shutdown(wait=False)`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_backup_worker.py`:

```python
def test_runner_pause_sets_job_paused(tmp_path):
    settings = make_settings(tmp_path, max_jobs=1)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    started = threading.Event()
    runner = JobRunner(store, settings, api=FakeApi(1000),
                       launcher=InThreadLauncher(blocking_downloader_factory(started)))
    runner.submit(job.id)
    assert started.wait(3)
    runner.pause(job.id)
    assert wait_until(lambda: store.get_job(job.id).status == PAUSED)
    runner.shutdown()
    store.close()


def test_runner_cancel_terminates_and_removes_job(tmp_path):
    settings = make_settings(tmp_path, max_jobs=1)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    started = threading.Event()
    runner = JobRunner(store, settings, api=FakeApi(1000),
                       launcher=InThreadLauncher(blocking_downloader_factory(started)))
    runner.submit(job.id)
    assert started.wait(3)
    assert runner.cancel(job.id) is True
    assert wait_until(lambda: store.get_job(job.id) is None)
    runner.shutdown()
    store.close()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_backup_worker.py::test_runner_pause_sets_job_paused tests/test_backup_worker.py::test_runner_cancel_terminates_and_removes_job -v`
Expected: FAIL — `JobRunner` has no `pause`/`cancel` and rejects `launcher=`.

- [ ] **Step 3: Rewrite `JobRunner`**

Replace the `JobRunner` class in `app/backup.py`:

```python
class JobRunner:
    def __init__(self, store, settings, api=None, launcher=None) -> None:
        self._store = store
        self._settings = settings
        self._api = api
        self._launcher = launcher
        self._stopping = threading.Event()
        self._registry = RunningRegistry()
        self._executor = ThreadPoolExecutor(max_workers=settings.max_concurrent_jobs)

    def submit(self, job_id) -> None:
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

    def shutdown(self, wait: bool = False) -> None:
        """Stop the runner. Default (wait=False) is for process shutdown: signal
        in-flight workers that we're stopping (so they leave their jobs resumable),
        terminate their child processes, and cancel not-yet-started jobs (which
        stay 'queued' for the next startup). wait=True drains to completion."""
        self._stopping.set()
        self._registry.terminate_all()
        self._executor.shutdown(wait=wait, cancel_futures=not wait)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_backup_worker.py -v`
Expected: PASS (entire file, including the two migrated runner tests and the two new concurrent ones).

- [ ] **Step 5: Commit**

```bash
git add app/backup.py tests/test_backup_worker.py
git commit -m "feat: JobRunner pause/cancel via registry; terminate child processes on shutdown"
```

---

### Task 5: API endpoints — pause, resume, and cancel-anything

Add `pause`/`resume` and extend `cancel` to stop running jobs and discard partial files for queued/paused jobs.

**Files:**
- Modify: `app/main.py` (import `PAUSED`; add `pause`/`resume`; rewrite `cancel`, current lines 106-114)
- Test: `tests/test_api.py` (extend `FakeRunner`; update the running-cancel test; add pause/resume/cancel-paused/restart tests)

**Interfaces:**
- Consumes: `runner.pause(job_id)`, `runner.cancel(job_id)` (Task 4); `app.db.PAUSED`; existing `delete_backup_files`, `store.requeue`, `store.delete_job`.
- Produces: `POST /api/jobs/{id}/pause`, `POST /api/jobs/{id}/resume`, extended `POST /api/jobs/{id}/cancel`.

- [ ] **Step 1: Write the failing tests**

In `tests/test_api.py`, extend `FakeRunner` (lines 8-17) and the import (line 4):

```python
from app.db import JobStore, FAILED, QUEUED, PAUSED, RUNNING


class FakeRunner:
    def __init__(self):
        self.submitted = []
        self.paused = []
        self.cancelled = []
        self.shutdowns = 0

    def submit(self, job_id):
        self.submitted.append(job_id)

    def pause(self, job_id):
        self.paused.append(job_id)

    def cancel(self, job_id):
        self.cancelled.append(job_id)
        return True

    def shutdown(self, wait=False):
        self.shutdowns += 1
```

Replace `test_cancel_running_job_is_rejected` (lines 135-139) with:

```python
def test_cancel_running_job_terminates_it(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    store.set_status(job.id, RUNNING)
    resp = client.post(f"/api/jobs/{job.id}/cancel")
    assert resp.status_code == 200
    assert job.id in runner.cancelled        # handed to the runner to terminate
```

Append new tests:

```python
def test_pause_only_running(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    assert client.post(f"/api/jobs/{job.id}/pause").status_code == 409   # queued
    store.set_status(job.id, RUNNING)
    resp = client.post(f"/api/jobs/{job.id}/pause")
    assert resp.status_code == 200
    assert job.id in runner.paused


def test_pause_missing_job_404(ctx):
    client, store, runner = ctx
    assert client.post("/api/jobs/999/pause").status_code == 404


def test_resume_only_paused(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    assert client.post(f"/api/jobs/{job.id}/resume").status_code == 409  # queued
    store.set_status(job.id, PAUSED)
    resp = client.post(f"/api/jobs/{job.id}/resume")
    assert resp.status_code == 200
    assert resp.json()["status"] == QUEUED
    assert job.id in runner.submitted


def test_resume_missing_job_404(ctx):
    client, store, runner = ctx
    assert client.post("/api/jobs/999/resume").status_code == 404


def test_cancel_paused_deletes_files_and_row(ctx, tmp_path):
    client, store, runner = ctx
    from app.backup import local_dir_for
    backup = tmp_path / "backups"
    job = store.create_job("o/n", "model")
    d = local_dir_for(backup, "model", "o/n")
    d.mkdir(parents=True)
    (d / "partial.bin").write_bytes(b"x" * 10)
    store.set_status(job.id, PAUSED)
    resp = client.post(f"/api/jobs/{job.id}/cancel")
    assert resp.status_code == 200
    assert store.get_job(job.id) is None      # row gone
    assert not d.exists()                      # files gone
    assert runner.cancelled == []              # no live process; deleted directly


def test_cancel_queued_deletes_partial_files(ctx, tmp_path):
    client, store, runner = ctx
    from app.backup import local_dir_for
    backup = tmp_path / "backups"
    job = store.create_job("o/n", "model")     # queued, but partial bytes on disk
    d = local_dir_for(backup, "model", "o/n")
    d.mkdir(parents=True)
    (d / "partial.bin").write_bytes(b"x" * 10)
    resp = client.post(f"/api/jobs/{job.id}/cancel")
    assert resp.status_code == 200
    assert store.get_job(job.id) is None
    assert not d.exists()


def test_startup_does_not_resume_paused_jobs(tmp_path):
    settings = make_settings(tmp_path)
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)
    paused = store.create_job("stay/paused", "model")
    store.set_status(paused.id, PAUSED)
    runner = FakeRunner()
    app = create_app(settings, store, runner, detect=lambda s, t: [])
    with TestClient(app):
        pass
    assert paused.id not in runner.submitted   # paused jobs are NOT auto-resumed
    store.close()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_api.py -v`
Expected: FAIL — pause/resume endpoints 404/405, and running-cancel still 409.

- [ ] **Step 3: Add/modify the endpoints**

In `app/main.py`, update the db import (line 13):

```python
from .db import COMPLETED, FAILED, JobStore, PAUSED, QUEUED, RUNNING
```

Replace the `cancel` endpoint (current lines 106-114) and add `pause`/`resume` next to it:

```python
    @app.post("/api/jobs/{job_id}/pause")
    def pause(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != RUNNING:
            raise HTTPException(status_code=409, detail="only running downloads can be paused")
        runner.pause(job_id)
        return {"pausing": job_id}

    @app.post("/api/jobs/{job_id}/resume")
    def resume(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != PAUSED:
            raise HTTPException(status_code=409, detail="only paused downloads can be resumed")
        store.requeue(job_id)
        runner.submit(job_id)
        return store.get_job(job_id).to_dict()

    @app.post("/api/jobs/{job_id}/cancel")
    def cancel(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status not in (QUEUED, RUNNING, PAUSED):
            raise HTTPException(
                status_code=409,
                detail="only queued, running, or paused jobs can be cancelled",
            )
        if job.status == RUNNING:
            # Hand off to the runner; the worker deletes files + row once the
            # child process dies (near-instant, even mid-file).
            runner.cancel(job_id)
            return {"cancelling": job_id}
        # queued / paused: no live process — discard partial files + row directly.
        delete_backup_files(settings.backup_dir, job.repo_type, job.slug)
        store.delete_job(job_id)
        return {"deleted": job_id}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_api.py -v`
Expected: PASS (whole file).

- [ ] **Step 5: Commit**

```bash
git add app/main.py tests/test_api.py
git commit -m "feat: pause/resume endpoints; cancel stops running jobs and discards partial files"
```

---

### Task 6: Dashboard — pause/resume/cancel controls

Status-driven row actions, a `paused` status color, and a confirm dialog on the now-destructive cancel.

**Files:**
- Modify: `app/static/index.html` (CSS status colors line 33; the `row()` action logic lines 138-152; add a `confirmCancel` helper near `confirmDelete` lines 133-137)
- Test: `tests/test_static.py`

**Interfaces:**
- Consumes: existing `act(id, what)` helper (POSTs to `/api/jobs/{id}/{what}`), `fmt()`, `esc()`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_static.py`:

```python
def test_running_and_paused_rows_expose_pause_resume_cancel(client):
    page = client.get("/").text
    assert ">Pause<" in page          # running rows can be paused
    assert ">Resume<" in page         # paused rows can be resumed
    assert "confirmCancel" in page    # cancel is confirm-guarded (now destroys data)
    assert ".st.paused" in page       # paused status has its own color rule
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_static.py::test_running_and_paused_rows_expose_pause_resume_cancel -v`
Expected: FAIL (`>Pause<` not in page).

- [ ] **Step 3: Update the dashboard**

In `app/static/index.html`, give `paused` its own status color. Replace the status-color rule (line 33):

```css
.st.completed{color:var(--ok)}.st.failed{color:var(--err)}.st.running{color:var(--accent)}.st.queued,.st.cancelled{color:var(--muted)}.st.paused{color:#d2a24c}
```

Add a spacing rule so two action buttons in a cell don't touch (add near the other `button.ghost` rules, after line 20):

```css
td button.ghost + button.ghost{margin-left:6px}
```

Add the `confirmCancel` helper right after `confirmDelete` (after line 137):

```javascript
function confirmCancel(id, slug, bytes) {
  if (!confirm(`Stop and discard ${slug} (${fmt(bytes)} downloaded)? This frees disk space and cannot be undone.`)) return;
  act(id, "cancel");
}
window.confirmCancel = confirmCancel;
```

Replace the `actions` ternary in `row()` (lines 139-145) with status-driven controls:

```javascript
  const actions =
      j.status === "running"
    ? `<button class="ghost" onclick="act(${j.id},'pause')">Pause</button>`
      + `<button class="ghost" onclick="confirmCancel(${j.id},'${esc(j.slug)}',${j.downloaded_bytes})">Cancel</button>`
    : j.status === "paused"
    ? `<button class="ghost" onclick="act(${j.id},'resume')">Resume</button>`
      + `<button class="ghost" onclick="confirmCancel(${j.id},'${esc(j.slug)}',${j.downloaded_bytes})">Cancel</button>`
    : j.status === "failed"
    ? `<button class="ghost" onclick="act(${j.id},'retry')">Retry</button>`
    : j.status === "queued"
    ? `<button class="ghost" onclick="act(${j.id},'cancel')">Cancel</button>`
    : j.status === "completed"
    ? `<button class="ghost" onclick="confirmDelete(${j.id},'${esc(j.slug)}',${j.total_bytes})">Delete</button>`
    : "";
```

- [ ] **Step 4: Run the static tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_static.py -v`
Expected: PASS (including the existing `test_completed_rows_have_confirm_guarded_delete`, which still finds `confirmDelete` and `confirm(`).

- [ ] **Step 5: Commit**

```bash
git add app/static/index.html tests/test_static.py
git commit -m "feat: dashboard pause/resume controls and confirm-guarded destructive cancel"
```

---

### Task 7: Integration test + docs

One opt-in test that runs the real `SubprocessLauncher` end to end (terminate mid-flight, then resume to completion), plus updating the architecture docs that describe the worker.

**Files:**
- Modify: `tests/test_integration.py`
- Modify: `CLAUDE.md` (the "Architecture" section that names the `downloader` collaborator and the worker engine)

**Interfaces:**
- Consumes: `JobRunner` (Task 4), `SubprocessLauncher` (Task 2), real `snapshot_download`.

- [ ] **Step 1: Write the integration test**

Append to `tests/test_integration.py`:

```python
import threading
import time
from app.db import PAUSED
from app.backup import JobRunner


def _wait_until(predicate, timeout=60.0, interval=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


@pytest.mark.integration
def test_pause_then_resume_completes(tmp_path):
    settings = Settings(
        hf_token=os.environ.get("HUGGINGFACE_ACCESS_KEY", ""),
        backup_dir=tmp_path / "backups",
        max_concurrent_jobs=1,
        max_workers=2,
        db_path=tmp_path / "jobs.db",
    )
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)
    job = store.create_job(TINY_PUBLIC_MODEL, "model")

    # Real subprocess launcher (default), real snapshot_download.
    runner = JobRunner(store, settings)
    runner.submit(job.id)
    assert _wait_until(lambda: store.get_job(job.id).status in ("running", "completed", PAUSED))

    # If it is still running, pause it and confirm it parks in 'paused'.
    if store.get_job(job.id).status == "running":
        runner.pause(job.id)
        assert _wait_until(lambda: store.get_job(job.id).status == PAUSED)

    # Resume (re-queue + submit); it must reach completion.
    store.requeue(job.id)
    runner.submit(job.id)
    assert _wait_until(lambda: store.get_job(job.id).status == COMPLETED, timeout=120)

    runner.shutdown()
    done = store.get_job(job.id)
    assert done.status == COMPLETED, done.error
    target = tmp_path / "backups" / "models" / "hf-internal-testing" / "tiny-random-gpt2"
    assert target.exists() and any(target.iterdir())
    store.close()
```

- [ ] **Step 2: Run the integration test (opt-in, needs network + token)**

Run: `.venv/bin/python -m pytest tests/test_integration.py -m integration -v`
Expected: PASS. (The tiny repo may download before a pause lands; the test tolerates that and still asserts completion via resume.)

- [ ] **Step 3: Update `CLAUDE.md`**

In `CLAUDE.md`, update the Architecture text that currently reads:

> `create_app()`, `JobRunner`, and `run_backup_job()` all accept injected collaborators (`api`, `downloader`, `detect`); `build_default_app()` is the only place that wires the real `HfApi` / `snapshot_download`.

Replace `downloader` with `launcher` and note the subprocess model. Add a sentence to the `backup.py` bullet:

> Each download now runs in a **terminable child process** (`app/launcher.py`, `SubprocessLauncher`, spawn start method) so a running download can be paused or cancelled near-instantly even mid-file; a `RunningRegistry` on `JobRunner` maps `job_id → handle`+intent, and the worker performs the single terminal transition (`paused`, delete-on-cancel, or `completed`/`failed`) after the child exits.

And update the status-lifecycle line to include `paused` and note that cancel now also stops running jobs and discards their files.

- [ ] **Step 4: Run the full default suite**

Run: `.venv/bin/python -m pytest -v`
Expected: PASS (all non-integration tests).

- [ ] **Step 5: Commit**

```bash
git add tests/test_integration.py CLAUDE.md
git commit -m "test: integration pause-then-resume; docs: launcher/subprocess worker model"
```

---

## Self-Review

**1. Spec coverage:**
- `paused` status + restart-stays-paused → Task 1 (constant + exclusion tests), Task 5 (`test_startup_does_not_resume_paused_jobs`). ✓
- Per-download terminable child process (spawn) → Task 2. ✓
- Launcher seam replacing `downloader`; in-thread fake keeps suite offline → Tasks 2-4. ✓
- Running registry + intent; race-close before registration → Task 3. ✓
- Race-free terminal transition in worker; "completed wins over late intent" → Task 3 tests. ✓
- Shutdown terminates children, leaves jobs `running` → Task 4. ✓
- Endpoints pause/resume; cancel for queued/running/paused; cancel discards files (incl. paused) → Task 5. ✓
- Dashboard buttons, `.st.paused`, confirm-guarded cancel → Task 6. ✓
- `pending_bytes`/planned and speed indicator exclude paused → Task 1 (db), no UI change needed (keys off running). ✓
- Opt-in real-subprocess integration test → Task 7. ✓

**2. Placeholder scan:** No TBD/TODO; every code step shows complete code; commands have expected output. ✓

**3. Type consistency:** `RunningRegistry.request(job_id, intent) -> bool`, `.intent()`, `.register()`, `.unregister()`, `.terminate_all()` consistent across Tasks 3-4. `ProcessHandle.wait()/terminate()/exitcode` and `Outcome(ok, error)` consistent across Tasks 2-3 (the `_FakeHandle` mirrors them). `run_backup_job(..., launcher=, stopping=, registry=)` arg order matches `JobRunner.submit`'s positional call. Endpoint return shapes (`pausing`/`cancelling`/`deleted`) are display-agnostic (frontend just refreshes). ✓
