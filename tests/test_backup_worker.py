import threading
import time
from pathlib import Path
import pytest
from app.config import Settings
import hashlib
import json
import socket
from app.db import JobStore, COMPLETED, FAILED, CANCELLED, RUNNING, PAUSED, QUEUED, RETRYING, VERIFYING
from app.backup import run_backup_job, run_verify_job, JobRunner, RunningRegistry, local_dir_for
from app.launcher import Outcome
from app.retry import is_retryable, BACKOFF_SECONDS, MAX_RETRIES


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
        # After join(), the fn always reported something (run() catches BaseException).
        # Return the actual outcome regardless of terminate state — mirrors ProcessHandle,
        # where terminate() on an already-exited process is a no-op and wait() still
        # reads the outcome from the queue.
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
                result["outcome"] = Outcome(ok=False, error=str(exc),
                                            retryable=is_retryable(exc))

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


def _git_blob_sha1_bytes(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\x00" + data).hexdigest()


class _Lfs:
    def __init__(self, sha256):
        self.sha256 = sha256


class _VSibling:
    def __init__(self, rfilename, size, blob_id=None, lfs=None):
        self.rfilename = rfilename
        self.size = size
        self.blob_id = blob_id
        self.lfs = lfs


class VerifyApi:
    """repo_info reports the hashes of `truth` (name -> bytes). Files ending in
    .bin are reported as LFS (lfs.sha256); everything else via git blob sha1."""
    def __init__(self, truth):
        self._truth = truth

    def repo_info(self, repo_id, repo_type, token=None, files_metadata=False):
        sibs = []
        for name, data in self._truth.items():
            if name.endswith(".bin"):
                sibs.append(_VSibling(name, len(data), lfs=_Lfs(hashlib.sha256(data).hexdigest())))
            else:
                sibs.append(_VSibling(name, len(data), blob_id=_git_blob_sha1_bytes(data)))
        return _Info(sibs)


def files_downloader_factory(files):
    def _download(*, local_dir, stop=None, **_):
        target = Path(local_dir)
        target.mkdir(parents=True, exist_ok=True)
        for name, data in files.items():
            (target / name).write_bytes(data)
    return _download


def _write_completed_repo(store, settings, slug, files):
    """Create a completed job with `files` already on disk and total_bytes set —
    the precondition for a manual verify."""
    job = store.create_job(slug, "model")
    d = local_dir_for(settings.backup_dir, "model", slug)
    d.mkdir(parents=True, exist_ok=True)
    for name, data in files.items():
        (d / name).write_bytes(data)
    store.set_status(job.id, COMPLETED)
    store.update_progress(job.id, 0, total_bytes=sum(len(b) for b in files.values()))
    return job


def make_settings(tmp_path, max_jobs=2, verify_downloads=False, stall_timeout=600.0):
    return Settings(
        hf_token="hf_test",
        backup_dir=tmp_path / "backups",
        max_concurrent_jobs=max_jobs,
        max_workers=4,
        db_path=tmp_path / "jobs.db",
        verify_downloads=verify_downloads,
        stall_timeout=stall_timeout,
    )


def fake_downloader_factory(payload=b"hello-world"):
    def _download(*, local_dir, stop=None, **_):
        target = Path(local_dir)
        target.mkdir(parents=True, exist_ok=True)
        (target / "model.bin").write_bytes(payload)
    return _download


def blocking_downloader_factory(started):
    """Writes a partial file, signals `started`, then blocks until terminated.
    Raises RuntimeError when the stop event fires so the Outcome is ok=False,
    letting the worker distinguish 'terminated' from 'completed naturally'."""
    def _download(*, local_dir, stop=None, **_):
        target = Path(local_dir)
        target.mkdir(parents=True, exist_ok=True)
        (target / "partial.bin").write_bytes(b"x" * 100)
        started.set()
        stop.wait(5)
        if stop.is_set():
            raise RuntimeError("terminated")
    return _download


def test_worker_completes_and_writes_files(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    payload = b"x" * 42
    run_backup_job(job.id, store, settings, api=FakeApi(42),
                   launcher=InThreadLauncher(fake_downloader_factory(payload)))
    done = store.get_job(job.id)
    assert done.status == COMPLETED
    assert done.total_bytes == 42
    assert done.downloaded_bytes == 42
    assert (tmp_path / "backups" / "models" / "o" / "n" / "model.bin").read_bytes() == payload
    store.close()


def test_worker_marks_failed_on_download_error(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")

    def boom(**kwargs):
        raise RuntimeError("network exploded")

    run_backup_job(job.id, store, settings, api=FakeApi(10), launcher=InThreadLauncher(boom))
    failed = store.get_job(job.id)
    assert failed.status == FAILED
    assert "network exploded" in failed.error
    store.close()


def test_worker_leaves_job_resumable_on_shutdown(tmp_path):
    # When the process is shutting down, snapshot_download's thread pool raises
    # "cannot schedule new futures after interpreter shutdown". That is NOT a real
    # download failure — the job must be left in 'running' so the startup re-queue
    # resumes it, not marked 'failed' (which would defeat auto-resume).
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    stopping = threading.Event()
    stopping.set()

    def boom(**kwargs):
        raise RuntimeError("cannot schedule new futures after interpreter shutdown")

    run_backup_job(job.id, store, settings, api=FakeApi(10),
                   launcher=InThreadLauncher(boom), stopping=stopping)
    resumable = store.get_job(job.id)
    assert resumable.status == RUNNING   # left resumable, NOT failed
    assert resumable.error is None
    store.close()


def test_worker_marks_failed_when_not_shutting_down(tmp_path):
    # A genuine download error (stopping event present but NOT set) still fails
    # the job, so normal failures are unaffected by the shutdown handling.
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    stopping = threading.Event()  # not set

    def boom(**kwargs):
        raise RuntimeError("network exploded")

    run_backup_job(job.id, store, settings, api=FakeApi(10),
                   launcher=InThreadLauncher(boom), stopping=stopping)
    failed = store.get_job(job.id)
    assert failed.status == FAILED
    assert "network exploded" in failed.error
    store.close()


def test_worker_skips_cancelled_job(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    store.set_status(job.id, CANCELLED)
    called = []

    def downloader(**kwargs):
        called.append(True)

    run_backup_job(job.id, store, settings, api=FakeApi(10), launcher=InThreadLauncher(downloader))
    assert called == []
    assert store.get_job(job.id).status == CANCELLED
    store.close()


def test_worker_marks_failed_when_sizing_fails(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")

    class FailingApi:
        def repo_info(self, *args, **kwargs):
            raise RuntimeError("size lookup failed")

    called = []
    def downloader(**kwargs):
        called.append(True)

    run_backup_job(job.id, store, settings, api=FailingApi(), launcher=InThreadLauncher(downloader))
    failed = store.get_job(job.id)
    assert failed.status == FAILED
    assert "size lookup failed" in failed.error
    assert called == []   # download never ran because sizing failed first
    store.close()


def test_worker_marks_failed_when_local_dir_cannot_be_created(tmp_path):
    settings = make_settings(tmp_path)
    # Make the would-be parent ("models") a FILE so mkdir(parents=True) fails.
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    (settings.backup_dir / "models").write_text("i am a file, not a dir")
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    run_backup_job(job.id, store, settings, api=FakeApi(10),
                   launcher=InThreadLauncher(fake_downloader_factory(b"x")))
    failed = store.get_job(job.id)
    assert failed.status == FAILED
    assert failed.error
    store.close()


def test_runner_runs_job_to_completion(tmp_path):
    settings = make_settings(tmp_path, max_jobs=1)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    runner = JobRunner(store, settings, api=FakeApi(11),
                       launcher=InThreadLauncher(fake_downloader_factory(b"y" * 11)),
                       dispatch_interval=0.02)
    runner.start()              # the dispatcher claims and runs the queued job
    assert wait_until(lambda: store.get_job(job.id).status == COMPLETED, timeout=5)
    runner.shutdown()
    store.close()


def test_runner_shutdown_sets_stopping_flag(tmp_path):
    # shutdown() must raise the stopping flag so in-flight workers know the
    # process is going away and leave their jobs resumable.
    settings = make_settings(tmp_path, max_jobs=1)
    store = JobStore(settings.db_path)
    runner = JobRunner(store, settings, api=FakeApi(1),
                       launcher=InThreadLauncher(fake_downloader_factory(b"z")))
    assert not runner._stopping.is_set()
    runner.shutdown()  # non-blocking by default
    assert runner._stopping.is_set()
    store.close()


def test_worker_fails_when_insufficient_disk(tmp_path, monkeypatch):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    # Pretend the filesystem is almost full (1 KB free); the repo "needs" 1 GB.
    monkeypatch.setattr("app.backup.free_disk_bytes", lambda path: 1000)
    called = []

    def downloader(**kwargs):
        called.append(True)

    run_backup_job(job.id, store, settings, api=FakeApi(1_000_000_000), launcher=InThreadLauncher(downloader))
    failed = store.get_job(job.id)
    assert failed.status == FAILED
    assert "disk space" in failed.error
    assert called == []  # download must NOT be attempted when it cannot fit
    store.close()


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


def test_runner_pause_sets_job_paused(tmp_path):
    settings = make_settings(tmp_path, max_jobs=1)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    started = threading.Event()
    runner = JobRunner(store, settings, api=FakeApi(1000),
                       launcher=InThreadLauncher(blocking_downloader_factory(started)),
                       dispatch_interval=0.02)
    runner.start()
    assert started.wait(3)
    runner.pause(job.id)
    assert wait_until(lambda: store.get_job(job.id).status == PAUSED)
    runner.shutdown()
    store.close()


class _OomHandle:
    exitcode = -9
    def terminate(self): pass
    def wait(self, timeout=None): return None   # killed, reported nothing


class _OomLauncher:
    def start(self, **kwargs):
        Path(kwargs["local_dir"]).mkdir(parents=True, exist_ok=True)
        return _OomHandle()


def test_worker_unexpected_exit_schedules_retry_when_budget_remains(tmp_path):
    # An 'outcome is None' exit (child killed, e.g. OOM) is treated as retryable:
    # with budget left it goes to RETRYING, not FAILED.
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    run_backup_job(job.id, store, settings, api=FakeApi(10), launcher=_OomLauncher(),
                   registry=None)
    j = store.get_job(job.id)
    assert j.status == RETRYING and j.retry_count == 1 and j.next_retry_at is not None
    store.close()


def test_worker_unexpected_exit_fails_when_retries_exhausted(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    for _ in range(MAX_RETRIES):
        store.schedule_retry(job.id, "x", 0)         # retry_count -> 5
    assert store.get_job(job.id).retry_count == MAX_RETRIES
    run_backup_job(job.id, store, settings, api=FakeApi(10), launcher=_OomLauncher(),
                   registry=None)
    assert store.get_job(job.id).status == FAILED
    store.close()


def test_runner_cancel_terminates_and_removes_job(tmp_path):
    settings = make_settings(tmp_path, max_jobs=1)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    started = threading.Event()
    runner = JobRunner(store, settings, api=FakeApi(1000),
                       launcher=InThreadLauncher(blocking_downloader_factory(started)),
                       dispatch_interval=0.02)
    runner.start()
    assert started.wait(3)
    assert runner.cancel(job.id) is True
    assert wait_until(lambda: store.get_job(job.id) is None)
    runner.shutdown()
    store.close()


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


def test_worker_stall_watchdog_requeues_as_retry_keeping_files(tmp_path):
    # A download child that is alive but writes no bytes for stall_timeout seconds
    # (a half-dead connection the downloader never timed out on) must be terminated
    # by the poller's watchdog and recorded as a transient failure -> RETRYING, so
    # the slot frees and the dispatcher re-attempts. Partial files are kept.
    settings = make_settings(tmp_path, stall_timeout=0.1)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    registry = RunningRegistry()
    started = threading.Event()
    t = threading.Thread(target=run_backup_job, kwargs=dict(
        job_id=job.id, store=store, settings=settings, api=FakeApi(1000),
        launcher=InThreadLauncher(blocking_downloader_factory(started)),
        registry=registry))
    t.start()
    assert started.wait(3)            # partial file written, download now idle
    t.join(8)                         # watchdog should fire and end the run
    j = store.get_job(job.id)
    assert j.status == RETRYING
    assert j.retry_count == 1
    assert j.next_retry_at is not None
    assert "stall" in (j.error or "")
    assert (tmp_path / "backups" / "models" / "o" / "n" / "partial.bin").exists()
    store.close()


def test_worker_does_not_stall_a_progressing_download(tmp_path):
    # A download that keeps writing bytes must never be killed by the watchdog,
    # even with a tiny stall_timeout: every write resets the no-progress timer.
    settings = make_settings(tmp_path, stall_timeout=0.2)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")

    def growing(*, local_dir, stop=None, **_):
        target = Path(local_dir)
        target.mkdir(parents=True, exist_ok=True)
        # ~1.2s of steady growth, each tick well under stall_timeout apart.
        for i in range(12):
            (target / f"part{i}.bin").write_bytes(b"x" * 1000)
            if stop is not None and stop.wait(0.1):
                return

    run_backup_job(job.id, store, settings, api=FakeApi(12000),
                   launcher=InThreadLauncher(growing), registry=RunningRegistry())
    j = store.get_job(job.id)
    assert j.status == COMPLETED      # progressed to the end, never stalled
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
    time.sleep(0.3)                                   # give the loop time to (not) over-dispatch
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


def test_worker_requeues_when_valve_closes_during_preflight(tmp_path):
    # Global-pause race: the valve closes after the dispatcher claimed this job
    # but before the worker registered its handle. request_all can't reach an
    # unregistered handle, so the worker must self-requeue from the valve flag
    # rather than download to completion.
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    store.set_flag("paused_all", "1")            # valve already closed at register time
    registry = RunningRegistry()
    started = threading.Event()
    run_backup_job(job.id, store, settings, api=FakeApi(1000),
                   launcher=InThreadLauncher(blocking_downloader_factory(started)),
                   registry=registry)
    j = store.get_job(job.id)
    assert j.status == QUEUED                     # requeued, not completed
    assert (tmp_path / "backups" / "models" / "o" / "n" / "partial.bin").exists()
    store.close()


def test_worker_auto_verifies_after_download(tmp_path):
    settings = make_settings(tmp_path, verify_downloads=True)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    files = {"config.json": b'{"a":1}', "model.bin": b"WEIGHTS-DATA"}
    run_backup_job(job.id, store, settings, api=VerifyApi(files),
                   launcher=InThreadLauncher(files_downloader_factory(files)),
                   registry=RunningRegistry())
    j = store.get_job(job.id)
    assert j.status == COMPLETED
    assert j.verify_status == "verified"
    assert j.verify_detail is None
    assert j.downloaded_bytes == j.total_bytes        # bar restored to 100%
    store.close()


def test_worker_auto_verify_detects_corruption(tmp_path):
    settings = make_settings(tmp_path, verify_downloads=True)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    truth = {"model.bin": b"GOOD-WEIGHTS"}            # what the Hub reports
    bad = {"model.bin": b"BADX-WEIGHTS"}              # what the downloader writes (same length)
    run_backup_job(job.id, store, settings, api=VerifyApi(truth),
                   launcher=InThreadLauncher(files_downloader_factory(bad)),
                   registry=RunningRegistry())
    j = store.get_job(job.id)
    assert j.status == COMPLETED
    assert j.verify_status == "corrupted"
    assert json.loads(j.verify_detail)["failures"] == [{"file": "model.bin", "reason": "mismatch"}]
    store.close()


def test_worker_skips_verify_when_disabled(tmp_path):
    settings = make_settings(tmp_path, verify_downloads=False)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    files = {"model.bin": b"WEIGHTS"}
    run_backup_job(job.id, store, settings, api=VerifyApi(files),
                   launcher=InThreadLauncher(files_downloader_factory(files)),
                   registry=RunningRegistry())
    j = store.get_job(job.id)
    assert j.status == COMPLETED
    assert j.verify_status == "unverified"            # never verified
    store.close()


def test_run_verify_job_marks_verified(tmp_path):
    settings = make_settings(tmp_path, verify_downloads=True)
    store = JobStore(settings.db_path)
    files = {"config.json": b'{"a":1}', "model.bin": b"WEIGHTS"}
    job = _write_completed_repo(store, settings, "o/n", files)
    run_verify_job(job.id, store, settings, api=VerifyApi(files), registry=RunningRegistry())
    j = store.get_job(job.id)
    assert j.status == COMPLETED and j.verify_status == "verified"
    store.close()


def test_run_verify_job_marks_corrupted(tmp_path):
    settings = make_settings(tmp_path, verify_downloads=True)
    store = JobStore(settings.db_path)
    truth = {"model.bin": b"GOOD-WEIGHTS"}
    job = _write_completed_repo(store, settings, "o/n", {"model.bin": b"BADX-WEIGHTS"})
    run_verify_job(job.id, store, settings, api=VerifyApi(truth), registry=RunningRegistry())
    j = store.get_job(job.id)
    assert j.status == COMPLETED and j.verify_status == "corrupted"
    store.close()


def test_verify_marks_unverifiable_when_hub_unreachable(tmp_path):
    settings = make_settings(tmp_path, verify_downloads=True)
    store = JobStore(settings.db_path)
    job = _write_completed_repo(store, settings, "o/n", {"model.bin": b"WEIGHTS"})

    class FailApi:
        def repo_info(self, *a, **k):
            raise RuntimeError("hub down")

    run_verify_job(job.id, store, settings, api=FailApi(), registry=RunningRegistry())
    j = store.get_job(job.id)
    assert j.status == COMPLETED
    assert j.verify_status == "unverified"            # cannot-verify != corrupted
    assert "could not reach" in json.loads(j.verify_detail)["note"]
    assert j.downloaded_bytes == j.total_bytes        # bar restored
    store.close()


def test_verify_aborts_on_preset_stop_intent(tmp_path):
    settings = make_settings(tmp_path, verify_downloads=True)
    store = JobStore(settings.db_path)
    files = {"model.bin": b"WEIGHTS"}
    job = _write_completed_repo(store, settings, "o/n", files)
    registry = RunningRegistry()
    registry._intents[job.id] = "stop_verify"          # stop requested before registration
    run_verify_job(job.id, store, settings, api=VerifyApi(files), registry=registry)
    j = store.get_job(job.id)
    assert j.status == COMPLETED
    assert j.verify_status == "unverified"             # interrupted -> inconclusive
    assert j.downloaded_bytes == j.total_bytes
    store.close()
