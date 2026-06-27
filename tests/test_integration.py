import os
import time
from pathlib import Path
import pytest
from app.config import Settings
from app.db import JobStore, COMPLETED, PAUSED, RETRYING
from app.backup import run_backup_job, JobRunner
from app.launcher import Outcome

TINY_PUBLIC_MODEL = "hf-internal-testing/tiny-random-gpt2"


@pytest.mark.integration
def test_backup_tiny_public_repo_end_to_end(tmp_path):
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

    run_backup_job(job.id, store, settings)  # real HfApi + real snapshot_download

    done = store.get_job(job.id)
    assert done.status == COMPLETED, done.error
    assert done.downloaded_bytes > 0
    target = tmp_path / "backups" / "models" / "hf-internal-testing" / "tiny-random-gpt2"
    assert target.exists() and any(target.iterdir())
    store.close()


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

    # Real subprocess launcher (default), real snapshot_download; the dispatcher
    # claims the queued job and starts it.
    runner = JobRunner(store, settings, dispatch_interval=0.1)
    runner.start()
    assert _wait_until(lambda: store.get_job(job.id).status in ("running", "completed", PAUSED))

    # If it is still running, pause it and confirm it parks in 'paused'.
    if store.get_job(job.id).status == "running":
        runner.pause(job.id)
        assert _wait_until(lambda: store.get_job(job.id).status == PAUSED)

    # Resume (re-queue); the dispatcher re-runs it to completion.
    store.requeue(job.id)
    assert _wait_until(lambda: store.get_job(job.id).status == COMPLETED, timeout=120)

    runner.shutdown()
    done = store.get_job(job.id)
    assert done.status == COMPLETED, done.error
    target = tmp_path / "backups" / "models" / "hf-internal-testing" / "tiny-random-gpt2"
    assert target.exists() and any(target.iterdir())
    store.close()


class _FlakyHandle:
    def __init__(self, fail):
        self._fail = fail
    def terminate(self): pass
    @property
    def exitcode(self): return 0
    def wait(self, timeout=None):
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
        Path(kwargs["local_dir"]).mkdir(parents=True, exist_ok=True)
        return _FlakyHandle(fail=(self._starts == 1))


@pytest.mark.integration
def test_transient_failure_auto_retries_to_completion(tmp_path, monkeypatch):
    settings = Settings(
        hf_token="hf_test", backup_dir=tmp_path / "backups",
        max_concurrent_jobs=1, max_workers=2, db_path=tmp_path / "jobs.db",
    )
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    # Make the backoff instant so the test doesn't wait 30s.
    monkeypatch.setattr("app.backup.BACKOFF_SECONDS", [0, 0, 0, 0, 0])

    class _Sib:
        size = 10
    class _Info:
        siblings = [_Sib()]
    class _Api:
        def repo_info(self, **k):
            return _Info()

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
