import threading
from pathlib import Path
import pytest
from app.config import Settings
from app.db import JobStore, COMPLETED, FAILED, CANCELLED, RUNNING
from app.backup import run_backup_job, JobRunner


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
    def _download(repo_id, repo_type, local_dir, token, max_workers):
        target = Path(local_dir)
        target.mkdir(parents=True, exist_ok=True)
        (target / "model.bin").write_bytes(payload)
    return _download


def test_worker_completes_and_writes_files(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    payload = b"x" * 42
    run_backup_job(job.id, store, settings, api=FakeApi(42),
                   downloader=fake_downloader_factory(payload))
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

    run_backup_job(job.id, store, settings, api=FakeApi(10), downloader=boom)
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
                   downloader=boom, stopping=stopping)
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
                   downloader=boom, stopping=stopping)
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

    run_backup_job(job.id, store, settings, api=FakeApi(10), downloader=downloader)
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

    run_backup_job(job.id, store, settings, api=FailingApi(), downloader=downloader)
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
                   downloader=fake_downloader_factory(b"x"))
    failed = store.get_job(job.id)
    assert failed.status == FAILED
    assert failed.error
    store.close()


def test_runner_runs_job_to_completion(tmp_path):
    settings = make_settings(tmp_path, max_jobs=1)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    runner = JobRunner(store, settings, api=FakeApi(11),
                       downloader=fake_downloader_factory(b"y" * 11))
    runner.submit(job.id)
    runner.shutdown(wait=True)  # drain: waits for completion
    assert store.get_job(job.id).status == COMPLETED
    store.close()


def test_runner_shutdown_sets_stopping_flag(tmp_path):
    # shutdown() must raise the stopping flag so in-flight workers know the
    # process is going away and leave their jobs resumable.
    settings = make_settings(tmp_path, max_jobs=1)
    store = JobStore(settings.db_path)
    runner = JobRunner(store, settings, api=FakeApi(1),
                       downloader=fake_downloader_factory(b"z"))
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

    run_backup_job(job.id, store, settings, api=FakeApi(1_000_000_000), downloader=downloader)
    failed = store.get_job(job.id)
    assert failed.status == FAILED
    assert "disk space" in failed.error
    assert called == []  # download must NOT be attempted when it cannot fit
    store.close()
