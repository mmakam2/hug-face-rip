from pathlib import Path
import pytest
from app.config import Settings
from app.db import JobStore, COMPLETED, FAILED, CANCELLED
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


def test_runner_runs_job_to_completion(tmp_path):
    settings = make_settings(tmp_path, max_jobs=1)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    runner = JobRunner(store, settings, api=FakeApi(11),
                       downloader=fake_downloader_factory(b"y" * 11))
    runner.submit(job.id)
    runner.shutdown()  # waits for completion
    assert store.get_job(job.id).status == COMPLETED
    store.close()
