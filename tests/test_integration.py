import os
import threading
import time
import pytest
from app.config import Settings
from app.db import JobStore, COMPLETED, PAUSED
from app.backup import run_backup_job, JobRunner

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
