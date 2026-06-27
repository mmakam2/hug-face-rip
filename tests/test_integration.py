import os
import pytest
from app.config import Settings
from app.db import JobStore, COMPLETED
from app.backup import run_backup_job

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
