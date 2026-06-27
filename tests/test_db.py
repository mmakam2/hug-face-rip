import sqlite3
import pytest
from app.db import JobStore, Job, QUEUED, RUNNING, COMPLETED, FAILED, PAUSED


@pytest.fixture
def store(tmp_path):
    s = JobStore(tmp_path / "jobs.db")
    yield s
    s.close()


def test_create_and_get(store):
    job = store.create_job("owner/name", "model")
    assert job.id > 0
    assert job.status == QUEUED
    assert job.total_bytes == 0 and job.downloaded_bytes == 0
    assert store.get_job(job.id).slug == "owner/name"


def test_unique_repo_type_slug(store):
    store.create_job("owner/name", "model")
    with pytest.raises(sqlite3.IntegrityError):
        store.create_job("owner/name", "model")
    # same slug, different type is allowed
    other = store.create_job("owner/name", "dataset")
    assert other.id > 0


def test_get_job_by_repo(store):
    store.create_job("owner/name", "model")
    assert store.get_job_by_repo("model", "owner/name") is not None
    assert store.get_job_by_repo("dataset", "owner/name") is None


def test_update_progress_and_percent(store):
    job = store.create_job("a/b", "model")
    store.update_progress(job.id, downloaded_bytes=50, total_bytes=200)
    refreshed = store.get_job(job.id)
    assert refreshed.downloaded_bytes == 50
    assert refreshed.total_bytes == 200
    assert refreshed.percent == 25.0
    # downloaded-only update keeps total
    store.update_progress(job.id, downloaded_bytes=100)
    assert store.get_job(job.id).total_bytes == 200


def test_percent_zero_total(store):
    job = store.create_job("a/b", "model")
    assert store.get_job(job.id).percent == 0.0


def test_set_status_and_error(store):
    job = store.create_job("a/b", "model")
    store.set_status(job.id, FAILED, error="boom")
    j = store.get_job(job.id)
    assert j.status == FAILED and j.error == "boom"
    store.set_status(job.id, COMPLETED)   # clears error
    assert store.get_job(job.id).error is None


def test_requeue_clears_error_keeps_progress(store):
    job = store.create_job("a/b", "model")
    store.update_progress(job.id, 80, 100)
    store.set_status(job.id, FAILED, error="x")
    store.requeue(job.id)
    j = store.get_job(job.id)
    assert j.status == QUEUED and j.error is None and j.downloaded_bytes == 80


def test_unfinished_jobs(store):
    a = store.create_job("a/b", "model")             # queued
    b = store.create_job("c/d", "dataset")
    store.set_status(b.id, RUNNING)
    c = store.create_job("e/f", "space")
    store.set_status(c.id, COMPLETED)
    ids = {j.id for j in store.unfinished_jobs()}
    assert ids == {a.id, b.id}


def test_list_jobs_orders_earliest_added_first(store):
    a = store.create_job("a/b", "model")
    b = store.create_job("c/d", "dataset")
    c = store.create_job("e/f", "space")
    ids = [j.id for j in store.list_jobs()]
    assert ids == [a.id, b.id, c.id]   # earliest added at top, latest at bottom


def test_to_dict_includes_percent(store):
    job = store.create_job("a/b", "model")
    store.update_progress(job.id, 1, 4)
    d = store.get_job(job.id).to_dict()
    assert d["percent"] == 25.0 and d["slug"] == "a/b"


def test_pending_bytes_sums_remaining_of_running_and_queued(store):
    a = store.create_job("a/b", "model")          # queued
    store.update_progress(a.id, 0, 100)            # remaining 100
    b = store.create_job("c/d", "model")           # running
    store.set_status(b.id, RUNNING)
    store.update_progress(b.id, 30, 100)           # remaining 70
    done = store.create_job("e/f", "model")        # completed -> excluded
    store.set_status(done.id, COMPLETED)
    store.update_progress(done.id, 100, 100)
    over = store.create_job("g/h", "model")        # downloaded > total -> clamps to 0
    store.update_progress(over.id, 120, 100)
    assert store.pending_bytes() == 170            # 100 + 70 + 0


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
