import sqlite3
import pytest
from app.db import JobStore, Job, QUEUED, RUNNING, COMPLETED, FAILED, PAUSED, RETRYING


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
