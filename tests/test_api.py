import pytest
from fastapi.testclient import TestClient
from app.config import Settings
from app.db import JobStore, FAILED, QUEUED, PAUSED, RUNNING
from app.main import create_app


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


def make_settings(tmp_path):
    return Settings(
        hf_token="hf_test",
        backup_dir=tmp_path / "backups",
        max_concurrent_jobs=2,
        max_workers=4,
        db_path=tmp_path / "jobs.db",
    )


FAKE_SIZE = 4242  # bytes the fake sizer reports for queued jobs


@pytest.fixture
def ctx(tmp_path):
    settings = make_settings(tmp_path)
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)
    runner = FakeRunner()
    detect = lambda slug, token: ["model", "dataset"] if slug == "o/n" else []
    sizer = lambda slug, repo_type, token: FAKE_SIZE
    app = create_app(settings, store, runner, detect=detect, sizer=sizer)
    client = TestClient(app)
    yield client, store, runner
    store.close()


def test_create_jobs_makes_one_per_detected_type(ctx):
    client, store, runner = ctx
    resp = client.post("/api/jobs", json={"slug": "o/n"})
    assert resp.status_code == 200
    jobs = resp.json()["jobs"]
    assert {j["repo_type"] for j in jobs} == {"model", "dataset"}
    assert all(j["status"] == QUEUED for j in jobs)
    assert len(runner.submitted) == 2


def test_queued_job_is_populated_with_its_size(ctx):
    client, store, runner = ctx
    client.post("/api/jobs", json={"slug": "o/n"})   # detect -> model + dataset, both queued
    jobs = store.list_jobs()
    assert all(j.status == QUEUED for j in jobs)
    assert all(j.total_bytes == FAKE_SIZE for j in jobs)   # size shown before it runs
    assert all(j.downloaded_bytes == 0 for j in jobs)      # nothing downloaded yet


def test_size_lookup_failure_still_queues_job_at_zero(tmp_path):
    settings = make_settings(tmp_path)
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)
    runner = FakeRunner()

    def boom(slug, repo_type, token):
        raise RuntimeError("hub unreachable")

    app = create_app(settings, store, runner, detect=lambda s, t: ["model"], sizer=boom)
    client = TestClient(app)
    resp = client.post("/api/jobs", json={"slug": "x/y"})
    assert resp.status_code == 200
    job = store.list_jobs()[0]
    assert job.status == QUEUED
    assert job.total_bytes == 0           # best-effort: failed sizing leaves 0
    assert job.id in runner.submitted     # job still queued to run
    store.close()


def test_create_unknown_slug_404(ctx):
    client, store, runner = ctx
    resp = client.post("/api/jobs", json={"slug": "ghost/x"})
    assert resp.status_code == 404
    assert runner.submitted == []


def test_create_blank_slug_400(ctx):
    client, store, runner = ctx
    resp = client.post("/api/jobs", json={"slug": "   "})
    assert resp.status_code == 400


def test_resubmit_existing_repo_requeues_not_duplicates(ctx):
    client, store, runner = ctx
    client.post("/api/jobs", json={"slug": "o/n"})
    client.post("/api/jobs", json={"slug": "o/n"})
    assert len(store.list_jobs()) == 2  # still just model + dataset


def test_list_jobs(ctx):
    client, store, runner = ctx
    client.post("/api/jobs", json={"slug": "o/n"})
    body = client.get("/api/jobs").json()
    assert len(body["jobs"]) == 2
    assert "percent" in body["jobs"][0]


def test_retry_only_failed(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    # queued job cannot be retried
    assert client.post(f"/api/jobs/{job.id}/retry").status_code == 409
    store.set_status(job.id, FAILED, error="x")
    resp = client.post(f"/api/jobs/{job.id}/retry")
    assert resp.status_code == 200
    assert resp.json()["status"] == QUEUED
    assert job.id in runner.submitted


def test_cancel_removes_queued_job_from_the_list(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    resp = client.post(f"/api/jobs/{job.id}/cancel")
    assert resp.status_code == 200
    assert store.get_job(job.id) is None                  # deleted, not just flagged
    assert client.get("/api/jobs").json()["jobs"] == []   # leaves the list


def test_cancel_running_job_terminates_it(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    store.set_status(job.id, RUNNING)
    resp = client.post(f"/api/jobs/{job.id}/cancel")
    assert resp.status_code == 200
    assert job.id in runner.cancelled        # handed to the runner to terminate


def test_readd_after_cancel_does_not_resurrect_cancelled_instances(ctx):
    client, store, runner = ctx
    client.post("/api/jobs", json={"slug": "o/n"})        # model + dataset, queued
    for j in store.list_jobs():
        client.post(f"/api/jobs/{j.id}/cancel")            # cancel both
    assert store.list_jobs() == []                         # all gone
    runner.submitted.clear()
    client.post("/api/jobs", json={"slug": "o/n"})        # re-add the same slug
    jobs = store.list_jobs()
    assert {j.repo_type for j in jobs} == {"model", "dataset"}  # fresh pair
    assert all(j.status == QUEUED for j in jobs)           # not resurrected from cancelled


def test_retry_missing_job_404(ctx):
    client, store, runner = ctx
    assert client.post("/api/jobs/999/retry").status_code == 404


@pytest.mark.parametrize("bad", ["notaslug", "a b/c", "../../etc/passwd", "/etc/passwd", "too/many/slashes", "owner/"])
def test_create_malformed_slug_returns_400(ctx, bad):
    client, store, runner = ctx
    assert client.post("/api/jobs", json={"slug": bad}).status_code == 400
    assert runner.submitted == []


def test_resubmit_running_job_does_not_double_run(ctx):
    client, store, runner = ctx
    client.post("/api/jobs", json={"slug": "o/n"})
    jobs = store.list_jobs()
    for j in jobs:
        store.set_status(j.id, "running")
    runner.submitted.clear()
    resp = client.post("/api/jobs", json={"slug": "o/n"})
    assert resp.status_code == 200
    assert runner.submitted == []                 # nothing re-submitted while running
    assert len(store.list_jobs()) == len(jobs)    # no duplicate jobs


def test_cancel_missing_job_404(ctx):
    client, store, runner = ctx
    assert client.post("/api/jobs/999/cancel").status_code == 404


def test_storage_reports_backup_disk_usage(ctx):
    client, store, runner = ctx
    body = client.get("/api/storage").json()
    assert body["total"] > 0
    assert body["used"] >= 0
    assert body["free"] >= 0
    assert body["free"] <= body["total"]
    assert body["used"] <= body["total"]


def test_storage_reports_planned_bytes(ctx):
    client, store, runner = ctx
    a = store.create_job("a/b", "model")
    store.update_progress(a.id, 0, 100)            # queued, remaining 100
    b = store.create_job("c/d", "model")
    store.set_status(b.id, "running")
    store.update_progress(b.id, 40, 100)           # running, remaining 60
    done = store.create_job("e/f", "model")
    store.set_status(done.id, "completed")
    store.update_progress(done.id, 50, 50)         # completed, excluded
    assert client.get("/api/storage").json()["planned"] == 160   # 100 + 60


def test_delete_completed_job_removes_files_and_row(ctx, tmp_path):
    client, store, runner = ctx
    from app.backup import local_dir_for
    backup = tmp_path / "backups"                       # ctx's settings.backup_dir
    job = store.create_job("o/n", "model")
    d = local_dir_for(backup, "model", "o/n")
    d.mkdir(parents=True)
    (d / "f.bin").write_bytes(b"x" * 10)
    store.set_status(job.id, "completed")
    resp = client.post(f"/api/jobs/{job.id}/delete")
    assert resp.status_code == 200
    assert store.get_job(job.id) is None                # row gone
    assert not d.exists()                                # files gone


def test_delete_rejects_non_completed_job(ctx, tmp_path):
    client, store, runner = ctx
    from app.backup import local_dir_for
    backup = tmp_path / "backups"
    job = store.create_job("o/n", "model")
    store.set_status(job.id, "running")                 # a live download
    d = local_dir_for(backup, "model", "o/n")
    d.mkdir(parents=True)
    (d / "f.bin").write_bytes(b"x")
    resp = client.post(f"/api/jobs/{job.id}/delete")
    assert resp.status_code == 409
    assert store.get_job(job.id) is not None             # still there
    assert d.exists()                                    # files untouched


def test_delete_missing_job_404(ctx):
    client, store, runner = ctx
    assert client.post("/api/jobs/999/delete").status_code == 404


def test_startup_resumes_unfinished_jobs(tmp_path):
    settings = make_settings(tmp_path)
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)
    leftover = store.create_job("resume/me", "model")  # queued
    runner = FakeRunner()
    app = create_app(settings, store, runner, detect=lambda s, t: [])
    with TestClient(app):  # triggers startup
        pass
    assert leftover.id in runner.submitted
    store.close()


def test_lifespan_shuts_down_runner_on_exit(tmp_path):
    # On shutdown the lifespan must call runner.shutdown() so in-flight/queued
    # jobs are left resumable instead of crashing into 'failed' during teardown.
    settings = make_settings(tmp_path)
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)
    runner = FakeRunner()
    app = create_app(settings, store, runner, detect=lambda s, t: [])
    with TestClient(app):  # triggers startup
        pass
    # exiting the context triggers lifespan shutdown
    assert runner.shutdowns == 1
    store.close()


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
