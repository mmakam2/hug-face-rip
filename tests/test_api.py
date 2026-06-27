import pytest
from fastapi.testclient import TestClient
from app.config import Settings
from app.db import JobStore, FAILED, QUEUED
from app.main import create_app


class FakeRunner:
    def __init__(self):
        self.submitted = []

    def submit(self, job_id):
        self.submitted.append(job_id)


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


def test_cancel_running_job_is_rejected(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    store.set_status(job.id, "running")
    assert client.post(f"/api/jobs/{job.id}/cancel").status_code == 409


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
