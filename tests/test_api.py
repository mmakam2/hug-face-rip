import pytest
from fastapi.testclient import TestClient
from app.config import Settings
from app.db import JobStore, FAILED, QUEUED, CANCELLED
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


@pytest.fixture
def ctx(tmp_path):
    settings = make_settings(tmp_path)
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)
    runner = FakeRunner()
    detect = lambda slug, token: ["model", "dataset"] if slug == "o/n" else []
    app = create_app(settings, store, runner, detect=detect)
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


def test_cancel_only_queued(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    resp = client.post(f"/api/jobs/{job.id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == CANCELLED
    store.set_status(job.id, "running")
    assert client.post(f"/api/jobs/{job.id}/cancel").status_code == 409


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
