import pytest
from fastapi.testclient import TestClient
from app.config import Settings
from app.db import JobStore, FAILED, QUEUED, PAUSED, RUNNING, RETRYING, COMPLETED, VERIFYING
from app.main import create_app


class FakeRunner:
    def __init__(self):
        self.started = False
        self.paused = []
        self.cancelled = []
        self.paused_all = 0
        self.resumed_all = 0
        self.shutdowns = 0
        self.verified = []
        self.stop_verified = []

    def start(self):
        self.started = True

    def pause(self, job_id):
        self.paused.append(job_id)

    def cancel(self, job_id):
        self.cancelled.append(job_id)
        return True

    def verify(self, job_id):
        self.verified.append(job_id)

    def stop_verify(self, job_id):
        self.stop_verified.append(job_id)

    def pause_all(self):
        self.paused_all += 1

    def resume_all(self):
        self.resumed_all += 1

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
    assert all(j["status"] == QUEUED for j in jobs)   # queued; the dispatcher starts them


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
    assert job.status == QUEUED            # queued for the dispatcher
    assert job.total_bytes == 0            # best-effort: failed sizing leaves 0
    store.close()


def test_create_unknown_slug_404(ctx):
    client, store, runner = ctx
    resp = client.post("/api/jobs", json={"slug": "ghost/x"})
    assert resp.status_code == 404
    assert store.list_jobs() == []


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
    assert store.get_job(job.id).status == QUEUED   # the dispatcher will pick it up


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
    assert store.list_jobs() == []


def test_resubmit_running_job_does_not_double_run(ctx):
    client, store, runner = ctx
    client.post("/api/jobs", json={"slug": "o/n"})
    jobs = store.list_jobs()
    for j in jobs:
        store.set_status(j.id, "running")
    resp = client.post("/api/jobs", json={"slug": "o/n"})
    assert resp.status_code == 200
    assert len(store.list_jobs()) == len(jobs)    # no duplicate jobs while running


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


def test_startup_resets_running_and_starts_dispatcher(tmp_path):
    settings = make_settings(tmp_path)
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)
    leftover = store.create_job("resume/me", "model")
    store.set_status(leftover.id, RUNNING)             # orphaned from a prior run
    runner = FakeRunner()
    app = create_app(settings, store, runner, detect=lambda s, t: [])
    with TestClient(app):
        pass
    assert runner.started is True
    assert store.get_job(leftover.id).status == QUEUED  # reset for the dispatcher
    store.close()


def test_lifespan_shuts_down_runner_on_exit(tmp_path):
    # On shutdown the lifespan must call runner.shutdown() so in-flight downloads
    # are terminated and left resumable instead of crashing into 'failed'.
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
    assert store.get_job(job.id).status == QUEUED   # dispatcher will resume it


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
    assert store.get_job(paused.id).status == PAUSED   # untouched by startup reset
    store.close()


def test_pause_all_and_resume_all(ctx):
    client, store, runner = ctx
    assert client.post("/api/pause-all").status_code == 200
    assert runner.paused_all == 1
    assert client.post("/api/resume-all").status_code == 200
    assert runner.resumed_all == 1


def test_storage_reports_paused_all_flag(ctx):
    client, store, runner = ctx
    assert client.get("/api/storage").json()["paused_all"] is False
    store.set_flag("paused_all", "1")
    assert client.get("/api/storage").json()["paused_all"] is True


def test_cancel_retrying_deletes_files_and_row(ctx, tmp_path):
    client, store, runner = ctx
    from app.backup import local_dir_for
    backup = tmp_path / "backups"
    job = store.create_job("o/n", "model")
    d = local_dir_for(backup, "model", "o/n")
    d.mkdir(parents=True)
    (d / "partial.bin").write_bytes(b"x" * 10)
    store.schedule_retry(job.id, "blip", 30)            # status retrying
    resp = client.post(f"/api/jobs/{job.id}/cancel")
    assert resp.status_code == 200
    assert store.get_job(job.id) is None
    assert not d.exists()
    assert runner.cancelled == []                        # no live process during backoff


def test_retry_resets_retry_count(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    store.schedule_retry(job.id, "blip", 30)            # retry_count -> 1, status retrying
    store.set_status(job.id, FAILED)                     # exhausted -> failed
    resp = client.post(f"/api/jobs/{job.id}/retry")
    assert resp.status_code == 200
    j = store.get_job(job.id)
    assert j.status == QUEUED and j.retry_count == 0


def test_verify_only_completed(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    assert client.post(f"/api/jobs/{job.id}/verify").status_code == 409   # queued
    store.set_status(job.id, COMPLETED)
    resp = client.post(f"/api/jobs/{job.id}/verify")
    assert resp.status_code == 200
    assert job.id in runner.verified


def test_verify_missing_job_404(ctx):
    client, store, runner = ctx
    assert client.post("/api/jobs/999/verify").status_code == 404


def test_stop_verify_only_verifying(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    store.set_status(job.id, COMPLETED)
    assert client.post(f"/api/jobs/{job.id}/stop-verify").status_code == 409
    store.set_status(job.id, VERIFYING)
    resp = client.post(f"/api/jobs/{job.id}/stop-verify")
    assert resp.status_code == 200
    assert job.id in runner.stop_verified


def test_redownload_only_corrupted_deletes_and_requeues(ctx, tmp_path):
    client, store, runner = ctx
    from app.backup import local_dir_for
    backup = tmp_path / "backups"
    job = store.create_job("o/n", "model")
    d = local_dir_for(backup, "model", "o/n")
    d.mkdir(parents=True)
    (d / "model.bin").write_bytes(b"corrupt")
    store.set_status(job.id, COMPLETED)
    assert client.post(f"/api/jobs/{job.id}/redownload").status_code == 409   # not corrupted
    store.set_verify_status(job.id, "corrupted", detail='{"failures": []}')
    resp = client.post(f"/api/jobs/{job.id}/redownload")
    assert resp.status_code == 200
    j = store.get_job(job.id)
    assert j.status == QUEUED
    assert j.verify_status == "unverified"
    assert j.verify_detail is None
    assert not d.exists()                          # corrupt files discarded


def test_redownload_missing_job_404(ctx):
    client, store, runner = ctx
    assert client.post("/api/jobs/999/redownload").status_code == 404


def test_verify_fields_in_list(ctx):
    client, store, runner = ctx
    store.create_job("a/b", "model")
    j = client.get("/api/jobs").json()["jobs"][0]
    assert j["verify_status"] == "unverified"
    assert j["verify_detail"] is None


def test_startup_resets_orphaned_verifying(tmp_path):
    settings = make_settings(tmp_path)
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)
    j = store.create_job("v/me", "model")
    store.set_status(j.id, VERIFYING)
    store.update_progress(j.id, 3, total_bytes=10)
    runner = FakeRunner()
    app = create_app(settings, store, runner, detect=lambda s, t: [])
    with TestClient(app):
        pass
    g = store.get_job(j.id)
    assert g.status == COMPLETED and g.verify_status == "unverified"
    assert g.downloaded_bytes == 10
    store.close()
