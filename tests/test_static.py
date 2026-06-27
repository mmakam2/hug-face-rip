import pytest
from fastapi.testclient import TestClient
from app.config import Settings
from app.db import JobStore
from app.main import create_app


@pytest.fixture
def client(tmp_path):
    settings = Settings(
        hf_token="hf_test",
        backup_dir=tmp_path / "backups",
        max_concurrent_jobs=2,
        max_workers=4,
        db_path=tmp_path / "jobs.db",
    )
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)

    class FakeRunner:
        def submit(self, job_id):
            pass

    app = create_app(settings, store, FakeRunner(), detect=lambda s, t: [])
    yield TestClient(app)
    store.close()


def test_index_served(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Hugging Face Rip" in resp.text
    assert "/api/jobs" in resp.text  # JS talks to the API


def test_repo_type_rendered_as_colored_badge(client):
    page = client.get("/").text
    assert 'class="badge ' in page   # row template tags each job with a typed badge
    for repo_type in ("model", "dataset", "space"):
        assert f".badge.{repo_type}" in page  # per-type color rule exists


def test_storage_bar_has_used_and_planned_segments(client):
    page = client.get("/").text
    assert "sused" in page            # current-usage segment
    assert ".splanned" in page        # planned (queued + running-remaining) segment
    assert ".splanned.over" in page   # overflow color rule when projected > capacity


def test_completed_rows_have_confirm_guarded_delete(client):
    page = client.get("/").text
    assert "confirmDelete" in page    # delete control wired into the row
    assert "confirm(" in page         # native confirmation before deleting


def test_running_and_paused_rows_expose_pause_resume_cancel(client):
    page = client.get("/").text
    assert ">Pause<" in page          # running rows can be paused
    assert ">Resume<" in page         # paused rows can be resumed
    assert "confirmCancel" in page    # cancel is confirm-guarded (now destroys data)
    assert ".st.paused" in page       # paused status has its own color rule


def test_global_pause_and_retrying_ui_present(client):
    page = client.get("/").text
    assert "pause-all" in page          # global pause endpoint wired
    assert "resume-all" in page         # global resume endpoint wired
    assert ".st.retrying" in page       # retrying status color rule
    assert "retryText" in page          # helper that renders "N/5 · next in …"


def test_verify_ui_present(client):
    page = client.get("/").text
    assert "verifyBadge" in page          # verified / corrupted / unverified badge helper
    assert "confirmRedownload" in page    # corrupted -> re-download control
    assert "stop-verify" in page          # stop an in-progress verification
    assert "/verify" in page or "'verify'" in page  # manual verify wired
    assert ".st.verifying" in page        # verifying status color rule
    assert ".vbadge" in page              # verify badge style
