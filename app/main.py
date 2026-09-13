import logging
import os
import re
import shutil
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from .backup import JobRunner, detect_repo_types, repo_total_bytes
from .config import load_settings
from .db import (
    COMPLETED, DELETING, FAILED, JobStore, PAUSED, QUEUED, RETRYING, RUNNING, VERIFYING,
)

logger = logging.getLogger(__name__)

SLUG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$")

STATIC_DIR = Path(__file__).parent / "static"


class SlugIn(BaseModel):
    slug: str


def create_app(settings, store, runner, detect=detect_repo_types, sizer=repo_total_bytes) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app):
        # Orphaned 'running' jobs (their processes died with the old interpreter)
        # go back to 'queued'; the dispatcher then drives everything per the valve.
        store.reset_running_to_queued()
        store.reset_verifying_to_completed()
        # A restart mid-rmtree leaves a 'deleting' row and a half-removed tree:
        # finish the job (rmtree is idempotent) instead of letting it linger.
        for job in store.deleting_jobs():
            runner.delete(job.id)
        runner.start()
        yield
        # On shutdown (e.g. systemd restart), stop the dispatcher and terminate
        # in-flight downloads; their jobs are left 'running' and the next startup
        # resets them to 'queued' for the dispatcher to resume.
        runner.shutdown()

    app = FastAPI(title="Hugging Face Rip", lifespan=lifespan)

    @app.middleware("http")
    async def log_actions_on_arrival(request, call_next):
        # uvicorn's access line is written only when the response is sent — and
        # skipped entirely if the client has gone away by then. A slow action
        # whose caller gave up (a browser abandoning a long delete) would leave
        # no trace, so log every state-changing request the moment it arrives.
        # GETs are the dashboard's 1.5s polling; uvicorn already logs those.
        if request.method != "GET":
            client = request.client.host if request.client else "-"
            logger.info("%s %s from %s", request.method, request.url.path, client)
        return await call_next(request)

    @app.get("/")
    def index():
        return FileResponse(STATIC_DIR / "index.html")

    @app.post("/api/jobs")
    def create_jobs(body: SlugIn):
        slug = body.slug.strip()
        if not slug:
            raise HTTPException(status_code=400, detail="slug is required")
        if not SLUG_RE.match(slug):
            raise HTTPException(status_code=400, detail="invalid slug; expected 'owner/name'")
        types = detect(slug, settings.hf_token)
        if not types:
            raise HTTPException(status_code=404, detail="repo not found or not accessible")
        created = []
        for repo_type in types:
            existing = store.get_job_by_repo(repo_type, slug)
            if existing is None:
                job = store.create_job(slug, repo_type)
                # Populate the size up front so the queued row shows its total
                # instead of 0. Best-effort: if the Hub lookup fails, queue the
                # job anyway and let run_backup_job compute the size when it runs.
                try:
                    total = sizer(slug, repo_type, settings.hf_token)
                except Exception:  # noqa: BLE001 - sizing must not block queuing
                    total = 0
                if total:
                    store.update_progress(job.id, 0, total_bytes=total)
                    job = store.get_job(job.id)
                # Left 'queued'; the dispatcher will start it.
            elif existing.status in (RUNNING, QUEUED, RETRYING, PAUSED, DELETING):
                # In progress / pending / paused / being deleted -> don't disturb.
                job = existing
            else:
                # completed / failed -> requeue with a fresh retry budget.
                store.requeue(existing.id)
                store.reset_retry(existing.id)
                job = store.get_job(existing.id)
            created.append(job.to_dict())
        return {"jobs": created}

    @app.get("/api/jobs")
    def list_jobs():
        return {"jobs": [job.to_dict() for job in store.list_jobs()]}

    @app.get("/api/storage")
    def storage():
        usage = shutil.disk_usage(settings.backup_dir)
        return {
            "path": str(settings.backup_dir),
            "total": usage.total,
            "used": usage.used,
            "free": usage.free,
            "planned": store.pending_bytes(),
            "paused_all": store.get_flag("paused_all", "0") == "1",
        }

    @app.post("/api/jobs/{job_id}/retry")
    def retry(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != FAILED:
            raise HTTPException(status_code=409, detail="only failed jobs can be retried")
        store.requeue(job_id)
        store.reset_retry(job_id)
        return store.get_job(job_id).to_dict()

    @app.post("/api/jobs/{job_id}/pause")
    def pause(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != RUNNING:
            raise HTTPException(status_code=409, detail="only running downloads can be paused")
        runner.pause(job_id)
        return {"pausing": job_id}

    @app.post("/api/jobs/{job_id}/resume")
    def resume(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != PAUSED:
            raise HTTPException(status_code=409, detail="only paused downloads can be resumed")
        store.requeue(job_id)
        return store.get_job(job_id).to_dict()

    @app.post("/api/jobs/{job_id}/cancel")
    def cancel(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status not in (QUEUED, RUNNING, PAUSED, RETRYING):
            raise HTTPException(
                status_code=409,
                detail="only queued, running, paused, or retrying jobs can be cancelled",
            )
        if job.status == RUNNING:
            # Hand off to the runner; once the child process dies (near-instant,
            # even mid-file) the worker starts the background delete.
            runner.cancel(job_id)
            return {"cancelling": job_id}
        # queued / paused / retrying: no live process — straight to the deleter.
        runner.delete(job_id)
        return {"deleting": job_id}

    @app.post("/api/jobs/{job_id}/verify")
    def verify(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != COMPLETED:
            raise HTTPException(status_code=409, detail="only completed downloads can be verified")
        runner.verify(job_id)
        return {"verifying": job_id}

    @app.post("/api/jobs/{job_id}/stop-verify")
    def stop_verify(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != VERIFYING:
            raise HTTPException(status_code=409, detail="only verifying jobs can be stopped")
        runner.stop_verify(job_id)
        return {"stopping": job_id}

    @app.post("/api/jobs/{job_id}/redownload")
    def redownload(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.verify_status != "corrupted":
            raise HTTPException(status_code=409, detail="only corrupted downloads can be re-downloaded")
        # Discard in the background, then the deleter requeues it for a fresh run.
        runner.delete(job_id, requeue=True)
        return {"deleting": job_id}

    @app.post("/api/pause-all")
    def pause_all():
        runner.pause_all()
        return {"paused_all": True}

    @app.post("/api/resume-all")
    def resume_all():
        runner.resume_all()
        return {"paused_all": False}

    @app.post("/api/jobs/{job_id}/delete")
    def delete(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != COMPLETED:
            raise HTTPException(status_code=409, detail="only completed downloads can be deleted")
        # Returns at once; the row shows 'deleting' (bar draining) until the
        # files are gone, then disappears. Unlinking hundreds of GB takes minutes.
        runner.delete(job_id)
        return {"deleting": job_id}

    return app


def build_default_app() -> FastAPI:
    from dotenv import load_dotenv

    load_dotenv()
    settings = load_settings()
    store = JobStore(settings.db_path)
    runner = JobRunner(store, settings)
    return create_app(settings, store, runner)


def configure_logging() -> None:
    """Send the app's INFO lines to stderr (the journal under systemd), in
    uvicorn's line style. uvicorn configures only its own loggers, so without
    this nothing below WARNING from `app.*` was ever visible. Idempotent, and
    scoped to the `app` logger so third-party libraries stay quiet."""
    app_logger = logging.getLogger("app")
    if any(getattr(h, "_hug_face_rip", False) for h in app_logger.handlers):
        return
    handler = logging.StreamHandler()
    handler._hug_face_rip = True
    handler.setFormatter(logging.Formatter("%(levelname)s:     %(message)s"))
    app_logger.addHandler(handler)
    app_logger.setLevel(logging.INFO)


def server_host_port(env=None):
    """Resolve the server bind address. Defaults to all interfaces (0.0.0.0:8000);
    override with the HOST and PORT environment variables."""
    env = os.environ if env is None else env
    host = env.get("HOST") or "0.0.0.0"
    port = int(env.get("PORT") or "8000")
    return host, port


if __name__ == "__main__":
    import uvicorn

    configure_logging()
    host, port = server_host_port()
    uvicorn.run(build_default_app, host=host, port=port, factory=True)
