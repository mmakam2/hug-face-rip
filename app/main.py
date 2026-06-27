import os
import re
import shutil
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from .backup import JobRunner, detect_repo_types
from .config import load_settings
from .db import FAILED, JobStore, QUEUED, RUNNING

SLUG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$")

STATIC_DIR = Path(__file__).parent / "static"


class SlugIn(BaseModel):
    slug: str


def create_app(settings, store, runner, detect=detect_repo_types) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app):
        for job in store.unfinished_jobs():
            runner.submit(job.id)
        yield

    app = FastAPI(title="HF Repo Backup", lifespan=lifespan)

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
                runner.submit(job.id)
            elif existing.status in (RUNNING, QUEUED):
                # Already downloading or pending — don't start a second
                # snapshot_download into the same directory.
                job = existing
            else:
                # completed / failed -> resume or retry
                store.requeue(existing.id)
                runner.submit(existing.id)
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
        }

    @app.post("/api/jobs/{job_id}/retry")
    def retry(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != FAILED:
            raise HTTPException(status_code=409, detail="only failed jobs can be retried")
        store.requeue(job_id)
        runner.submit(job_id)
        return store.get_job(job_id).to_dict()

    @app.post("/api/jobs/{job_id}/cancel")
    def cancel(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != QUEUED:
            raise HTTPException(status_code=409, detail="only queued jobs can be cancelled")
        store.delete_job(job_id)
        return {"deleted": job_id}

    return app


def build_default_app() -> FastAPI:
    from dotenv import load_dotenv

    load_dotenv()
    settings = load_settings()
    store = JobStore(settings.db_path)
    runner = JobRunner(store, settings)
    return create_app(settings, store, runner)


def server_host_port(env=None):
    """Resolve the server bind address. Defaults to all interfaces (0.0.0.0:8000);
    override with the HOST and PORT environment variables."""
    env = os.environ if env is None else env
    host = env.get("HOST") or "0.0.0.0"
    port = int(env.get("PORT") or "8000")
    return host, port


if __name__ == "__main__":
    import uvicorn

    host, port = server_host_port()
    uvicorn.run(build_default_app, host=host, port=port, factory=True)
