import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Optional

from huggingface_hub import HfApi
from huggingface_hub.utils import (
    GatedRepoError,
    HfHubHTTPError,
    RepositoryNotFoundError,
)

REPO_TYPES = ["model", "dataset", "space"]


def detect_repo_types(slug: str, token: str, api: Optional[HfApi] = None) -> List[str]:
    api = api or HfApi()
    found: List[str] = []
    for repo_type in REPO_TYPES:
        try:
            api.repo_info(repo_id=slug, repo_type=repo_type, token=token or None)
            found.append(repo_type)
        except (RepositoryNotFoundError, GatedRepoError, HfHubHTTPError, ValueError):
            continue
    return found


def repo_total_bytes(slug: str, repo_type: str, token: str, api: Optional[HfApi] = None) -> int:
    api = api or HfApi()
    info = api.repo_info(
        repo_id=slug, repo_type=repo_type, token=token or None, files_metadata=True
    )
    return sum((sibling.size or 0) for sibling in (info.siblings or []))


def local_dir_for(backup_dir: Path, repo_type: str, slug: str) -> Path:
    return Path(backup_dir) / f"{repo_type}s" / slug


def directory_size(path: Path) -> int:
    path = Path(path)
    if not path.exists():
        return 0
    total = 0
    for item in path.rglob("*"):
        if item.is_file() and ".cache" not in item.relative_to(path).parts:
            total += item.stat().st_size
    return total


POLL_INTERVAL = 1.5


def run_backup_job(job_id, store, settings, api=None, downloader=None) -> None:
    if downloader is None:
        from huggingface_hub import snapshot_download
        downloader = snapshot_download

    job = store.get_job(job_id)
    if job is None or job.status == "cancelled":
        return

    store.set_status(job_id, "running")
    local_dir = local_dir_for(settings.backup_dir, job.repo_type, job.slug)

    stop = threading.Event()

    def _poll():
        while not stop.is_set():
            store.update_progress(job_id, directory_size(local_dir))
            stop.wait(POLL_INTERVAL)

    poller = threading.Thread(target=_poll, daemon=True)
    try:
        backup_root = settings.backup_dir.resolve()
        if not local_dir.resolve().is_relative_to(backup_root):
            raise ValueError(f"refusing to write outside backup dir: {local_dir}")
        local_dir.mkdir(parents=True, exist_ok=True)
        total = repo_total_bytes(job.slug, job.repo_type, settings.hf_token, api=api)
        store.update_progress(job_id, directory_size(local_dir), total_bytes=total)
        poller.start()
        downloader(
            repo_id=job.slug,
            repo_type=job.repo_type,
            local_dir=str(local_dir),
            token=settings.hf_token or None,
            max_workers=settings.max_workers,
        )
        stop.set()
        poller.join(timeout=2)
        final = directory_size(local_dir)
        store.update_progress(job_id, total if total else final, total_bytes=total)
        store.set_status(job_id, "completed")
    except Exception as exc:  # noqa: BLE001 - surface any failure on the job
        stop.set()
        if poller.is_alive():
            poller.join(timeout=2)
        store.set_status(job_id, "failed", error=str(exc)[:500])


class JobRunner:
    def __init__(self, store, settings, api=None, downloader=None) -> None:
        self._store = store
        self._settings = settings
        self._api = api
        self._downloader = downloader
        self._executor = ThreadPoolExecutor(max_workers=settings.max_concurrent_jobs)

    def submit(self, job_id) -> None:
        self._executor.submit(
            run_backup_job, job_id, self._store, self._settings, self._api, self._downloader
        )

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True)
