import shutil
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

from .db import PAUSED

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


def delete_backup_files(backup_dir, repo_type: str, slug: str) -> None:
    """Delete a backup's downloaded files from disk.

    Refuses to touch anything that is not strictly inside ``backup_dir`` (and the
    backup root itself), so a bad repo_type/slug can never rmtree outside it.
    A missing directory is a no-op.
    """
    root = Path(backup_dir).resolve()
    target = local_dir_for(backup_dir, repo_type, slug).resolve()
    if target == root or not target.is_relative_to(root):
        raise ValueError(f"refusing to delete outside backup dir: {target}")
    if target.exists():
        shutil.rmtree(target)


def directory_size(path: Path) -> int:
    path = Path(path)
    if not path.exists():
        return 0
    total = 0
    for item in path.rglob("*"):
        if not item.is_file():
            continue
        in_cache = ".cache" in item.relative_to(path).parts
        # Count completed files, plus Xet's in-flight .incomplete staging which
        # lives under .cache/huggingface/download/ — those bytes are on disk and
        # would otherwise make progress look frozen until files finalize.
        if not in_cache or item.suffix == ".incomplete":
            total += item.stat().st_size
    return total


def free_disk_bytes(path) -> int:
    """Free bytes on the filesystem that holds ``path``.

    The target directory may not exist yet, so walk up to the nearest existing
    ancestor before asking the OS.
    """
    p = Path(path)
    while not p.exists() and p.parent != p:
        p = p.parent
    return shutil.disk_usage(p).free


POLL_INTERVAL = 1.5


class RunningRegistry:
    """Tracks the live download handle and stop intent for each running job.

    Endpoints reach a running download only through here. The worker registers
    its handle after start and unregisters in a finally; unregister clears the
    intent too, so a paused-then-resumed job (same job_id) never inherits a stale
    'pause' from its previous run.
    """
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._handles = {}
        self._intents = {}

    def register(self, job_id, handle) -> None:
        with self._lock:
            self._handles[job_id] = handle

    def unregister(self, job_id) -> None:
        with self._lock:
            self._handles.pop(job_id, None)
            self._intents.pop(job_id, None)

    def intent(self, job_id):
        with self._lock:
            return self._intents.get(job_id)

    def request(self, job_id, intent) -> bool:
        """Record a stop intent; terminate the live handle if one is registered.
        Returns True if a handle was terminated (False if none was running yet —
        the worker will honor the recorded intent once it registers)."""
        with self._lock:
            self._intents[job_id] = intent
            handle = self._handles.get(job_id)
        if handle is not None:
            handle.terminate()
            return True
        return False

    def terminate_all(self) -> None:
        with self._lock:
            handles = list(self._handles.values())
        for handle in handles:
            handle.terminate()


def run_backup_job(job_id, store, settings, api=None, launcher=None,
                   stopping=None, registry=None) -> None:
    if launcher is None:
        from .launcher import SubprocessLauncher
        launcher = SubprocessLauncher()

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
        already = directory_size(local_dir)
        store.update_progress(job_id, already, total_bytes=total)

        # Pre-flight: refuse a download that cannot physically fit, instead of
        # filling the disk / exhausting memory and getting OOM-killed mid-run.
        free = free_disk_bytes(settings.backup_dir)
        remaining = total - already
        if total and remaining > free:
            raise RuntimeError(
                f"not enough disk space for {job.slug}: needs ~{remaining / 1e9:.1f} GB "
                f"more, only {free / 1e9:.1f} GB free in {settings.backup_dir}"
            )

        poller.start()
        handle = launcher.start(
            repo_id=job.slug,
            repo_type=job.repo_type,
            local_dir=str(local_dir),
            token=settings.hf_token or None,
            max_workers=settings.max_workers,
        )
        if registry is not None:
            registry.register(job_id, handle)
            # Close the race where pause/cancel landed before registration.
            if registry.intent(job_id) is not None:
                handle.terminate()

        outcome = handle.wait()
        stop.set()
        poller.join(timeout=2)

        intent = registry.intent(job_id) if registry is not None else None

        if outcome is not None and outcome.ok:
            # Finished before any stop signal landed -> completion wins.
            final = directory_size(local_dir)
            store.update_progress(job_id, total if total else final, total_bytes=total)
            store.set_status(job_id, "completed")
        elif intent == "pause":
            store.set_status(job_id, PAUSED)
        elif intent == "cancel":
            delete_backup_files(settings.backup_dir, job.repo_type, job.slug)
            store.delete_job(job_id)
        elif stopping is not None and stopping.is_set():
            # Process-wide shutdown terminated the child; leave the job 'running'
            # so the startup re-queue resumes it instead of failing it.
            return
        else:
            err = outcome.error if outcome is not None else \
                f"download process exited unexpectedly (code {handle.exitcode})"
            store.set_status(job_id, "failed", error=str(err)[:500])
    except Exception as exc:  # noqa: BLE001 - surface any pre-download failure
        stop.set()
        if poller.is_alive():
            poller.join(timeout=2)
        if stopping is not None and stopping.is_set():
            return
        store.set_status(job_id, "failed", error=str(exc)[:500])
    finally:
        if registry is not None:
            registry.unregister(job_id)


class JobRunner:
    def __init__(self, store, settings, api=None, downloader=None) -> None:
        self._store = store
        self._settings = settings
        self._api = api
        self._downloader = downloader
        self._stopping = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=settings.max_concurrent_jobs)

    def submit(self, job_id) -> None:
        self._executor.submit(
            run_backup_job, job_id, self._store, self._settings,
            self._api, self._downloader, self._stopping,
        )

    def shutdown(self, wait: bool = False) -> None:
        """Stop the runner. Default (wait=False) is for process shutdown: signal
        in-flight workers that we're stopping (so they leave their jobs resumable)
        and cancel not-yet-started jobs (which stay 'queued' for the next startup
        to resume) without blocking on the in-flight download. wait=True drains
        every job to completion instead."""
        self._stopping.set()
        self._executor.shutdown(wait=wait, cancel_futures=not wait)
