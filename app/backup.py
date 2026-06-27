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
from .retry import is_retryable, BACKOFF_SECONDS, MAX_RETRIES

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
        try:
            if not item.is_file():
                continue
            in_cache = ".cache" in item.relative_to(path).parts
            # Count completed files, plus Xet's in-flight .incomplete staging which
            # lives under .cache/huggingface/download/ — those bytes are on disk and
            # would otherwise make progress look frozen until files finalize.
            if not in_cache or item.suffix == ".incomplete":
                total += item.stat().st_size
        except OSError:
            # A file can vanish between rglob() and stat() when the cancel path's
            # rmtree runs concurrently with the poller thread.  Skip the gone item
            # rather than crashing the daemon poller with a traceback.
            continue
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

    def request_all(self, intent) -> None:
        """Record an intent for every running job and terminate each handle."""
        with self._lock:
            items = list(self._handles.items())
            for job_id, _ in items:
                self._intents[job_id] = intent
        for _, handle in items:
            handle.terminate()


def _record_failure(store, job_id, retry_count, message, retryable) -> None:
    """Either schedule an auto-retry (transient + budget remaining) or mark the
    job permanently failed."""
    msg = str(message)[:500]
    if retryable and retry_count < MAX_RETRIES:
        store.schedule_retry(job_id, msg, BACKOFF_SECONDS[retry_count])
    else:
        store.set_status(job_id, "failed", error=msg)


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
            store.reset_retry(job_id)
        elif intent == "pause":
            store.set_status(job_id, PAUSED)
        elif intent == "cancel":
            delete_backup_files(settings.backup_dir, job.repo_type, job.slug)
            store.delete_job(job_id)
        elif intent == "requeue":
            # Global pause: stop and return to the queue (keep files), no retry change.
            store.set_status(job_id, "queued")
        elif stopping is not None and stopping.is_set():
            # Process-wide shutdown terminated the child; leave the job 'running'
            # so the startup reset re-queues it instead of failing it.
            return
        else:
            if outcome is not None:
                message, retryable = outcome.error, outcome.retryable
            else:
                message = f"download process exited unexpectedly (code {handle.exitcode})"
                retryable = True   # unexpected child exit (e.g. OOM) is worth a retry
            _record_failure(store, job_id, job.retry_count, message, retryable)
    except Exception as exc:  # noqa: BLE001 - surface any pre-download failure
        stop.set()
        if poller.is_alive():
            poller.join(timeout=2)
        if stopping is not None and stopping.is_set():
            return
        _record_failure(store, job_id, job.retry_count, str(exc), is_retryable(exc))
    finally:
        if registry is not None:
            registry.unregister(job_id)


DISPATCH_INTERVAL = 1.0


class JobRunner:
    def __init__(self, store, settings, api=None, launcher=None,
                 dispatch_interval: float = DISPATCH_INTERVAL) -> None:
        self._store = store
        self._settings = settings
        self._api = api
        self._launcher = launcher
        self._interval = dispatch_interval
        self._stopping = threading.Event()
        self._registry = RunningRegistry()
        self._executor = ThreadPoolExecutor(max_workers=settings.max_concurrent_jobs)
        self._dispatcher = None

    def start(self) -> None:
        """Start the dispatcher daemon (idempotent). It is the only thing that
        starts downloads: while the valve is open and a slot is free it claims and
        runs the lowest-id eligible job."""
        if self._dispatcher is not None:
            return
        self._dispatcher = threading.Thread(target=self._dispatch_loop, daemon=True)
        self._dispatcher.start()

    def _dispatch_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                if self._store.get_flag("paused_all", "0") != "1":   # valve open
                    while self._store.running_count() < self._settings.max_concurrent_jobs:
                        job = self._store.next_runnable_job()
                        if job is None:
                            break
                        if self._store.claim(job.id):
                            self._submit(job.id)
            except Exception:  # noqa: BLE001 - the loop must never die
                pass
            self._stopping.wait(self._interval)

    def _submit(self, job_id) -> None:
        self._executor.submit(
            run_backup_job, job_id, self._store, self._settings,
            self._api, self._launcher, self._stopping, self._registry,
        )

    def pause(self, job_id) -> None:
        """Stop a running download but keep its files (worker sets it 'paused')."""
        self._registry.request(job_id, "pause")

    def cancel(self, job_id) -> bool:
        """Stop a running download; the worker deletes its files + row once the
        child dies. Returns True if a live download was terminated."""
        return self._registry.request(job_id, "cancel")

    def pause_all(self) -> None:
        """Close the global valve and return every running download to 'queued'
        (keeping files). Per-job 'paused' jobs are untouched."""
        self._store.set_flag("paused_all", "1")
        self._registry.request_all("requeue")

    def resume_all(self) -> None:
        """Open the global valve; the dispatcher resumes held work by priority."""
        self._store.set_flag("paused_all", "0")

    def shutdown(self, wait: bool = False) -> None:
        """Stop the dispatcher and runner. Default (wait=False) is for process
        shutdown: signal in-flight workers (so they leave jobs resumable),
        terminate their child processes, and stop accepting new work. wait=True
        drains running jobs to completion."""
        self._stopping.set()
        if self._dispatcher is not None:
            self._dispatcher.join(timeout=2)
        self._registry.terminate_all()
        self._executor.shutdown(wait=wait, cancel_futures=not wait)
