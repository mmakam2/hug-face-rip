"""Run one Hugging Face download in a terminable child process.

The download runs in a separate process (spawn start method) so the worker can
terminate it mid-file. The child only downloads and reports its outcome over a
queue; it never touches the DB. Spawn (not fork) avoids deadlocking a forked
copy of the multithreaded server process.
"""
import multiprocessing as mp
import queue as _queue
from dataclasses import dataclass
from typing import Optional


@dataclass
class Outcome:
    ok: bool
    error: Optional[str] = None


def _download_entry(queue, kwargs):
    """Child entry point. Runs the real snapshot_download and reports the result.

    Reports every failure (including unusual ones) as an error string rather than
    crashing silently. Never imported with side effects, so it is spawn-safe.
    """
    try:
        from huggingface_hub import snapshot_download
        snapshot_download(**kwargs)
        queue.put(("ok", None))
    except BaseException as exc:  # noqa: BLE001 - surface anything as a job error
        queue.put(("error", str(exc)[:500]))


class ProcessHandle:
    def __init__(self, process, queue) -> None:
        self._process = process
        self._queue = queue

    def terminate(self) -> None:
        """SIGTERM the child if it is still alive. A no-op once it has exited."""
        if self._process.is_alive():
            self._process.terminate()

    @property
    def exitcode(self) -> Optional[int]:
        return self._process.exitcode

    def wait(self, timeout=None) -> Optional[Outcome]:
        """Block until the child exits, then return the Outcome it reported.

        Returns None if the child exited without reporting one (i.e. it was
        terminated mid-download). The reported payload is tiny, so reading it
        after join carries no risk of the feeder-thread deadlock that large
        Queue items can cause.
        """
        self._process.join(timeout)
        try:
            tag, error = self._queue.get_nowait()
        except _queue.Empty:
            return None
        return Outcome(ok=(tag == "ok"), error=error)


class SubprocessLauncher:
    def __init__(self, ctx=None, entry=_download_entry) -> None:
        self._ctx = ctx or mp.get_context("spawn")
        self._entry = entry

    def start(self, **kwargs) -> ProcessHandle:
        queue = self._ctx.Queue()
        process = self._ctx.Process(
            target=self._entry, args=(queue, kwargs), daemon=True
        )
        process.start()
        return ProcessHandle(process, queue)
