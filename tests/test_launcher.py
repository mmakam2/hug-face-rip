from app.launcher import SubprocessLauncher, Outcome
from tests import _proc_targets


def test_successful_download_reports_ok():
    launcher = SubprocessLauncher(entry=_proc_targets.ok_target)
    handle = launcher.start(repo_id="o/n", repo_type="model",
                            local_dir="/tmp/x", token=None, max_workers=2)
    outcome = handle.wait(timeout=10)
    assert outcome == Outcome(ok=True, error=None)


def test_failed_download_reports_error():
    launcher = SubprocessLauncher(entry=_proc_targets.error_target)
    handle = launcher.start(repo_id="o/n", repo_type="model",
                            local_dir="/tmp/x", token=None, max_workers=2)
    outcome = handle.wait(timeout=10)
    assert outcome is not None and outcome.ok is False
    assert outcome.error == "boom"


def test_terminate_stops_a_running_download_and_reports_no_outcome():
    launcher = SubprocessLauncher(entry=_proc_targets.sleep_target)
    handle = launcher.start(repo_id="o/n", repo_type="model",
                            local_dir="/tmp/x", token=None, max_workers=2)
    handle.terminate()
    outcome = handle.wait(timeout=10)
    assert outcome is None                # killed before it put anything on the queue
    assert handle.exitcode not in (0, None)   # exited via signal (negative on POSIX)
