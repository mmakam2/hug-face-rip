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


def test_outcome_carries_retryable_flag():
    from app.launcher import SubprocessLauncher, Outcome
    from tests import _proc_targets
    launcher = SubprocessLauncher(entry=_proc_targets.retryable_error_target)
    handle = launcher.start(repo_id="o/n", repo_type="model",
                            local_dir="/tmp/x", token=None, max_workers=2)
    outcome = handle.wait(timeout=10)
    assert outcome == Outcome(ok=False, error="dns blip", retryable=True)


def test_two_tuple_outcome_defaults_retryable_false():
    # The existing ok/error targets still put 2-tuples; wait() must tolerate them.
    from app.launcher import SubprocessLauncher, Outcome
    from tests import _proc_targets
    launcher = SubprocessLauncher(entry=_proc_targets.error_target)
    handle = launcher.start(repo_id="o/n", repo_type="model",
                            local_dir="/tmp/x", token=None, max_workers=2)
    assert handle.wait(timeout=10) == Outcome(ok=False, error="boom", retryable=False)
