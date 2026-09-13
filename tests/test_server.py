import pytest
from app.main import server_host_port


def test_default_binds_all_interfaces():
    assert server_host_port({}) == ("0.0.0.0", 8000)


def test_env_overrides_host_and_port():
    assert server_host_port({"HOST": "127.0.0.1", "PORT": "9000"}) == ("127.0.0.1", 9000)


def test_blank_env_values_fall_back_to_defaults():
    assert server_host_port({"HOST": "", "PORT": ""}) == ("0.0.0.0", 8000)


def test_configure_logging_sends_app_info_lines_to_stderr_once(capsys):
    # uvicorn only configures its own loggers; without this, the app's INFO
    # lines (deletes, action arrivals) never reach the journal — only WARNINGs
    # did, via Python's last-resort handler.
    import logging
    from app.main import configure_logging
    app_logger = logging.getLogger("app")
    before = list(app_logger.handlers)
    try:
        configure_logging()
        configure_logging()                       # idempotent: no duplicate lines
        added = [h for h in app_logger.handlers if h not in before]
        assert len(added) == 1
        assert app_logger.level == logging.INFO
        logging.getLogger("app.backup").info("deleted x/y")
        err = capsys.readouterr().err
        assert err.count("deleted x/y") == 1
        assert err.startswith("INFO:")             # matches uvicorn's line style
    finally:
        for h in app_logger.handlers[:]:
            if h not in before:
                app_logger.removeHandler(h)
        app_logger.setLevel(logging.NOTSET)


# runpy warns that app.main is already imported (by the other tests); re-executing
# it as __main__ is exactly the launch we are reproducing, so that is expected.
@pytest.mark.filterwarnings("ignore:.*found in sys.modules.*:RuntimeWarning")
def test_running_as_main_module_keeps_request_logger_under_app(monkeypatch):
    # The service is launched with `python -m app.main`, which executes the
    # module as `__main__`. A logger named via __name__ would then sit outside
    # the `app` hierarchy that configure_logging() attaches its handler to, and
    # the arrival lines for POSTs would silently vanish from the journal.
    import logging
    import runpy
    import uvicorn
    monkeypatch.setattr(uvicorn, "run", lambda *a, **k: None)   # don't start a server
    app_logger = logging.getLogger("app")
    before = list(app_logger.handlers)
    try:
        g = runpy.run_module("app.main", run_name="__main__")
        assert g["logger"].name == "app.main"
        assert g["logger"].getEffectiveLevel() == logging.INFO   # handler + level apply
    finally:
        for h in app_logger.handlers[:]:
            if h not in before:
                app_logger.removeHandler(h)
        app_logger.setLevel(logging.NOTSET)
