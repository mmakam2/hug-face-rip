import errno
import socket

import httpx
import pytest
from huggingface_hub.utils import HfHubHTTPError, RepositoryNotFoundError

from app.retry import is_retryable, BACKOFF_SECONDS, MAX_RETRIES


def _http_error(status, cls=HfHubHTTPError):
    req = httpx.Request("GET", "https://huggingface.co/x")
    return cls("msg", response=httpx.Response(status, request=req))


def test_backoff_schedule_is_the_agreed_five():
    assert BACKOFF_SECONDS == [30, 60, 120, 240, 480]
    assert MAX_RETRIES == 5


@pytest.mark.parametrize("exc", [
    socket.gaierror(-3, "Temporary failure in name resolution"),
    httpx.ConnectError("connection failed"),
    httpx.ConnectTimeout("connect timed out"),
    httpx.ReadTimeout("read timed out"),
    httpx.NetworkError("network down"),
    OSError(errno.ECONNRESET, "Connection reset by peer"),
])
def test_network_errors_are_retryable(exc):
    assert is_retryable(exc) is True


def test_5xx_and_429_http_errors_are_retryable():
    for status in (429, 500, 502, 503, 504):
        assert is_retryable(_http_error(status)) is True


@pytest.mark.parametrize("status", [401, 403, 404])
def test_auth_and_notfound_http_errors_are_permanent(status):
    assert is_retryable(_http_error(status)) is False


def test_repo_not_found_is_permanent():
    assert is_retryable(_http_error(404, cls=RepositoryNotFoundError)) is False


@pytest.mark.parametrize("exc", [
    ValueError("bad slug"),
    RuntimeError("not enough disk space for x/y"),
])
def test_other_errors_are_permanent(exc):
    assert is_retryable(exc) is False
