"""Backoff schedule and transient-vs-permanent error classification.

Pure and import-light so it is usable in both the download child process and
the parent worker without side effects. huggingface_hub is built on httpx here,
so transient failures are httpx transport errors and 429/5xx HTTP responses.
"""
import errno
import socket

import httpx
from huggingface_hub.utils import HfHubHTTPError

BACKOFF_SECONDS = [30, 60, 120, 240, 480]
MAX_RETRIES = 5

_RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}
# OSError errno values that indicate a transient network condition. (DNS
# EAI_AGAIN is handled separately via socket.gaierror below.)
_RETRYABLE_OS_ERRNO = {errno.ECONNRESET, errno.ECONNREFUSED, errno.ECONNABORTED,
                       errno.ETIMEDOUT, errno.EHOSTUNREACH, errno.ENETUNREACH}


def is_retryable(exc: BaseException) -> bool:
    """True if the failure looks transient (worth an automatic retry).

    Retryable: httpx transport failures (connect errors, timeouts, network /
    protocol errors — all ``httpx.TransportError``), 429/5xx Hub HTTP responses,
    raw DNS ``gaierror``, and connection-related ``OSError``\\s. Permanent:
    4xx (auth / not-found), bad slugs, disk-space failures, everything else.
    """
    # Hub HTTP status errors: retry only rate-limit / server-side statuses.
    if isinstance(exc, HfHubHTTPError):
        resp = getattr(exc, "response", None)
        return getattr(resp, "status_code", None) in _RETRYABLE_HTTP_STATUS

    # httpx transport failures: connect errors, timeouts, network/protocol errors.
    if isinstance(exc, httpx.TransportError):
        return True

    # Raw DNS failure (EAI_AGAIN "Temporary failure in name resolution"), in case
    # it ever propagates unwrapped rather than inside an httpx.ConnectError.
    if isinstance(exc, socket.gaierror):
        return True

    # Other connection-related OSErrors (ConnectionResetError, etc.).
    if isinstance(exc, OSError) and exc.errno in _RETRYABLE_OS_ERRNO:
        return True

    return False
