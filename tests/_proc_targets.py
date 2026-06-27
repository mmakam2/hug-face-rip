"""Picklable child-process entry points for launcher unit tests.

Lives in a plain (non-test_) module so the spawn-method child can import it by
qualified name. Each mirrors the (queue, kwargs) contract of _download_entry.
"""
import time


def ok_target(queue, kwargs):
    queue.put(("ok", None))


def error_target(queue, kwargs):
    queue.put(("error", "boom"))


def sleep_target(queue, kwargs):
    time.sleep(30)            # long enough that the test always terminates it first
    queue.put(("ok", None))   # unreached when terminated


def retryable_error_target(queue, kwargs):
    queue.put(("error", "dns blip", True))
