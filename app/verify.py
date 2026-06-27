"""Hash downloaded files and compare them to Hugging Face's reported hashes.

Pure and offline-testable: a function over (local_dir, expected-list) -> report,
with no DB, network, or mocks. LFS-tracked files carry a content SHA256
(`sibling.lfs.sha256`); plain git files carry only a git blob OID
(`sibling.blob_id`, a SHA1 over ``b"blob <size>\\0" + content``), so a full check
needs both algorithms. Hashing streams in chunks and checks a stop Event between
chunks, so a long verification aborts near-instantly without a subprocess.
"""
import hashlib
import os
from dataclasses import dataclass, field
from typing import List, NamedTuple
from pathlib import Path

CHUNK = 1 << 20  # 1 MiB


class VerifyAborted(Exception):
    """Raised when a stop event fires mid-verification."""


class FileHash(NamedTuple):
    rfilename: str
    algo: str          # "sha256" | "git-sha1"
    expected: str


@dataclass
class VerifyReport:
    ok: bool
    failures: List[dict] = field(default_factory=list)


def _stream(path, hasher, stop) -> str:
    with open(path, "rb") as f:
        while True:
            if stop is not None and stop.is_set():
                raise VerifyAborted()
            chunk = f.read(CHUNK)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def sha256_file(path, stop=None) -> str:
    return _stream(path, hashlib.sha256(), stop)


def git_blob_sha1(path, stop=None) -> str:
    size = os.path.getsize(path)
    h = hashlib.sha1()
    h.update(b"blob %d\0" % size)   # git object header, then the raw content
    return _stream(path, h, stop)


def expected_file_hashes(siblings) -> List[FileHash]:
    """Map repo siblings to (rfilename, algo, expected_hex). LFS files use their
    sha256; plain git files use their blob_id. A sibling with neither (shouldn't
    happen with files_metadata=True) is skipped — it can't be verified."""
    out: List[FileHash] = []
    for s in siblings:
        lfs = getattr(s, "lfs", None)
        if lfs is not None and getattr(lfs, "sha256", None):
            out.append(FileHash(s.rfilename, "sha256", lfs.sha256))
        elif getattr(s, "blob_id", None):
            out.append(FileHash(s.rfilename, "git-sha1", s.blob_id))
    return out


def verify_repo(local_dir, expected, stop=None, on_progress=None) -> VerifyReport:
    """Hash each expected file under local_dir and compare to its reference hash.
    Missing declared files fail; extra local files and anything under .cache/ are
    ignored (only declared siblings are checked). on_progress(cumulative_bytes) is
    called after each hashed file. Raises VerifyAborted if stop is set mid-run."""
    local_dir = Path(local_dir)
    failures: List[dict] = []
    hashed = 0
    for fh in expected:
        if stop is not None and stop.is_set():
            raise VerifyAborted()
        path = local_dir / fh.rfilename
        if not path.is_file():
            failures.append({"file": fh.rfilename, "reason": "missing"})
            continue
        try:
            actual = (sha256_file(path, stop) if fh.algo == "sha256"
                      else git_blob_sha1(path, stop))
        except VerifyAborted:
            raise
        except OSError:
            failures.append({"file": fh.rfilename, "reason": "read-error"})
            continue
        if actual != fh.expected:
            failures.append({"file": fh.rfilename, "reason": "mismatch"})
        hashed += path.stat().st_size
        if on_progress is not None:
            on_progress(hashed)
    return VerifyReport(ok=not failures, failures=failures)
