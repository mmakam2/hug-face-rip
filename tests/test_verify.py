import hashlib
import shutil
import subprocess
import threading

import pytest

from app.verify import (
    FileHash,
    VerifyAborted,
    VerifyReport,
    expected_file_hashes,
    git_blob_sha1,
    sha256_file,
    verify_repo,
)


def _git_blob_sha1_bytes(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\x00" + data).hexdigest()


def test_sha256_file_matches_hashlib(tmp_path):
    p = tmp_path / "f.bin"
    data = b"a" * (3 * 1024 * 1024 + 7)        # spans multiple chunks
    p.write_bytes(data)
    assert sha256_file(p) == hashlib.sha256(data).hexdigest()


def test_git_blob_sha1_matches_formula(tmp_path):
    p = tmp_path / "config.json"
    data = b'{"hello": "world"}'
    p.write_bytes(data)
    assert git_blob_sha1(p) == _git_blob_sha1_bytes(data)


@pytest.mark.skipif(shutil.which("git") is None, reason="git not installed")
def test_git_blob_sha1_matches_git_hash_object(tmp_path):
    p = tmp_path / "blob.txt"
    p.write_bytes(b"hello")
    out = subprocess.check_output(["git", "hash-object", str(p)]).decode().strip()
    assert git_blob_sha1(p) == out


def test_expected_file_hashes_picks_algo_per_file():
    class _Lfs:
        def __init__(self, sha256):
            self.sha256 = sha256

    class _Sib:
        def __init__(self, rfilename, blob_id=None, lfs=None):
            self.rfilename = rfilename
            self.blob_id = blob_id
            self.lfs = lfs

    sibs = [
        _Sib("model.safetensors", blob_id="deadbeef", lfs=_Lfs("abc123")),
        _Sib("config.json", blob_id="cafef00d", lfs=None),
        _Sib("weird.no-hash"),   # neither lfs nor blob_id -> skipped
    ]
    out = expected_file_hashes(sibs)
    assert FileHash("model.safetensors", "sha256", "abc123") in out
    assert FileHash("config.json", "git-sha1", "cafef00d") in out
    assert all(f.rfilename != "weird.no-hash" for f in out)


def _expected_for(local_dir, files):
    """Build the expected list the way the Hub would report it: .bin -> lfs/sha256,
    everything else -> git blob sha1."""
    out = []
    for name, data in files.items():
        if name.endswith(".bin"):
            out.append(FileHash(name, "sha256", hashlib.sha256(data).hexdigest()))
        else:
            out.append(FileHash(name, "git-sha1", _git_blob_sha1_bytes(data)))
    return out


def test_verify_repo_all_good(tmp_path):
    files = {"config.json": b'{"a":1}', "model.bin": b"WEIGHTS-DATA"}
    for n, d in files.items():
        (tmp_path / n).write_bytes(d)
    report = verify_repo(tmp_path, _expected_for(tmp_path, files))
    assert isinstance(report, VerifyReport)
    assert report.ok is True
    assert report.failures == []


def test_verify_repo_detects_mismatch(tmp_path):
    files = {"model.bin": b"GOOD-WEIGHTS"}
    expected = _expected_for(tmp_path, files)
    (tmp_path / "model.bin").write_bytes(b"BAD!-WEIGHTS")   # same length, different bytes
    report = verify_repo(tmp_path, expected)
    assert report.ok is False
    assert report.failures == [{"file": "model.bin", "reason": "mismatch"}]


def test_verify_repo_reports_missing_file(tmp_path):
    files = {"config.json": b"x", "model.bin": b"y"}
    expected = _expected_for(tmp_path, files)
    (tmp_path / "config.json").write_bytes(b"x")            # model.bin never written
    report = verify_repo(tmp_path, expected)
    assert report.ok is False
    assert {"file": "model.bin", "reason": "missing"} in report.failures


def test_verify_repo_calls_on_progress_with_cumulative_bytes(tmp_path):
    files = {"a.bin": b"x" * 10, "b.bin": b"y" * 20}
    for n, d in files.items():
        (tmp_path / n).write_bytes(d)
    seen = []
    verify_repo(tmp_path, _expected_for(tmp_path, files), on_progress=seen.append)
    assert seen[-1] == 30        # cumulative bytes hashed across both files


def test_verify_repo_aborts_when_stop_set(tmp_path):
    files = {"model.bin": b"z" * (5 * 1024 * 1024)}
    expected = _expected_for(tmp_path, files)
    (tmp_path / "model.bin").write_bytes(files["model.bin"])
    stop = threading.Event()
    stop.set()                   # already requested before we start
    with pytest.raises(VerifyAborted):
        verify_repo(tmp_path, expected, stop=stop)
