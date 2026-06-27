# SHA256 Integrity Verification — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Verify every downloaded file against Hugging Face's reported hashes — automatically after each download (env-gated) and on demand for completed backups — surfacing mismatches as a distinct `corrupted` result with a one-click clean re-download.

**Architecture:** A new pure module `app/verify.py` hashes files (SHA256 for LFS-tracked files, git-blob-SHA1 for plain git files) and compares them to the hashes from `repo_info(files_metadata=True)`. Verification runs cooperatively in the worker thread (a stop `Event` checked between chunks, so it aborts near-instantly without a subprocess) and plugs into the existing `RunningRegistry`. A new `verifying` lifecycle status plus `verify_status`/`verify_detail` columns carry the outcome; the dashboard renders badges, a Verify button, a Stop button, and Re-download.

**Tech Stack:** Python 3, FastAPI, SQLite (`JobStore`), `huggingface_hub` 1.21, pytest (offline, Hub mocked), vanilla JS dashboard (`app/static/index.html`).

## Global Constraints

- **No system `pip`/`python`** — always use `.venv/bin/python` and `.venv/bin/python -m pytest`.
- **Do not read `.env`** — reference env vars by name only.
- **TDD throughout** — failing test first, minimal code, green, commit. Default suite is offline (`pytest.ini` sets `-m "not integration"`); never add network to unit tests.
- **Hash reference source:** LFS files → `sibling.lfs.sha256` (SHA256); plain git files → `sibling.blob_id` (git blob SHA1 = `sha1(b"blob <size>\0" + content)`).
- **Cannot-verify ≠ corrupted:** a Hub lookup failure leaves the job `completed` + `unverified` with a note, never `corrupted`.
- **A verifying job's download is already complete** — interrupting verification (Stop / pause-all / shutdown / orphan reset) always lands it back at `completed` + `unverified`; it is never deleted or re-queued for download. Only explicit Re-download discards files.
- **Commit style:** end commit messages with `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`. Work is on branch `feature/sha256-integrity-verification`.

---

## Task 1: `app/verify.py` — pure verification core

**Files:**
- Create: `app/verify.py`
- Test: `tests/test_verify.py`

**Interfaces:**
- Produces:
  - `CHUNK: int` (1 MiB read size)
  - `class VerifyAborted(Exception)`
  - `class FileHash(NamedTuple)` with fields `rfilename: str`, `algo: str` (`"sha256"`|`"git-sha1"`), `expected: str`
  - `@dataclass class VerifyReport` with `ok: bool`, `failures: list[dict]` (each `{"file": str, "reason": str}`, reason ∈ `mismatch|missing|read-error`)
  - `sha256_file(path, stop=None) -> str`
  - `git_blob_sha1(path, stop=None) -> str`
  - `expected_file_hashes(siblings) -> list[FileHash]`
  - `verify_repo(local_dir, expected, stop=None, on_progress=None) -> VerifyReport`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_verify.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_verify.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.verify'`.

- [ ] **Step 3: Write the implementation**

Create `app/verify.py`:

```python
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
from pathlib import Path
from typing import List, NamedTuple, Optional

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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_verify.py -q`
Expected: PASS (all tests; the `git hash-object` test runs since this is a git repo).

- [ ] **Step 5: Commit**

```bash
git add app/verify.py tests/test_verify.py
git commit -m "feat(verify): pure SHA256 + git-blob-SHA1 file verification core

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Data model — verify columns, status, store helpers

**Files:**
- Modify: `app/db.py` (status constants, `_SCHEMA`, migration, `Job`, helpers, `running_count`)
- Test: `tests/test_db.py`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `VERIFYING = "verifying"` constant.
  - `Job.verify_status: str = "unverified"`, `Job.verify_detail: Optional[str] = None` (both in `to_dict()`).
  - `JobStore.set_verify_status(job_id, verify_status, detail=None)`.
  - `JobStore.reset_verifying_to_completed()`.
  - `JobStore.running_count()` now counts `running` + `verifying`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_db.py`:

```python
def test_running_count_includes_verifying(store):
    a = store.create_job("a/b", "model")
    store.set_status(a.id, "running")
    b = store.create_job("c/d", "model")
    store.set_status(b.id, "verifying")
    assert store.running_count() == 2


def test_set_verify_status_persists(store):
    j = store.create_job("a/b", "model")
    assert store.get_job(j.id).verify_status == "unverified"   # default
    assert store.get_job(j.id).verify_detail is None
    store.set_verify_status(j.id, "corrupted", detail='{"failures": []}')
    g = store.get_job(j.id)
    assert g.verify_status == "corrupted"
    assert g.verify_detail == '{"failures": []}'
    assert "verify_status" in g.to_dict() and "verify_detail" in g.to_dict()


def test_reset_verifying_to_completed(store):
    j = store.create_job("a/b", "model")
    store.set_status(j.id, "verifying")
    store.update_progress(j.id, 5, total_bytes=10)
    store.reset_verifying_to_completed()
    g = store.get_job(j.id)
    assert g.status == "completed"
    assert g.verify_status == "unverified"
    assert g.downloaded_bytes == 10        # bar restored to total


def test_migration_adds_verify_columns_to_old_db(tmp_path):
    import sqlite3
    dbp = tmp_path / "old.db"
    con = sqlite3.connect(dbp)
    con.executescript(
        "CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, slug TEXT NOT NULL,"
        " repo_type TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',"
        " total_bytes INTEGER NOT NULL DEFAULT 0, downloaded_bytes INTEGER NOT NULL DEFAULT 0,"
        " error TEXT, created_at TEXT NOT NULL DEFAULT (datetime('now')),"
        " updated_at TEXT NOT NULL DEFAULT (datetime('now')), UNIQUE(repo_type, slug));"
    )
    con.execute("INSERT INTO jobs (slug, repo_type, status) VALUES ('keep/me','model','completed')")
    con.commit(); con.close()

    store = JobStore(dbp)
    job = store.get_job_by_repo("model", "keep/me")
    assert job is not None
    assert job.verify_status == "unverified"
    assert job.verify_detail is None
    store.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_db.py -q -k "verify or reset_verifying"`
Expected: FAIL — `AttributeError`/`OperationalError` (no `verify_status` column / no `set_verify_status`).

- [ ] **Step 3: Implement the schema and helpers**

In `app/db.py`, add the status constant after `CANCELLED = "cancelled"`:

```python
VERIFYING = "verifying"
```

In `_SCHEMA`, add the two columns to the `jobs` `CREATE TABLE` (after `next_retry_at TEXT,`):

```python
    verify_status TEXT NOT NULL DEFAULT 'unverified',
    verify_detail TEXT,
```

In `JobStore.__init__`, after the existing `next_retry_at` migration block and before the `app_state` seed, add:

```python
        if "verify_status" not in cols:
            self._conn.execute(
                "ALTER TABLE jobs ADD COLUMN verify_status TEXT NOT NULL DEFAULT 'unverified'")
        if "verify_detail" not in cols:
            self._conn.execute("ALTER TABLE jobs ADD COLUMN verify_detail TEXT")
```

In the `Job` dataclass, add two fields after `next_retry_at`:

```python
    verify_status: str = "unverified"
    verify_detail: Optional[str] = None
```

Change `running_count` to count verifications too:

```python
    def running_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE status IN ('running', 'verifying')"
            ).fetchone()
        return row[0]
```

Add two new methods (place them near `set_status` / `reset_running_to_queued`):

```python
    def set_verify_status(self, job_id: int, verify_status: str,
                          detail: Optional[str] = None) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET verify_status = ?, verify_detail = ?, "
                "updated_at = datetime('now') WHERE id = ?",
                (verify_status, detail, job_id),
            )
            self._conn.commit()

    def reset_verifying_to_completed(self) -> None:
        """On startup, a job orphaned mid-verification (process died) lands back
        at 'completed' + 'unverified' with its bar restored — the download itself
        was already complete, so it is never re-queued for download."""
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status = 'completed', verify_status = 'unverified', "
                "downloaded_bytes = total_bytes, updated_at = datetime('now') "
                "WHERE status = 'verifying'"
            )
            self._conn.commit()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_db.py -q`
Expected: PASS (new tests + all existing `test_db.py` tests, including the old migration test).

- [ ] **Step 5: Commit**

```bash
git add app/db.py tests/test_db.py
git commit -m "feat(db): verify_status/verify_detail columns, verifying status, helpers

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Config — `VERIFY_DOWNLOADS`

**Files:**
- Modify: `app/config.py` (`Settings`, `_bool`, `load_settings`)
- Test: `tests/test_config.py`

**Interfaces:**
- Produces: `Settings.verify_downloads: bool` (default `True`), parsed from env `VERIFY_DOWNLOADS`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_config.py`:

```python
def test_verify_downloads_defaults_on(tmp_path):
    s = load_settings(base_env(tmp_path))
    assert s.verify_downloads is True


def test_verify_downloads_disabled_by_env(tmp_path):
    for val in ("0", "false", "no", "off", "FALSE"):
        s = load_settings(base_env(tmp_path) | {"VERIFY_DOWNLOADS": val})
        assert s.verify_downloads is False, val


def test_verify_downloads_enabled_by_env(tmp_path):
    s = load_settings(base_env(tmp_path) | {"VERIFY_DOWNLOADS": "1"})
    assert s.verify_downloads is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_config.py -q -k verify_downloads`
Expected: FAIL — `TypeError` (unexpected/ missing `verify_downloads`) or `AttributeError`.

- [ ] **Step 3: Implement**

In `app/config.py`, add the field to `Settings` (last field, with a default so existing constructors keep working):

```python
    verify_downloads: bool = True
```

Add a bool parser next to `_int`:

```python
def _bool(env: Mapping[str, str], key: str, default: bool) -> bool:
    raw = env.get(key)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off")
```

In `load_settings`, add to the returned `Settings(...)`:

```python
        verify_downloads=_bool(env, "VERIFY_DOWNLOADS", True),
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_config.py -q`
Expected: PASS (new + existing config tests).

- [ ] **Step 5: Commit**

```bash
git add app/config.py tests/test_config.py
git commit -m "feat(config): VERIFY_DOWNLOADS env flag (default on)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Worker — verify phase, manual verify job, auto-verify

**Files:**
- Modify: `app/backup.py` (imports, `repo_expected_hashes`, `_StopHandle`, `_verify_phase`, `run_verify_job`, `run_backup_job` ok-branch, `JobRunner.verify`/`stop_verify`)
- Test: `tests/test_backup_worker.py` (update `make_settings`, add fakes + tests)

**Interfaces:**
- Consumes: `app.verify.verify_repo`, `expected_file_hashes`, `VerifyAborted` (Task 1); `JobStore.set_verify_status` (Task 2); `Settings.verify_downloads` (Task 3); existing `RunningRegistry`.
- Produces:
  - `repo_expected_hashes(slug, repo_type, token, api=None) -> list[FileHash]`
  - `run_verify_job(job_id, store, settings, api=None, registry=None, stopping=None) -> None`
  - `JobRunner.verify(job_id)`, `JobRunner.stop_verify(job_id)`
  - `run_backup_job` now runs `_verify_phase` after a successful download when `settings.verify_downloads`.

- [ ] **Step 1: Update the test helper and add fakes/tests**

In `tests/test_backup_worker.py`, change the imports line that pulls from `app.db` and `app.backup`:

```python
from app.db import JobStore, COMPLETED, FAILED, CANCELLED, RUNNING, PAUSED, QUEUED, RETRYING, VERIFYING
from app.backup import run_backup_job, run_verify_job, JobRunner, RunningRegistry, local_dir_for
```

Add `import hashlib` and `import json` at the top (next to the existing `import socket`).

Change `make_settings` to default verification **off** (so the existing worker tests are unaffected) and accept an override:

```python
def make_settings(tmp_path, max_jobs=2, verify_downloads=False):
    return Settings(
        hf_token="hf_test",
        backup_dir=tmp_path / "backups",
        max_concurrent_jobs=max_jobs,
        max_workers=4,
        db_path=tmp_path / "jobs.db",
        verify_downloads=verify_downloads,
    )
```

Add verification fakes and a file-writing downloader after the existing `FakeApi` class:

```python
def _git_blob_sha1_bytes(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\x00" + data).hexdigest()


class _Lfs:
    def __init__(self, sha256):
        self.sha256 = sha256


class _VSibling:
    def __init__(self, rfilename, size, blob_id=None, lfs=None):
        self.rfilename = rfilename
        self.size = size
        self.blob_id = blob_id
        self.lfs = lfs


class VerifyApi:
    """repo_info reports the hashes of `truth` (name -> bytes). Files ending in
    .bin are reported as LFS (lfs.sha256); everything else via git blob sha1."""
    def __init__(self, truth):
        self._truth = truth

    def repo_info(self, repo_id, repo_type, token=None, files_metadata=False):
        sibs = []
        for name, data in self._truth.items():
            if name.endswith(".bin"):
                sibs.append(_VSibling(name, len(data), lfs=_Lfs(hashlib.sha256(data).hexdigest())))
            else:
                sibs.append(_VSibling(name, len(data), blob_id=_git_blob_sha1_bytes(data)))
        return _Info(sibs)


def files_downloader_factory(files):
    def _download(*, local_dir, stop=None, **_):
        target = Path(local_dir)
        target.mkdir(parents=True, exist_ok=True)
        for name, data in files.items():
            (target / name).write_bytes(data)
    return _download


def _write_completed_repo(store, settings, slug, files):
    """Create a completed job with `files` already on disk and total_bytes set —
    the precondition for a manual verify."""
    job = store.create_job(slug, "model")
    d = local_dir_for(settings.backup_dir, "model", slug)
    d.mkdir(parents=True, exist_ok=True)
    for name, data in files.items():
        (d / name).write_bytes(data)
    store.set_status(job.id, COMPLETED)
    store.update_progress(job.id, 0, total_bytes=sum(len(b) for b in files.values()))
    return job
```

Add the test cases:

```python
def test_worker_auto_verifies_after_download(tmp_path):
    settings = make_settings(tmp_path, verify_downloads=True)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    files = {"config.json": b'{"a":1}', "model.bin": b"WEIGHTS-DATA"}
    run_backup_job(job.id, store, settings, api=VerifyApi(files),
                   launcher=InThreadLauncher(files_downloader_factory(files)),
                   registry=RunningRegistry())
    j = store.get_job(job.id)
    assert j.status == COMPLETED
    assert j.verify_status == "verified"
    assert j.verify_detail is None
    assert j.downloaded_bytes == j.total_bytes        # bar restored to 100%
    store.close()


def test_worker_auto_verify_detects_corruption(tmp_path):
    settings = make_settings(tmp_path, verify_downloads=True)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    truth = {"model.bin": b"GOOD-WEIGHTS"}            # what the Hub reports
    bad = {"model.bin": b"BADX-WEIGHTS"}              # what the downloader writes (same length)
    run_backup_job(job.id, store, settings, api=VerifyApi(truth),
                   launcher=InThreadLauncher(files_downloader_factory(bad)),
                   registry=RunningRegistry())
    j = store.get_job(job.id)
    assert j.status == COMPLETED
    assert j.verify_status == "corrupted"
    assert json.loads(j.verify_detail)["failures"] == [{"file": "model.bin", "reason": "mismatch"}]
    store.close()


def test_worker_skips_verify_when_disabled(tmp_path):
    settings = make_settings(tmp_path, verify_downloads=False)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    files = {"model.bin": b"WEIGHTS"}
    run_backup_job(job.id, store, settings, api=VerifyApi(files),
                   launcher=InThreadLauncher(files_downloader_factory(files)),
                   registry=RunningRegistry())
    j = store.get_job(job.id)
    assert j.status == COMPLETED
    assert j.verify_status == "unverified"            # never verified
    store.close()


def test_run_verify_job_marks_verified(tmp_path):
    settings = make_settings(tmp_path, verify_downloads=True)
    store = JobStore(settings.db_path)
    files = {"config.json": b'{"a":1}', "model.bin": b"WEIGHTS"}
    job = _write_completed_repo(store, settings, "o/n", files)
    run_verify_job(job.id, store, settings, api=VerifyApi(files), registry=RunningRegistry())
    j = store.get_job(job.id)
    assert j.status == COMPLETED and j.verify_status == "verified"
    store.close()


def test_run_verify_job_marks_corrupted(tmp_path):
    settings = make_settings(tmp_path, verify_downloads=True)
    store = JobStore(settings.db_path)
    truth = {"model.bin": b"GOOD-WEIGHTS"}
    job = _write_completed_repo(store, settings, "o/n", {"model.bin": b"BADX-WEIGHTS"})
    run_verify_job(job.id, store, settings, api=VerifyApi(truth), registry=RunningRegistry())
    j = store.get_job(job.id)
    assert j.status == COMPLETED and j.verify_status == "corrupted"
    store.close()


def test_verify_marks_unverifiable_when_hub_unreachable(tmp_path):
    settings = make_settings(tmp_path, verify_downloads=True)
    store = JobStore(settings.db_path)
    job = _write_completed_repo(store, settings, "o/n", {"model.bin": b"WEIGHTS"})

    class FailApi:
        def repo_info(self, *a, **k):
            raise RuntimeError("hub down")

    run_verify_job(job.id, store, settings, api=FailApi(), registry=RunningRegistry())
    j = store.get_job(job.id)
    assert j.status == COMPLETED
    assert j.verify_status == "unverified"            # cannot-verify != corrupted
    assert "could not reach" in json.loads(j.verify_detail)["note"]
    assert j.downloaded_bytes == j.total_bytes        # bar restored
    store.close()


def test_verify_aborts_on_preset_stop_intent(tmp_path):
    settings = make_settings(tmp_path, verify_downloads=True)
    store = JobStore(settings.db_path)
    files = {"model.bin": b"WEIGHTS"}
    job = _write_completed_repo(store, settings, "o/n", files)
    registry = RunningRegistry()
    registry._intents[job.id] = "stop_verify"          # stop requested before registration
    run_verify_job(job.id, store, settings, api=VerifyApi(files), registry=registry)
    j = store.get_job(job.id)
    assert j.status == COMPLETED
    assert j.verify_status == "unverified"             # interrupted -> inconclusive
    assert j.downloaded_bytes == j.total_bytes
    store.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_backup_worker.py -q -k "verify"`
Expected: FAIL — `ImportError: cannot import name 'run_verify_job'` (and `local_dir_for` is importable already).

- [ ] **Step 3: Implement the worker changes**

In `app/backup.py`, update the imports near the top:

```python
import json
```
(add with the other stdlib imports), and change the db/verify imports:

```python
from .db import PAUSED, VERIFYING, COMPLETED
from .retry import is_retryable, BACKOFF_SECONDS, MAX_RETRIES
from .verify import verify_repo, expected_file_hashes, VerifyAborted
```

Add a helper next to `repo_total_bytes`:

```python
def repo_expected_hashes(slug, repo_type, token, api=None):
    api = api or HfApi()
    info = api.repo_info(
        repo_id=slug, repo_type=repo_type, token=token or None, files_metadata=True
    )
    return expected_file_hashes(info.siblings or [])
```

Add the stop handle near `RunningRegistry`:

```python
class _StopHandle:
    """Registry handle for the verify phase. terminate() sets the stop event the
    hashing loop checks between chunks — there's no child process to kill."""
    def __init__(self, stop) -> None:
        self._stop = stop

    def terminate(self) -> None:
        self._stop.set()
```

Add the verify phase and manual entry point (place after `run_backup_job`):

```python
def _verify_phase(job_id, store, settings, api=None, registry=None, stopping=None) -> None:
    """Hash a job's files against the Hub's reported hashes and record the verdict.
    Always lands the job at 'completed': verified, corrupted, or — if it was
    interrupted or the Hub was unreachable — unverified. Never deletes or requeues."""
    job = store.get_job(job_id)
    if job is None:
        return
    store.set_status(job_id, VERIFYING)
    local_dir = local_dir_for(settings.backup_dir, job.repo_type, job.slug)
    total = job.total_bytes

    # The caller (download path) clears the download handle/intent before calling
    # us, so we must NOT unregister at the top — on the manual path a stop_verify
    # intent may have been preset between submit and now, and the post-register
    # check below relies on it still being present. The single outer finally
    # always unregisters, clearing the handle and any (preset or leaked) intent.
    try:
        try:
            expected = repo_expected_hashes(job.slug, job.repo_type, settings.hf_token, api=api)
        except Exception:  # noqa: BLE001 - cannot-verify is NOT corruption
            store.update_progress(job_id, total)
            store.set_status(job_id, COMPLETED)
            store.set_verify_status(
                job_id, "unverified",
                detail=json.dumps({"note": "could not reach the Hub to verify; try again"}))
            return

        stop = threading.Event()
        if registry is not None:
            registry.register(job_id, _StopHandle(stop))
            # Honor a stop that landed between submit/claim and registration, or a closed valve.
            if (store.get_flag("paused_all", "0") == "1"
                    or registry.intent(job_id) is not None
                    or (stopping is not None and stopping.is_set())):
                stop.set()

        aborted = False
        try:
            report = verify_repo(
                local_dir, expected, stop=stop,
                on_progress=lambda n: store.update_progress(job_id, n))
        except VerifyAborted:
            aborted = True
            report = None

        store.update_progress(job_id, total)   # restore the bar to 100%
        store.set_status(job_id, COMPLETED)
        if aborted or (stopping is not None and stopping.is_set()):
            store.set_verify_status(job_id, "unverified", detail=None)
        elif report.ok:
            store.set_verify_status(job_id, "verified", detail=None)
        else:
            store.set_verify_status(
                job_id, "corrupted", detail=json.dumps({"failures": report.failures}))
    finally:
        if registry is not None:
            registry.unregister(job_id)


def run_verify_job(job_id, store, settings, api=None, registry=None, stopping=None) -> None:
    """Dispatcher/executor entry point for a manual verify on a completed job."""
    if store.get_job(job_id) is None:
        return
    _verify_phase(job_id, store, settings, api=api, registry=registry, stopping=stopping)
```

In `run_backup_job`, replace the success branch body:

```python
        if outcome is not None and outcome.ok:
            # Finished before any stop signal landed -> completion wins.
            final = directory_size(local_dir)
            store.update_progress(job_id, total if total else final, total_bytes=total)
            store.reset_retry(job_id)
            if settings.verify_downloads:
                # Drop the download handle/intent so verification starts from a
                # clean slate (a stale stop that lost to completion won't suppress it).
                if registry is not None:
                    registry.unregister(job_id)
                _verify_phase(job_id, store, settings, api=api,
                              registry=registry, stopping=stopping)
            else:
                store.set_status(job_id, "completed")
```

In `JobRunner`, add two methods (after `cancel`):

```python
    def verify(self, job_id) -> None:
        """Run an integrity check on a completed job (manual Verify button). Marks
        it 'verifying' up front so a second click is rejected and it occupies a
        slot, then runs on the shared executor (max_concurrent_jobs still bounds it)."""
        self._store.set_status(job_id, VERIFYING)
        self._executor.submit(
            run_verify_job, job_id, self._store, self._settings,
            self._api, self._registry, self._stopping,
        )

    def stop_verify(self, job_id) -> None:
        """Stop an in-progress verification; the worker lands it back at completed."""
        self._registry.request(job_id, "stop_verify")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_backup_worker.py -q`
Expected: PASS (new verify tests + all existing worker/dispatcher tests unchanged).

- [ ] **Step 5: Commit**

```bash
git add app/backup.py tests/test_backup_worker.py
git commit -m "feat(backup): post-download + manual verify phase, JobRunner.verify

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: API — verify / stop-verify / redownload endpoints + orphan reset

**Files:**
- Modify: `app/main.py` (import `VERIFYING`, lifespan reset, three endpoints)
- Test: `tests/test_api.py` (extend `FakeRunner`, add tests)

**Interfaces:**
- Consumes: `JobRunner.verify`/`stop_verify` (Task 4); `JobStore.reset_verifying_to_completed`, `set_verify_status`, `Job.verify_status` (Task 2).
- Produces: `POST /api/jobs/{id}/verify`, `POST /api/jobs/{id}/stop-verify`, `POST /api/jobs/{id}/redownload`.

- [ ] **Step 1: Write the failing tests**

In `tests/test_api.py`, extend `FakeRunner` with verify hooks (add inside `__init__` and as methods):

```python
        self.verified = []
        self.stop_verified = []
```
```python
    def verify(self, job_id):
        self.verified.append(job_id)

    def stop_verify(self, job_id):
        self.stop_verified.append(job_id)
```

Update the db import line to include `COMPLETED` and `VERIFYING`:

```python
from app.db import JobStore, FAILED, QUEUED, PAUSED, RUNNING, RETRYING, COMPLETED, VERIFYING
```

Add tests:

```python
def test_verify_only_completed(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    assert client.post(f"/api/jobs/{job.id}/verify").status_code == 409   # queued
    store.set_status(job.id, COMPLETED)
    resp = client.post(f"/api/jobs/{job.id}/verify")
    assert resp.status_code == 200
    assert job.id in runner.verified


def test_verify_missing_job_404(ctx):
    client, store, runner = ctx
    assert client.post("/api/jobs/999/verify").status_code == 404


def test_stop_verify_only_verifying(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    store.set_status(job.id, COMPLETED)
    assert client.post(f"/api/jobs/{job.id}/stop-verify").status_code == 409
    store.set_status(job.id, VERIFYING)
    resp = client.post(f"/api/jobs/{job.id}/stop-verify")
    assert resp.status_code == 200
    assert job.id in runner.stop_verified


def test_redownload_only_corrupted_deletes_and_requeues(ctx, tmp_path):
    client, store, runner = ctx
    from app.backup import local_dir_for
    backup = tmp_path / "backups"
    job = store.create_job("o/n", "model")
    d = local_dir_for(backup, "model", "o/n")
    d.mkdir(parents=True)
    (d / "model.bin").write_bytes(b"corrupt")
    store.set_status(job.id, COMPLETED)
    assert client.post(f"/api/jobs/{job.id}/redownload").status_code == 409   # not corrupted
    store.set_verify_status(job.id, "corrupted", detail='{"failures": []}')
    resp = client.post(f"/api/jobs/{job.id}/redownload")
    assert resp.status_code == 200
    j = store.get_job(job.id)
    assert j.status == QUEUED
    assert j.verify_status == "unverified"
    assert j.verify_detail is None
    assert not d.exists()                          # corrupt files discarded


def test_redownload_missing_job_404(ctx):
    client, store, runner = ctx
    assert client.post("/api/jobs/999/redownload").status_code == 404


def test_verify_fields_in_list(ctx):
    client, store, runner = ctx
    store.create_job("a/b", "model")
    j = client.get("/api/jobs").json()["jobs"][0]
    assert j["verify_status"] == "unverified"
    assert j["verify_detail"] is None


def test_startup_resets_orphaned_verifying(tmp_path):
    settings = make_settings(tmp_path)
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)
    j = store.create_job("v/me", "model")
    store.set_status(j.id, VERIFYING)
    store.update_progress(j.id, 3, total_bytes=10)
    runner = FakeRunner()
    app = create_app(settings, store, runner, detect=lambda s, t: [])
    with TestClient(app):
        pass
    g = store.get_job(j.id)
    assert g.status == COMPLETED and g.verify_status == "unverified"
    assert g.downloaded_bytes == 10
    store.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_api.py -q -k "verify or redownload"`
Expected: FAIL — endpoints return 404/405 or `AttributeError` (no `verify` route).

- [ ] **Step 3: Implement the endpoints and reset**

In `app/main.py`, update the db import to include `VERIFYING`:

```python
from .db import COMPLETED, FAILED, JobStore, PAUSED, QUEUED, RETRYING, RUNNING, VERIFYING
```

In the lifespan hook, add the orphan reset next to the running reset:

```python
        store.reset_running_to_queued()
        store.reset_verifying_to_completed()
        runner.start()
```

Add the three endpoints (place after the `cancel` endpoint, before `pause_all`):

```python
    @app.post("/api/jobs/{job_id}/verify")
    def verify(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != COMPLETED:
            raise HTTPException(status_code=409, detail="only completed downloads can be verified")
        runner.verify(job_id)
        return {"verifying": job_id}

    @app.post("/api/jobs/{job_id}/stop-verify")
    def stop_verify(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != VERIFYING:
            raise HTTPException(status_code=409, detail="only verifying jobs can be stopped")
        runner.stop_verify(job_id)
        return {"stopping": job_id}

    @app.post("/api/jobs/{job_id}/redownload")
    def redownload(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.verify_status != "corrupted":
            raise HTTPException(status_code=409, detail="only corrupted downloads can be re-downloaded")
        delete_backup_files(settings.backup_dir, job.repo_type, job.slug)
        store.requeue(job_id)
        store.reset_retry(job_id)
        store.set_verify_status(job_id, "unverified", detail=None)
        return store.get_job(job_id).to_dict()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/test_api.py -q`
Expected: PASS (new + existing API tests).

- [ ] **Step 5: Commit**

```bash
git add app/main.py tests/test_api.py
git commit -m "feat(api): verify, stop-verify, redownload endpoints + orphan reset

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Dashboard — verify badges, buttons, failure list

**Files:**
- Modify: `app/static/index.html` (CSS, `verifyBadge`/`verifyFailures`/`confirmRedownload` helpers, `row()` actions + cells)
- Test: `tests/test_static.py` (string-presence assertions)

**Interfaces:**
- Consumes: `verify_status`/`verify_detail` on each job from `/api/jobs`; endpoints `verify`, `stop-verify`, `redownload`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_static.py`:

```python
def test_verify_ui_present(client):
    page = client.get("/").text
    assert "verifyBadge" in page          # verified / corrupted / unverified badge helper
    assert "confirmRedownload" in page    # corrupted -> re-download control
    assert "stop-verify" in page          # stop an in-progress verification
    assert "/verify" in page or "'verify'" in page  # manual verify wired
    assert ".st.verifying" in page        # verifying status color rule
    assert ".vbadge" in page              # verify badge style
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_static.py -q -k verify_ui`
Expected: FAIL — none of those strings are in the page yet.

- [ ] **Step 3: Implement the UI**

In `app/static/index.html`, add CSS rules inside `<style>` (next to the `.st.*` rules):

```css
.st.verifying{color:#d2a24c}
.vbadge{display:inline-block;margin-left:8px;font-size:11px;font-weight:600;vertical-align:middle}
.vbadge.ok{color:var(--ok)}.vbadge.err{color:var(--err)}.vbadge.muted{color:var(--muted)}
```

Add helper functions in `<script>` (just before `function row(j)`):

```javascript
function verifyBadge(j) {
  if (j.status !== "completed") return "";
  if (j.verify_status === "verified") return `<span class="vbadge ok">✓ verified</span>`;
  if (j.verify_status === "corrupted") return `<span class="vbadge err">⚠ corrupted</span>`;
  let note = "";
  try { const d = JSON.parse(j.verify_detail || "null"); if (d && d.note) note = ` · ${esc(d.note)}`; }
  catch (e) {}
  return `<span class="vbadge muted">unverified${note}</span>`;
}
function verifyFailures(j) {
  if (j.verify_status !== "corrupted") return "";
  let items = "", n = 0;
  try {
    const d = JSON.parse(j.verify_detail || "null");
    const fs = (d && d.failures) || [];
    n = fs.length;
    items = fs.slice(0, 8).map(f => `${esc(f.file)} (${esc(f.reason)})`).join("<br>");
    if (n > 8) items += `<br>…and ${n - 8} more`;
  } catch (e) {}
  return `<div class="err">⚠ integrity check failed${items ? `:<br>${items}` : ""}</div>`;
}
function confirmRedownload(id, slug) {
  if (!confirm(`Re-download ${slug}? This deletes the current (corrupted) files and downloads them again.`)) return;
  act(id, "redownload");
}
window.confirmRedownload = confirmRedownload;
```

In `row(j)`, extend the `actions` ternary. Add a `verifying` branch at the top and replace the `completed` branch:

```javascript
  const actions =
      j.status === "running"
    ? `<button class="ghost" onclick="act(${j.id},'pause')">Pause</button>`
      + `<button class="ghost" onclick="confirmCancel(${j.id},'${esc(j.slug)}',${j.downloaded_bytes})">Cancel</button>`
    : j.status === "verifying"
    ? `<button class="ghost" onclick="act(${j.id},'stop-verify')">Stop</button>`
    : j.status === "paused"
    ? `<button class="ghost" onclick="act(${j.id},'resume')">Resume</button>`
      + `<button class="ghost" onclick="confirmCancel(${j.id},'${esc(j.slug)}',${j.downloaded_bytes})">Cancel</button>`
    : j.status === "retrying"
    ? `<button class="ghost" onclick="confirmCancel(${j.id},'${esc(j.slug)}',${j.downloaded_bytes})">Cancel</button>`
    : j.status === "failed"
    ? `<button class="ghost" onclick="act(${j.id},'retry')">Retry</button>`
    : j.status === "queued"
    ? `<button class="ghost" onclick="act(${j.id},'cancel')">Cancel</button>`
    : j.status === "completed"
    ? (j.verify_status === "corrupted"
        ? `<button class="ghost" onclick="confirmRedownload(${j.id},'${esc(j.slug)}')">Re-download</button>`
          + `<button class="ghost" onclick="confirmDelete(${j.id},'${esc(j.slug)}',${j.total_bytes})">Delete</button>`
        : `<button class="ghost" onclick="act(${j.id},'verify')">Verify</button>`
          + `<button class="ghost" onclick="confirmDelete(${j.id},'${esc(j.slug)}',${j.total_bytes})">Delete</button>`)
    : "";
```

In the same `row(j)` return template, add the verify badge to the status cell and the failure list to the repo cell. Change the repo `<td>` and the status `<td>`:

```javascript
  return `<tr>
    <td><div class="repo">${esc(j.slug)}</div><span class="badge ${esc(j.repo_type)}">${esc(j.repo_type)}</span>${j.error ? `<div class="err">${esc(j.error)}</div>` : ""}${verifyFailures(j)}</td>
    <td><span class="st ${esc(j.status)}">${esc(label)}</span>${verifyBadge(j)}</td>
    <td><div class="bar"><span style="width:${j.percent}%"></span></div><div class="type">${j.percent}%</div></td>
    <td class="size">${fmt(j.downloaded_bytes)} / ${fmt(j.total_bytes)}</td>
    <td>${actions}</td></tr>`;
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `.venv/bin/python -m pytest tests/test_static.py -q`
Expected: PASS (new + existing static tests).

- [ ] **Step 5: Commit**

```bash
git add app/static/index.html tests/test_static.py
git commit -m "feat(ui): verify badge, Verify/Stop/Re-download buttons, failure list

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Docs — CLAUDE.md and .env.example

**Files:**
- Modify: `CLAUDE.md`
- Modify: `.env.example`

**Interfaces:** none (documentation only).

- [ ] **Step 1: Update `.env.example`**

Add after the `DB_PATH` block (before the bind-address section):

```bash
# Verify every finished download against Hugging Face's reported hashes (SHA256
# for LFS weights, git-blob-SHA1 for plain files) before showing it as done.
# Set 0 to skip the post-download hash pass on slow / very large-repo hosts;
# the manual Verify button on the dashboard always works regardless.
VERIFY_DOWNLOADS=1
```

- [ ] **Step 2: Update `CLAUDE.md`**

Make these edits in the Architecture section:

- In the `db.py` bullet, extend the status lifecycle and note the new columns:
  `queued → running → verifying → completed | failed | paused | retrying`, and add that the `verifying` status is transient (counts toward a concurrency slot), with `verify_status` (`unverified | verified | corrupted`) and `verify_detail` (JSON: `{"failures":[…]}` when corrupted, `{"note":…}` when the Hub was unreachable) carrying the outcome. Note the migration adds these two columns, and that `reset_verifying_to_completed()` rescues an orphaned `verifying` job on startup.
- In the `backup.py` bullet, add: after a successful download the worker runs a `_verify_phase` (gated by `VERIFY_DOWNLOADS`) that hashes the files via `app/verify.py` cooperatively in-thread (a stop `Event` checked between chunks, registered in `RunningRegistry` like a download), then records `verified`/`corrupted`. A manual `run_verify_job` (via `JobRunner.verify`) does the same on demand. Interrupting a verify always returns the job to `completed`/`unverified` — never deletes or requeues.
- Add a new module bullet for **`app/verify.py`** — the pure hash core (`sha256_file`, `git_blob_sha1`, `expected_file_hashes`, `verify_repo`); LFS→sha256, plain git→git-blob-sha1.
- In the `main.py` bullet, add the new endpoints: `POST /api/jobs/{id}/verify|stop-verify|redownload`, and that the lifespan also resets orphaned `verifying` jobs.
- In the `config.py` bullet, add `VERIFY_DOWNLOADS` (default on) to the optional env vars.
- Add a third entry to "Two things that are easy to get wrong" (retitle to "Things that are easy to get wrong"): **Integrity uses two hash algorithms** — HF reports SHA256 only for LFS files (`lfs.sha256`); plain git files have only a git blob OID (`blob_id` = `sha1("blob <len>\0"+bytes)`), so `verify_repo` checks each with the right one. And **cannot-verify ≠ corrupted**: a Hub lookup failure leaves the job `completed`/`unverified` with a note, not `corrupted`.

- [ ] **Step 3: Run the full suite**

Run: `.venv/bin/python -m pytest -q`
Expected: PASS — entire offline suite green.

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md .env.example
git commit -m "docs: document SHA256 integrity verification feature

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification

- [ ] **Run the full offline suite:** `.venv/bin/python -m pytest -q` → all pass.
- [ ] **Optional manual smoke:** run the app (`.venv/bin/python -m app.main`), back up a small public repo (e.g. `hf-internal-testing/tiny-random-gpt2`), confirm it ends `completed` with **✓ verified**; then click **Verify** on it again and watch the `verifying` state and progress; corrupt a file on disk and click Verify to see **⚠ corrupted** + the failed-file list + **Re-download**.
- [ ] **Merge** per the repo workflow: `git checkout master && git merge --no-ff feature/sha256-integrity-verification`, then push.
```
