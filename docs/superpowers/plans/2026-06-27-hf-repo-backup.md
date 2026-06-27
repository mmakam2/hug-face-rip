# HF Repo Backup Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A small FastAPI web app that backs up entire Hugging Face Hub repos (model/dataset/space) to a local folder, with bounded concurrency, live progress, and automatic resume.

**Architecture:** FastAPI serves a polling dashboard and a JSON API. A bounded `ThreadPoolExecutor` runs downloads via `huggingface_hub.snapshot_download(local_dir=...)`, which gives parallel file downloads, integrity checks, and automatic resume for free. Jobs persist in SQLite so they survive restarts; on startup, unfinished jobs are re-queued and resume from disk.

**Tech Stack:** Python 3.10+, FastAPI, Uvicorn, `huggingface_hub`, `python-dotenv`, Pydantic v2, pytest.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-06-27-hf-repo-backup-design.md`.
- `.env` variables (exact names): `HUGGINGFACE_ACCESS_KEY` (required), `BACKUP_DIR` (required), `MAX_CONCURRENT_JOBS` (default `2`), `MAX_WORKERS` (default `8`), `DB_PATH` (default `jobs.db`).
- A repo's unique identity is the tuple **`(repo_type, slug)`**. Enforced by `UNIQUE(repo_type, slug)` in SQLite.
- Backup destination per repo: `BACKUP_DIR/<repo_type>s/<slug>` (e.g. `BACKUP_DIR/datasets/bigcode/the-stack`). The `s` pluralizes the type; the slug keeps its `/`.
- Repo type is auto-detected by probing `model`, `dataset`, `space`; **every** matching type becomes its own job.
- Job statuses: `queued`, `running`, `completed`, `failed`, `cancelled`.
- Never print `.env` contents or token values. Reference variables by name only.
- TDD throughout: failing test → run (fail) → minimal impl → run (pass) → commit. Unit tests mock the Hub; one integration test (marked `integration`) hits the real Hub.
- Run all commands from the repo root `/root/hug-face-rip`.

---

### Task 1: Project setup & dependencies

**Files:**
- Create: `requirements.txt`
- Create: `pytest.ini`
- Create: `.env.example`
- Create: `app/__init__.py`
- Create: `tests/__init__.py`
- Test: `tests/test_smoke.py`

**Interfaces:**
- Consumes: nothing.
- Produces: the `app` package (importable) and the test harness used by all later tasks.

- [ ] **Step 1: Write `requirements.txt`**

```
fastapi>=0.110
uvicorn[standard]>=0.29
huggingface_hub>=0.23
python-dotenv>=1.0
pydantic>=2.0
pytest>=8.0
httpx>=0.27
```

- [ ] **Step 2: Write `pytest.ini`** (integration tests skipped by default)

```ini
[pytest]
markers =
    integration: hits the real Hugging Face Hub (requires network)
addopts = -q -m "not integration"
testpaths = tests
```

- [ ] **Step 3: Write `.env.example`** (template only — real `.env` is gitignored)

```
HUGGINGFACE_ACCESS_KEY=hf_your_token_here
BACKUP_DIR=./backups
MAX_CONCURRENT_JOBS=2
MAX_WORKERS=8
DB_PATH=jobs.db
```

- [ ] **Step 4: Create empty package files**

Create `app/__init__.py` and `tests/__init__.py`, both empty.

- [ ] **Step 5: Write the failing smoke test** — `tests/test_smoke.py`

```python
def test_app_package_imports():
    import app
    assert app is not None
```

- [ ] **Step 6: Install deps and run the smoke test**

Run:
```bash
python -m pip install -r requirements.txt
python -m pytest tests/test_smoke.py -v
```
Expected: PASS (`app` package imports).

- [ ] **Step 7: Commit**

```bash
git add requirements.txt pytest.ini .env.example app/__init__.py tests/__init__.py tests/test_smoke.py
git commit -m "chore: project scaffolding and dependencies"
```

---

### Task 2: Configuration loader (`config.py`)

**Files:**
- Create: `app/config.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: `.env` variables (via an injectable mapping for tests).
- Produces:
  - `class ConfigError(Exception)`
  - `@dataclass(frozen=True) Settings` with fields `hf_token: str`, `backup_dir: Path`, `max_concurrent_jobs: int`, `max_workers: int`, `db_path: Path`.
  - `load_settings(env: Mapping[str, str] | None = None) -> Settings`

- [ ] **Step 1: Write the failing tests** — `tests/test_config.py`

```python
import pytest
from app.config import load_settings, Settings, ConfigError


def base_env(tmp_path):
    return {
        "HUGGINGFACE_ACCESS_KEY": "hf_test",
        "BACKUP_DIR": str(tmp_path / "backups"),
    }


def test_loads_required_values_and_defaults(tmp_path):
    s = load_settings(base_env(tmp_path))
    assert isinstance(s, Settings)
    assert s.hf_token == "hf_test"
    assert s.backup_dir.exists()          # created if missing
    assert s.max_concurrent_jobs == 2     # default
    assert s.max_workers == 8             # default
    assert s.db_path.name == "jobs.db"    # default


def test_custom_numeric_values(tmp_path):
    env = base_env(tmp_path) | {"MAX_CONCURRENT_JOBS": "5", "MAX_WORKERS": "16", "DB_PATH": "/tmp/x.db"}
    s = load_settings(env)
    assert s.max_concurrent_jobs == 5
    assert s.max_workers == 16
    assert str(s.db_path) == "/tmp/x.db"


def test_missing_token_raises(tmp_path):
    env = base_env(tmp_path)
    del env["HUGGINGFACE_ACCESS_KEY"]
    with pytest.raises(ConfigError, match="HUGGINGFACE_ACCESS_KEY"):
        load_settings(env)


def test_missing_backup_dir_raises(tmp_path):
    env = base_env(tmp_path)
    del env["BACKUP_DIR"]
    with pytest.raises(ConfigError, match="BACKUP_DIR"):
        load_settings(env)


def test_unconstructable_backup_dir_raises(tmp_path):
    blocker = tmp_path / "blocker"
    blocker.write_text("i am a file")
    env = {"HUGGINGFACE_ACCESS_KEY": "hf_test", "BACKUP_DIR": str(blocker / "sub")}
    with pytest.raises(ConfigError, match="BACKUP_DIR"):
        load_settings(env)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_config.py -v`
Expected: FAIL (`ModuleNotFoundError: app.config`).

- [ ] **Step 3: Write `app/config.py`**

```python
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Optional


class ConfigError(Exception):
    """Raised when required configuration is missing or invalid."""


@dataclass(frozen=True)
class Settings:
    hf_token: str
    backup_dir: Path
    max_concurrent_jobs: int
    max_workers: int
    db_path: Path


def _int(env: Mapping[str, str], key: str, default: int) -> int:
    raw = env.get(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(f"{key} must be an integer, got {raw!r}")


def load_settings(env: Optional[Mapping[str, str]] = None) -> Settings:
    env = os.environ if env is None else env

    token = env.get("HUGGINGFACE_ACCESS_KEY")
    if not token:
        raise ConfigError("HUGGINGFACE_ACCESS_KEY is not set")

    backup_dir_raw = env.get("BACKUP_DIR")
    if not backup_dir_raw:
        raise ConfigError("BACKUP_DIR is not set")

    backup_dir = Path(backup_dir_raw).expanduser()
    try:
        backup_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ConfigError(f"cannot create BACKUP_DIR {backup_dir}: {exc}")
    if not os.access(backup_dir, os.W_OK):
        raise ConfigError(f"BACKUP_DIR {backup_dir} is not writable")

    return Settings(
        hf_token=token,
        backup_dir=backup_dir,
        max_concurrent_jobs=_int(env, "MAX_CONCURRENT_JOBS", 2),
        max_workers=_int(env, "MAX_WORKERS", 8),
        db_path=Path(env.get("DB_PATH") or "jobs.db"),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_config.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add app/config.py tests/test_config.py
git commit -m "feat: env-driven settings loader with validation"
```

---

### Task 3: Job store (`db.py`)

**Files:**
- Create: `app/db.py`
- Test: `tests/test_db.py`

**Interfaces:**
- Consumes: a SQLite path (`Settings.db_path`).
- Produces:
  - Status constants `QUEUED, RUNNING, COMPLETED, FAILED, CANCELLED` (strings).
  - `@dataclass Job` with `id, slug, repo_type, status, total_bytes, downloaded_bytes, error, created_at, updated_at`; `.percent -> float`; `.to_dict() -> dict` (adds `percent`).
  - `class JobStore` with `create_job(slug, repo_type) -> Job`, `get_job(job_id) -> Job | None`, `get_job_by_repo(repo_type, slug) -> Job | None`, `list_jobs() -> list[Job]`, `update_progress(job_id, downloaded_bytes, total_bytes=None)`, `set_status(job_id, status, error=None)`, `requeue(job_id)`, `unfinished_jobs() -> list[Job]`, `close()`.

- [ ] **Step 1: Write the failing tests** — `tests/test_db.py`

```python
import sqlite3
import pytest
from app.db import JobStore, Job, QUEUED, RUNNING, COMPLETED, FAILED


@pytest.fixture
def store(tmp_path):
    s = JobStore(tmp_path / "jobs.db")
    yield s
    s.close()


def test_create_and_get(store):
    job = store.create_job("owner/name", "model")
    assert job.id > 0
    assert job.status == QUEUED
    assert job.total_bytes == 0 and job.downloaded_bytes == 0
    assert store.get_job(job.id).slug == "owner/name"


def test_unique_repo_type_slug(store):
    store.create_job("owner/name", "model")
    with pytest.raises(sqlite3.IntegrityError):
        store.create_job("owner/name", "model")
    # same slug, different type is allowed
    other = store.create_job("owner/name", "dataset")
    assert other.id > 0


def test_get_job_by_repo(store):
    store.create_job("owner/name", "model")
    assert store.get_job_by_repo("model", "owner/name") is not None
    assert store.get_job_by_repo("dataset", "owner/name") is None


def test_update_progress_and_percent(store):
    job = store.create_job("a/b", "model")
    store.update_progress(job.id, downloaded_bytes=50, total_bytes=200)
    refreshed = store.get_job(job.id)
    assert refreshed.downloaded_bytes == 50
    assert refreshed.total_bytes == 200
    assert refreshed.percent == 25.0
    # downloaded-only update keeps total
    store.update_progress(job.id, downloaded_bytes=100)
    assert store.get_job(job.id).total_bytes == 200


def test_percent_zero_total(store):
    job = store.create_job("a/b", "model")
    assert store.get_job(job.id).percent == 0.0


def test_set_status_and_error(store):
    job = store.create_job("a/b", "model")
    store.set_status(job.id, FAILED, error="boom")
    j = store.get_job(job.id)
    assert j.status == FAILED and j.error == "boom"
    store.set_status(job.id, COMPLETED)   # clears error
    assert store.get_job(job.id).error is None


def test_requeue_clears_error_keeps_progress(store):
    job = store.create_job("a/b", "model")
    store.update_progress(job.id, 80, 100)
    store.set_status(job.id, FAILED, error="x")
    store.requeue(job.id)
    j = store.get_job(job.id)
    assert j.status == QUEUED and j.error is None and j.downloaded_bytes == 80


def test_unfinished_jobs(store):
    a = store.create_job("a/b", "model")             # queued
    b = store.create_job("c/d", "dataset")
    store.set_status(b.id, RUNNING)
    c = store.create_job("e/f", "space")
    store.set_status(c.id, COMPLETED)
    ids = {j.id for j in store.unfinished_jobs()}
    assert ids == {a.id, b.id}


def test_to_dict_includes_percent(store):
    job = store.create_job("a/b", "model")
    store.update_progress(job.id, 1, 4)
    d = store.get_job(job.id).to_dict()
    assert d["percent"] == 25.0 and d["slug"] == "a/b"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_db.py -v`
Expected: FAIL (`ModuleNotFoundError: app.db`).

- [ ] **Step 3: Write `app/db.py`**

```python
import sqlite3
import threading
from dataclasses import asdict, dataclass
from typing import List, Optional

QUEUED = "queued"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"
CANCELLED = "cancelled"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT NOT NULL,
    repo_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    total_bytes INTEGER NOT NULL DEFAULT 0,
    downloaded_bytes INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(repo_type, slug)
);
"""


@dataclass
class Job:
    id: int
    slug: str
    repo_type: str
    status: str
    total_bytes: int
    downloaded_bytes: int
    error: Optional[str]
    created_at: str
    updated_at: str

    @property
    def percent(self) -> float:
        if self.total_bytes <= 0:
            return 0.0
        return min(100.0, round(self.downloaded_bytes / self.total_bytes * 100, 1))

    def to_dict(self) -> dict:
        data = asdict(self)
        data["percent"] = self.percent
        return data


class JobStore:
    def __init__(self, db_path) -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def _to_job(self, row: sqlite3.Row) -> Job:
        return Job(**{key: row[key] for key in row.keys()})

    def create_job(self, slug: str, repo_type: str) -> Job:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO jobs (slug, repo_type) VALUES (?, ?)", (slug, repo_type)
            )
            self._conn.commit()
            job_id = cur.lastrowid
        return self.get_job(job_id)

    def get_job(self, job_id: int) -> Optional[Job]:
        with self._lock:
            row = self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._to_job(row) if row else None

    def get_job_by_repo(self, repo_type: str, slug: str) -> Optional[Job]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE repo_type = ? AND slug = ?", (repo_type, slug)
            ).fetchone()
        return self._to_job(row) if row else None

    def list_jobs(self) -> List[Job]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM jobs ORDER BY id DESC").fetchall()
        return [self._to_job(r) for r in rows]

    def update_progress(self, job_id: int, downloaded_bytes: int, total_bytes: Optional[int] = None) -> None:
        with self._lock:
            if total_bytes is None:
                self._conn.execute(
                    "UPDATE jobs SET downloaded_bytes = ?, updated_at = datetime('now') WHERE id = ?",
                    (downloaded_bytes, job_id),
                )
            else:
                self._conn.execute(
                    "UPDATE jobs SET downloaded_bytes = ?, total_bytes = ?, updated_at = datetime('now') WHERE id = ?",
                    (downloaded_bytes, total_bytes, job_id),
                )
            self._conn.commit()

    def set_status(self, job_id: int, status: str, error: Optional[str] = None) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status = ?, error = ?, updated_at = datetime('now') WHERE id = ?",
                (status, error, job_id),
            )
            self._conn.commit()

    def requeue(self, job_id: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status = 'queued', error = NULL, updated_at = datetime('now') WHERE id = ?",
                (job_id,),
            )
            self._conn.commit()

    def unfinished_jobs(self) -> List[Job]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE status IN ('queued', 'running') ORDER BY id"
            ).fetchall()
        return [self._to_job(r) for r in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_db.py -v`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add app/db.py tests/test_db.py
git commit -m "feat: SQLite job store with (repo_type, slug) identity"
```

---

### Task 4: Backup helpers — detection, sizing, paths (`backup.py` part 1)

**Files:**
- Create: `app/backup.py`
- Test: `tests/test_backup_helpers.py`

**Interfaces:**
- Consumes: `huggingface_hub.HfApi` (injectable as `api`), `Settings`.
- Produces:
  - `REPO_TYPES = ["model", "dataset", "space"]`
  - `detect_repo_types(slug: str, token: str, api=None) -> list[str]`
  - `repo_total_bytes(slug: str, repo_type: str, token: str, api=None) -> int`
  - `local_dir_for(backup_dir: Path, repo_type: str, slug: str) -> Path`
  - `directory_size(path: Path) -> int` (excludes the `.cache` resume-metadata subtree)

- [ ] **Step 1: Write the failing tests** — `tests/test_backup_helpers.py`

```python
from pathlib import Path
import pytest
from huggingface_hub.utils import RepositoryNotFoundError
from app.backup import detect_repo_types, repo_total_bytes, local_dir_for, directory_size


class _Sibling:
    def __init__(self, size):
        self.size = size


class _Info:
    def __init__(self, siblings):
        self.siblings = siblings


class FakeApi:
    """repo_info returns a mapping value or raises RepositoryNotFoundError."""
    def __init__(self, table):
        self.table = table  # {(slug, repo_type): _Info}
        self.calls = []

    def repo_info(self, repo_id, repo_type, token=None, files_metadata=False):
        self.calls.append((repo_id, repo_type))
        value = self.table.get((repo_id, repo_type))
        if value is None:
            raise RepositoryNotFoundError(f"{repo_id} ({repo_type}) not found")
        return value


def test_detect_returns_all_matching_types():
    api = FakeApi({("o/n", "model"): _Info([]), ("o/n", "dataset"): _Info([])})
    assert detect_repo_types("o/n", "tok", api=api) == ["model", "dataset"]


def test_detect_none_match_returns_empty():
    api = FakeApi({})
    assert detect_repo_types("ghost/repo", "tok", api=api) == []


def test_repo_total_bytes_sums_siblings():
    api = FakeApi({("o/n", "model"): _Info([_Sibling(100), _Sibling(250), _Sibling(None)])})
    assert repo_total_bytes("o/n", "model", "tok", api=api) == 350


def test_local_dir_for_pluralizes_type_and_keeps_slug():
    base = Path("/backups")
    assert local_dir_for(base, "dataset", "bigcode/the-stack") == base / "datasets" / "bigcode" / "the-stack"
    assert local_dir_for(base, "model", "gpt2") == base / "models" / "gpt2"


def test_directory_size_counts_files_excluding_cache(tmp_path):
    (tmp_path / "a.bin").write_bytes(b"x" * 10)
    sub = tmp_path / "nested"
    sub.mkdir()
    (sub / "b.bin").write_bytes(b"y" * 5)
    cache = tmp_path / ".cache" / "huggingface"
    cache.mkdir(parents=True)
    (cache / "meta").write_bytes(b"z" * 1000)
    assert directory_size(tmp_path) == 15


def test_directory_size_missing_path_is_zero(tmp_path):
    assert directory_size(tmp_path / "nope") == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_backup_helpers.py -v`
Expected: FAIL (`ImportError`/`ModuleNotFoundError: app.backup`).

- [ ] **Step 3: Write the helper portion of `app/backup.py`**

```python
from pathlib import Path
from typing import List, Optional

from huggingface_hub import HfApi
from huggingface_hub.utils import (
    GatedRepoError,
    HfHubHTTPError,
    RepositoryNotFoundError,
)

REPO_TYPES = ["model", "dataset", "space"]


def detect_repo_types(slug: str, token: str, api: Optional[HfApi] = None) -> List[str]:
    api = api or HfApi()
    found: List[str] = []
    for repo_type in REPO_TYPES:
        try:
            api.repo_info(repo_id=slug, repo_type=repo_type, token=token or None)
            found.append(repo_type)
        except (RepositoryNotFoundError, GatedRepoError, HfHubHTTPError):
            continue
    return found


def repo_total_bytes(slug: str, repo_type: str, token: str, api: Optional[HfApi] = None) -> int:
    api = api or HfApi()
    info = api.repo_info(
        repo_id=slug, repo_type=repo_type, token=token or None, files_metadata=True
    )
    return sum((sibling.size or 0) for sibling in (info.siblings or []))


def local_dir_for(backup_dir: Path, repo_type: str, slug: str) -> Path:
    return Path(backup_dir) / f"{repo_type}s" / slug


def directory_size(path: Path) -> int:
    path = Path(path)
    if not path.exists():
        return 0
    total = 0
    for item in path.rglob("*"):
        if item.is_file() and ".cache" not in item.relative_to(path).parts:
            total += item.stat().st_size
    return total
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_backup_helpers.py -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add app/backup.py tests/test_backup_helpers.py
git commit -m "feat: repo-type detection, size, and path helpers"
```

---

### Task 5: Backup worker & runner (`backup.py` part 2)

**Files:**
- Modify: `app/backup.py` (append worker + runner)
- Test: `tests/test_backup_worker.py`

**Interfaces:**
- Consumes: `JobStore` (Task 3), `Settings` (Task 2), helpers (Task 4).
- Produces:
  - `POLL_INTERVAL = 1.5`
  - `run_backup_job(job_id, store, settings, api=None, downloader=None) -> None` — sets `running`, sizes the repo, downloads to `local_dir_for(...)`, polls on-disk progress, marks `completed` or `failed`. Skips if the job is missing or already `cancelled`. `downloader` defaults to `huggingface_hub.snapshot_download`; signature used: `downloader(repo_id=, repo_type=, local_dir=, token=, max_workers=)`.
  - `class JobRunner` with `__init__(self, store, settings, api=None, downloader=None)`, `submit(job_id)`, `shutdown()`. Bounded by `ThreadPoolExecutor(max_workers=settings.max_concurrent_jobs)`.

- [ ] **Step 1: Write the failing tests** — `tests/test_backup_worker.py`

```python
from pathlib import Path
import pytest
from app.config import Settings
from app.db import JobStore, COMPLETED, FAILED, CANCELLED
from app.backup import run_backup_job, JobRunner


class _Sibling:
    def __init__(self, size):
        self.size = size


class _Info:
    def __init__(self, siblings):
        self.siblings = siblings


class FakeApi:
    def __init__(self, total):
        self._total = total

    def repo_info(self, repo_id, repo_type, token=None, files_metadata=False):
        return _Info([_Sibling(self._total)])


def make_settings(tmp_path, max_jobs=2):
    return Settings(
        hf_token="hf_test",
        backup_dir=tmp_path / "backups",
        max_concurrent_jobs=max_jobs,
        max_workers=4,
        db_path=tmp_path / "jobs.db",
    )


def fake_downloader_factory(payload=b"hello-world"):
    def _download(repo_id, repo_type, local_dir, token, max_workers):
        target = Path(local_dir)
        target.mkdir(parents=True, exist_ok=True)
        (target / "model.bin").write_bytes(payload)
    return _download


def test_worker_completes_and_writes_files(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    payload = b"x" * 42
    run_backup_job(job.id, store, settings, api=FakeApi(42),
                   downloader=fake_downloader_factory(payload))
    done = store.get_job(job.id)
    assert done.status == COMPLETED
    assert done.total_bytes == 42
    assert done.downloaded_bytes == 42
    assert (tmp_path / "backups" / "models" / "o" / "n" / "model.bin").read_bytes() == payload
    store.close()


def test_worker_marks_failed_on_download_error(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")

    def boom(**kwargs):
        raise RuntimeError("network exploded")

    run_backup_job(job.id, store, settings, api=FakeApi(10), downloader=boom)
    failed = store.get_job(job.id)
    assert failed.status == FAILED
    assert "network exploded" in failed.error
    store.close()


def test_worker_skips_cancelled_job(tmp_path):
    settings = make_settings(tmp_path)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    store.set_status(job.id, CANCELLED)
    called = []

    def downloader(**kwargs):
        called.append(True)

    run_backup_job(job.id, store, settings, api=FakeApi(10), downloader=downloader)
    assert called == []
    assert store.get_job(job.id).status == CANCELLED
    store.close()


def test_runner_runs_job_to_completion(tmp_path):
    settings = make_settings(tmp_path, max_jobs=1)
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model")
    runner = JobRunner(store, settings, api=FakeApi(11),
                       downloader=fake_downloader_factory(b"y" * 11))
    runner.submit(job.id)
    runner.shutdown()  # waits for completion
    assert store.get_job(job.id).status == COMPLETED
    store.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_backup_worker.py -v`
Expected: FAIL (`ImportError: cannot import name 'run_backup_job'`).

- [ ] **Step 3: Append worker + runner to `app/backup.py`**

Add these imports at the top of `app/backup.py` (alongside the existing imports):

```python
import threading
from concurrent.futures import ThreadPoolExecutor
```

Then append to the end of `app/backup.py`:

```python
POLL_INTERVAL = 1.5


def run_backup_job(job_id, store, settings, api=None, downloader=None) -> None:
    if downloader is None:
        from huggingface_hub import snapshot_download
        downloader = snapshot_download

    job = store.get_job(job_id)
    if job is None or job.status == "cancelled":
        return

    store.set_status(job_id, "running")
    local_dir = local_dir_for(settings.backup_dir, job.repo_type, job.slug)
    local_dir.mkdir(parents=True, exist_ok=True)

    stop = threading.Event()

    def _poll():
        while not stop.is_set():
            store.update_progress(job_id, directory_size(local_dir))
            stop.wait(POLL_INTERVAL)

    poller = threading.Thread(target=_poll, daemon=True)
    try:
        total = repo_total_bytes(job.slug, job.repo_type, settings.hf_token, api=api)
        store.update_progress(job_id, directory_size(local_dir), total_bytes=total)
        poller.start()
        downloader(
            repo_id=job.slug,
            repo_type=job.repo_type,
            local_dir=str(local_dir),
            token=settings.hf_token or None,
            max_workers=settings.max_workers,
        )
        stop.set()
        poller.join(timeout=2)
        final = directory_size(local_dir)
        store.update_progress(job_id, total if total else final, total_bytes=total)
        store.set_status(job_id, "completed")
    except Exception as exc:  # noqa: BLE001 - surface any failure on the job
        stop.set()
        poller.join(timeout=2)
        store.set_status(job_id, "failed", error=str(exc)[:500])


class JobRunner:
    def __init__(self, store, settings, api=None, downloader=None) -> None:
        self._store = store
        self._settings = settings
        self._api = api
        self._downloader = downloader
        self._executor = ThreadPoolExecutor(max_workers=settings.max_concurrent_jobs)

    def submit(self, job_id) -> None:
        self._executor.submit(
            run_backup_job, job_id, self._store, self._settings, self._api, self._downloader
        )

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_backup_worker.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Run the whole suite to confirm nothing regressed**

Run: `python -m pytest -v`
Expected: PASS (all unit tests; integration skipped).

- [ ] **Step 6: Commit**

```bash
git add app/backup.py tests/test_backup_worker.py
git commit -m "feat: bounded-concurrency download worker with progress polling"
```

---

### Task 6: API & app factory with startup resume (`main.py`)

**Files:**
- Create: `app/main.py`
- Test: `tests/test_api.py`

**Interfaces:**
- Consumes: `Settings`, `JobStore`, `JobRunner`, `detect_repo_types`.
- Produces:
  - `class SlugIn(BaseModel)` with `slug: str`.
  - `create_app(settings, store, runner, detect=detect_repo_types) -> FastAPI` with routes: `GET /`, `POST /api/jobs`, `GET /api/jobs`, `POST /api/jobs/{job_id}/retry`, `POST /api/jobs/{job_id}/cancel`; and a startup hook that re-submits `store.unfinished_jobs()`.
  - `build_default_app() -> FastAPI` (loads `.env`, wires real store + runner) for `uvicorn app.main:build_default_app --factory`.

- [ ] **Step 1: Write the failing tests** — `tests/test_api.py`

```python
import pytest
from fastapi.testclient import TestClient
from app.config import Settings
from app.db import JobStore, FAILED, QUEUED, CANCELLED
from app.main import create_app


class FakeRunner:
    def __init__(self):
        self.submitted = []

    def submit(self, job_id):
        self.submitted.append(job_id)


def make_settings(tmp_path):
    return Settings(
        hf_token="hf_test",
        backup_dir=tmp_path / "backups",
        max_concurrent_jobs=2,
        max_workers=4,
        db_path=tmp_path / "jobs.db",
    )


@pytest.fixture
def ctx(tmp_path):
    settings = make_settings(tmp_path)
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)
    runner = FakeRunner()
    detect = lambda slug, token: ["model", "dataset"] if slug == "o/n" else []
    app = create_app(settings, store, runner, detect=detect)
    client = TestClient(app)
    yield client, store, runner
    store.close()


def test_create_jobs_makes_one_per_detected_type(ctx):
    client, store, runner = ctx
    resp = client.post("/api/jobs", json={"slug": "o/n"})
    assert resp.status_code == 200
    jobs = resp.json()["jobs"]
    assert {j["repo_type"] for j in jobs} == {"model", "dataset"}
    assert all(j["status"] == QUEUED for j in jobs)
    assert len(runner.submitted) == 2


def test_create_unknown_slug_404(ctx):
    client, store, runner = ctx
    resp = client.post("/api/jobs", json={"slug": "ghost/x"})
    assert resp.status_code == 404
    assert runner.submitted == []


def test_create_blank_slug_400(ctx):
    client, store, runner = ctx
    resp = client.post("/api/jobs", json={"slug": "   "})
    assert resp.status_code == 400


def test_resubmit_existing_repo_requeues_not_duplicates(ctx):
    client, store, runner = ctx
    client.post("/api/jobs", json={"slug": "o/n"})
    client.post("/api/jobs", json={"slug": "o/n"})
    assert len(store.list_jobs()) == 2  # still just model + dataset


def test_list_jobs(ctx):
    client, store, runner = ctx
    client.post("/api/jobs", json={"slug": "o/n"})
    body = client.get("/api/jobs").json()
    assert len(body["jobs"]) == 2
    assert "percent" in body["jobs"][0]


def test_retry_only_failed(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    # queued job cannot be retried
    assert client.post(f"/api/jobs/{job.id}/retry").status_code == 409
    store.set_status(job.id, FAILED, error="x")
    resp = client.post(f"/api/jobs/{job.id}/retry")
    assert resp.status_code == 200
    assert resp.json()["status"] == QUEUED
    assert job.id in runner.submitted


def test_cancel_only_queued(ctx):
    client, store, runner = ctx
    job = store.create_job("a/b", "model")
    resp = client.post(f"/api/jobs/{job.id}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == CANCELLED
    store.set_status(job.id, "running")
    assert client.post(f"/api/jobs/{job.id}/cancel").status_code == 409


def test_retry_missing_job_404(ctx):
    client, store, runner = ctx
    assert client.post("/api/jobs/999/retry").status_code == 404


def test_startup_resumes_unfinished_jobs(tmp_path):
    settings = make_settings(tmp_path)
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)
    leftover = store.create_job("resume/me", "model")  # queued
    runner = FakeRunner()
    app = create_app(settings, store, runner, detect=lambda s, t: [])
    with TestClient(app):  # triggers startup
        pass
    assert leftover.id in runner.submitted
    store.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_api.py -v`
Expected: FAIL (`ModuleNotFoundError: app.main`).

- [ ] **Step 3: Write `app/main.py`**

```python
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from .backup import JobRunner, detect_repo_types
from .config import load_settings
from .db import CANCELLED, FAILED, JobStore, QUEUED

STATIC_DIR = Path(__file__).parent / "static"


class SlugIn(BaseModel):
    slug: str


def create_app(settings, store, runner, detect=detect_repo_types) -> FastAPI:
    app = FastAPI(title="HF Repo Backup")

    @app.on_event("startup")
    def _resume_unfinished() -> None:
        for job in store.unfinished_jobs():
            runner.submit(job.id)

    @app.get("/")
    def index():
        return FileResponse(STATIC_DIR / "index.html")

    @app.post("/api/jobs")
    def create_jobs(body: SlugIn):
        slug = body.slug.strip()
        if not slug:
            raise HTTPException(status_code=400, detail="slug is required")
        types = detect(slug, settings.hf_token)
        if not types:
            raise HTTPException(status_code=404, detail="repo not found or not accessible")
        created = []
        for repo_type in types:
            existing = store.get_job_by_repo(repo_type, slug)
            if existing:
                store.requeue(existing.id)
                job = store.get_job(existing.id)
            else:
                job = store.create_job(slug, repo_type)
            runner.submit(job.id)
            created.append(job.to_dict())
        return {"jobs": created}

    @app.get("/api/jobs")
    def list_jobs():
        return {"jobs": [job.to_dict() for job in store.list_jobs()]}

    @app.post("/api/jobs/{job_id}/retry")
    def retry(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != FAILED:
            raise HTTPException(status_code=409, detail="only failed jobs can be retried")
        store.requeue(job_id)
        runner.submit(job_id)
        return store.get_job(job_id).to_dict()

    @app.post("/api/jobs/{job_id}/cancel")
    def cancel(job_id: int):
        job = store.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != QUEUED:
            raise HTTPException(status_code=409, detail="only queued jobs can be cancelled")
        store.set_status(job_id, CANCELLED)
        return store.get_job(job_id).to_dict()

    return app


def build_default_app() -> FastAPI:
    from dotenv import load_dotenv

    load_dotenv()
    settings = load_settings()
    store = JobStore(settings.db_path)
    runner = JobRunner(store, settings)
    return create_app(settings, store, runner)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_api.py -v`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add app/main.py tests/test_api.py
git commit -m "feat: FastAPI routes, job creation per type, startup resume"
```

---

### Task 7: Dashboard frontend (`static/index.html`)

**Files:**
- Create: `app/static/index.html`
- Test: `tests/test_static.py`

**Interfaces:**
- Consumes: `GET /api/jobs`, `POST /api/jobs`, `POST /api/jobs/{id}/retry`, `POST /api/jobs/{id}/cancel`.
- Produces: a single self-contained HTML page (inline CSS/JS), polling every 1.5s.

- [ ] **Step 1: Write the failing test** — `tests/test_static.py`

```python
import pytest
from fastapi.testclient import TestClient
from app.config import Settings
from app.db import JobStore
from app.main import create_app


@pytest.fixture
def client(tmp_path):
    settings = Settings(
        hf_token="hf_test",
        backup_dir=tmp_path / "backups",
        max_concurrent_jobs=2,
        max_workers=4,
        db_path=tmp_path / "jobs.db",
    )
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)

    class FakeRunner:
        def submit(self, job_id):
            pass

    app = create_app(settings, store, FakeRunner(), detect=lambda s, t: [])
    yield TestClient(app)
    store.close()


def test_index_served(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Hugging Face Repo Backup" in resp.text
    assert "/api/jobs" in resp.text  # JS talks to the API
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_static.py -v`
Expected: FAIL (404 / file not found — `app/static/index.html` does not exist).

- [ ] **Step 3: Create `app/static/index.html`**

```html
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>HF Repo Backup</title>
<style>
:root{--bg:#0f1115;--card:#171a21;--line:#262b36;--fg:#e6e9ef;--muted:#8b94a7;--accent:#ffb000;--ok:#3fb950;--err:#f85149}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
.wrap{max-width:920px;margin:0 auto;padding:36px 20px}
h1{font-size:22px;margin:0 0 4px;letter-spacing:-.01em}
.sub{color:var(--muted);margin:0 0 24px}
code{background:var(--card);padding:1px 6px;border-radius:5px}
form{display:flex;gap:8px;margin-bottom:26px}
input{flex:1;background:var(--card);border:1px solid var(--line);color:var(--fg);padding:11px 13px;border-radius:8px;font-size:15px}
input:focus{outline:none;border-color:var(--accent)}
button{background:var(--accent);color:#1b1b1b;border:0;border-radius:8px;padding:11px 18px;font-weight:600;cursor:pointer}
button.ghost{background:transparent;color:var(--muted);border:1px solid var(--line);padding:6px 11px;font-weight:500}
button.ghost:hover{color:var(--fg);border-color:var(--muted)}
table{width:100%;border-collapse:collapse}
th{text-align:left;color:var(--muted);font-weight:500;font-size:12px;text-transform:uppercase;letter-spacing:.04em;padding:0 10px 10px}
td{padding:12px 10px;border-top:1px solid var(--line);vertical-align:middle}
.repo{font-weight:600}
.type{color:var(--muted);font-size:13px}
.bar{height:6px;background:var(--line);border-radius:99px;overflow:hidden;min-width:130px}
.bar>span{display:block;height:100%;background:var(--accent);transition:width .4s ease}
.st{font-size:13px;font-weight:600;text-transform:capitalize}
.st.completed{color:var(--ok)}.st.failed{color:var(--err)}.st.running{color:var(--accent)}.st.queued,.st.cancelled{color:var(--muted)}
.err{color:var(--err);font-size:12px;margin-top:4px;max-width:420px}
.size{color:var(--muted);font-size:13px;white-space:nowrap}
.empty{color:var(--muted);text-align:center;padding:44px 0}
</style>
</head>
<body>
<div class="wrap">
  <h1>Hugging Face Repo Backup</h1>
  <p class="sub">Paste a repo slug (e.g. <code>bigscience/bloom</code>) to back it up to your local folder.</p>
  <form id="f">
    <input id="slug" placeholder="owner/name" autocomplete="off" autofocus />
    <button type="submit">Back up</button>
  </form>
  <table>
    <thead><tr><th>Repo</th><th>Status</th><th>Progress</th><th>Size</th><th></th></tr></thead>
    <tbody id="rows"></tbody>
  </table>
  <div id="empty" class="empty">No backups yet.</div>
</div>
<script>
const fmt = b => {
  if (!b) return "0 B";
  const u = ["B","KB","MB","GB","TB"];
  const i = Math.min(u.length - 1, Math.floor(Math.log(b) / Math.log(1024)));
  return (b / Math.pow(1024, i)).toFixed(i ? 1 : 0) + " " + u[i];
};
async function api(path, opts) {
  const r = await fetch(path, opts);
  if (!r.ok) {
    const e = await r.json().catch(() => ({ detail: r.statusText }));
    throw new Error(e.detail || "request failed");
  }
  return r.json();
}
document.getElementById("f").addEventListener("submit", async e => {
  e.preventDefault();
  const el = document.getElementById("slug");
  const slug = el.value.trim();
  if (!slug) return;
  try {
    await api("/api/jobs", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ slug }) });
    el.value = "";
    refresh();
  } catch (err) { alert(err.message); }
});
async function act(id, what) {
  try { await api(`/api/jobs/${id}/${what}`, { method: "POST" }); refresh(); }
  catch (e) { alert(e.message); }
}
window.act = act;
function row(j) {
  const actions = j.status === "failed"
    ? `<button class="ghost" onclick="act(${j.id},'retry')">Retry</button>`
    : j.status === "queued"
    ? `<button class="ghost" onclick="act(${j.id},'cancel')">Cancel</button>`
    : "";
  return `<tr>
    <td><div class="repo">${j.slug}</div><div class="type">${j.repo_type}</div>${j.error ? `<div class="err">${j.error}</div>` : ""}</td>
    <td><span class="st ${j.status}">${j.status}</span></td>
    <td><div class="bar"><span style="width:${j.percent}%"></span></div><div class="type">${j.percent}%</div></td>
    <td class="size">${fmt(j.downloaded_bytes)} / ${fmt(j.total_bytes)}</td>
    <td>${actions}</td></tr>`;
}
async function refresh() {
  try {
    const { jobs } = await api("/api/jobs");
    document.getElementById("rows").innerHTML = jobs.map(row).join("");
    document.getElementById("empty").style.display = jobs.length ? "none" : "block";
  } catch (e) { /* transient; keep polling */ }
}
refresh();
setInterval(refresh, 1500);
</script>
</body>
</html>
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_static.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/static/index.html tests/test_static.py
git commit -m "feat: polling dashboard frontend"
```

---

### Task 8: Integration test, README, and manual run

**Files:**
- Create: `tests/test_integration.py`
- Create: `README.md`

**Interfaces:**
- Consumes: the whole stack + the real Hugging Face Hub (public repo, no token required).
- Produces: an end-to-end proof and run instructions.

- [ ] **Step 1: Write the integration test** — `tests/test_integration.py`

```python
import os
import pytest
from app.config import Settings
from app.db import JobStore, COMPLETED
from app.backup import run_backup_job

TINY_PUBLIC_MODEL = "hf-internal-testing/tiny-random-gpt2"


@pytest.mark.integration
def test_backup_tiny_public_repo_end_to_end(tmp_path):
    settings = Settings(
        hf_token=os.environ.get("HUGGINGFACE_ACCESS_KEY", ""),
        backup_dir=tmp_path / "backups",
        max_concurrent_jobs=1,
        max_workers=2,
        db_path=tmp_path / "jobs.db",
    )
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path)
    job = store.create_job(TINY_PUBLIC_MODEL, "model")

    run_backup_job(job.id, store, settings)  # real HfApi + real snapshot_download

    done = store.get_job(job.id)
    assert done.status == COMPLETED, done.error
    assert done.downloaded_bytes > 0
    target = tmp_path / "backups" / "models" / "hf-internal-testing" / "tiny-random-gpt2"
    assert target.exists() and any(target.iterdir())
    store.close()
```

- [ ] **Step 2: Run the integration test (requires network)**

Run: `python -m pytest tests/test_integration.py -m integration -v`
Expected: PASS (downloads the tiny repo into `models/hf-internal-testing/tiny-random-gpt2`).

- [ ] **Step 3: Run the full default suite once more**

Run: `python -m pytest -v`
Expected: PASS (all unit tests; integration skipped by default config).

- [ ] **Step 4: Write `README.md`**

````markdown
# HF Repo Backup Dashboard

A small FastAPI web app that backs up entire Hugging Face Hub repositories
(models, datasets, spaces) to a local folder — with bounded concurrency,
live progress, and automatic resume.

## Setup

1. Install dependencies:
   ```bash
   python -m pip install -r requirements.txt
   ```
2. Create a `.env` (see `.env.example`):
   ```
   HUGGINGFACE_ACCESS_KEY=hf_your_token_here
   BACKUP_DIR=./backups
   MAX_CONCURRENT_JOBS=2
   MAX_WORKERS=8
   DB_PATH=jobs.db
   ```

## Run

```bash
uvicorn app.main:build_default_app --factory --reload
```

Open http://127.0.0.1:8000 and paste a repo slug (e.g. `bigscience/bloom`).
Each repo is saved to `BACKUP_DIR/<repo_type>s/<owner>/<name>`. Closing and
restarting the server resumes any in-flight backups automatically.

## Tests

```bash
python -m pytest                       # unit tests (Hub mocked)
python -m pytest -m integration        # end-to-end against the real Hub (network)
```
````

- [ ] **Step 5: Manually verify the running app**

Run (in one terminal):
```bash
uvicorn app.main:build_default_app --factory
```
Then in another terminal confirm it serves and accepts a job:
```bash
curl -s http://127.0.0.1:8000/ | grep -o "Hugging Face Repo Backup"
curl -s -X POST http://127.0.0.1:8000/api/jobs -H "Content-Type: application/json" -d '{"slug":"hf-internal-testing/tiny-random-gpt2"}'
curl -s http://127.0.0.1:8000/api/jobs
```
Expected: the title string prints; POST returns a job with `repo_type":"model"`; GET shows it progressing to `completed`. Stop the server with Ctrl-C.

- [ ] **Step 6: Commit**

```bash
git add tests/test_integration.py README.md
git commit -m "test: end-to-end backup integration test and README"
```

---

## Self-Review Notes

- **Spec coverage:** config/`.env` (Task 2), `(repo_type, slug)` identity + SQLite (Task 3), auto-detect-all-types + sizing + type-scoped paths (Task 4), bounded concurrency + resume-friendly download + progress (Task 5), API + create-one-job-per-type + startup resume + retry/cancel (Task 6), dashboard (Task 7), error handling surfaced as `failed` (Task 5 worker), integration proof (Task 8). All spec sections map to a task.
- **Resume (two layers):** within-download resume is inherent to `snapshot_download(local_dir=...)` skipping complete files; across-restart resume is the startup hook re-submitting `unfinished_jobs()` (Task 6) which re-runs the same download (Task 5).
- **Type consistency:** `Settings` fields, `Job` fields/`to_dict`, helper signatures, and `run_backup_job`/`JobRunner`/`create_app` signatures are referenced identically across tasks. The injectable `downloader(repo_id=, repo_type=, local_dir=, token=, max_workers=)` keyword signature matches `snapshot_download`.
- **No placeholders:** every code and test step contains complete, runnable content.
