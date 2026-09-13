# Multiple Storage Targets (local + NFS) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let each backup job be saved to one of several named directories (local ZFS disk or a TrueNAS NFS export), chosen at submit time, with an unmounted share never written to.

**Architecture:** `Settings.targets` (ordered name→path, first is default) replaces the single `backup_dir` everywhere a path is resolved; jobs carry a `target` name; a marker file `.hug-face-rip` at a non-default target's root is the "online" signal that gates dispatch, submission and the storage panel. The OS (systemd mount + automount units) owns the NFS mount.

**Tech Stack:** Python 3.13, FastAPI, SQLite (`app/db.py`), pytest (offline, fakes injected), systemd `.mount`/`.automount`, NFSv4 client (`nfs-common`).

## Global Constraints

- Tests run with `.venv/bin/python -m pytest` (no system python/pip); the default suite must stay offline.
- Uniqueness of jobs stays `UNIQUE(repo_type, slug)` — one copy of a repo across all targets.
- Only the **default** (first) target is created/write-checked at startup; other targets are never `mkdir`ed by the app.
- Marker filename is exactly `.hug-face-rip` (`app.config.MARKER`).
- `BACKUP_TARGETS` format: comma-separated `name=path`; names match `^[A-Za-z0-9_-]+$`; first entry is the default; when unset, `BACKUP_DIR` → single target `local`.
- Commit messages end with the session's attribution lines; branch `feature/storage-targets-nfs`, merged `--no-ff` into `master`.

---

### Task 1: Config — targets, default, marker-based online check

**Files:**
- Modify: `app/config.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Produces: `Settings.targets: Mapping[str, Path]` (ordered, first = default), `Settings.default_target -> str`, `Settings.target_dir(name) -> Path` (raises `KeyError`), `config.MARKER = ".hug-face-rip"`, `config.target_online(settings, name) -> bool`.
- `Settings(backup_dir=...)` without `targets` still works: `targets == {"local": backup_dir}`.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_config.py`)

```python
def test_backup_targets_parsed_in_order_first_is_default(tmp_path):
    env = {"HUGGINGFACE_ACCESS_KEY": "hf_test",
           "BACKUP_TARGETS": f"local={tmp_path / 'a'}, nas={tmp_path / 'b'}"}
    s = load_settings(env)
    assert list(s.targets) == ["local", "nas"]
    assert s.default_target == "local"
    assert s.backup_dir == tmp_path / "a"           # legacy field = default target
    assert s.target_dir("nas") == tmp_path / "b"
    assert (tmp_path / "a").is_dir()                 # default is created
    assert not (tmp_path / "b").exists()             # others are NOT created


def test_backup_targets_ignores_backup_dir_when_set(tmp_path):
    env = {"HUGGINGFACE_ACCESS_KEY": "hf_test", "BACKUP_DIR": str(tmp_path / "old"),
           "BACKUP_TARGETS": f"main={tmp_path / 'new'}"}
    s = load_settings(env)
    assert list(s.targets) == ["main"] and s.backup_dir == tmp_path / "new"


def test_unset_backup_targets_falls_back_to_backup_dir_as_local(tmp_path):
    s = load_settings(base_env(tmp_path))
    assert s.targets == {"local": tmp_path / "backups"}
    assert s.default_target == "local"


@pytest.mark.parametrize("raw", ["bad name=/x", "=/x", "noequals", "a=/x,a=/y", "", " , "])
def test_invalid_backup_targets_raise(tmp_path, raw):
    env = {"HUGGINGFACE_ACCESS_KEY": "hf_test", "BACKUP_TARGETS": raw}
    if raw.strip(" ,") == "":
        env["BACKUP_DIR"] = str(tmp_path / "b")     # blank BACKUP_TARGETS == unset
        assert load_settings(env).default_target == "local"
        return
    with pytest.raises(ConfigError, match="BACKUP_TARGETS"):
        load_settings(env)


def test_settings_without_targets_defaults_to_local(tmp_path):
    s = Settings(hf_token="t", backup_dir=tmp_path, max_concurrent_jobs=1,
                 max_workers=1, db_path=tmp_path / "j.db")
    assert s.targets == {"local": tmp_path} and s.default_target == "local"
    with pytest.raises(KeyError):
        s.target_dir("nope")


def test_target_online_requires_marker_for_non_default(tmp_path):
    from app.config import target_online, MARKER
    s = Settings(hf_token="t", backup_dir=tmp_path / "a", max_concurrent_jobs=1,
                 max_workers=1, db_path=tmp_path / "j.db",
                 targets={"local": tmp_path / "a", "nas": tmp_path / "nas"})
    assert target_online(s, "local") is False        # default: needs the directory
    (tmp_path / "a").mkdir()
    assert target_online(s, "local") is True
    (tmp_path / "nas").mkdir()                       # an empty mountpoint dir...
    assert target_online(s, "nas") is False          # ...is offline without the marker
    (tmp_path / "nas" / MARKER).write_text("")
    assert target_online(s, "nas") is True
```

- [ ] **Step 2: Run to verify they fail**

Run: `.venv/bin/python -m pytest tests/test_config.py -q`
Expected: failures — `AttributeError: 'Settings' object has no attribute 'targets'`, `ImportError` for `target_online`.

- [ ] **Step 3: Implement** in `app/config.py`

```python
import re
...
MARKER = ".hug-face-rip"          # present at a target's root => the share is really mounted
_TARGET_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class Settings:
    hf_token: str
    backup_dir: Path
    max_concurrent_jobs: int
    max_workers: int
    db_path: Path
    verify_downloads: bool = True
    stall_timeout: float = 600.0
    # Ordered name -> directory; the first entry is the default target. None means
    # the single legacy target "local" at backup_dir.
    targets: Optional[Mapping[str, Path]] = None

    def __post_init__(self) -> None:
        if self.targets is None:
            object.__setattr__(self, "targets", {"local": Path(self.backup_dir)})
        elif not self.targets:
            raise ValueError("at least one backup target is required")

    @property
    def default_target(self) -> str:
        return next(iter(self.targets))

    def target_dir(self, name: str) -> Path:
        if name not in self.targets:
            raise KeyError(f"unknown backup target {name!r}")
        return Path(self.targets[name])


def target_online(settings: Settings, name: str) -> bool:
    """Whether a target can be written to right now. The default target only
    needs its directory; any other target must carry the marker file at its
    root — an unmounted NFS share is just an empty local directory, and
    writing a 300 GB repo into that would fill the root disk."""
    path = settings.target_dir(name)
    try:
        if name == settings.default_target:
            return path.is_dir()
        return (path / MARKER).is_file()
    except OSError:          # e.g. a soft-mounted share timing out
        return False


def _parse_targets(raw: str) -> "dict[str, Path]":
    targets: "dict[str, Path]" = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        name, sep, path = entry.partition("=")
        name, path = name.strip(), path.strip()
        if not sep or not name or not path:
            raise ConfigError(f"BACKUP_TARGETS entry {entry!r} must be name=path")
        if not _TARGET_NAME_RE.match(name):
            raise ConfigError(f"BACKUP_TARGETS name {name!r} must match [A-Za-z0-9_-]+")
        if name in targets:
            raise ConfigError(f"BACKUP_TARGETS name {name!r} is listed twice")
        targets[name] = Path(path).expanduser()
    return targets
```

and in `load_settings`, replace the `BACKUP_DIR` block with:

```python
    raw_targets = (env.get("BACKUP_TARGETS") or "").strip(" ,")
    if raw_targets:
        targets = _parse_targets(raw_targets)
        if not targets:
            raise ConfigError("BACKUP_TARGETS has no name=path entries")
        backup_dir = next(iter(targets.values()))
        label = f"BACKUP_TARGETS default target {next(iter(targets))!r}"
    else:
        backup_dir_raw = env.get("BACKUP_DIR")
        if not backup_dir_raw:
            raise ConfigError("BACKUP_DIR is not set")
        backup_dir = Path(backup_dir_raw).expanduser()
        targets = {"local": backup_dir}
        label = "BACKUP_DIR"

    # Only the default target is created/checked: the others may be mounts that are
    # legitimately absent right now, and creating them would mask that.
    try:
        backup_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ConfigError(f"cannot create {label} {backup_dir}: {exc}")
    if not os.access(backup_dir, os.W_OK):
        raise ConfigError(f"{label} {backup_dir} is not writable")

    return Settings(..., backup_dir=backup_dir, targets=targets, ...)
```

- [ ] **Step 4: Run to verify they pass**: `.venv/bin/python -m pytest tests/test_config.py -q` → all pass.
- [ ] **Step 5: Commit**: `git add app/config.py tests/test_config.py && git commit -m "feat(config): BACKUP_TARGETS with default target and marker-based online check"`

---

### Task 2: Retry — NFS I/O errors are transient

**Files:**
- Modify: `app/retry.py:19-20`
- Test: `tests/test_retry.py`

- [ ] **Step 1: Failing test** (append to `tests/test_retry.py`)

```python
@pytest.mark.parametrize("code", [errno.EIO, errno.ESTALE, errno.ENOTCONN, errno.EHOSTDOWN])
def test_nfs_style_io_errors_are_retryable(code):
    # A soft-mounted share dropping mid-download surfaces as EIO/ESTALE/ENOTCONN;
    # the job should back off and resume, not fail permanently.
    assert is_retryable(OSError(code, "share went away")) is True
```
(add `import errno` / `import pytest` at the top if missing.)

- [ ] **Step 2: Run**: `.venv/bin/python -m pytest tests/test_retry.py -q` → 4 failures (`assert False is True`).
- [ ] **Step 3: Implement**: extend the set

```python
_RETRYABLE_OS_ERRNO = {errno.ECONNRESET, errno.ECONNREFUSED, errno.ECONNABORTED,
                       errno.ETIMEDOUT, errno.EHOSTUNREACH, errno.ENETUNREACH,
                       # A network share (soft NFS mount) going away mid-transfer.
                       errno.EIO, errno.ESTALE, errno.ENOTCONN, errno.EHOSTDOWN}
```
- [ ] **Step 4: Run** → pass. **Step 5: Commit** `feat(retry): treat NFS I/O errors as transient`.

---

### Task 3: DB — `target` column, migration, filtered queries

**Files:**
- Modify: `app/db.py`
- Test: `tests/test_db.py`

**Interfaces:**
- Produces: `JobStore(db_path, default_target="local")`; `Job.target: str`; `create_job(slug, repo_type, target=None)` (None → store default); `next_runnable_job(targets=None)` (None = any; a list restricts; `[]` → None); `pending_bytes(target=None)`.

- [ ] **Step 1: Failing tests** (append to `tests/test_db.py`)

```python
def test_jobs_carry_a_target_defaulting_to_store_default(tmp_path):
    s = JobStore(tmp_path / "j.db", default_target="nas")
    a = s.create_job("o/a", "model")
    b = s.create_job("o/b", "model", target="local")
    assert a.target == "nas" and b.target == "local"
    assert s.get_job(a.id).to_dict()["target"] == "nas"
    s.close()


def test_migration_adds_target_to_old_table_with_configured_default(tmp_path):
    db = tmp_path / "old.db"
    conn = sqlite3.connect(db)
    conn.executescript("""
        CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, slug TEXT NOT NULL,
          repo_type TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
          total_bytes INTEGER NOT NULL DEFAULT 0, downloaded_bytes INTEGER NOT NULL DEFAULT 0,
          error TEXT, created_at TEXT NOT NULL DEFAULT (datetime('now')),
          updated_at TEXT NOT NULL DEFAULT (datetime('now')), UNIQUE(repo_type, slug));
        INSERT INTO jobs (slug, repo_type, status) VALUES ('old/row', 'model', 'completed');
    """)
    conn.commit(); conn.close()
    s = JobStore(db, default_target="zfs")
    assert s.get_job_by_repo("model", "old/row").target == "zfs"
    s.close()


def test_next_runnable_job_can_be_restricted_to_online_targets(store):
    nas = store.create_job("o/nas", "model", target="nas")
    loc = store.create_job("o/loc", "model", target="local")
    assert store.next_runnable_job().id == nas.id                 # unrestricted: lowest id
    assert store.next_runnable_job(targets=["local"]).id == loc.id
    assert store.next_runnable_job(targets=[]) is None            # nothing online


def test_pending_bytes_per_target(store):
    a = store.create_job("o/a", "model", target="local"); store.update_progress(a.id, 10, 100)
    b = store.create_job("o/b", "model", target="nas");   store.update_progress(b.id, 0, 50)
    assert store.pending_bytes() == 140
    assert store.pending_bytes(target="nas") == 50
    assert store.pending_bytes(target="local") == 90
```

- [ ] **Step 2: Run** `.venv/bin/python -m pytest tests/test_db.py -q` → `TypeError: unexpected keyword 'default_target'` / `'target'`.
- [ ] **Step 3: Implement** in `app/db.py`:
  - schema: add `target TEXT NOT NULL DEFAULT 'local',` after `repo_type`.
  - `Job`: add `target: str = "local"` (last field).
  - `__init__(self, db_path, default_target: str = "local")`: keep `self._default_target`; migration
    ```python
    if "target" not in cols:
        quoted = default_target.replace("'", "''")
        self._conn.execute(
            f"ALTER TABLE jobs ADD COLUMN target TEXT NOT NULL DEFAULT '{quoted}'")
    ```
  - `create_job(self, slug, repo_type, target=None)`: `INSERT INTO jobs (slug, repo_type, target) VALUES (?, ?, ?)` with `target or self._default_target`.
  - `next_runnable_job(self, targets=None)`: if `targets is not None`: `if not targets: return None`; add `AND target IN (` + `,`.join("?"*len) + `)` wrapping the existing OR clause in parentheses; params = tuple(targets).
  - `pending_bytes(self, target=None)`: add `AND target = ?` when given.
- [ ] **Step 4: Run** → pass (whole `tests/test_db.py`). **Step 5: Commit** `feat(db): per-job storage target with migration and filtered queries`.

---

### Task 4: Engine — resolve the job's target everywhere, offline guard, dispatcher filter

**Files:**
- Modify: `app/backup.py` (`run_delete_job`, `run_backup_job`, `_verify_phase`, `JobRunner._dispatch_loop`)
- Test: `tests/test_backup_worker.py`

**Interfaces:**
- Consumes: `settings.target_dir(name)`, `settings.targets`, `config.target_online`, `store.next_runnable_job(targets=...)`.
- Produces: `backup.TargetOffline(RuntimeError)`.

- [ ] **Step 1: Failing tests** (append to `tests/test_backup_worker.py`)

```python
def _two_target_settings(tmp_path, **kw):
    s = make_settings(tmp_path, **kw)
    from dataclasses import replace
    return replace(s, targets={"local": s.backup_dir, "nas": tmp_path / "nas"})


def test_worker_writes_into_the_jobs_target_dir(tmp_path):
    from app.config import MARKER
    settings = _two_target_settings(tmp_path)
    (tmp_path / "nas").mkdir(); (tmp_path / "nas" / MARKER).write_text("")
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model", target="nas")
    run_backup_job(job.id, store, settings, api=FakeApi(11),
                   launcher=InThreadLauncher(fake_downloader_factory()))
    assert store.get_job(job.id).status == COMPLETED
    assert (tmp_path / "nas" / "models" / "o" / "n" / "model.bin").exists()
    assert not (settings.backup_dir / "models" / "o" / "n").exists()


def test_worker_offline_target_is_a_transient_failure(tmp_path):
    settings = _two_target_settings(tmp_path)        # no marker -> nas offline
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model", target="nas")
    run_backup_job(job.id, store, settings, api=FakeApi(11),
                   launcher=InThreadLauncher(fake_downloader_factory()))
    j = store.get_job(job.id)
    assert j.status == RETRYING and "offline" in j.error
    assert not (tmp_path / "nas").exists()            # nothing was written locally


def test_dispatcher_skips_jobs_whose_target_is_offline(tmp_path):
    settings = _two_target_settings(tmp_path, max_jobs=2)
    store = JobStore(settings.db_path)
    nas = store.create_job("o/nas", "model", target="nas")     # lower id, offline
    loc = store.create_job("o/loc", "model", target="local")
    runner = JobRunner(store, settings, api=FakeApi(11),
                       launcher=InThreadLauncher(fake_downloader_factory()),
                       dispatch_interval=0.02)
    runner.start()
    assert wait_until(lambda: store.get_job(loc.id).status == COMPLETED)
    assert store.get_job(nas.id).status == QUEUED               # held, never claimed
    runner.shutdown()
    store.close()


def test_deleter_removes_from_the_jobs_target_dir(tmp_path):
    from app.backup import run_delete_job
    from app.config import MARKER
    settings = _two_target_settings(tmp_path)
    nas = tmp_path / "nas"; (nas / "models" / "o" / "n").mkdir(parents=True)
    (nas / MARKER).write_text(""); (nas / "models" / "o" / "n" / "f.bin").write_bytes(b"x")
    store = JobStore(settings.db_path)
    job = store.create_job("o/n", "model", target="nas")
    store.set_status(job.id, COMPLETED)
    run_delete_job(job.id, store, settings)
    assert not (nas / "models" / "o" / "n").exists() and store.get_job(job.id) is None
```

- [ ] **Step 2: Run** → the first test fails: file written under `backups/models/o/n` instead of `nas/...`; the second: status `completed`; third: nas job runs; fourth: dir survives.
- [ ] **Step 3: Implement** in `app/backup.py`:
  - `from .config import target_online`; `class TargetOffline(RuntimeError): """The job's storage target is not mounted/marked right now (transient)."""`
  - `run_delete_job`: `root = settings.target_dir(job.target)`; `local_dir = local_dir_for(root, ...)`; `remover(root, job.repo_type, job.slug)`.
  - `run_backup_job`: after `store.set_status(job_id, "running")`: `root = settings.target_dir(job.target)`; `local_dir = local_dir_for(root, ...)`. Inside the `try`, first line:
    ```python
        if not target_online(settings, job.target):
            raise TargetOffline(f"target '{job.target}' is offline (not mounted?) — will retry")
        backup_root = root.resolve()
    ```
    and `free = free_disk_bytes(root)` / message `... free in {root}`. In the `except Exception as exc` branch: `retryable = True if isinstance(exc, TargetOffline) else is_retryable(exc)`.
  - `_verify_phase`: `local_dir = local_dir_for(settings.target_dir(job.target), ...)`.
  - `_dispatch_loop`: before the inner `while`: `online = [n for n in self._settings.targets if target_online(self._settings, n)]`; `job = self._store.next_runnable_job(targets=online)`.
- [ ] **Step 4: Run** `tests/test_backup_worker.py` → all pass, 0 warnings. **Step 5: Commit** `feat(backup): resolve per-job storage target; hold jobs for offline targets`.

---

### Task 5: API — target on submit, per-target storage

**Files:**
- Modify: `app/main.py` (`SlugIn`, `create_jobs`, `storage`, `build_default_app`)
- Test: `tests/test_api.py`

**Interfaces:**
- Consumes: `settings.targets`, `settings.default_target`, `target_online`, `store.create_job(..., target)`, `store.pending_bytes(target=...)`.
- Produces: `POST /api/jobs` body `{slug, target?}`; `GET /api/storage` adds `targets: [{name, path, default, online, total, used, free, planned}]`.

- [ ] **Step 1: Failing tests** (append to `tests/test_api.py`)

```python
@pytest.fixture
def ctx2(tmp_path):
    """Two targets: 'local' (default, exists) and 'nas' (online only with the marker)."""
    from dataclasses import replace
    from app.config import MARKER
    settings = replace(make_settings(tmp_path),
                       targets={"local": tmp_path / "backups", "nas": tmp_path / "nas"})
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.db_path, default_target="local")
    runner = FakeRunner(store)
    app = create_app(settings, store, runner,
                     detect=lambda slug, token: ["model"], sizer=lambda s, r, t: 10)
    client = TestClient(app)
    def bring_nas_online():
        (tmp_path / "nas").mkdir(exist_ok=True); (tmp_path / "nas" / MARKER).write_text("")
    yield client, store, bring_nas_online
    store.close()


def test_create_job_on_a_named_target(ctx2):
    client, store, online = ctx2
    online()
    resp = client.post("/api/jobs", json={"slug": "o/n", "target": "nas"})
    assert resp.status_code == 200
    assert resp.json()["jobs"][0]["target"] == "nas"
    assert store.list_jobs()[0].target == "nas"


def test_create_job_defaults_to_the_default_target(ctx2):
    client, store, _ = ctx2
    assert client.post("/api/jobs", json={"slug": "o/n"}).json()["jobs"][0]["target"] == "local"


def test_create_job_unknown_target_400(ctx2):
    client, store, _ = ctx2
    resp = client.post("/api/jobs", json={"slug": "o/n", "target": "usb"})
    assert resp.status_code == 400 and "target" in resp.json()["detail"]
    assert store.list_jobs() == []


def test_create_job_offline_target_409(ctx2):
    client, store, _ = ctx2                      # no marker -> nas offline
    resp = client.post("/api/jobs", json={"slug": "o/n", "target": "nas"})
    assert resp.status_code == 409 and "offline" in resp.json()["detail"]
    assert store.list_jobs() == []


def test_storage_lists_every_target_with_online_flag(ctx2):
    client, store, online = ctx2
    j = store.create_job("o/n", "model", target="nas"); store.update_progress(j.id, 0, 40)
    s = client.get("/api/storage").json()
    by = {t["name"]: t for t in s["targets"]}
    assert by["local"]["default"] is True and by["local"]["online"] is True
    assert by["nas"]["online"] is False and by["nas"]["total"] == 0
    assert by["nas"]["planned"] == 40 and by["local"]["planned"] == 0
    assert s["planned"] == 40                    # top-level: all targets, as before
    online()
    s = client.get("/api/storage").json()
    nas = next(t for t in s["targets"] if t["name"] == "nas")
    assert nas["online"] is True and nas["total"] > 0
```

- [ ] **Step 2: Run** → 422 (unknown body field ignored → target missing), `KeyError: 'targets'`, etc.
- [ ] **Step 3: Implement** in `app/main.py`:
  - `from .config import load_settings, target_online`
  - `class SlugIn(BaseModel): slug: str; target: Optional[str] = None` (`from typing import Optional`).
  - `create_jobs`, after slug validation:
    ```python
        target = (body.target or settings.default_target).strip()
        if target not in settings.targets:
            raise HTTPException(status_code=400, detail=f"unknown target {target!r}")
        if not target_online(settings, target):
            raise HTTPException(status_code=409,
                                detail=f"target '{target}' is offline (not mounted?)")
    ```
    and `store.create_job(slug, repo_type, target)` for new rows (existing rows keep their target).
  - `storage`:
    ```python
        targets = []
        for name in settings.targets:
            path = settings.target_dir(name)
            online = target_online(settings, name)
            if online:
                try:
                    u = shutil.disk_usage(path); total, used, free = u.total, u.used, u.free
                except OSError:
                    online, total, used, free = False, 0, 0, 0
            else:
                total = used = free = 0
            targets.append({"name": name, "path": str(path), "default": name == settings.default_target,
                            "online": online, "total": total, "used": used, "free": free,
                            "planned": store.pending_bytes(target=name)})
        head = targets[0]
        return {"path": head["path"], "total": head["total"], "used": head["used"],
                "free": head["free"], "planned": store.pending_bytes(),
                "paused_all": store.get_flag("paused_all", "0") == "1", "targets": targets}
    ```
  - `build_default_app`: `store = JobStore(settings.db_path, default_target=settings.default_target)`.
- [ ] **Step 4: Run** `tests/test_api.py` → pass (existing storage tests still pass because top-level fields are unchanged). **Step 5: Commit** `feat(api): choose a storage target per job; per-target storage report`.

---

### Task 6: Dashboard — target select, per-target bars, badges

**Files:**
- Modify: `app/static/index.html`
- Test: `tests/test_static.py`

- [ ] **Step 1: Failing test**

```python
def test_multi_target_ui_present(client):
    page = client.get("/").text
    assert 'id="target"' in page             # target <select> beside the slug input
    assert "s.targets" in page               # storage panel iterates the targets list
    assert "badge target" in page            # per-row target badge
    assert "offline" in page                 # offline state rendered
```
- [ ] **Step 2: Run** → fails on `id="target"`.
- [ ] **Step 3: Implement** in `index.html`:
  - Form: `<select id="target" hidden></select>` between the input and the button; CSS `select{background:var(--card);border:1px solid var(--line);color:var(--fg);padding:11px 13px;border-radius:8px;font-size:15px}`.
  - `.badge.target{background:rgba(255,176,0,.14);color:var(--accent)}`; `.storage` becomes a column of `.srow` flex rows (same inner pieces as today); `.srow.offline .sval{color:var(--err)}`.
  - State: `let targets = [];` `const offlineTargets = () => new Set(targets.filter(t => !t.online).map(t => t.name));`
  - Submit: `const body = { slug }; const sel = document.getElementById("target"); if (!sel.hidden) body.target = sel.value;`
  - `loadStorage(speed)`: after fetching `s`, `targets = s.targets || [];` fill the select (`<option value=name ${!t.online ? "disabled" : ""}>name${t.online ? "" : " (offline)"}</option>`, keep previous value when still present, preselect the default), `sel.hidden = targets.length < 2`; render one `.srow` per target (offline → `<div class="sval">offline</div>`), speed text appended to the first row.
  - `row(j)`: `${targets.length > 1 ? `<span class="badge target">${esc(j.target)}</span>` : ""}` after the type badge; label: `j.status === "queued" && offlineTargets().has(j.target) ? \`held · ${esc(j.target)} offline\`` before the `pausedAll` case.
- [ ] **Step 4: Run** `tests/test_static.py` + `node --check` on the extracted script → pass. **Step 5: Commit** `feat(dashboard): pick a storage target; per-target storage bars`.

---

### Task 7: Deploy units, docs, install, verify

**Files:**
- Create: `deploy/mnt-truenas.mount`, `deploy/mnt-truenas.automount`
- Modify: `deploy/hug-face-rip.service`, `.env.example`, `README.md`, `CLAUDE.md`

- [ ] **Step 1: Units**

`deploy/mnt-truenas.mount`
```ini
[Unit]
Description=TrueNAS NFS export for hug-face-rip backups
# Soft mount + short timeouts: if the NAS goes away, I/O returns errors (the app
# retries the job) instead of hanging the progress poller and the dashboard's
# storage endpoint in uninterruptible sleep. retry=0: a failed mount attempt
# returns at once instead of retrying for minutes under the automount.

[Mount]
What=truenas.babendums.com:/mnt/RAIDZ1_1TB/hug-face-rip
Where=/mnt/truenas
Type=nfs4
Options=soft,timeo=50,retrans=2,retry=0,noatime,_netdev

[Install]
WantedBy=multi-user.target
```
`deploy/mnt-truenas.automount`
```ini
[Unit]
Description=Automount TrueNAS NFS export for hug-face-rip backups

[Automount]
Where=/mnt/truenas

[Install]
WantedBy=multi-user.target
```
`deploy/hug-face-rip.service`: add after the PORT line
```ini
# Named storage targets; first is the default. 'truenas' is the NFS export mounted
# by mnt-truenas.(auto)mount — it is only usable while /mnt/truenas/.hug-face-rip
# exists (i.e. the share is really mounted). No RequiresMountsFor: the app must
# start with the NAS down.
Environment=BACKUP_TARGETS=local=/mnt/zfshug,truenas=/mnt/truenas
```
- [ ] **Step 2: Docs**: `.env.example` (BACKUP_TARGETS block under BACKUP_DIR), README (targets paragraph + NFS install steps), CLAUDE.md (config/db/backup/main bullets, deployment section, "easy to get wrong" #5: the marker rule).
- [ ] **Step 3: Full suite** `.venv/bin/python -m pytest` → all pass, 0 warnings. Commit `feat(deploy): TrueNAS NFS mount units + docs`.
- [ ] **Step 4: Merge & push**: `git checkout master && git merge --no-ff feature/storage-targets-nfs && git push origin master && git branch -d feature/storage-targets-nfs`.
- [ ] **Step 5: Install on the host** (root, in the container):
```bash
apt-get install -y nfs-common
cp deploy/mnt-truenas.mount deploy/mnt-truenas.automount /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now mnt-truenas.automount
ls -la /mnt/truenas            # triggers the mount; must list the export
touch /mnt/truenas/.hug-face-rip && ls -la /mnt/truenas/.hug-face-rip
cp deploy/hug-face-rip.service /etc/systemd/system/ && systemctl daemon-reload && systemctl restart hug-face-rip
```
- [ ] **Step 6: Verify**: `curl -s localhost:8000/api/storage` shows both targets online; submit a tiny public repo to `truenas` (`curl -X POST localhost:8000/api/jobs -H 'Content-Type: application/json' -d '{"slug":"hf-internal-testing/tiny-random-bert","target":"truenas"}'`), watch it complete + verify, confirm files under `/mnt/truenas/models/...`, then delete it from the dashboard/API and confirm removal + journal lines.
