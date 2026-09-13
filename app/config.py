import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Mapping, Optional


class ConfigError(Exception):
    """Raised when required configuration is missing or invalid."""


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
    """Whether a target can be written to right now. The default target is
    always online (load_settings created and write-checked it); any other
    target must carry the marker file at its root — an unmounted NFS share is
    just an empty local directory, and writing a 300 GB repo into that would
    fill the root disk."""
    path = settings.target_dir(name)
    if name == settings.default_target:
        return True
    try:
        return (path / MARKER).is_file()
    except OSError:          # e.g. a soft-mounted share timing out
        return False


def _parse_targets(raw: str) -> Dict[str, Path]:
    targets: Dict[str, Path] = {}
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


def _int(env: Mapping[str, str], key: str, default: int) -> int:
    raw = env.get(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(f"{key} must be an integer, got {raw!r}")


def _float(env: Mapping[str, str], key: str, default: float) -> float:
    raw = env.get(key)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        raise ConfigError(f"{key} must be a number, got {raw!r}")


def _bool(env: Mapping[str, str], key: str, default: bool) -> bool:
    raw = env.get(key)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off")


def load_settings(env: Optional[Mapping[str, str]] = None) -> Settings:
    env = os.environ if env is None else env

    token = env.get("HUGGINGFACE_ACCESS_KEY")
    if not token:
        raise ConfigError("HUGGINGFACE_ACCESS_KEY is not set")

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

    return Settings(
        hf_token=token,
        backup_dir=backup_dir,
        targets=targets,
        max_concurrent_jobs=_int(env, "MAX_CONCURRENT_JOBS", 2),
        max_workers=_int(env, "MAX_WORKERS", 8),
        db_path=Path(env.get("DB_PATH") or "jobs.db"),
        verify_downloads=_bool(env, "VERIFY_DOWNLOADS", True),
        stall_timeout=_float(env, "STALL_TIMEOUT_SECONDS", 600.0),
    )
