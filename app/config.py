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
    verify_downloads: bool = True


def _int(env: Mapping[str, str], key: str, default: int) -> int:
    raw = env.get(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(f"{key} must be an integer, got {raw!r}")


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
        verify_downloads=_bool(env, "VERIFY_DOWNLOADS", True),
    )
