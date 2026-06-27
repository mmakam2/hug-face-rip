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


def test_invalid_integer_raises(tmp_path):
    env = base_env(tmp_path) | {"MAX_CONCURRENT_JOBS": "notanumber"}
    with pytest.raises(ConfigError):
        load_settings(env)
