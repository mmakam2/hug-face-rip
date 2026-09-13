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


def test_stall_timeout_defaults_to_600(tmp_path):
    s = load_settings(base_env(tmp_path))
    assert s.stall_timeout == 600.0


def test_stall_timeout_from_env(tmp_path):
    s = load_settings(base_env(tmp_path) | {"STALL_TIMEOUT_SECONDS": "45"})
    assert s.stall_timeout == 45.0


def test_invalid_stall_timeout_raises(tmp_path):
    with pytest.raises(ConfigError):
        load_settings(base_env(tmp_path) | {"STALL_TIMEOUT_SECONDS": "notanumber"})


# --- multiple storage targets ---

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
