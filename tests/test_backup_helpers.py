from pathlib import Path
from unittest.mock import MagicMock
import pytest
from huggingface_hub.utils import RepositoryNotFoundError
from app.backup import detect_repo_types, repo_total_bytes, local_dir_for, directory_size, delete_backup_files


class _Sibling:
    def __init__(self, size):
        self.size = size


class _Info:
    def __init__(self, siblings):
        self.siblings = siblings


def _fake_response():
    """Minimal mock response accepted by HfHubHTTPError.__init__."""
    resp = MagicMock()
    resp.headers = {}
    return resp


class FakeApi:
    """repo_info returns a mapping value or raises RepositoryNotFoundError."""
    def __init__(self, table):
        self.table = table  # {(slug, repo_type): _Info}
        self.calls = []

    def repo_info(self, repo_id, repo_type, token=None, files_metadata=False):
        self.calls.append((repo_id, repo_type))
        value = self.table.get((repo_id, repo_type))
        if value is None:
            raise RepositoryNotFoundError(
                f"{repo_id} ({repo_type}) not found", response=_fake_response()
            )
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


def test_directory_size_counts_incomplete_staging_under_cache(tmp_path):
    # Xet stages in-flight downloads as .cache/huggingface/download/*.incomplete;
    # those bytes are really on disk and must count toward progress.
    (tmp_path / "done.safetensors").write_bytes(b"x" * 10)
    download = tmp_path / ".cache" / "huggingface" / "download"
    download.mkdir(parents=True)
    (download / "abc.incomplete").write_bytes(b"y" * 7)
    # non-staging .cache cruft (metadata, locks) stays excluded
    (download / "abc.metadata").write_bytes(b"z" * 1000)
    assert directory_size(tmp_path) == 17  # 10 completed + 7 in-flight


def test_directory_size_missing_path_is_zero(tmp_path):
    assert directory_size(tmp_path / "nope") == 0


def test_delete_backup_files_removes_dir_within_backup(tmp_path):
    backup = tmp_path / "backups"
    d = local_dir_for(backup, "model", "owner/name")
    d.mkdir(parents=True)
    (d / "weights.bin").write_bytes(b"x" * 100)
    delete_backup_files(backup, "model", "owner/name")
    assert not d.exists()


def test_delete_backup_files_missing_dir_is_noop(tmp_path):
    backup = tmp_path / "backups"
    backup.mkdir()
    delete_backup_files(backup, "model", "ghost/repo")  # nothing there -> no error


def test_delete_backup_files_refuses_path_outside_backup(tmp_path):
    backup = tmp_path / "backups"
    backup.mkdir()
    outside = tmp_path / "secret"
    outside.mkdir()
    (outside / "important").write_bytes(b"keep")
    # slug crafted to escape backup/models/ via traversal
    with pytest.raises(ValueError):
        delete_backup_files(backup, "model", "../../secret")
    assert (outside / "important").exists()  # untouched
