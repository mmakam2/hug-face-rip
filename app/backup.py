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
