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

    def delete_job(self, job_id: int) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
            self._conn.commit()

    def unfinished_jobs(self) -> List[Job]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE status IN ('queued', 'running') ORDER BY id"
            ).fetchall()
        return [self._to_job(r) for r in rows]

    def pending_bytes(self) -> int:
        """Bytes still to be downloaded across running + queued jobs.

        The per-row max(..., 0) guards against a row whose reported progress
        exceeds its total (e.g. transient over-counting of in-flight staging).
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(SUM(MAX(total_bytes - downloaded_bytes, 0)), 0) "
                "FROM jobs WHERE status IN ('running', 'queued')"
            ).fetchone()
        return row[0] or 0

    def close(self) -> None:
        with self._lock:
            self._conn.close()
