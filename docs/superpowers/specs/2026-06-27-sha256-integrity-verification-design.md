# SHA256 Integrity Verification — Design

**Date:** 2026-06-27
**Status:** Approved (pending spec review)

## Goal

After a download completes — automatically, and on demand for already-completed
backups — verify every file on disk against Hugging Face's reported hashes, so a
silently corrupt or truncated file (Xet staging glitch, OOM mid-file, disk
error) is caught instead of sitting in the backup looking "done". A mismatch
surfaces as a distinct **corrupted** result with the offending files listed and
a one-click clean re-download.

## Coverage: two hash algorithms

`repo_info(files_metadata=True)` reports hashes differently per file, confirmed
against the live Hub (`huggingface_hub` 1.21.0, `RepoSibling`):

- **LFS-tracked files** (the large weights — `.safetensors`, `.bin`, `.h5`,
  `.tflite`, `.msgpack`, …) carry `sibling.lfs.sha256`, a content **SHA256**.
- **Plain git files** (`config.json`, `tokenizer.json`, `.gitattributes`,
  `README.md`, …) carry only `sibling.blob_id`, a **git blob SHA1** —
  `sha1(b"blob " + str(size) + b"\0" + content)`.

So full integrity needs both algorithms. "Verified ✓" means **every** file in
the repo matched. `snapshot_download(local_dir=...)` writes raw blob bytes to
disk (no working-tree smudge/EOL filters), so the on-disk bytes equal the bytes
the `blob_id` is computed over — git-sha1 of the local file equals `blob_id`.
Tests assert this against `git hash-object`.

## Scope

- **In scope:** a pure verification module; automatic post-download verification
  (env-gated, default on); a manual Verify button for completed jobs; a distinct
  `corrupted` result; clean Re-download of a corrupted repo; a `verifying`
  lifecycle status; verify progress on the existing bar; UI + API + config +
  offline tests.
- **Out of scope (future):** partial re-download of only the bad files (v1
  re-fetches the whole repo); granular per-file streaming progress beyond the
  byte bar; verifying arbitrary local folders not tracked as jobs.

## New module — `app/verify.py` (pure, offline-testable)

A pure unit over `(local_dir, expected-list)` → report. No network, no DB, no
mocks; tests use real temp files.

- `sha256_file(path, stop=None) -> str` — stream the file in chunks (1 MiB),
  checking `stop` (a `threading.Event`) each chunk so a long hash aborts within
  one chunk. Returns lowercase hex.
- `git_blob_sha1(path, stop=None) -> str` — same streaming, but seed the SHA1
  with the git blob header `b"blob %d\0" % size` before the content.
- `expected_file_hashes(siblings) -> list[FileHash]` where
  `FileHash = (rfilename, algo, expected_hex)`; `algo = "sha256"` when
  `sibling.lfs` is present (use `lfs.sha256`), else `"git-sha1"` from `blob_id`.
  A sibling with neither (should not happen with `files_metadata=True`) is
  skipped and noted.
- `verify_repo(local_dir, expected, stop=None, on_progress=None) -> VerifyReport`
  — for each `FileHash`, resolve `local_dir / rfilename`; if absent →
  `missing`; else hash with the matching algo and compare → `mismatch` on
  inequality, `read-error` on `OSError`. `on_progress(bytes_hashed_so_far)` is
  called as it goes. If `stop` is set mid-run, raise `VerifyAborted` (caller
  maps it to the interrupt path). Returns
  `VerifyReport(ok: bool, failures: list[{"file", "reason"}])`.

`reason ∈ {mismatch, missing, read-error}`. Files under `.cache/` and any extra
local files not in `expected` are ignored — only declared repo files are
checked, and a missing declared file is a failure.

## Why a thread, not a subprocess

Downloads run in a terminable child process because `snapshot_download` cannot
be interrupted cooperatively. Hashing is **our** loop, so it runs directly in
the worker thread and aborts by checking a stop `Event` between chunks —
near-instant, no `SIGTERM`, no launcher change. It plugs into the existing
`RunningRegistry`: the verify phase registers a tiny stop-handle whose
`terminate()` sets the event, so per-job stop, global pause-all, and shutdown
all reach a running verification through the same path as a download.

## Data model (`db.py`)

Migrated in `JobStore.__init__` exactly like the retry columns (additive
`ALTER TABLE` guarded by a `PRAGMA table_info` check), so a pre-existing DB
upgrades in place.

- New lifecycle status **`verifying`** — active; it occupies a concurrency slot.
  Full lifecycle becomes:
  `queued → running → verifying → completed | failed | paused | retrying`
  (verifying is entered from `running` after a successful download, or from
  `completed` via the manual Verify button).
- `verify_status TEXT NOT NULL DEFAULT 'unverified'` →
  `unverified | verified | corrupted`. Orthogonal to `status`: it is the
  persistent verification outcome shown on a completed job.
- `verify_detail TEXT` (nullable JSON **object**, so the UI branches on a stable
  shape): `{"failures": [{"file": ..., "reason": ...}, ...]}` when `corrupted`,
  or `{"note": "..."}` when **unverifiable** (e.g. Hub unreachable while fetching
  reference hashes — see below). `null` when verified clean.
- `running_count()` counts `status IN ('running', 'verifying')` so a
  verification occupies a slot and the dispatcher doesn't over-subscribe.
- `Job` dataclass gains `verify_status` and `verify_detail`; `to_dict()`
  exposes both.

New `JobStore` helpers (each guarded by the existing lock, following the current
style):

- `set_verify_status(job_id, verify_status, detail=None)`.
- `set_status(..., 'verifying')` reused for entering the phase.

### Verify progress reuses the existing bar

While `verifying`, `on_progress` writes `downloaded_bytes = bytes_hashed`, so the
existing progress bar fills as hashing proceeds (status rendered amber). The
file is fully present during any verification, so the correct end value is known:
**every exit path from the verify phase restores `downloaded_bytes = total_bytes`**
(the bar returns to 100%). This invariant avoids leaving a completed download
showing a partial bar and needs no new progress column.

## Control flow (`app/backup.py`)

### Shared phase — `_verify_phase(job_id, store, settings, api, registry, stopping)`

1. `store.set_status(job_id, 'verifying')`.
2. Fetch expected hashes via `repo_info(files_metadata=True)` →
   `expected_file_hashes(...)`. **If this network call fails**, the result is
   *unverifiable*, **not** corrupted: restore `downloaded_bytes = total`, set
   `status = completed`, leave `verify_status = unverified`, and write a note to
   `verify_detail` ("could not reach the Hub to verify; try again"). Return.
3. Build a stop `Event`; `registry.register(job_id, StopHandle(stop))`. Honor an
   intent already recorded between claim and registration (mirrors the existing
   download race handling): if global pause is on or a stop intent exists,
   set the event immediately.
4. Run `verify_repo(local_dir, expected, stop, on_progress)` where `on_progress`
   does `store.update_progress(job_id, bytes_hashed)`.
5. On normal completion, read `registry.intent(job_id)`:
   - no intent → restore `downloaded_bytes = total`, `status = completed`,
     `verify_status = verified` (clean) or `corrupted` with the failures JSON in
     `verify_detail`.
   - `stop_verify` / `requeue` (global pause-all) / shutdown (`stopping` set) →
     restore `downloaded_bytes = total`, `status = completed`,
     `verify_status = unverified`. **A verifying job's download is already
     complete, so it is never deleted and never re-queued for download.**
6. `finally`: `registry.unregister(job_id)` (clears handle + intent, as today).

`VerifyAborted` from `verify_repo` is caught and routed through step 5's
interrupt branch.

### Automatic — inside `run_backup_job`

After the existing `outcome.ok` branch sets progress and `reset_retry`, instead
of jumping to `completed`:

- If `settings.verify_downloads` → call `_verify_phase(...)` (which sets the
  final `completed` + `verify_status`).
- Else → behave exactly as today: `status = completed`,
  `verify_status = unverified`.

The download child's handle is unregistered in the existing `finally`;
`_verify_phase` re-registers its own stop-handle under the same `job_id`
(`register` overwrites), and the outer `finally` unregisters once.

### Manual — `JobRunner.verify(job_id)` and `run_verify_job(...)`

- `JobRunner.verify(job_id)` submits `run_verify_job` to the **same**
  `ThreadPoolExecutor` (so `max_concurrent_jobs` still bounds total active
  work). `run_verify_job` loads the job and calls `_verify_phase`. Status moves
  to `verifying` when the task actually starts (not at click time), so a verify
  that's queued behind full slots doesn't show a misleading state.
- `JobRunner.stop_verify(job_id)` → `registry.request(job_id, "stop_verify")`.
- Global `pause_all()` already calls `registry.request_all("requeue")`, which
  reaches a verify stop-handle and ends it via step 5's interrupt branch (→
  `completed` + `unverified`). No change needed there beyond the intent mapping.

### Re-download (corrupted → clean fetch)

`JobRunner` / endpoint for a `corrupted` job: `delete_backup_files(...)` then
`store.requeue(job_id)` + `store.reset_retry(job_id)` and clear
`verify_status`/`verify_detail` back to `unverified`/`null`. The dispatcher
re-downloads from scratch. v1 deletes the **whole** repo dir (simple and
correct); partial re-download of only the bad files is a future optimization
(noted because it requires also clearing the matching
`.cache/huggingface/download/<file>.metadata` so the downloader refetches).

## API (`app/main.py`)

- `POST /api/jobs/{id}/verify` — 404 if missing; **409 unless `completed`**;
  else `runner.verify(id)` and return `{"verifying": id}`.
- `POST /api/jobs/{id}/stop-verify` — 404 if missing; 409 unless `verifying`;
  else `runner.stop_verify(id)` and return `{"stopping": id}`.
- `POST /api/jobs/{id}/redownload` — 404 if missing; **409 unless
  `verify_status == corrupted`**; else delete files + requeue + reset and return
  the refreshed job.
- `Job.to_dict()` already flows through `/api/jobs`; `verify_status` and
  `verify_detail` ride along automatically.

## Config (`app/config.py`, `.env.example`)

- `Settings.verify_downloads: bool`, read from `VERIFY_DOWNLOADS` (default `1`/
  true; `0`/`false`/`no` disables). Parsed in `load_settings()` alongside the
  other optional env vars.
- `.env.example` gains a documented `VERIFY_DOWNLOADS=1` line (default on; set
  `0` to skip the post-download hash pass on slow or very large-repo hosts; the
  manual Verify button always works).

## UI (`app/static/index.html`)

- **`verifying`** row → amber status text "verifying", progress bar driven by
  the (re-used) `downloaded_bytes/total_bytes` percent, and a **Stop** button
  (`stop-verify`).
- **`completed`** row → a small verify badge after the status:
  - `verify_status == "verified"` → green **✓ verified**.
  - `verify_status == "corrupted"` → red **⚠ corrupted**; render the failed-file
    list (from `verify_detail`) in the existing `.err` area, and show a
    **Re-download** button (reusing the existing confirm-dialog pattern, since it
    discards and refetches) alongside the current **Delete**.
  - `verify_status == "unverified"` → muted **unverified**; show a **Verify**
    button (verification is also re-runnable on already-verified jobs).
- A transient *unverifiable* note (Hub unreachable) is shown from `verify_detail`
  next to the unverified badge.
- New CSS classes follow the existing `.st.*` / `.badge` / `.err` conventions
  (greens reuse `--ok`, reds reuse `--err`, amber reuses the paused/retrying
  `#d2a24c`).

## Edge cases

- **Hub unreachable at verify time** → *unverifiable*, not corrupted: stays
  `completed` + `unverified` with a note; Verify can be retried. The distinction
  (cannot-check ≠ failed-check) is deliberate.
- **Missing expected file** → `corrupted` (`reason: missing`) — catches an
  incomplete snapshot that still reported `ok`.
- **Extra local files / `.cache/`** → ignored; only declared siblings are
  checked.
- **Repo with no LFS files** → every file verified via git-sha1; still a full
  check.
- **Interrupt during verify** (Stop, pause-all, shutdown) → `completed` +
  `unverified`, files untouched. Only explicit Re-download deletes.
- **Slot accounting** → `running_count` includes `verifying`, so the dispatcher
  treats a verification as occupying a slot.
- **Re-verify** of a `verified`/`corrupted` job is allowed (idempotent).

## Testing (offline, existing fake patterns)

- `tests/test_verify.py` (new):
  - `git_blob_sha1` of sample files equals `git hash-object` output;
    `sha256_file` equals `hashlib`.
  - `expected_file_hashes` maps lfs→sha256 and plain→git-sha1 from fake
    siblings.
  - `verify_repo` over a temp dir: all-good → `ok`; flipped byte → `mismatch`;
    deleted file → `missing`; a set `stop` event mid-run → `VerifyAborted`.
- `tests/test_backup_worker.py` (extend): a fake launcher reports `ok`, then the
  worker runs `_verify_phase` → assert `completed` + `verified`; a tampered file
  → `completed` + `corrupted` with `verify_detail`; `verify_downloads=False`
  skips verification (→ `completed` + `unverified`).
- `tests/test_api.py` (extend): `/verify` on a non-completed job → 409;
  `/redownload` on a non-corrupted job → 409; `verify_status`/`verify_detail`
  present in `/api/jobs`.
- `tests/test_db.py` (extend): migration adds the two columns to a pre-existing
  table with correct defaults; `running_count` counts `verifying`.

## Docs

Update `CLAUDE.md`: the status-lifecycle line (add `verifying` + the
`verify_status` outcome), the `db.py`/`backup.py`/`main.py`/`config.py`
descriptions, a new `app/verify.py` entry, the endpoint list (`verify`,
`stop-verify`, `redownload`), and the `VERIFY_DOWNLOADS` env var. Add an
integrity note to the "two things easy to get wrong" section (LFS→sha256 vs
plain→git-sha1; cannot-verify ≠ corrupted).
