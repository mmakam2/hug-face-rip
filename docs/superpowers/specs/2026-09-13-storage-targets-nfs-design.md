# Multiple Storage Targets (local + NFS) — Design

**Date:** 2026-09-13
**Status:** Approved (user chose: systemd-managed mount; one copy per repo)

## Goal

Back repos up to more than one directory, chosen per download, so a TrueNAS
NFS export (`truenas.babendums.com:/mnt/RAIDZ1_1TB/hug-face-rip`) can sit next
to the local ZFS disk. The app sees **named directories**; the OS mounts NFS.

## Scope

- **In:** `BACKUP_TARGETS` config; a `target` column on jobs; target chosen at
  submit (dropdown); every disk operation resolved per job target; **offline
  detection** so an unmounted share is never written to; per-target storage
  bars; NFS mount units + install steps; docs; tests.
- **Out (YAGNI):** moving a repo between targets, a second copy of the same
  repo on another target (uniqueness stays `repo_type + slug`), per-target
  concurrency limits, the app mounting anything itself.

## Config (`app/config.py`)

```
BACKUP_TARGETS=local=/mnt/zfshug,truenas=/mnt/truenas
```

Comma-separated `name=path`; names match `^[A-Za-z0-9_-]+$`, must be unique,
paths are `expanduser`ed. **The first entry is the default target.** When
`BACKUP_TARGETS` is unset, `BACKUP_DIR` (required, as today) becomes the single
target named `local`; when it is set, `BACKUP_DIR` is ignored. `Settings` keeps
`backup_dir` (= the default target's path, so existing callers/tests keep
working) and gains `targets: Mapping[str, Path]` (ordered) plus helpers
`default_target`, `target_dir(name)`.

Only the **default** target is created/write-checked at startup (as today).
Other targets are *not* created: an absent mount must stay an empty directory
that the app refuses to use.

## Offline detection

A non-default target is **online** iff `<path>/.hug-face-rip` is a regular file
(`config.MARKER`). The marker is created once on the share after its first
mount. Without it (share not mounted, NAS down, wrong path) the target is
offline: `POST /api/jobs` for it is refused with 409, the dispatcher does not
claim its queued jobs (they show as *held · target offline*), and the storage
panel greys it out. The default target is online iff its directory exists.

Why a marker and not `ismount`: it is filesystem-agnostic, testable with a
tmp dir, and also catches "mounted the wrong export".

## Database (`app/db.py`)

- `jobs.target TEXT NOT NULL DEFAULT 'local'`; `JobStore(db_path,
  default_target="local")` migrates a pre-existing table with
  `ALTER TABLE jobs ADD COLUMN target … DEFAULT '<default_target>'` so every
  existing row lands on the configured default.
- `Job.target`; `create_job(slug, repo_type, target)`.
- `next_runnable_job(targets=None)`: when given, only jobs whose target is in
  the list (the dispatcher passes the currently-online names).
- `pending_bytes(target=None)`: optional filter for per-target bars.

## Engine (`app/backup.py`, `app/retry.py`)

- Every `settings.backup_dir` use becomes `settings.target_dir(job.target)`:
  download dir, pre-flight free-space check, deleter, verify.
- Worker guard: if the job's target is offline when the worker starts (it went
  away between claim and start), raise `TargetOffline` → recorded as a
  **transient** failure (`retrying`, backoff, files kept).
- Dispatcher: computes the online target names each tick and passes them to
  `next_runnable_job`.
- `retry.is_retryable`: add `EIO`, `ESTALE`, `ENOTCONN`, `EHOSTDOWN` to the
  transient errno set so a share dropping mid-download backs off and resumes
  instead of failing permanently.

## API (`app/main.py`)

- `POST /api/jobs` body `{slug, target?}`; missing → default; unknown → 400;
  offline → 409 `target 'x' is offline (not mounted?)`. Job dicts include
  `target`.
- `GET /api/storage`: keeps today's top-level fields (they describe the default
  target) and adds `targets: [{name, path, default, online, total, used, free,
  planned}]` (zeros when offline).

## Dashboard (`app/static/index.html`)

- A `<select>` beside the slug input, filled from `/api/storage`, hidden when
  only one target exists; default preselected.
- Storage panel: one bar per target (name + path + used/free + planned); an
  offline target shows *offline* in red instead of numbers.
- Rows: a small `target` badge after the type badge when >1 target; queued
  rows whose target is offline read *held · <name> offline*.

## Deployment

- `deploy/mnt-truenas.mount` + `deploy/mnt-truenas.automount`:
  `What=truenas.babendums.com:/mnt/RAIDZ1_1TB/hug-face-rip`, `Where=/mnt/truenas`,
  `Type=nfs4`, `Options=soft,timeo=50,retrans=2,retry=0,noatime,_netdev`.
  `soft` + short timeouts: a NAS outage returns errors (→ retrying jobs, slow
  but live dashboard) instead of hanging the poller and the storage endpoint
  in D-state forever. The automount re-mounts on next access once the NAS is
  back; `retry=0` keeps a failed attempt from blocking for minutes.
- `deploy/hug-face-rip.service`: `Environment=BACKUP_TARGETS=local=/mnt/zfshug,truenas=/mnt/truenas`.
  No `RequiresMountsFor`: the app must start with the NAS down.
- Install: `apt-get install -y nfs-common`; copy the two units;
  `systemctl daemon-reload && systemctl enable --now mnt-truenas.automount`;
  `ls /mnt/truenas` (triggers the mount); `touch /mnt/truenas/.hug-face-rip`;
  install the service unit; restart. TrueNAS side: the export must authorize
  this host and map root (the app runs as root).
- Outage behaviour to document: while the NAS is unreachable, calls that
  touch it (`is_file`, `statfs`) can take up to ~10 s each, so the dashboard's
  storage panel and the dispatcher tick slow down; local downloads continue.

## Testing (offline)

- config: parse/order/default; unset → `local`=BACKUP_DIR; bad name, duplicate
  → `ConfigError`; non-default targets not created; `target_online` marker rule.
- retry: `OSError(EIO)` / `ESTALE` retryable.
- db: fresh schema has `target`; migration adds it with the configured
  default; `next_runnable_job(targets=…)` filter; `pending_bytes(target)`.
- worker/dispatcher: files land under the job's target dir; offline target at
  start → `retrying`; dispatcher leaves offline-target jobs queued and still
  runs online ones.
- api: create with target / unknown 400 / offline 409 / default; storage
  `targets` list with online flags; rows carry `target`.
- static: select element, per-target bars, target badge, held label.
