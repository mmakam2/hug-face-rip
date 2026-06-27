# HF Repo Backup Dashboard

A small FastAPI web app that backs up entire Hugging Face Hub repositories
(models, datasets, spaces) to a local folder — with bounded concurrency,
live progress, and automatic resume.

## Setup

1. Install dependencies:
   ```bash
   python -m pip install -r requirements.txt
   ```
2. Create a `.env` (see `.env.example`):
   ```
   HUGGINGFACE_ACCESS_KEY=hf_your_token_here
   BACKUP_DIR=./backups
   MAX_CONCURRENT_JOBS=2
   MAX_WORKERS=8
   DB_PATH=jobs.db
   ```

## Run

The simplest way binds to all interfaces (`0.0.0.0:8000`) by default:

```bash
.venv/bin/python -m app.main
```

Override the bind address with the `HOST` / `PORT` environment variables:

```bash
HOST=127.0.0.1 PORT=9000 .venv/bin/python -m app.main
```

Or run via uvicorn directly (e.g. with autoreload during development):

```bash
.venv/bin/uvicorn app.main:build_default_app --factory --host 0.0.0.0 --port 8000 --reload
```

Open the dashboard (e.g. http://127.0.0.1:8000, or `http://<this-host-ip>:8000`
from another machine) and paste a repo slug (e.g. `bigscience/bloom`). Each repo
is saved to `BACKUP_DIR/<repo_type>s/<owner>/<name>`. Closing and restarting the
server resumes any in-flight backups automatically.

> **Security note:** binding to `0.0.0.0` exposes the dashboard to your whole
> network. It has no authentication and triggers downloads using your Hugging
> Face token, so only run it on a trusted network (or behind a firewall). To
> restrict it to this machine only, set `HOST=127.0.0.1`.

## Run as a service (systemd)

A unit file is provided at [`deploy/hf-backup.service`](deploy/hf-backup.service)
(paths assume the app lives at `/root/hug-face-rip`). Install it with:

```bash
sudo cp deploy/hf-backup.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now hf-backup.service
sudo systemctl status hf-backup.service
```

It restarts on failure and, because the app re-queues unfinished jobs on
startup, an interrupted download resumes automatically.

### Tuning for small / memory-constrained hosts

The unit sets a few environment knobs that matter on low-RAM boxes (e.g. a
small VM or LXC container):

- **`HF_HUB_DISABLE_XET=1`** — **important.** Hugging Face's Xet downloader
  uses adaptive concurrency that ramps download buffers into the **gigabytes**,
  which OOM-kills the process on a constrained host. Disabling it falls back to
  plain HTTP streaming with near-constant (~100 MB) memory. Keep this set unless
  the host has plenty of RAM headroom.
- **`MAX_WORKERS` / `MAX_CONCURRENT_JOBS`** — lowered (2 / 1) to bound the number
  of files downloaded in parallel.
- **`MemoryMax=2560M`** — a hard cgroup cap so a runaway download is contained to
  this service instead of taking down the whole host.

The app also performs a **pre-flight disk-space check**: before downloading, it
compares the repo's total size against free space in `BACKUP_DIR` and fails the
job with a clear message (rather than filling the disk) if it cannot fit.

## Tests

```bash
.venv/bin/python -m pytest                       # unit tests (Hub mocked)
.venv/bin/python -m pytest -m integration        # end-to-end against the real Hub (network)
```
