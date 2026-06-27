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

## Tests

```bash
.venv/bin/python -m pytest                       # unit tests (Hub mocked)
.venv/bin/python -m pytest -m integration        # end-to-end against the real Hub (network)
```
