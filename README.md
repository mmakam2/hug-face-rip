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

```bash
.venv/bin/uvicorn app.main:build_default_app --factory --reload
```

Open http://127.0.0.1:8000 and paste a repo slug (e.g. `bigscience/bloom`).
Each repo is saved to `BACKUP_DIR/<repo_type>s/<owner>/<name>`. Closing and
restarting the server resumes any in-flight backups automatically.

## Tests

```bash
.venv/bin/python -m pytest                       # unit tests (Hub mocked)
.venv/bin/python -m pytest -m integration        # end-to-end against the real Hub (network)
```
