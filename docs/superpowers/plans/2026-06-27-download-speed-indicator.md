# Download Speed Indicator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Show a single aggregate download-speed readout (e.g. `↓ 12.4 MB/s`) in the dashboard storage bar, computed client-side from data the page already polls.

**Architecture:** Pure function `computeSpeed(prevSample, jobs, now)` derives the instantaneous aggregate rate by summing positive per-job byte deltas across jobs that are `running` in two consecutive samples; a thin `updateSpeed(jobs)` wrapper applies EMA smoothing and resets when idle. The smoothed value is rendered as an extra segment in the existing storage-bar value line. No backend, DB, or API changes.

**Tech Stack:** Vanilla JS embedded in `app/static/index.html`. Node 20 (already present) is used only for an off-tree sanity check of the pure function.

## Global Constraints

- Client-side only — no changes to `app/*.py`, the DB schema, or any API endpoint. (verbatim from spec: "no backend / DB / API change")
- Reuse the existing `fmt()` byte formatter and `esc()` helper; follow the file's existing inline-`style=` pattern (as the `+planned` span already does).
- No committed JS test harness — the pure function is sanity-checked via a throwaway Node script under the scratchpad, and the feature is verified by running the app.
- Display-only: the speed value must never feed back into job status, progress, or completion logic.

---

### Task 1: Aggregate speed computation + rendering

**Files:**
- Modify: `app/static/index.html` (script block, ~lines 65–146)
- Sanity check (not committed): `/tmp/claude-0/-root-hug-face-rip/3528eaac-976f-4371-8890-ce13f5861c3e/scratchpad/speed_check.mjs`

**Interfaces:**
- Consumes: the `/api/jobs` response shape already used by `row()` — each job has `id`, `status`, `downloaded_bytes`. Also reuses existing `fmt(bytes)` and `esc(s)`.
- Produces:
  - `computeSpeed(prevSample, jobs, now) -> { speed: number|null, sample: { at: number, bytes: Map<number, number> } }`
  - `updateSpeed(jobs) -> number|null` (smoothed bytes/sec to display, or `null`)
  - `loadStorage(speed)` gains an optional `speed: number|null` parameter.

- [ ] **Step 1: Add module state + the two speed functions**

In `app/static/index.html`, immediately after the `esc` helper (line 72) and before `async function api(...)`, insert:

```js
// --- aggregate download speed (client-side, display-only) ---
let lastSample = null;     // { at: <ms>, bytes: Map<jobId, downloaded_bytes> }
let smoothedSpeed = null;  // EMA of bytes/sec across running jobs, or null when idle

// Pure: instantaneous aggregate rate (bytes/sec) + the new sample. Sums only
// positive per-job deltas for jobs running in BOTH samples, so a completed or
// deleted job (drops out), a newly started/resumed job (no prior entry), and
// Xet staging re-counts (negative blip) never produce a spurious spike.
function computeSpeed(prevSample, jobs, now) {
  const bytes = new Map();
  for (const j of jobs) {
    if (j.status === "running") bytes.set(j.id, j.downloaded_bytes || 0);
  }
  const sample = { at: now, bytes };
  if (!prevSample || now <= prevSample.at) return { speed: null, sample };
  const elapsed = (now - prevSample.at) / 1000;
  let delta = 0;
  for (const [id, cur] of bytes) {
    const prev = prevSample.bytes.get(id);
    if (prev !== undefined) delta += Math.max(0, cur - prev);
  }
  return { speed: delta / elapsed, sample };
}

// Update state from the latest jobs list; return the smoothed rate to display,
// or null when nothing is downloading / not enough data yet.
function updateSpeed(jobs) {
  const running = jobs.some(j => j.status === "running");
  const { speed, sample } = computeSpeed(lastSample, jobs, Date.now());
  lastSample = sample;
  if (!running) { smoothedSpeed = null; return null; }   // idle -> reset, start fresh next run
  if (speed === null) return smoothedSpeed;              // first sample of this run (stays null)
  smoothedSpeed = smoothedSpeed === null ? speed : 0.3 * speed + 0.7 * smoothedSpeed;
  return smoothedSpeed;
}
```

- [ ] **Step 2: Sanity-check the pure function with Node**

Write `speed_check.mjs` to the scratchpad path above with a copy of `computeSpeed` and these assertions, then run it. This is a throwaway check — do **not** add it to git.

```js
function computeSpeed(prevSample, jobs, now) {
  const bytes = new Map();
  for (const j of jobs) {
    if (j.status === "running") bytes.set(j.id, j.downloaded_bytes || 0);
  }
  const sample = { at: now, bytes };
  if (!prevSample || now <= prevSample.at) return { speed: null, sample };
  const elapsed = (now - prevSample.at) / 1000;
  let delta = 0;
  for (const [id, cur] of bytes) {
    const prev = prevSample.bytes.get(id);
    if (prev !== undefined) delta += Math.max(0, cur - prev);
  }
  return { speed: delta / elapsed, sample };
}
const assert = (c, m) => { if (!c) { console.error("FAIL:", m); process.exit(1); } };

// 1. No prior sample -> null speed, but the sample captures running bytes.
let r = computeSpeed(null, [{ id: 1, status: "running", downloaded_bytes: 100 }], 1000);
assert(r.speed === null, "first sample null");
assert(r.sample.bytes.get(1) === 100, "captures running bytes");

// 2. Steady: +1500 bytes over 1.5s -> 1000 B/s.
r = computeSpeed({ at: 1000, bytes: new Map([[1, 100]]) },
  [{ id: 1, status: "running", downloaded_bytes: 1600 }], 2500);
assert(r.speed === 1000, "steady 1000 B/s, got " + r.speed);

// 3. Completed job drops out -> no negative spike; only running job 2 counts.
r = computeSpeed({ at: 1000, bytes: new Map([[1, 5000], [2, 100]]) },
  [{ id: 1, status: "completed", downloaded_bytes: 5000 },
   { id: 2, status: "running", downloaded_bytes: 1100 }], 2000);
assert(r.speed === 1000, "completed ignored, got " + r.speed);

// 4. Newly running job skipped its first round (existing bytes don't spike).
r = computeSpeed({ at: 1000, bytes: new Map([[1, 100]]) },
  [{ id: 1, status: "running", downloaded_bytes: 200 },
   { id: 2, status: "running", downloaded_bytes: 9999 }], 2000);
assert(r.speed === 100, "new job skipped first round, got " + r.speed);

// 5. Byte count goes backwards (Xet jitter) -> clamped to 0.
r = computeSpeed({ at: 1000, bytes: new Map([[1, 500]]) },
  [{ id: 1, status: "running", downloaded_bytes: 400 }], 2000);
assert(r.speed === 0, "negative delta clamped, got " + r.speed);

console.log("OK");
```

Run: `node /tmp/claude-0/-root-hug-face-rip/3528eaac-976f-4371-8890-ce13f5861c3e/scratchpad/speed_check.mjs`
Expected: `OK` (exit 0). Any `FAIL: …` means the copied function diverged from Step 1 — fix Step 1 and re-run.

- [ ] **Step 3: Render the speed in the storage bar**

In `loadStorage`, add the `speed` parameter and a speed segment. Change the signature and the `.sval` line. `Math.round` keeps `fmt()` safe for sub-1-byte values (avoids a negative unit index) and shows `↓ 0 B/s` during a stall.

Change the function header:

```js
async function loadStorage(speed) {
```

Just before the `el.innerHTML =` assignment, add:

```js
    const speedTxt = speed != null
      ? ` · <span style="color:var(--accent)">↓ ${fmt(Math.round(speed))}/s</span>`
      : "";
```

And append `${speedTxt}` to the end of the `.sval` div (after `${plannedTxt}`):

```js
      `<div class="sval"><b>${fmt(s.used)}</b> / ${fmt(s.total)} used · ${fmt(s.free)} free${plannedTxt}${speedTxt}</div>`;
```

- [ ] **Step 4: Wire `refresh()` to compute speed and pass it to storage**

Replace the entire `refresh` function with the version below. It fetches jobs first, derives the smoothed speed, renders rows, then renders storage with the speed. If the jobs fetch throws, `speed` stays `null` and storage still renders.

```js
async function refresh() {
  let speed = null;
  try {
    const { jobs } = await api("/api/jobs");
    speed = updateSpeed(jobs);
    document.getElementById("rows").innerHTML = jobs.map(row).join("");
    document.getElementById("empty").style.display = jobs.length ? "none" : "block";
  } catch (e) { /* transient; keep polling */ }
  loadStorage(speed);
}
```

- [ ] **Step 5: Verify rendering in the running app**

Start the server and exercise it in a browser:

Run: `.venv/bin/python -m app.main` (or `HOST=127.0.0.1 PORT=8000 .venv/bin/python -m app.main`)

Then:
1. Open the dashboard, add a small repo (e.g. paste a tiny model slug).
2. Confirm that while it is `running`, a `↓ <rate>/s` segment appears at the end of the storage-bar value line and updates each ~1.5s without wild flicker.
3. Confirm the segment disappears once all downloads reach `completed` (no running jobs).
4. Confirm no console errors.

Stop the server when done (Ctrl-C). If a real download isn't practical in this environment, note that Step 2 already proves the math and this step verifies wiring — at minimum load the page with no jobs and confirm the storage bar still renders and no segment shows.

- [ ] **Step 6: Commit**

```bash
git add app/static/index.html
git commit -m "feat: aggregate download-speed indicator on dashboard

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Document the indicator in the README

**Files:**
- Modify: `README.md:67-68`

**Interfaces:**
- Consumes: nothing. Pure docs.
- Produces: nothing consumed by other tasks.

- [ ] **Step 1: Update the dashboard description**

Replace the sentence at `README.md:67-68`:

Old:
```
Each row shows live progress, and a disk-usage bar shows current usage plus
*planned* usage from queued and in-flight jobs (so you can see whether what's
queued will fit). You can **retry** a failed backup, **cancel** a queued one, or
```

New:
```
Each row shows live progress, and a disk-usage bar shows current usage plus
*planned* usage from queued and in-flight jobs (so you can see whether what's
queued will fit). While downloads are running it also shows the **aggregate
download speed** across all active jobs. You can **retry** a failed backup,
**cancel** a queued one, or
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: note the dashboard download-speed indicator

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- Aggregate client-side speed → Task 1 Steps 1, 4. ✓
- `computeSpeed(prevSample, jobs, now)` pure function with stated algorithm → Task 1 Step 1. ✓
- Per-job positive deltas; completed/new/resumed/Xet-jitter handling → Task 1 Step 1 + verified Task 1 Step 2 (cases 3,4,5). ✓
- Wall-clock elapsed via `Date.now()` → Task 1 Step 1 (`updateSpeed`). ✓
- EMA smoothing (0.3/0.7) + explicit reset-when-idle / first-sample logic → Task 1 Step 1 (`updateSpeed`). ✓
- Render in storage bar `.sval`, reuse `fmt()`, only when `smoothedSpeed != null` → Task 1 Steps 3, 4. ✓
- Data-flow wiring (jobs → speed → rows → `loadStorage(speed)`; fetch-fail → null) → Task 1 Step 4. ✓
- Edge cases (first load, backgrounded, all finish, fetch fail) → handled by Step 1 logic; manually spot-checked in Step 5. ✓
- Manual verification, no Node toolchain committed → Task 1 Steps 2, 5 (scratchpad only). ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code; commands have expected output. ✓

**Type consistency:** `computeSpeed` returns `{ speed, sample }` and is consumed that way in `updateSpeed`; `updateSpeed(jobs)` returns the value passed as `loadStorage`'s `speed` param; `lastSample`/`smoothedSpeed` names consistent across Steps 1, 3, 4. ✓
