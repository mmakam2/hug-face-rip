# Download Speed Indicator — Design

**Date:** 2026-06-27
**Status:** Approved (pending spec review)

## Goal

Show a single **aggregate download speed** on the dashboard — one combined
throughput number across all currently-running backup jobs (e.g. `↓ 12.4 MB/s`).
Not per-job, no ETA. Display-only.

## Scope

- **In scope:** one aggregate rate, computed client-side, rendered in the
  existing storage bar.
- **Out of scope:** per-job speed, per-job ETA, persistence across reloads,
  any backend / DB / API change.

## Approach

Computed **client-side** in `app/static/index.html`, driven by the existing
1.5s `refresh()` loop. No backend changes: no DB column, no API change. The
speed is a derived, display-only value, so it does not need to live in the
backend "source of truth" (which governs progress correctness and download
completion, not a cosmetic rate readout).

## Computation

State (module-level in the dashboard script):

- `lastSample` — `{ at: <ms epoch>, bytes: Map<jobId → downloaded_bytes> }`,
  or `null` before the first sample.
- `smoothedSpeed` — last EMA-smoothed bytes/sec, or `null`.

A pure function does the math so it can be reasoned about and (optionally)
tested in isolation:

```
computeSpeed(prevSample, jobs, now) -> { speed, sample }
```

Algorithm, each refresh after `/api/jobs` returns:

1. Build `currentBytes` = `Map<jobId → downloaded_bytes>` over jobs with
   `status === "running"`.
2. If there is no `prevSample`, or `now <= prevSample.at`, return
   `{ speed: null, sample: { at: now, bytes: currentBytes } }`
   (not enough data this round).
3. `elapsed = (now - prevSample.at) / 1000` seconds.
4. `deltaBytes = sum over jobId in currentBytes of
   max(0, currentBytes[jobId] - prevSample.bytes[jobId])`, where a jobId
   absent from `prevSample.bytes` contributes 0 (skipped its first round).
5. `instant = deltaBytes / elapsed`.
6. Return `{ speed: instant, sample: { at: now, bytes: currentBytes } }`.

The caller applies EMA smoothing and stores state. Order matters so the reset
case is unambiguous:

1. `result = computeSpeed(lastSample, jobs, now)`
2. `lastSample = result.sample`
3. If there are **no `running` jobs**, reset `smoothedSpeed = null` (a later
   download starts fresh) and stop.
4. Else if `result.speed == null` (first sample of this download), leave
   `smoothedSpeed` unchanged (stays `null` until the next round produces a rate).
5. Else EMA:
   `smoothedSpeed = (smoothedSpeed == null) ? result.speed
                    : 0.3 * result.speed + 0.7 * smoothedSpeed`

### Why per-job positive deltas (not one grand total)

Diffing a single summed total breaks at job boundaries. Per-job deltas make it
robust:

- **Completed / deleted job** drops out of the running set → contributes
  nothing, instead of a negative spike when the summed total falls.
- **Newly started or resumed job** is absent from `prevSample` → skipped its
  first round, so its already-on-disk bytes don't register as a one-time burst.
- **Xet staging re-count jitter** → `max(0, …)` clamps small negative blips.

### Why wall-clock elapsed (not the fixed 1.5s)

`refresh()` cadence drifts with network latency and tab throttling. Using the
actual `now - prevSample.at` keeps the rate honest when polls are late.

## UI / placement

Append to the storage bar's existing value line (`.sval`), after the
free/planned text:

```
<b>1.2 TB</b> / 4.0 TB used · 412 GB free · +18 GB planned · ↓ 12.4 MB/s
```

- Reuse the existing `fmt()` byte formatter, with a `/s` suffix and a `↓` glyph.
- Render only when `smoothedSpeed != null`. Per the caller logic above, that is
  exactly "≥1 running job and at least one rate has been computed" — when nothing
  is downloading the value is reset to `null` and the segment is omitted.
- During a genuine stall the value reads near 0 — useful signal, kept as-is.

### Data flow wiring

`refresh()` is restructured so the speed is computed from the jobs response and
passed into storage rendering explicitly:

1. `await api("/api/jobs")` → jobs.
2. `computeSpeed(...)` + EMA update → `smoothedSpeed`; update `lastSample`.
3. Render job rows (unchanged).
4. `loadStorage(speed)` — takes an optional speed and appends the `↓ …/s`
   segment when present.

If the jobs fetch throws, `speed` stays `null` and `loadStorage(null)` still
renders the disk usage without a speed segment.

## Edge cases

- **First load / tab reload:** no `prevSample` → nothing shown that round;
  converges within ~2 cycles.
- **Backgrounded tab:** timer throttled → large `elapsed` → low average that
  self-corrects. Acceptable.
- **All jobs finish:** running set empties → no segment shown; `smoothedSpeed`
  reset to `null` so a later download starts fresh.
- **Jobs fetch fails:** speed `null`, storage still renders.

## Testing

The dashboard is a single static HTML file; the repo has **no JS test harness**
(tests are pytest with a mocked Hub). Per decision, verification is:

1. Reasoning through the edge cases above.
2. Manually running the app and watching the indicator during a real download
   (start a backup, confirm a plausible non-flickering rate appears, confirm it
   disappears when downloads finish).

No Node test toolchain is introduced for this one function.
