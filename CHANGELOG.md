# Changelog

## 0.4.0 — AudioMuse-AI 3.2–3.4 support

AudioMuse-AI 3.2 replaced Redis/RQ with a task queue living in Postgres, and
3.3.1 moved the core to numpy 2 and librosa 1.0. Both landed inside APIs this
plugin builds on. Everything below still works on 2.6.2 and every 3.x.

### The queue rewrite (core 3.2)

- **Scan task rows follow the core's id again.** `jobs._rq_task_id()` read the
  running RQ job's id; `rq` is not installed from 3.2, so it returned `None` and
  every scan invented a uuid instead. That left a second row in Active Tasks
  beside the queue's own, made the Cancel button tombstone a row nothing was
  polling (so cancelling a scan no longer stopped it launching more albums), and
  produced a `func IS NULL` row exposed to the core's stale-row sweep. Replaced
  by `jobs._current_task_id()`, which asks `taskqueue.current_task_id()` first
  and falls back to the RQ job id on older cores.
- **Lost album jobs are detected again.** `enqueue()` returns a task-id string
  on 3.2+ where it used to return an RQ `Job`, so the `job.get_status()` probe
  raised `AttributeError` into a bare `except` and silently never fired: a scan
  whose worker died drained forever waiting on a child that would never report.
  `_child_job_lost` now polls the child's queue row on 3.2+ and keeps the RQ
  path for older cores.
- **A long backlog no longer ends a scan early.** The core fails any live task
  row it does not own once it has gone 30 minutes without an update. Album child
  rows are written once at launch, so a child waiting behind a big library was
  failed out from under the parent, counted done, and the scan reported finished
  while the album was still queued. `_touch_live_children` refreshes the rows
  that actually approach the deadline.
- **Bulk actions work again.** The 3.2 queue guard counts live plugin tasks, not
  just core ones, so the first track a route queued blocked the second: every bulk
  button — Deep scan all non-CLEAN, Force re-queue, bulk selected, album
  re-analyze without an album id — would have queued exactly one track. They now
  start a single orchestrator task (`bulk_analyze_job`) that claims the tracks and
  feeds them onto the default queue from inside a worker, capped at
  `MAX_QUEUED_ANALYSIS_JOBS` in flight, reporting tracks done/remaining under
  Active Tasks — the same shape the library scan uses for albums. Cancelling it
  untags whatever it claimed but never launched. The overview's green
  "Queued N deep scans" banner becomes "started — follow progress under Active
  Tasks", because N is now decided when the job starts, not when you click.
  The orphan-recovery cron feeds its backlog through the same throttle.
- **Buttons explain a busy queue instead of returning 500.** From 3.2 the core
  refuses a plugin enqueue issued from the web process while another queue job
  is running. Scan, Re-analyze, Deep analyze, the bulk actions and Re-analyze
  album now report that and stop, and the deep-scan buttons undo the
  "deep scan queued" tag they had already written, so a refused click can no
  longer leave a track stuck until the orphan-recovery TTL. Force-requeue only
  bumps `deep_pending_since` for rows that actually reached the queue, so a row
  it could not queue stays visible to the recovery cron.

### The dependency bump (core 3.3.1)

- **Deep scans of m4a/mp4/ALAC/opus/wma work again.** librosa 1.0 removed the
  audioread backend, so `librosa.load` reads only what libsndfile can open.
  Normal scans already had an ffmpeg tier for those containers; deep scans had
  none and died on their first chunk — a deep scan of an ALAC `.m4a` came back
  INCONCLUSIVE. Both the deep scan and the single-window fallback now fall back
  to ffmpeg. The fallback is deliberately limited to containers libsndfile
  cannot open at all: for a corrupt FLAC, the decode failure is the finding, and
  ffmpeg decoding its intact prefix would launder that into a verdict.
- **matplotlib floor raised to `>=3.10`** for the core's numpy 2.4.6. matplotlib
  3.8's C extensions are built against the numpy 1.x ABI and fail to import
  under numpy 2, and pip does not upgrade an already-satisfied requirement.

### Also

- Album and library overviews gained a **max confidence %** filter alongside the
  existing minimum, and every verdict chip now shows how many tracks match it.

## 0.3.1

- Self-healing provider-id backfill: library and album scans re-run the
  `provider_track_id`/`server_id` backfill from `track_server_map` at start
  (one cheap idempotent UPDATE). The 0.3.0 install-time backfill was a
  one-shot that could race the core's v3 migration — on a stuck 3.0 boot the
  map table exists but is still empty when the plugin updates (the core
  commits the id relabel and the map in one transaction, on the next boot), so
  the columns stayed NULL. The backfill also repairs rows pointing at a
  media-server entry the core has since re-keyed (delete/re-add or provider
  migration); a scan during such a window reported every track as
  `not_in_score` because the id translation ran against a server id whose
  mappings did not exist yet. No re-analysis needed — once the map is
  consistent, the next scan heals the rows and settles everything as skipped.

## 0.3.0 — AudioMuse-AI 3.0 support

- v3.0's multi-server architecture keys `score` by canonical fingerprint ids
  (`fp_...`) instead of native media-server ids. All id handling now translates
  at the edges (`track_server_map` / `tasks.mediaserver.registry`), fixing scans
  reporting every track as `not_in_score` on 3.x. Results rows are keyed by the
  canonical id; new `provider_track_id`/`server_id` columns keep the native id
  for downloads and re-analysis. The `on_song_analyzed` hook now uses
  `media_item._catalog_item_id` (its `item_id` field is the native id on v3).
- **Lossless data migration**: the results FK is recreated as
  `ON UPDATE CASCADE ON DELETE CASCADE`, so the core's in-place id relabel
  migrates our rows automatically — verdicts, spectrograms, verified flags and
  the MD5 change-detection cache all survive, no re-analysis needed. This also
  fixes core 3.0 boot migrations that were failing/retrying because of the old
  plugin FK (update the plugin + Apply restart; the next boot completes the
  core migration). A defensive re-key via `track_server_map` covers tables
  whose FK was removed. Verified against live Postgres 16 in four upgrade
  scenarios (plugin-first upgrade, stuck 3.0 migration, orphaned rows,
  fresh 3.x install).
- Multi-server scanning: a manual library scan plans across **every**
  configured media server, launching each album child bound to the server it
  was listed on (the core's server binding is a contextvar and does not cross
  RQ job boundaries, so every job binds its own). Tracks shared between
  servers collapse onto one canonical id — no duplicate rows. Per-track
  re-analysis and deep scans bind the server that supplied the row (stored
  `server_id`), so downloads hit the right catalogue. Cron scans follow the
  schedule's server scope (the core binds each run).
- Task rows are keyed by the RQ job id: the 3.x janitor fails any top-level
  task row whose id has no RQ job behind it, so a route-invented uuid row got
  reaped as "orphaned" mid-scan (Error 9999 / "task disappeared from the
  queue"). Routes no longer pre-create rows; each job creates its own, keyed
  by its job id. The orchestrator also reports progress while settling
  unchanged albums, so long planning phases no longer look stalled.
- Hook-analyzed tracks skip the next scan cheaply: `on_song_analyzed` now
  stores the same metadata fingerprint a scan computes (from the raw media
  item), so a follow-up `changed` scan skips those tracks outright instead of
  re-downloading each one to discover its MD5 is unchanged.
- Single codebase supports both 2.6.2 and 3.0.1+ (graceful fallbacks when the
  3.x translation APIs are absent).

## 0.2.2

- Overview shows LOWPASSED counts: a header total and a per-album "Lowpassed"
  column (amber), both excluding manually verified tracks — the ambiguous
  possibly-dark-master bucket worth deep scanning.
- Secondary buttons (Filter, Re-analyze, Deep analyze, Deep scan all
  non-CLEAN) restyled amber/brown; the core's default gray-on-white was barely
  visible. A queued Deep analyze button now renders as clearly disabled.

## 0.2.1

- License: AGPL-3.0-only (LICENSE file + SPDX headers in the shipped modules).

## 0.2.0

- Search matches album, artist and song title (shows albums containing a
  match; per-album counts still cover the whole album).
- Deep-scan queueing is idempotent: repeat clicks while the "deep scan
  queued" tag is set enqueue nothing; the per-song button is disabled while
  pending. A plain Re-analyze clears an orphaned tag (escape hatch for lost
  jobs).
- Overview: "Deep scan all non-CLEAN" queues a whole-file deep scan for every
  unverified non-CLEAN track in the library (default queue) and reports how
  many were queued.

## 0.1.0 — first release

Everything below was developed pre-release; the version counter was reset to
0.1.0 when the repo was restructured (dist/ release flow, committed test
fixtures, unit tests). The original increments are preserved here.

### Pre-release history

- **0.6.2** — Detection recalibrated against real ground-truth fixtures built
  from a confirmed-genuine DSD file: silent-shelf threshold corrected for
  16-bit dither (−68 dB, catches 320 kbps→FLAC fakes that previously passed),
  aliasing images now mirrored around candidate source Nyquists (fixes
  source-rate attribution), hi-res UPSAMPLED requires corroboration (silent
  shelf / aliasing / sharp edge) so gradual dark masters stay CLEAN, deep-scan
  edge-variance thresholds retuned (≤300 wall / ≥2000 dark master) plus a
  pinned-edge check. Test harness in `tests/`.
- **0.6.1** — Album page: "Deep scan all non-CLEAN" button queues a whole-file
  deep scan for every track whose verdict is not CLEAN (verified and
  already-queued tracks are skipped; jobs run on the default queue so all
  workers share them).
- **0.6.0** — Overview filter rework: filter albums by the statuses their
  songs contain via per-verdict chips (any combination), optionally matching
  unverified tracks only. Combines with the min-suspect-count and has-verified
  filters; pagination preserves all filters.
- **0.5.1** — Deep-scan tracking: queuing a deep scan tags the track
  (`deep_pending`), shown as a "deep scan queued" badge on the song and
  cleared automatically when the scan finishes (even on failure). The album
  overview shows per-album tags: "deep scan ×N" (amber) and "verified ×N"
  (green).
- **0.5.0** — Deep analyze button per track: scans the entire file (chunked)
  and tracks the spectral edge over time — an edge that follows the music
  downgrades UPSAMPLED/FAKE_SUSPECT to a likely genuine dark master, a
  constant wall raises confidence. Manual "verified" checkbox per track:
  verified tracks are excluded from all suspect counts, shown as an overview
  column, and filterable.
- **0.4.1** — Fix: album names containing &, ?, # or other URL-special
  characters broke the album page link. All album links and pagination URLs
  are now properly URL-encoded.
- **0.4.0** — Fake 24-bit detection: PCM samples probed for zero-padded low
  bits (new UPSCALED verdict, effective/container bits stored and shown per
  track). UPSAMPLED confidence corroborated by aliasing-image analysis.
- **0.3.1** — UPSAMPLED detection matches the cutoff against every standard
  rate's Nyquist (22.05/24/44.1/48 kHz), so 48 kHz→96 kHz upsamples are
  caught, not just 44.1 kHz sources. Confidence graded by edge sharpness and
  shelf silence.
- **0.3.0** — New UPSAMPLED verdict for fake hi-res: a >48 kHz container whose
  content stops at CD bandwidth flagged as a resampled source instead of
  CLEAN. UPSAMPLED counts as suspect in the overview filters.
- **0.2.0** — Parallel scans: one task per album fanned out across all
  workers, with a parent task tracking done/remaining. Fewer false
  FAKE_SUSPECT verdicts: content reaching ~20.5 kHz counts as full bandwidth,
  and a sharp edge with audible noise above the cutoff is LOWPASSED, not
  FAKE. UI: filter albums by suspect count, re-analyze a whole album.
- **0.1.0 (original)** — Initial release: library scan grouped by album,
  change detection (metadata fingerprint + audio MD5), fake-lossless
  verdicts, base64 spectrograms, per-track re-analysis, on_song_analyzed
  hook, optional weekly incremental cron.
