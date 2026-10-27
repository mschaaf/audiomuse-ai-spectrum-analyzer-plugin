# TODO

Open items for 0.4.0 and after. Verified items are in the CHANGELOG
("Verification"); this lists only what is still untested or unfixed.

## Still to test before releasing 0.4.0

Against a live AudioMuse-AI 3.6.4:

- [ ] **Multi-server core:** register a second server, then run the scan and
      the weekly cron across both. Check `unmapped` counts for a secondary
      server and that the hook keys rows by the right server.
- [ ] **Single-album re-analyze** (`/album/rescan`, the root `scan_album_job`
      path) and the per-track rescan and deep-rescan buttons.
- [ ] **Hook without a server id:** the registry-failure case where
      `on_song_analyzed` gets no `server_id` and must skip rather than guess.
- [ ] **Upgrade from an old table:** install 0.3.3 on a throwaway core, analyze
      some tracks, then upgrade to this build. Check the migration, the
      backfill and that existing rows keep working.
- [ ] **Seeded `rq` claims through the UI:** the requeue-unknown route and its
      duplicate-risk confirmation on quarantined rows.
- [ ] **Bigger library:** a scan with more albums than `MAX_QUEUED_ANALYSIS_JOBS`
      to watch the throttle on the real queue.

## Release steps

- [ ] Open the PR to `main`; CI needs the 0.4.0 entry in
      `plugins/SpectrumAnalyzer/plugin.json` (present).
- [ ] `./build.sh 0.4.0` (explicit version) so `sourceUrl` and `checksum` are
      filled in; never rebuild a published version.

## Known defects / follow-ups

- [ ] **Multi-server cron:** the scan writes its own SUCCESS row, so core's
      cancel check between servers raises `TaskCancelled` and later servers can
      be skipped; child-album accounting also needs isolating per invocation.
      Own PR, with tests. Documented in the README.
- [ ] **CI Postgres job:** run `tests/test_postgres_integration.py` in
      `.github/workflows/build.yml` with a `services: postgres` container.
- [ ] `FakeCursor` in `tests/test_bulk_jobs.py` silently accepts unknown SQL;
      make it reject statements it does not recognise.
- [ ] Scope the migration's id re-key by server.
- [ ] Render the `unmapped` count on the overview page (task details only now).
