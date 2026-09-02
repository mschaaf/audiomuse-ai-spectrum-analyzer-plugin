# SpectrumAnalyzer - an AudioMuse-AI plugin
# https://github.com/nguyentn1197/audiomuse-ai-spectrum-analyzer-plugin
# Copyright (C) 2026 Nguyen
# SPDX-License-Identifier: AGPL-3.0-only
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License v3.0. See the LICENSE file
# in the project root.

"""Worker-side jobs for the SpectrumAnalyzer plugin.

Scan architecture (mirrors the core's run_analysis_task / analyze_album_task)
-----------------------------------------------------------------------------
scan_library_job  parent orchestrator, runs on the *high* queue: plans the
                  scan, enqueues one scan_album_job per album on the *default*
                  queue (so multiple workers process albums in parallel),
                  throttles in-flight children, and tracks done/remaining by
                  reading the children's task_status rows.
scan_album_job    child task: analyzes one album's tracks; also used
                  standalone by the "Re-analyze album" button (mode=force).

Scan modes
----------
changed : skip tracks whose metadata fingerprint is unchanged (default; no
          download for unchanged tracks).
verify  : download everything, but re-analyze only when the audio MD5 changed
          (catches in-place file edits invisible to metadata).
force   : re-download and re-analyze everything.
"""

import contextlib
import hashlib
import json
import os
import shutil
import tempfile
import time
import uuid
from datetime import timedelta

from plugin.api import (
    config, enqueue, get_db, get_setting, table, logger, save_task_status,
    TASK_STATUS_PENDING, TASK_STATUS_STARTED, TASK_STATUS_PROGRESS,
    TASK_STATUS_SUCCESS, TASK_STATUS_FAILURE, TASK_STATUS_REVOKED,
)
from tasks.mediaserver import get_recent_albums, get_tracks_from_album, download_track

from . import dsp

TASK_TYPE = 'plugin.spectrum_analyzer.scan'
ALBUM_TASK_TYPE = 'plugin.spectrum_analyzer.scan_album'
BULK_TASK_TYPE = 'plugin.spectrum_analyzer.bulk_analyze'

COUNTER_KEYS = ('analyzed', 'skipped', 'unchanged', 'not_in_score', 'error')
TERMINAL_STATES = (TASK_STATUS_SUCCESS, TASK_STATUS_FAILURE, TASK_STATUS_REVOKED)
POLL_SECONDS = 5

# Raw media-server fields that identify the underlying file cheaply.
# Navidrome's getAlbum tracks keep the raw Subsonic fields (size, suffix,
# bitRate, created, ...); other providers contribute what they have.
_FP_KEYS = ('path', 'Path', 'size', 'Size', 'suffix', 'Container',
            'bitRate', 'bitrate', 'created', 'changed', 'duration')


# ------------------------------------------------- canonical-id translation --
# AudioMuse-AI 3.0 keys `score` by canonical fingerprint ids (fp_...) and maps
# them to native media-server ids in `track_server_map`. Media-server track
# dicts still carry native ids in 'Id'/'id', so everything we store is keyed by
# the canonical id and translated at the edges. On a pre-3.0 core the registry
# module does not exist and both directions collapse to identity/no-op.

def _active_server_id():
    try:
        from plugin.api import active_server_id
    except ImportError:
        return None  # pre-3.0 core: single server
    try:
        return active_server_id()
    except Exception:
        return None


def _list_server_ids():
    """Every configured server's id, default first; [None] on 2.x or failure
    (None = the lone/default server everywhere in this module)."""
    try:
        from plugin.api import list_servers
    except ImportError:
        return [None]
    try:
        ids = [s.get('server_id') for s in list_servers()]
        return [i for i in ids if i] or [None]
    except Exception:
        logger.exception('spectrum_analyzer: listing servers failed')
        return [None]


def _current_task_id():
    """The queue's own id for the running job, to key our task_status row by.

    A top-level job's row MUST carry the id the core queued the job under, and
    the two queue generations disagree about what that id is:

    3.2+ (Postgres queue) — `taskqueue.current_task_id()`. The row already
      exists when the task starts (`enqueue()` writes it), so keying by this
      updates that row in place instead of adding a second one beside it. It is
      also the row the Cancel button replaces with a REVOKED tombstone, which is
      what `revoked()` polls for, and a row the queue owns (`func` is set) is
      never touched by the stale-in-process sweep.
    2.x/3.0-3.1 (RQ) — the RQ job id, for the mirror-image reason: that
      core's janitor failed any top-level non-terminal row whose id `Job.fetch`
      could not find, so a self-invented uuid got reaped mid-scan.

    Either way routes must not pre-create the row: run_plugin_task looks it up
    by the job id before the task runs and, when it exists, overwrites it with a
    bare SUCCESS (details wiped) afterwards. The job creates its own row.
    """
    try:
        import taskqueue
        task_id = taskqueue.current_task_id()   # 3.2+
        if task_id:
            return str(task_id)
    except Exception:
        pass
    try:
        from rq import get_current_job          # <= 3.1 (rq is gone in 3.2)
        job = get_current_job()
        return job.id if job is not None else None
    except Exception:
        return None


def _queue_backend():
    """Which queue generation is running: the 3.2+ Postgres queue or legacy RQ.

    The single generation detector: claim creation, task attachment and the
    reconciler all classify through this, so they cannot disagree about which
    status adapter to use. The `taskqueue` module exists only on 3.2+; before
    that the queue is RQ (and `rq` is not installed from 3.2, so the two
    imports are mutually exclusive in practice).
    """
    try:
        import taskqueue  # noqa: F401
        return 'postgres'
    except Exception:
        return 'rq'


def _bind_server(server_id):
    """Context manager binding every media-server call in scope to server_id.

    The core's binding is a contextvar, which does NOT cross RQ job boundaries:
    a child job never inherits the parent's binding, so every job that lists,
    matches or downloads tracks must bind the server it was planned against
    itself. No-op (default server) when server_id is None or on a 2.x core.
    """
    if not server_id:
        return contextlib.nullcontext()
    try:
        from plugin.api import use_server
    except ImportError:
        return contextlib.nullcontext()
    return use_server(server_id)


def _native_to_fp(native_ids, server_id=None):
    """{native_id: canonical id}. Identity mapping on a pre-3.0 core."""
    ids = [str(i) for i in native_ids if i]
    if not ids:
        return {}
    try:
        from tasks.mediaserver.registry import reverse_translate_ids
    except ImportError:
        return {i: i for i in ids}
    try:
        mapped = reverse_translate_ids(ids, server_id=server_id)
    except Exception:
        logger.exception('spectrum_analyzer: native->canonical translation failed')
        return {i: i for i in ids}
    return {i: str(mapped.get(i, i)) for i in ids}


def _fp_to_native(fp_ids, server_id=None):
    """{canonical id: native provider id} for this server; {} on a pre-3.0 core."""
    ids = [str(i) for i in fp_ids if i]
    if not ids:
        return {}
    try:
        from tasks.mediaserver.registry import translate_ids
    except ImportError:
        return {}
    try:
        return {str(k): str(v) for k, v in translate_ids(ids, server_id=server_id).items()}
    except Exception:
        logger.exception('spectrum_analyzer: canonical->native translation failed')
        return {}


def meta_fingerprint(track):
    parts = [f'{k}={track.get(k)}' for k in _FP_KEYS if track.get(k) is not None]
    if not parts:
        return None
    return hashlib.md5('|'.join(parts).encode('utf-8', 'replace')).hexdigest()


def file_md5(path):
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def _settings():
    return {
        'segment_seconds': int(get_setting('segment_seconds', 90)),
        'drop_db': int(get_setting('drop_db', 40)),
        'img_w': int(get_setting('img_w', 800)),
        'img_h': int(get_setting('img_h', 280)),
        'skip_clean_spectrograms': bool(get_setting('skip_clean_spectrograms', False)),
        # the old deep_orphan_ttl_hours value is read as the fallback so an
        # existing setting migrates on first use; it is a warning threshold
        # now, not an automatic re-queue deadline
        'deep_pending_warning_hours': int(
            get_setting('deep_pending_warning_hours',
                        get_setting('deep_orphan_ttl_hours', 2))),
    }


def _track_suffix(track):
    s = track.get('suffix') or track.get('Container')
    if s:
        return str(s).lower().lstrip('.')
    p = track.get('Path') or track.get('path') or ''
    return os.path.splitext(p)[1].lstrip('.').lower()


def _track_bitrate(track):
    for k in ('bitRate', 'bitrate', 'Bitrate'):
        v = track.get(k)
        if v:
            try:
                return int(v)
            except (TypeError, ValueError):
                pass
    return None


def _upsert(item_id, info, result, meta_fp, audio_md5):
    tbl = table('results')
    db = get_db()
    cur = db.cursor()
    cur.execute(
        'INSERT INTO ' + tbl + ' (item_id, provider_track_id, server_id, title, '
        'artist, album, album_id, file_path, '
        'suffix, bitrate, meta_fp, audio_md5, sample_rate, seg_offset, seg_seconds, '
        'cutoff_hz, edge_db_khz, shelf_db, verdict, est_source, confidence, details, '
        'container_bits, effective_bits, spectrogram_b64, analysis_rev, deep_eligible, '
        'analyzed_at) '
        'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now()) '
        'ON CONFLICT (item_id) DO UPDATE SET '
        'provider_track_id=EXCLUDED.provider_track_id, server_id=EXCLUDED.server_id, '
        'title=EXCLUDED.title, artist=EXCLUDED.artist, album=EXCLUDED.album, '
        'album_id=EXCLUDED.album_id, file_path=EXCLUDED.file_path, suffix=EXCLUDED.suffix, '
        'bitrate=EXCLUDED.bitrate, meta_fp=EXCLUDED.meta_fp, audio_md5=EXCLUDED.audio_md5, '
        'sample_rate=EXCLUDED.sample_rate, seg_offset=EXCLUDED.seg_offset, '
        'seg_seconds=EXCLUDED.seg_seconds, cutoff_hz=EXCLUDED.cutoff_hz, '
        'edge_db_khz=EXCLUDED.edge_db_khz, shelf_db=EXCLUDED.shelf_db, '
        'verdict=EXCLUDED.verdict, est_source=EXCLUDED.est_source, '
        'confidence=EXCLUDED.confidence, details=EXCLUDED.details, '
        'container_bits=EXCLUDED.container_bits, effective_bits=EXCLUDED.effective_bits, '
        'spectrogram_b64=EXCLUDED.spectrogram_b64, '
        'analysis_rev=EXCLUDED.analysis_rev, deep_eligible=EXCLUDED.deep_eligible, '
        'analyzed_at=now()',
        (
            item_id, info.get('provider_track_id'), info.get('server_id'),
            info.get('title'), info.get('artist'), info.get('album'),
            info.get('album_id'), info.get('file_path'), info.get('suffix'),
            info.get('bitrate'), meta_fp, audio_md5,
            result['sample_rate'], result['seg_offset'], result['seg_seconds'],
            result['cutoff_hz'], result['edge_db_khz'], result['shelf_db'],
            result['verdict'], result['est_source'], result['confidence'],
            result['details'], result.get('container_bits'),
            result.get('effective_bits'), result['spectrogram_b64'],
            result['analysis_rev'], result.get('deep_eligible', True),
        ),
    )
    db.commit()
    cur.close()


def _touch_fingerprint(item_id, meta_fp):
    db = get_db()
    cur = db.cursor()
    cur.execute('UPDATE ' + table('results') + ' SET meta_fp=%s WHERE item_id=%s',
                (meta_fp, item_id))
    db.commit()
    cur.close()


def _existing_rows(item_ids=None):
    """item_id -> (meta_fp, audio_md5, analysis_rev), optionally restricted
    to item_ids."""
    db = get_db()
    cur = db.cursor()
    if item_ids is None:
        cur.execute('SELECT item_id, meta_fp, audio_md5, analysis_rev FROM '
                    + table('results'))
    else:
        cur.execute('SELECT item_id, meta_fp, audio_md5, analysis_rev FROM '
                    + table('results') + ' WHERE item_id = ANY(%s)', (list(item_ids),))
    rows = {r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()}
    cur.close()
    return rows


def _backfill_provider_ids():
    """Re-run migrate()'s provider_track_id/server_id backfill from
    track_server_map.

    migrate() is a one-shot that can race the core's v3 migration: on a stuck
    3.0 boot the map table exists but is still empty when the plugin updates
    (the canonicalization commits relabel + map in one transaction, on the
    NEXT boot), so the install-time backfill finds nothing. Scans re-apply it
    — one cheap idempotent UPDATE — healing rows whenever the map has
    entries, including ones added later by a server alignment sweep."""
    db = get_db()
    cur = db.cursor()
    try:
        cur.execute("SELECT to_regclass('track_server_map')")
        if cur.fetchone()[0] is None:
            return
        # also repairs rows pointing at a server that no longer exists (the
        # core can re-key a server entry; its map is rebuilt by an alignment
        # sweep, ours follows here)
        cur.execute(
            'UPDATE ' + table('results') + ' r'
            ' SET provider_track_id = m.provider_track_id, server_id = m.server_id'
            ' FROM track_server_map m'
            ' WHERE m.item_id = r.item_id'
            '   AND (r.provider_track_id IS NULL OR r.server_id IS NULL'
            '        OR NOT EXISTS (SELECT 1 FROM music_servers s'
            '                       WHERE s.server_id = r.server_id))')
        healed = cur.rowcount
        db.commit()
        if healed:
            logger.info('spectrum_analyzer: backfilled provider ids for %d rows',
                        healed)
    except Exception:
        db.rollback()
        logger.exception('spectrum_analyzer: provider-id backfill failed')
    finally:
        cur.close()


def _score_ids(item_ids=None):
    db = get_db()
    cur = db.cursor()
    if item_ids is None:
        cur.execute('SELECT item_id FROM score')
    else:
        cur.execute('SELECT item_id FROM score WHERE item_id = ANY(%s)',
                    (list(item_ids),))
    ids = {r[0] for r in cur.fetchall()}
    cur.close()
    return ids


# --------------------------------------------------- task-status plumbing --

def _task_state(task_id):
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT status FROM task_status WHERE task_id = %s', (task_id,))
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def _child_rows(parent_task_id):
    """[(task_id, status, details_dict)] for every child of the parent scan."""
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT task_id, status, details FROM task_status'
                ' WHERE parent_task_id = %s', (parent_task_id,))
    rows = cur.fetchall()
    cur.close()
    out = []
    for child_id, status, details in rows:
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except ValueError:
                details = {}
        out.append((child_id, status, details if isinstance(details, dict) else {}))
    return out


def _aggregate(child_rows, base=None):
    total = dict(base) if base else {k: 0 for k in COUNTER_KEYS}
    for _child_id, _status, details in child_rows:
        for k in COUNTER_KEYS:
            v = details.get(k)
            if isinstance(v, (int, float)):
                total[k] = total.get(k, 0) + int(v)
    return total


def _child_job_lost(handle):
    """True when the queued job behind a child died without settling itself.

    Only ever asked about a child whose OWN row is still non-terminal, so "the
    job has ended" and "the job was lost" are the same statement here: a child
    that finished normally wrote its terminal row before its job ended.

    `enqueue()` hands back different things either side of the 3.2 queue
    rewrite — the queue's task id (a str subclass) after it, an RQ Job before
    it — so the check has to know both shapes.
    """
    if handle is None:
        return False
    if isinstance(handle, str):
        return _queue_job_over(str(handle))     # 3.2+
    return _rq_job_lost(handle)                 # <= 3.1


def _queue_job_over(queue_task_id):
    """True when a 3.2+ queue row has reached an end: terminal, or gone.

    Retention only ever deletes terminal rows, so a row that has disappeared
    ran to an end too. A queue row goes terminal strictly AFTER the child wrote
    its own terminal row (the child returns, then run_plugin_task and the worker
    finalize), so this can never overtake a child that did settle.
    """
    if not queue_task_id:
        return False
    try:
        state = _task_state(queue_task_id)
    except Exception:
        return False  # transient DB error: decide on a later poll
    return state is None or state in TERMINAL_STATES


def _rq_job_status_value(job):
    """The lowercase status string of a live RQ job.

    Reads the enum's .value rather than str(): rq 1.x JobStatus is a str-mixin
    Enum whose str() form changed in Python 3.11, and rq 2.x is a plain Enum,
    so only .value is stable across both. Raises whatever
    job.get_status(refresh=True) raises (e.g. NoSuchJobError) so the caller
    decides how to interpret a vanished or unreadable job.
    """
    status = job.get_status(refresh=True)
    if status is None:
        return ''
    return getattr(status, 'value', str(status))


def _rq_job_lost(job):
    """True when the RQ job backing a child died without writing a terminal
    task_status row (worker crash, job dropped from Redis). Pre-3.2 cores."""
    try:
        from rq.exceptions import NoSuchJobError
    except ImportError:
        NoSuchJobError = ()
    try:
        state = _rq_job_status_value(job)
    except NoSuchJobError:
        # a finished child writes its terminal row before the job expires,
        # so a vanished job behind a non-terminal row means it was lost
        return True
    except Exception:
        return False  # transient Redis error: decide on a later poll
    return state in ('failed', 'stopped', 'canceled')


def _touch_live_children(parent_task_id):
    """Refresh child rows going stale, so the core does not fail them for us.

    A 3.2+ core fails any LIVE task_status row the queue does not own (`func IS
    NULL`, which is every row written through save_task_status) once it has gone
    QUEUE_INLINE_STALE_SECONDS — 30 minutes by default — without an
    update. A running child rewrites its own row after every track, but a child
    still QUEUED behind a full library of albums was written once at launch and
    not since: past that deadline the sweep would fail it out from under us, the
    parent would count it done, and the scan would report finished while the
    album was still waiting for a worker. Touching only the rows actually
    approaching the deadline keeps this a no-op UPDATE in the normal case.
    """
    stale = getattr(config, 'QUEUE_INLINE_STALE_SECONDS', None)
    if not stale:
        return  # pre-3.2 core: no sweep to dodge
    try:
        refresh_after = max(60.0, float(stale) / 2.0)
    except (TypeError, ValueError):
        return
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'UPDATE task_status SET timestamp = NOW()'
            ' WHERE parent_task_id = %s AND NOT (status = ANY(%s))'
            ' AND timestamp < NOW() - make_interval(secs => %s)',
            (parent_task_id, list(TERMINAL_STATES), refresh_after))
        db.commit()
        cur.close()
    except Exception:
        logger.exception('spectrum_analyzer: could not refresh child task rows')


# ------------------------------------------------------------- album task --

def _analyze_download(track, info, meta_fp, settings, existing, mode, deep=False):
    """Download one track, decide skip/recompute, store. Returns a status tag."""
    item_id = info['item_id']
    tmp = tempfile.mkdtemp(prefix='spectrum_')
    try:
        path = download_track(tmp, track)
        if not path or not os.path.exists(path):
            return 'error'
        md5 = file_md5(path)
        prev = existing.get(item_id)
        rev = dsp.analysis_rev(settings['drop_db'], settings['segment_seconds'])
        if mode != 'force' and prev and prev[1] == md5 and prev[2] == rev:
            # bytes unchanged and analyzed by the current analyzer;
            # just refresh the cheap fingerprint
            _touch_fingerprint(item_id, meta_fp)
            return 'unchanged'
        result = dsp.analyze_file(
            path, suffix=info.get('suffix'), bitrate_kbps=info.get('bitrate'),
            segment_seconds=settings['segment_seconds'], drop_db=settings['drop_db'],
            img_w=settings['img_w'], img_h=settings['img_h'], deep=deep,
            skip_spectrogram_for_clean=settings['skip_clean_spectrograms'],
        )
        _upsert(item_id, info, result, meta_fp, md5)
        return 'analyzed'
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _native_id(track):
    v = track.get('Id') or track.get('id')
    return str(v) if v else None


def _info_from_track(track, album, fp_map=None, server_id=None):
    """fp_map: {native_id: canonical_id} from a per-album _native_to_fp batch;
    item_id is the canonical id (identical to the native id on a v2 core)."""
    native = _native_id(track)
    canonical = (fp_map or {}).get(native, native)
    return {
        'item_id': canonical,
        'provider_track_id': native,
        'server_id': server_id,
        'title': track.get('Name') or track.get('title'),
        'artist': track.get('AlbumArtist') or track.get('artist'),
        'album': track.get('Album') or (album or {}).get('Name'),
        'album_id': (album or {}).get('Id') or track.get('albumId'),
        'file_path': track.get('FilePath') or track.get('Path') or track.get('path'),
        'suffix': _track_suffix(track),
        'bitrate': _track_bitrate(track),
    }


def _album_server(album_id):
    """Server that previously supplied this album's rows (None if unknown)."""
    if not album_id:
        return None
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute('SELECT server_id FROM ' + table('results')
                    + ' WHERE album_id=%s AND server_id IS NOT NULL LIMIT 1',
                    (album_id,))
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None
    except Exception:
        return None


def scan_album_job(album_id, album_name, mode='changed', task_id=None,
                   parent_task_id=None, server_id=None):
    """Analyze one album. Child of scan_library_job (which passes the server it
    planned the album against), or standalone for the per-album Re-analyze
    button (mode=force, no parent; server recovered from the stored rows)."""
    task_id = task_id or _current_task_id() or str(uuid.uuid4())
    settings = _settings()
    rev = dsp.analysis_rev(settings['drop_db'], settings['segment_seconds'])
    counters = {k: 0 for k in COUNTER_KEYS}

    def status(state, progress, detail):
        try:
            save_task_status(task_id, ALBUM_TASK_TYPE, state,
                             parent_task_id=parent_task_id,
                             sub_type_identifier=album_name,
                             progress=progress,
                             details={'mode': mode, 'album': album_name,
                                      'info': detail, **counters})
        except Exception:
            logger.exception('spectrum_analyzer: save_task_status failed')

    if parent_task_id and _task_state(parent_task_id) == TASK_STATUS_REVOKED:
        status(TASK_STATUS_REVOKED, 100, 'parent scan was cancelled')
        return counters

    status(TASK_STATUS_STARTED, 0, 'listing tracks')
    try:
        if parent_task_id is None:  # standalone: parent already did this
            _backfill_provider_ids()
        server_id = server_id or _active_server_id() or _album_server(album_id)
        with _bind_server(server_id):
            tracks = get_tracks_from_album(album_id)
            # one batched native->canonical translation per album, like the
            # core's attach_catalog_item_ids; score and our rows are keyed by
            # canonical ids
            fp_map = _native_to_fp([_native_id(t) for t in tracks], server_id)
            ids = [i for i in fp_map.values() if i]
            score_ids = _score_ids(ids)
            existing = _existing_rows(ids)
            total = max(1, len(tracks))

            for i, track in enumerate(tracks):
                info = _info_from_track(track,
                                        {'Id': album_id, 'Name': album_name},
                                        fp_map, server_id)
                item_id = info['item_id']
                if not item_id:
                    continue
                if item_id not in score_ids:
                    # not analyzed by the core yet -> no score row to attach to
                    # (the on_song_analyzed hook will pick it up later)
                    counters['not_in_score'] += 1
                else:
                    meta_fp = meta_fingerprint(track)
                    prev = existing.get(item_id)
                    if (mode == 'changed' and prev and meta_fp
                            and prev[0] == meta_fp and prev[2] == rev):
                        counters['skipped'] += 1
                    else:
                        try:
                            tag = _analyze_download(track, info, meta_fp,
                                                    settings, existing, mode)
                            counters[tag] += 1
                        except Exception:
                            logger.exception('spectrum_analyzer: failed on %s',
                                             info.get('title'))
                            counters['error'] += 1
                status(TASK_STATUS_PROGRESS, int((i + 1) * 100 / total),
                       info.get('title') or '?')

        status(TASK_STATUS_SUCCESS, 100, 'done')
        return counters
    except Exception as exc:
        status(TASK_STATUS_FAILURE, 100, str(exc))
        raise


# ------------------------------------------------------ parent orchestrator --

def scan_library_job(mode='changed', task_id=None, all_servers=False):
    """Fan the library scan out into one scan_album_job per album.

    Runs on the high queue so it never competes with its own children for a
    default worker. In 'changed' mode albums whose every track is unchanged
    (or not yet in score) are settled here without launching a child.

    all_servers=True (the UI scan buttons) plans the scan across every
    configured media server, launching each album child with the server it was
    listed on; duplicates across servers collapse onto the same canonical id.
    The cron entry point keeps the default False: a schedule's server scope
    already runs the task once per server, bound by the core.
    """
    task_id = task_id or _current_task_id() or str(uuid.uuid4())
    max_in_flight = max(1, int(getattr(config, 'MAX_QUEUED_ANALYSIS_JOBS', 25)))

    def status(state, progress, detail, extra=None):
        try:
            save_task_status(task_id, TASK_TYPE, state, progress=progress,
                             details={'mode': mode, 'info': detail, **(extra or {})})
        except Exception:
            logger.exception('spectrum_analyzer: save_task_status failed')

    def revoked():
        return _task_state(task_id) == TASK_STATUS_REVOKED

    status(TASK_STATUS_STARTED, 0, 'listing albums')
    try:
        _backfill_provider_ids()
        bound = _active_server_id()  # set when a cron scope bound this run
        server_ids = [bound] if bound or not all_servers else _list_server_ids()
        plan = []  # (server_id, album) pairs, albums listed per server
        for sid in server_ids:
            with _bind_server(sid):
                for album in get_recent_albums(0):  # 0 = every album
                    plan.append((sid, album))
        albums_total = max(1, len(plan))
        albums_skipped = 0
        launched = 0
        launched_jobs = {}  # child task_id -> queue handle, for lost-job reconciliation
        last_touch = [0.0]  # monotonic clock of the last _touch_live_children
        parent_counters = {k: 0 for k in COUNTER_KEYS}
        score_ids = _score_ids() if mode == 'changed' else None
        existing = _existing_rows() if mode == 'changed' else None
        s = _settings()
        rev = dsp.analysis_rev(s['drop_db'], s['segment_seconds'])

        def snapshot(detail):
            # cheap and almost always a no-op, but it has to run on a clock of
            # its own: snapshot() is called per album while planning and every
            # POLL_SECONDS while waiting, and neither cadence is a deadline
            now = time.monotonic()
            if now - last_touch[0] >= 60.0:
                last_touch[0] = now
                _touch_live_children(task_id)
            rows = _child_rows(task_id)
            done = 0
            for child_id, state, _details in rows:
                if state in TERMINAL_STATES:
                    done += 1
                elif _child_job_lost(launched_jobs.get(child_id)):
                    # worker died / job dropped: settle the row so the scan
                    # can finish instead of draining forever
                    logger.warning('spectrum_analyzer: child %s lost, marking FAILURE',
                                   child_id)
                    save_task_status(child_id, ALBUM_TASK_TYPE, TASK_STATUS_FAILURE,
                                     parent_task_id=task_id, progress=100,
                                     details={'mode': mode, 'error': 1,
                                              'info': 'worker job failed or was lost'})
                    launched_jobs.pop(child_id, None)
                    done += 1
            agg = _aggregate(rows, base=parent_counters)
            albums_done = albums_skipped + done
            extra = {
                'albums_total': albums_total, 'albums_launched': launched,
                'albums_no_work': albums_skipped, 'albums_done': albums_done,
                'albums_remaining': albums_total - albums_done,
                'servers': len(server_ids), **agg,
            }
            return done, int(albums_done * 100 / albums_total), extra

        def launch(album, album_name, sid):
            child_id = str(uuid.uuid4())
            save_task_status(child_id, ALBUM_TASK_TYPE, TASK_STATUS_PENDING,
                             parent_task_id=task_id, sub_type_identifier=album_name,
                             details={'mode': mode, 'album': album_name,
                                      'info': 'queued'})
            launched_jobs[child_id] = enqueue(
                scan_album_job, album.get('Id'), album_name, mode=mode,
                task_id=child_id, parent_task_id=task_id, server_id=sid)

        for sid, album in plan:
            if revoked():
                logger.info('spectrum_analyzer scan %s cancelled during launch', task_id)
                return
            album_name = album.get('Name') or '?'

            if mode == 'changed':
                # settle all-unchanged albums here, without a child job
                try:
                    with _bind_server(sid):
                        tracks = get_tracks_from_album(album.get('Id'))
                except Exception:
                    logger.exception('spectrum_analyzer: album listing failed: %s',
                                     album_name)
                    parent_counters['error'] += 1
                    albums_skipped += 1
                    done, progress, extra = snapshot(album_name)
                    status(TASK_STATUS_PROGRESS, progress,
                           f'listing failed: {album_name}', extra)
                    continue
                fp_map = _native_to_fp([_native_id(t) for t in tracks], sid)
                pending = 0
                tallies = {'skipped': 0, 'not_in_score': 0}
                for track in tracks:
                    item_id = fp_map.get(_native_id(track))
                    if not item_id:
                        continue
                    if item_id not in score_ids:
                        tallies['not_in_score'] += 1
                    else:
                        prev = existing.get(item_id)
                        fp = meta_fingerprint(track)
                        if prev and fp and prev[0] == fp and prev[2] == rev:
                            tallies['skipped'] += 1
                        else:
                            pending += 1
                if pending == 0:
                    parent_counters['skipped'] += tallies['skipped']
                    parent_counters['not_in_score'] += tallies['not_in_score']
                    albums_skipped += 1
                    # keep progress moving on the settle path too: a library of
                    # mostly-unchanged albums would otherwise show no update
                    # (and look stalled) for the whole planning phase
                    done, progress, extra = snapshot(album_name)
                    status(TASK_STATUS_PROGRESS, progress,
                           f'unchanged: {album_name}', extra)
                    continue

            # throttle: keep at most max_in_flight children unfinished
            while True:
                done, progress, extra = snapshot(f'scanning: {album_name}')
                if launched - done < max_in_flight:
                    break
                if revoked():
                    logger.info('spectrum_analyzer scan %s cancelled while throttled',
                                task_id)
                    return
                status(TASK_STATUS_PROGRESS, progress,
                       f'waiting for workers ({launched - done} albums in flight)',
                       extra)
                time.sleep(POLL_SECONDS)

            launch(album, album_name, sid)
            launched += 1
            done, progress, extra = snapshot(album_name)
            status(TASK_STATUS_PROGRESS, progress, f'queued: {album_name}', extra)

        # all launched; wait for the children to drain
        while True:
            if revoked():
                logger.info('spectrum_analyzer scan %s cancelled while draining',
                            task_id)
                return
            done, progress, extra = snapshot('waiting for album tasks')
            if done >= launched:
                break
            status(TASK_STATUS_PROGRESS, progress,
                   f'{launched - done} albums remaining', extra)
            time.sleep(POLL_SECONDS)

        done, progress, extra = snapshot('done')
        status(TASK_STATUS_SUCCESS, 100, 'done', extra)
        logger.info('spectrum_analyzer scan finished: %s', extra)
        return extra
    except Exception as exc:
        status(TASK_STATUS_FAILURE, 0, str(exc))
        raise


def _queue_states(task_ids):
    """{task_id: status} for the queue rows still present, None if unreadable.

    A row that has vanished is simply absent from the mapping: the core's
    retention sweep only ever deletes TERMINAL rows, and every root enqueue
    runs it, so our own finished children are swept away as we launch the next
    ones. "Missing" and "finished" therefore mean the same thing to callers.
    """
    if not task_ids:
        return {}
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute('SELECT task_id, status FROM task_status WHERE task_id = ANY(%s)',
                    (list(task_ids),))
        rows = cur.fetchall()
        cur.close()
        return {r[0]: r[1] for r in rows}
    except Exception:
        logger.exception('spectrum_analyzer: could not read queue rows')
        return None


def _rq_job_finished(job):
    """Pre-3.2: has this RQ job stopped occupying a worker, however it ended?

    Only a genuinely missing job frees the throttle slot. A transient Redis
    error must keep the handle in flight: treating it as finished would let
    the throttle launch past MAX_QUEUED_ANALYSIS_JOBS, and the job may still
    run.
    """
    try:
        from rq.exceptions import NoSuchJobError
    except ImportError:
        NoSuchJobError = ()
    try:
        state = _rq_job_status_value(job)
    except NoSuchJobError:
        return True
    except Exception:
        return False  # transient Redis error: decide on a later poll
    return state in ('finished', 'failed', 'stopped', 'canceled')


def _prune_finished(handles):
    """Drop the finished jobs from {handle key: handle}; return how many remain.

    `enqueue()` returns the queue's task id (a str subclass) on 3.2+ and an RQ
    Job before that, so both shapes are pruned here -- the 3.2+ ones with a
    single batched query rather than one per in-flight job.
    """
    if not handles:
        return 0
    queue_ids = [key for key, h in handles.items() if isinstance(h, str)]
    states = _queue_states(queue_ids) if queue_ids else {}
    if states is None:
        return len(handles)  # transient DB error: re-decide on the next poll
    for key, handle in list(handles.items()):
        if isinstance(handle, str):
            state = states.get(key)
            over = state is None or state in TERMINAL_STATES
        else:
            over = _rq_job_finished(handle)
        if over:
            handles.pop(key, None)
    return len(handles)


# ------------------------------------------------- deep-scan claim model --
#
# deep_pending is the UI-facing idempotency marker; the claim columns are what
# actually own a deep scan. A claim (distinct id) is created BEFORE the child
# is enqueued, the queue task is attached after enqueue() returns, and every
# later mutation is fenced on the claim id, so a stale parent or child from an
# older generation can never clear or overwrite a newer claim. The
# claim-to-attach gap is not automatically recoverable: a parent that dies in
# it leaves a claimed row with no task id, and the reconciler surfaces that as
# outcome_unknown instead of guessing whether the enqueue happened.

CLAIM_LIVE_STATES = ('claimed', 'launched', 'running')


def _claim_detail(kind, message='', queue=None):
    d = {'kind': kind, 'message': message,
         'recorded_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
    if queue:
        d['queue'] = queue
    return json.dumps(d)


def _detail_queue(detail):
    try:
        d = json.loads(detail) if isinstance(detail, str) else (detail or {})
        q = d.get('queue')
        return q if isinstance(q, str) and q else None
    except (TypeError, ValueError):
        return None


def _new_deep_claims(item_ids=None, album=None, select=None, queue='default'):
    """Claim untagged eligible tracks for a deep run.

    One UPDATE ... RETURNING so two clicks cannot claim the same track twice;
    gen_random_uuid() gives each row its own claim id, so a batch claim never
    shares one. Returns [(item_id, claim_id)].
    """
    tbl = table('results')
    sql = ('UPDATE ' + tbl +
           ' SET deep_pending=TRUE, deep_pending_since=now(),'
           " deep_claim_id=gen_random_uuid()::text,"
           " deep_claim_state='claimed', deep_claimed_at=now(),"
           ' deep_queue_backend=%s, deep_task_id=NULL,'
           ' deep_completed_claim_id=NULL, deep_completed_at=NULL,'
           ' deep_claim_detail=%s'
           ' WHERE NOT deep_pending AND deep_eligible')
    params = [_queue_backend(), _claim_detail('queued', queue=queue)]
    ids = [str(i) for i in (item_ids or []) if i]
    if select == 'non_clean':
        sql += " AND verdict IS DISTINCT FROM 'CLEAN' AND NOT verified"
        if album is not None:
            sql += ' AND album = %s'
            params.append(album)
    else:
        sql += ' AND item_id = ANY(%s)'
        params.append(ids)
    db = get_db()
    cur = db.cursor()
    try:
        cur.execute(sql + ' RETURNING item_id, deep_claim_id', tuple(params))
        claims = [(r[0], r[1]) for r in cur.fetchall()]
        db.commit()
    finally:
        cur.close()
    return claims


def _requeue_claims(select):
    """Re-claim already-tagged rows for an explicit manual retry.

    select='requeue_all'     rows with no claim metadata at all (legacy) or
                             whose claim is already marked failed.
    select='requeue_unknown' rows whose claim has no attached queue task, or
                             whose outcome the reconciler could not establish.
                             The route requires a separate duplicate-risk
                             confirmation before this is offered.

    Live claims (claimed/launched/running with an attached task) match neither
    select, so a re-queue can never replace a scan that may still be running.
    The select takes row locks; each replacement is a compare-and-set on the
    prior claim id, so a concurrent action is never overwritten.

    Returns (claims, restore_map): claims is [(item_id, new claim_id)],
    restore_map is {item_id: prior record} so a failed launch can put the
    prior record back (itself CAS on the new claim id).
    """
    tbl = table('results')
    if select == 'requeue_all':
        eligible = ('(deep_claim_id IS NULL OR deep_claim_state = \'failed\')')
    elif select == 'requeue_unknown':
        eligible = ('(deep_task_id IS NULL OR deep_claim_state = '
                    '\'outcome_unknown\')')
    else:
        return [], {}
    db = get_db()
    cur = db.cursor()
    try:
        cur.execute(
            'SELECT item_id, deep_pending_since, deep_claim_id, deep_claim_state,'
            ' deep_queue_backend, deep_task_id, deep_claimed_at, deep_claim_detail,'
            ' deep_completed_claim_id, deep_completed_at'
            ' FROM ' + tbl +
            ' WHERE deep_pending AND deep_eligible AND ' + eligible +
            ' FOR UPDATE')
        rows = cur.fetchall()
        claims = []
        restore_map = {}
        for (item_id, p_since, p_claim, p_state, p_backend, p_task,
             p_claimed, p_detail, p_marker, p_completed) in rows:
            cur.execute(
                'UPDATE ' + tbl +
                ' SET deep_pending_since=now(),'
                " deep_claim_id=gen_random_uuid()::text,"
                " deep_claim_state='claimed', deep_claimed_at=now(),"
                ' deep_queue_backend=%s, deep_task_id=NULL,'
                ' deep_completed_claim_id=NULL, deep_completed_at=NULL,'
                ' deep_claim_detail=%s'
                ' WHERE item_id=%s AND deep_claim_id IS NOT DISTINCT FROM %s'
                ' RETURNING deep_claim_id',
                (_queue_backend(), _claim_detail('queued'), item_id, p_claim))
            row = cur.fetchone()
            if row:
                claims.append((item_id, row[0]))
                restore_map[item_id] = (p_since, p_claim, p_state, p_backend,
                                        p_task, p_claimed, p_detail, p_marker,
                                        p_completed)
        db.commit()
    finally:
        cur.close()
    return claims, restore_map


def _restore_deep_claim(item_id, new_claim_id, prior):
    """Put a re-queue claim back to its prior record after a failed launch.

    CAS on the NEW claim id: if a concurrent action already replaced the claim
    in the meantime, the restore affects zero rows and the newer ownership is
    never overwritten. Returns True when the restore landed.
    """
    (p_since, p_claim, p_state, p_backend, p_task, p_claimed, p_detail,
     p_marker, p_completed) = prior
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'UPDATE ' + table('results') +
            ' SET deep_pending_since=%s, deep_claim_id=%s, deep_claim_state=%s,'
            ' deep_queue_backend=%s, deep_task_id=%s, deep_claimed_at=%s,'
            ' deep_claim_detail=%s, deep_completed_claim_id=%s,'
            ' deep_completed_at=%s'
            ' WHERE item_id=%s AND deep_claim_id=%s',
            (p_since, p_claim, p_state, p_backend, p_task, p_claimed,
             p_detail, p_marker, p_completed, item_id, new_claim_id))
        ok = cur.rowcount > 0
        db.commit()
        cur.close()
        return ok
    except Exception:
        logger.exception('spectrum_analyzer: could not restore deep claim for %s',
                         item_id)
        return False


def _clear_deep_claims_many(claims):
    """Untag deep claims that were never launched (cancellation or a failed
    launch). Each clear is fenced on the claim id, so a row re-claimed by a
    concurrent action is left alone. Returns True when every clear landed."""
    ok = True
    for item_id, claim_id in (claims or []):
        if not claim_id:
            continue
        try:
            db = get_db()
            cur = db.cursor()
            cur.execute(
                'UPDATE ' + table('results') +
                ' SET deep_pending=FALSE, deep_pending_since=NULL,'
                ' deep_claim_id=NULL, deep_task_id=NULL, deep_claim_state=NULL,'
                ' deep_claimed_at=NULL, deep_queue_backend=NULL,'
                ' deep_claim_detail=NULL'
                ' WHERE item_id=%s AND deep_claim_id=%s',
                (item_id, claim_id))
            db.commit()
            cur.close()
        except Exception:
            logger.exception('spectrum_analyzer: could not untag unlaunched '
                             'deep scan %s', item_id)
            ok = False
    return ok


def _attach_deep_task(item_id, claim_id, handle):
    """Record the queue task id on a fresh claim, without regressing a fast
    child that already moved it to running. A zero-row result is correct: the
    child finished and cleared the claim before the attach landed."""
    if not (claim_id and handle):
        return
    task_id = str(getattr(handle, 'id', None) or handle)
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'UPDATE ' + table('results') +
            ' SET deep_task_id = %s,'
            " deep_claim_state = CASE WHEN deep_claim_state = 'claimed'"
            "                         THEN 'launched' ELSE deep_claim_state END"
            ' WHERE item_id = %s AND deep_claim_id = %s AND deep_pending',
            (task_id, item_id, claim_id))
        db.commit()
        cur.close()
    except Exception:
        logger.exception('spectrum_analyzer: could not attach queue task to '
                         'deep claim for %s', item_id)


def _deep_claim_start(item_id, claim_id):
    """Advance a matching claim to running; False when it must not run.

    A matching claim already running is an idempotent re-entry: the core's
    worker-loss reclaim redelivers the same queue row (same claim id) after a
    dead worker, and the replacement must run the analysis. A missing or
    different claim id means a newer generation owns the track, so this stale
    child returns without doing duplicate work. A DB error is treated like a
    superseded claim: not running is the only safe choice.
    """
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            'UPDATE ' + table('results') +
            " SET deep_claim_state = 'running'"
            ' WHERE item_id = %s AND deep_claim_id = %s AND deep_pending'
            " AND deep_claim_state IN ('claimed', 'launched', 'running')",
            (item_id, claim_id))
        ok = cur.rowcount > 0
        db.commit()
        cur.close()
        return ok
    except Exception:
        logger.exception('spectrum_analyzer: could not start deep claim for %s',
                         item_id)
        return False


def _deep_claim_failed(item_id, claim_id, exc):
    """Mark a matching claim failed (keeping deep_pending) before the child
    re-raises so the core queue records FAILURE.

    A failure of this state write is logged and PROPAGATED: the claim must
    not be left silently in a stale state, and the core task must not be
    recorded successful while the claim is stale.
    """
    db = get_db()
    cur = db.cursor()
    try:
        cur.execute(
            'UPDATE ' + table('results') +
            " SET deep_claim_state = 'failed', deep_claim_detail = %s"
            ' WHERE item_id = %s AND deep_claim_id = %s AND deep_pending'
            " AND deep_claim_state IN ('claimed', 'launched', 'running')",
            (_claim_detail('failed', str(exc)[:500]), item_id, claim_id))
        db.commit()
    except Exception:
        logger.exception('spectrum_analyzer: could not record deep claim '
                         'failure for %s', item_id)
        raise
    finally:
        cur.close()


def _deep_claim_done(item_id, claim_id):
    """Complete a matching claim.

    Two guarded, separately COMMITTED writes: first the claim-specific
    completion marker, then the clear of the active claim. The marker is
    durable before the clear, so if the process dies between the two commits
    the reconciler's gone-queue-row branch sees it and clears the claim
    instead of guessing outcome_unknown. A failed write PROPAGATES so the
    core task is not recorded successful while the claim state is stale.
    """
    db = get_db()
    cur = db.cursor()
    try:
        cur.execute(
            'UPDATE ' + table('results') +
            ' SET deep_completed_claim_id = %s, deep_completed_at = now()'
            ' WHERE item_id = %s AND deep_claim_id = %s AND deep_pending',
            (claim_id, item_id, claim_id))
        db.commit()
        cur.execute(
            'UPDATE ' + table('results') +
            ' SET deep_pending=FALSE, deep_pending_since=NULL,'
            ' deep_claim_id=NULL, deep_task_id=NULL, deep_claim_state=NULL,'
            ' deep_claimed_at=NULL, deep_queue_backend=NULL,'
            ' deep_claim_detail=NULL'
            ' WHERE item_id = %s AND deep_claim_id = %s'
            ' AND deep_completed_claim_id = %s',
            (item_id, claim_id, claim_id))
        db.commit()
    finally:
        cur.close()


def _clear_legacy_deep_pending(item_id):
    """Escape hatch for pre-claim rows only: a plain re-analysis, or a deep
    child queued by a pre-upgrade version (no claim id in its args), clears a
    pending tag only when no claim owns the row."""
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute('UPDATE ' + table('results') +
                    ' SET deep_pending=FALSE, deep_pending_since=NULL'
                    ' WHERE item_id=%s AND deep_claim_id IS NULL', (item_id,))
        db.commit()
        cur.close()
    except Exception:
        logger.exception('spectrum_analyzer: could not clear legacy '
                         'deep_pending for %s', item_id)


def _rq_job_state(job_id, queue_name='default'):
    """Status of a legacy RQ job by id, for the reconciler.

    Returns an RQ state string, 'missing' when the job is gone from Redis
    (NoSuchJobError), or None when the status is unreadable (transport error,
    rq absent). None and 'missing' are deliberately distinct: only a
    confirmed-missing job may be treated as ended.
    """
    try:
        from rq.job import Job
        from rq.exceptions import NoSuchJobError
        from redis import Redis
    except ImportError:
        return None
    try:
        # Job.fetch raises NoSuchJobError when the job key is gone and does not
        # filter by origin (Queue.fetch_job would conflate a job that lives in
        # another queue with a missing one). The default Redis() connection is
        # the same one rq 1.x Queue(name) used.
        job = Job.fetch(job_id, connection=Redis())
        return _rq_job_status_value(job)
    except NoSuchJobError:
        return 'missing'
    except Exception:
        logger.exception('spectrum_analyzer: could not read RQ job %s '
                         '(queue %s)', job_id, queue_name)
        return None


class _LaunchFailed(Exception):
    """An infrastructure failure while launching children.

    A cooperative cancellation returns (launched, unlaunched) normally; this
    exception is for a should_stop() or enqueue() that RAISED. It carries the
    original cause plus what was and was not launched, so the parent can clean
    up exactly the claims it never handed to the queue and keep its own row in
    TASK_STATUS_FAILURE instead of reporting a cancellation.
    """

    def __init__(self, cause, launched, unlaunched):
        super().__init__(str(cause))
        self.cause = cause
        self.launched = list(launched)
        self.unlaunched = list(unlaunched)


def _launch_throttled(targets, deep, claim_ids=None, on_progress=None,
                      should_stop=None):
    """Queue one analyze_track_job per target, then wait for them to drain.

    Keeps at most MAX_QUEUED_ANALYSIS_JOBS of them unfinished, the same
    throttle scan_library_job applies to its album children, so a thousand
    deep scans do not land on the queue at once. Draining before returning
    costs nothing: every child is itself a live `plugin.%` root, so the queue
    guard stays busy either way, and staying alive is what makes the parent's
    progress mean anything.

    claim_ids maps item_id -> deep claim id; each child is enqueued with its
    claim and the queue task id is attached to the claim right after
    enqueue() returns.

    Returns (launched, unlaunched). `should_stop()` is polled before each
    launch and throughout the waits, so a cancelled parent stops promptly and
    gets back what it never launched (to untag it). A should_stop() or
    enqueue() that RAISES is an infrastructure failure, not a cancellation:
    it raises _LaunchFailed so the parent fails and cleans up.
    `on_progress(launched, in_flight, waiting)` is called on every change of
    state.
    """
    max_in_flight = max(1, int(getattr(config, 'MAX_QUEUED_ANALYSIS_JOBS', 25)))
    targets = list(targets)
    claim_ids = claim_ids or {}
    handles = {}
    launched = []

    for i, item_id in enumerate(targets):
        while True:
            in_flight = _prune_finished(handles)
            try:
                if should_stop and should_stop():
                    return launched, targets[i:]
            except Exception as exc:
                raise _LaunchFailed(exc, launched, targets[i:])
            if in_flight < max_in_flight:
                break
            if on_progress:
                on_progress(len(launched), in_flight, True)
            time.sleep(POLL_SECONDS)
        try:
            handle = enqueue(analyze_track_job, item_id, deep=deep,
                             claim_id=claim_ids.get(item_id))
        except Exception as exc:
            raise _LaunchFailed(exc, launched, targets[i:])
        handles[str(getattr(handle, 'id', None) or handle)] = handle
        if deep:
            _attach_deep_task(item_id, claim_ids.get(item_id), handle)
        launched.append(item_id)
        if on_progress:
            on_progress(len(launched), len(handles), False)

    while True:
        in_flight = _prune_finished(handles)
        if in_flight <= 0:
            return launched, []
        try:
            if should_stop and should_stop():
                return launched, []
        except Exception as exc:
            raise _LaunchFailed(exc, launched, [])
        if on_progress:
            on_progress(len(launched), in_flight, True)
        time.sleep(POLL_SECONDS)


def bulk_analyze_job(item_ids=None, album=None, select=None, deep=False,
                     task_id=None):
    """Parent orchestrator for every bulk track action, on the *high* queue.

    Exists because of the 3.2 queue guard: `plugin.api.enqueue()` refuses an
    enqueue issued from the web process while any live `plugin.%` root exists,
    and the first track a route queues IS such a root -- so a route loop can
    only ever queue one track. A task already running on a worker is exempt, so
    the fan-out has to happen from in here.

    Same shape as scan_library_job one level down: claim the work, launch one
    analyze_track_job per track onto the *default* queue so every worker shares
    the load, keep at most MAX_QUEUED_ANALYSIS_JOBS in flight, and report
    done/remaining. A deep run claims each track with its own claim id (or
    re-claims it, for the manual retry routes); cancelling the parent or a
    failed launch cleans up exactly the claims that never reached the queue,
    and a re-queue run restores the prior record instead.
    """
    task_id = task_id or _current_task_id() or str(uuid.uuid4())
    label = 'deep scan' if deep else 're-analysis'
    claim_ids = {}
    restore_map = {}

    def status(state, progress, detail, extra=None):
        try:
            save_task_status(task_id, BULK_TASK_TYPE, state, progress=progress,
                             details={'deep': bool(deep), 'info': detail,
                                      **(extra or {})})
        except Exception:
            logger.exception('spectrum_analyzer: save_task_status failed')

    def revoked():
        return _task_state(task_id) == TASK_STATUS_REVOKED

    def cleanup_unlaunched(unlaunched):
        """Undo the claims for tracks that never reached the queue."""
        if not deep:
            return
        pairs = [(i, claim_ids[i]) for i in unlaunched if i in claim_ids]
        if not pairs:
            return
        if select in ('requeue_all', 'requeue_unknown'):
            for item_id, claim_id in pairs:
                prior = restore_map.get(item_id)
                if prior:
                    _restore_deep_claim(item_id, claim_id, prior)
        else:
            _clear_deep_claims_many(pairs)

    status(TASK_STATUS_STARTED, 0, f'selecting tracks for {label}')
    try:
        if deep:
            if select in ('requeue_all', 'requeue_unknown'):
                claims, restore_map = _requeue_claims(select)
            else:
                claims = _new_deep_claims(item_ids, album, select,
                                           queue='default')
        else:
            claims = [(str(i), None) for i in (item_ids or []) if i]
        targets = [item_id for item_id, _claim in claims]
        claim_ids = {item_id: claim for item_id, claim in claims}
        total = len(targets)

        def extra(count, in_flight):
            done = count - in_flight
            return {'tracks_total': total, 'tracks_launched': count,
                    'tracks_done': done, 'tracks_remaining': total - done}

        def progress(count, in_flight):
            done = count - in_flight
            return int(done * 100 / total) if total else 100

        if not total:
            status(TASK_STATUS_SUCCESS, 100, 'nothing to do', extra(0, 0))
            logger.info('spectrum_analyzer bulk %s: nothing to do', label)
            return extra(0, 0)

        def report(count, in_flight, waiting):
            detail = (f'waiting for workers ({in_flight} tracks in flight)'
                      if waiting else f'queued {count}/{total}')
            status(TASK_STATUS_PROGRESS, progress(count, in_flight),
                   detail, extra(count, in_flight))

        launched, unlaunched = _launch_throttled(
            targets, deep, claim_ids=claim_ids, on_progress=report,
            should_stop=revoked)

        if unlaunched or revoked():
            logger.info('spectrum_analyzer bulk %s %s cancelled (%d of %d launched)',
                        label, task_id, len(launched), total)
            cleanup_unlaunched(unlaunched)
            return

        done = extra(len(launched), 0)
        status(TASK_STATUS_SUCCESS, 100, 'done', done)
        logger.info('spectrum_analyzer bulk %s finished: %s', label, done)
        return done
    except _LaunchFailed as f:
        # an infrastructure error, not a cancellation: keep the parent row in
        # FAILURE and clean up only what never reached the queue
        status(TASK_STATUS_FAILURE, 0, str(f.cause))
        cleanup_unlaunched(f.unlaunched)
        raise f.cause from f
    except Exception as exc:
        status(TASK_STATUS_FAILURE, 0, str(exc))
        raise


def claim_deep(item_id, queue='high'):
    """Claim one track for a deep scan; its claim id, or None when the track
    is already tagged or not deep-eligible. The direct deep route uses this so
    the page updates immediately and the button stays idempotent."""
    claims = _new_deep_claims(item_ids=[item_id], queue=queue)
    return claims[0][1] if claims else None


def analyze_track_job(item_id, deep=False, claim_id=None):
    """Manual re-run of one song. Always recomputes. deep=True analyzes the
    entire file instead of a segment (dark-master check).

    A deep run owns a claim (claim_id): the child advances it to running,
    completes it when the analysis lands, and marks it failed before
    re-raising so the core queue records FAILURE. A missing or different
    claim id means a newer generation owns the track, so this stale child
    does no work. A plain re-analysis — or a deep child queued by a
    pre-claim version, which carries no claim id — clears only a legacy
    (pre-claim) pending tag, the escape hatch for a tag orphaned by a lost
    deep-scan job.
    """
    if deep and claim_id:
        if not _deep_claim_start(item_id, claim_id):
            logger.info('spectrum_analyzer: deep claim for %s superseded, '
                        'stale child skips analysis', item_id)
            return
        try:
            result = _analyze_track(item_id, deep=True)
        except Exception as exc:
            try:
                _deep_claim_failed(item_id, claim_id, exc)
            except Exception as db_exc:
                # the failure-state write itself failed: propagate it, chained
                # to the original analysis error, so the task is not recorded
                # successful and the state-write failure stays visible
                raise db_exc from exc
            raise
        _deep_claim_done(item_id, claim_id)
        return result
    try:
        return _analyze_track(item_id, deep=deep)
    finally:
        _clear_legacy_deep_pending(item_id)


def _analyze_track(item_id, deep):
    settings = _settings()
    db = get_db()
    cur = db.cursor()
    cur.execute(
        'SELECT title, artist, album, album_id, file_path, suffix, bitrate,'
        ' provider_track_id, server_id FROM '
        + table('results') + ' WHERE item_id=%s', (item_id,))
    row = cur.fetchone()
    cur.close()
    if not row:
        logger.warning('spectrum_analyzer: no stored row for %s', item_id)
        return

    (title, artist, album, album_id, file_path, suffix, bitrate,
     provider_id, row_server) = row
    server_id = row_server or _active_server_id()
    # the media server speaks native ids: stored provider id, else translate
    # the canonical id, else item_id itself (v2 rows, where they are the same)
    native = (provider_id
              or _fp_to_native([item_id], server_id).get(item_id)
              or item_id)

    with _bind_server(server_id):
        # Prefer fresh metadata from the media server (also refreshes fingerprint)
        track = None
        if album_id:
            try:
                for t in get_tracks_from_album(album_id):
                    if _native_id(t) in (native, item_id):
                        track = t
                        break
            except Exception:
                logger.exception('spectrum_analyzer: album lookup failed for rescan')
        if track is None:
            # minimal item accepted by every provider backend
            track = {'id': native, 'Id': native, 'title': title,
                     'suffix': suffix, 'path': file_path, 'Path': file_path}

        info = _info_from_track(track, {'Id': album_id, 'Name': album},
                                {_native_id(track): item_id}, server_id)
        info['title'] = info['title'] or title
        info['artist'] = info['artist'] or artist
        info['album'] = info['album'] or album
        _analyze_download(track, info, meta_fingerprint(track), settings, {},
                          'force', deep=deep)
    logger.info('spectrum_analyzer: re-analyzed %s%s', info.get('title'),
                ' (deep)' if deep else '')


def scan_changed_task():
    """Entry point for Scheduled Tasks (cron/run-now): incremental scan."""
    return scan_library_job(mode='changed')


def _reconcile_settle(item_id, claim_id, state, detail=None):
    """Apply one reconciliation outcome to a matching claim.

    state None clears the claim entirely; 'failed' and 'outcome_unknown' keep
    deep_pending and record the state for the UI and the manual retry routes.
    Every write is fenced on the claim id, so a concurrent re-claim is never
    overwritten.
    """
    try:
        db = get_db()
        cur = db.cursor()
        if state is None:
            cur.execute(
                'UPDATE ' + table('results') +
                ' SET deep_pending=FALSE, deep_pending_since=NULL,'
                ' deep_claim_id=NULL, deep_task_id=NULL, deep_claim_state=NULL,'
                ' deep_claimed_at=NULL, deep_queue_backend=NULL,'
                ' deep_claim_detail=NULL'
                ' WHERE item_id=%s AND deep_claim_id=%s',
                (item_id, claim_id))
        else:
            cur.execute(
                'UPDATE ' + table('results') +
                ' SET deep_claim_state=%s, deep_claim_detail=%s'
                ' WHERE item_id=%s AND deep_claim_id=%s AND deep_pending'
                " AND deep_claim_state IN ('claimed', 'launched', 'running')",
                (state, detail or _claim_detail(state), item_id, claim_id))
        db.commit()
        cur.close()
    except Exception:
        logger.exception('spectrum_analyzer: could not reconcile deep claim '
                         'for %s', item_id)


def reconcile_deep_pending_task():
    """Cron: reconcile deep claims against the queue. Never re-queues.

    The core owns its queue: 3.2+ persists NEW work before a worker claims it
    and reclaims dead RUNNING work, so a worker restart loses nothing the core
    has not already handled. What this reconciles is the plugin's own claim
    state, which the core cannot see:

    - a claim whose queue row is terminal is settled from that row:
      SUCCESS / REVOKED clears it, FAILURE marks it failed for a manual retry;
    - a claim whose queue row is gone is settled from its own completion
      marker when one matches, and becomes outcome_unknown past the warning
      grace otherwise (retention deletes terminal rows, so a missing row
      proves nothing by itself);
    - a claim with no attached queue task (the parent died between claiming
      and enqueuing) becomes outcome_unknown past the grace;
    - live and unreadable states are left alone, and legacy pre-claim rows
      are left for the manual retry routes.

    An uncertain outcome is a badge, not a retry: nothing here ever enqueues.
    """
    grace = _settings()['deep_pending_warning_hours']
    db = get_db()
    cur = db.cursor()
    cur.execute('SELECT now()')
    db_now = cur.fetchone()[0]
    cur.execute(
        'SELECT item_id, deep_claim_id, deep_task_id, deep_queue_backend,'
        ' deep_claim_state, deep_completed_claim_id, deep_claim_detail,'
        ' COALESCE(deep_claimed_at, deep_pending_since)'
        ' FROM ' + table('results') +
        ' WHERE deep_pending AND deep_eligible')
    rows = cur.fetchall()
    cur.close()

    counts = {'live': 0, 'settled': 0, 'unknown': 0, 'unreadable': 0,
              'legacy': 0}

    def past_grace(since):
        return since is None or since < db_now - timedelta(hours=grace)

    attached = {}   # backend -> [(item_id, claim_id, task_id, marker, detail)]
    taskless = []   # (item_id, claim_id, since)
    for (item_id, claim_id, task_id, backend, _state, marker, detail,
         since) in rows:
        if claim_id is None:
            counts['legacy'] += 1
            continue
        if task_id:
            attached.setdefault(backend or _queue_backend(), []).append(
                (item_id, claim_id, task_id, marker, detail, since))
        else:
            taskless.append((item_id, claim_id, since))

    for backend, claims in attached.items():
        if backend == 'postgres':
            states = _queue_states([task_id for _i, _c, task_id, _m, _d,
                                    _s in claims])
            if states is None:
                counts['unreadable'] += len(claims)
                continue
            for item_id, claim_id, task_id, marker, _detail, since in claims:
                qstate = states.get(task_id)
                if qstate in (TASK_STATUS_PENDING, TASK_STATUS_STARTED,
                              TASK_STATUS_PROGRESS):
                    counts['live'] += 1
                elif qstate in (TASK_STATUS_SUCCESS, TASK_STATUS_REVOKED):
                    _reconcile_settle(item_id, claim_id, None)
                    counts['settled'] += 1
                elif qstate == TASK_STATUS_FAILURE:
                    _reconcile_settle(item_id, claim_id, 'failed',
                                      _claim_detail('failed',
                                                    'queue task failed'))
                    counts['settled'] += 1
                elif marker == claim_id:
                    # the child committed its completion but the clear never
                    # landed; the gone row is retention, not a loss
                    _reconcile_settle(item_id, claim_id, None)
                    counts['settled'] += 1
                elif past_grace(since):
                    _reconcile_settle(
                        item_id, claim_id, 'outcome_unknown',
                        _claim_detail('outcome_unknown',
                                      'queue row gone, no completion marker'))
                    counts['unknown'] += 1
                else:
                    counts['live'] += 1
        else:  # rq
            for item_id, claim_id, task_id, marker, detail, since in claims:
                qstate = _rq_job_state(task_id, _detail_queue(detail)
                                       or 'default')
                if qstate in ('queued', 'started', 'deferred'):
                    counts['live'] += 1
                elif qstate == 'finished':
                    _reconcile_settle(item_id, claim_id, None)
                    counts['settled'] += 1
                elif qstate in ('stopped', 'canceled'):
                    _reconcile_settle(item_id, claim_id, None)
                    counts['settled'] += 1
                elif qstate == 'failed':
                    _reconcile_settle(item_id, claim_id, 'failed',
                                      _claim_detail('failed',
                                                    'queue task failed'))
                    counts['settled'] += 1
                elif qstate == 'missing':
                    if marker == claim_id:
                        _reconcile_settle(item_id, claim_id, None)
                        counts['settled'] += 1
                    elif past_grace(since):
                        _reconcile_settle(
                            item_id, claim_id, 'outcome_unknown',
                            _claim_detail('outcome_unknown',
                                          'queue job gone, no completion marker'))
                        counts['unknown'] += 1
                    else:
                        counts['live'] += 1
                else:  # None: a transport error is unreadable, not missing
                    counts['unreadable'] += 1

    for item_id, claim_id, since in taskless:
        if past_grace(since):
            _reconcile_settle(
                item_id, claim_id, 'outcome_unknown',
                _claim_detail('outcome_unknown',
                              'no queue task was ever attached'))
            counts['unknown'] += 1
        else:
            counts['live'] += 1

    if counts['legacy']:
        logger.warning('spectrum_analyzer: %d legacy deep-pending row(s) '
                       'predate the claim model; only the manual retry routes '
                       'may re-queue them', counts['legacy'])
    logger.info('spectrum_analyzer: deep claim reconciliation: %d live, '
                '%d settled, %d outcome-unknown, %d unreadable',
                counts['live'], counts['settled'], counts['unknown'],
                counts['unreadable'])
    return counts


def on_song_analyzed(song):
    """Piggyback on core analysis: the audio file is already on disk."""
    try:
        if not get_setting('hook_enabled', True):
            return
        native_id = song.get('item_id')  # v3: the NATIVE provider id
        path = song.get('audio_path')
        if not native_id or not path or not os.path.exists(path):
            return
        # v3 attaches the canonical fp id (what score is keyed by) to the raw
        # media item; on v2 it is absent and the native id IS the score key
        media_item = song.get('media_item') or {}
        item_id = str(media_item.get('_catalog_item_id') or native_id)

        # media_item is the raw provider track dict — the same shape the scans
        # fingerprint — so store the fingerprint a scan would compute and the
        # next 'changed' scan skips this track outright, no download needed
        meta_fp = meta_fingerprint(media_item) or None

        md5 = file_md5(path)
        settings = _settings()
        rev = dsp.analysis_rev(settings['drop_db'], settings['segment_seconds'])
        db = get_db()
        cur = db.cursor()
        cur.execute('SELECT audio_md5, analysis_rev FROM ' + table('results')
                    + ' WHERE item_id=%s', (item_id,))
        row = cur.fetchone()
        cur.close()
        if row and row[0] == md5 and row[1] == rev:
            if meta_fp:
                _touch_fingerprint(item_id, meta_fp)  # backfill older hook rows
            return  # same bytes, same analyzer, nothing to do

        meta = song.get('metadata') or {}
        info = {
            'item_id': item_id,
            'provider_track_id': str(native_id),
            'server_id': song.get('server_id'),
            'title': meta.get('title'),
            'artist': meta.get('artist') or meta.get('album_artist'),
            'album': meta.get('album') or meta.get('album_name'),
            'album_id': meta.get('album_id'),
            'file_path': meta.get('file_path'),
            'suffix': os.path.splitext(path)[1].lstrip('.').lower(),
            'bitrate': None,
        }
        result = dsp.analyze_file(
            path, suffix=info['suffix'], bitrate_kbps=None,
            segment_seconds=settings['segment_seconds'], drop_db=settings['drop_db'],
            img_w=settings['img_w'], img_h=settings['img_h'],
            skip_spectrogram_for_clean=settings['skip_clean_spectrograms'],
        )
        # (if the payload had no media_item — e.g. an older core — meta_fp is
        # NULL and the next 'changed' scan downloads once, notices the MD5
        # matches, and just backfills the fingerprint)
        _upsert(item_id, info, result, meta_fp, md5)
    except Exception:
        logger.exception('spectrum_analyzer: on_song_analyzed failed')
