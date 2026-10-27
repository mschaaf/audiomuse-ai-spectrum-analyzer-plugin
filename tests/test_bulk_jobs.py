"""Bulk fan-out orchestration and the deep claim model, on a stubbed core.

jobs.py talks to AudioMuse-AI, so the rest of the suite leaves it alone. What
is faked here is deliberately only the plumbing — plugin.api, plus enough of a
task_status / results table to answer the handful of statements these functions
run — so that the ORCHESTRATION being exercised is the real code: the claim,
the throttle, the in-flight bookkeeping, the claim lifecycle and the
reconciliation.

What this pins down: the throttle never exceeds MAX_QUEUED_ANALYSIS_JOBS and
does actually wait; a finished (or retention-deleted) queue row frees its slot;
the deep claim skips CLEAN/verified/already-tagged tracks and is idempotent;
cancelling or a failed launch cleans up exactly the claims that never reached
the queue; the claim lifecycle (start / attach / done / failed / restore) is
fenced on the claim id; and the reconciler settles claims from the queue
without ever re-queueing.

What it does NOT pin down: that any of the SQL is valid Postgres, or that the
core behaves as modelled here. FakeCursor matches on statement fragments, so
editing those statements means editing this file too — that coupling is the
price of covering this layer at all.
"""
import contextlib
import importlib.util
import os
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta

_HERE = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.join(os.path.dirname(_HERE), 'plugins', 'SpectrumAnalyzer')

MAX_IN_FLIGHT = 3

# a fixed "database now" so grace-period arithmetic is deterministic
DB_NOW = datetime(2026, 9, 2, 12, 0, 0)

TASK_ROWS = {}       # task_id -> status
TASK_PARENT = {}     # task_id -> parent_task_id, as last saved
TASK_DETAILS = {}    # task_id -> details dict, as last saved
RESULT_ROWS = {}     # item_id -> column dict
SCORE_IDS = set()    # item_ids the core has analyzed (a score row exists)
ENQUEUED = []        # every enqueue() the code under test made
DOWNLOADS = []       # every download_track() call: (provider track id, path)
SQL_LOG = []         # every statement executed: (normalized sql, params)
CLOSED_CURSORS = []  # every cursor closed, in order
CLAIM_SEQ = [0]      # claim-id counter

# controls for the stubbed plugin.api / registry / queue, reset by _reset()
CORE = {}
MEDIA = {}           # 'albums': [album dicts], 'tracks': {album id: [tracks]}
AUTO = object()      # _tag's default claim id: generate a fresh one


def _reset_stub_controls():
    CORE.clear()
    CORE.update({
        'default_server': 'srv1',  # the server the default-server rules apply to
        'active_server': None,     # what plugin.api.active_server_id() says
        'servers': [],             # server ids list_servers() reports
        'bound': [],               # every server id use_server() was given
        'current_task': None,      # what taskqueue.current_task_id() says
        'translate_fail': False,   # registry raises when True
        # the secondary server's native <-> canonical pairs; anything not
        # listed is absent there, so a lookup omits it. The default server
        # is the identity mapping.
        'secondary': {},
    })
    MEDIA.clear()
    MEDIA.update({'albums': [], 'tracks': {}})


def _is_default(server_id):
    return server_id in (None, CORE['default_server'])


def _reverse_translate_ids(provider_ids, server_id=None, conn=None):
    """Registry stand-in: {provider id: canonical id}.

    On the default server every unknown provider id maps to itself, as the core
    does (only the forward lookup drops canonical ids it cannot place).
    """
    if CORE['translate_fail']:
        raise RuntimeError('registry unavailable')
    ids = [str(i) for i in provider_ids if i]
    if _is_default(server_id):
        return {i: i for i in ids}
    return {i: CORE['secondary'][i] for i in ids if i in CORE['secondary']}


def _translate_ids(item_ids, server_id=None, conn=None):
    """Registry stand-in: {canonical id: provider id}, same rules inverted."""
    if CORE['translate_fail']:
        raise RuntimeError('registry unavailable')
    ids = [str(i) for i in item_ids if i]
    if _is_default(server_id):
        return {i: i for i in ids if not i.startswith('fp_')}
    inverse = {c: n for n, c in CORE['secondary'].items()}
    return {i: inverse[i] for i in ids if i in inverse}


def next_claim_id(item_id):
    CLAIM_SEQ[0] += 1
    return 'claim-%s-%d' % (item_id, CLAIM_SEQ[0])


def _clear_row(row):
    row['deep_pending'] = False
    row['deep_pending_since'] = None
    row['deep_claim_id'] = None
    row['deep_task_id'] = None
    row['deep_claim_state'] = None
    row['deep_claimed_at'] = None
    row['deep_queue_backend'] = None
    row['deep_claim_detail'] = None


class FakeCursor:
    """Answers only the statements jobs.py actually runs here."""

    def __init__(self):
        self.rows = []
        self.rowcount = 0

    def execute(self, sql, params=()):
        s = ' '.join(sql.split())
        SQL_LOG.append((s, tuple(params)))
        if 'to_regclass' in s:
            # migrate-era backfill: no track_server_map table on this core
            self.rows = [(None,)]
        elif s.startswith('SELECT task_id, status FROM task_status'):
            self.rows = [(t, TASK_ROWS[t]) for t in params[0] if t in TASK_ROWS]
        elif s.startswith('SELECT task_id, status, details FROM task_status'):
            # _child_rows: the children a parent saved, with their details
            self.rows = [(t, TASK_ROWS[t], TASK_DETAILS.get(t, {}))
                         for t, parent in TASK_PARENT.items()
                         if parent == params[0] and t in TASK_ROWS]
        elif s.startswith('SELECT status FROM task_status'):
            self.rows = [(TASK_ROWS[params[0]],)] if params[0] in TASK_ROWS else []
        elif s == 'SELECT now()::timestamp':
            self.rows = [(DB_NOW,)]
        elif s.startswith('SELECT item_id FROM score'):
            wanted = set(params[0]) if params else None
            self.rows = [(i,) for i in SCORE_IDS if wanted is None or i in wanted]
        elif s.startswith('SELECT item_id, meta_fp, audio_md5, analysis_rev FROM'):
            wanted = set(params[0]) if params else None
            self.rows = [(i, r.get('meta_fp'), r.get('audio_md5'),
                          r.get('analysis_rev'))
                         for i, r in RESULT_ROWS.items()
                         if wanted is None or i in wanted]
        elif s.startswith('SELECT title, artist, album, album_id'):
            # _analyze_track's stored-row read
            row = RESULT_ROWS.get(params[0])
            self.rows = [] if row is None else [(
                row.get('title'), row.get('artist'), row.get('album'),
                row.get('album_id'), row.get('file_path'), row.get('suffix'),
                row.get('bitrate'), row.get('provider_track_id'),
                row.get('server_id'))]
        elif ('IS NOT DISTINCT FROM' in s
              and "SET deep_claim_state='outcome_unknown'" in s):
            # _quarantine_legacy_backend: fenced on backend, claim id and state
            detail, item_id, backend, claim_id = params
            row = RESULT_ROWS.get(item_id)
            self.rowcount = 0
            if (row is not None and row['deep_pending']
                    and row['deep_queue_backend'] == backend
                    and row['deep_claim_id'] == claim_id
                    and row['deep_claim_state'] in (None, 'claimed', 'launched',
                                                    'running', 'failed')):
                row['deep_claim_state'] = 'outcome_unknown'
                row['deep_claim_detail'] = detail
                self.rowcount = 1
            self.rows = []
        elif 'COALESCE(deep_claimed_at, deep_pending_since)' in s:
            # the reconciler's pending-claim scan
            self.rows = [
                (item_id, row['deep_claim_id'], row['deep_task_id'],
                 row['deep_queue_backend'], row['deep_claim_state'],
                 row['deep_completed_claim_id'], row['deep_claim_detail'],
                 row['deep_claimed_at'] or row['deep_pending_since'])
                for item_id, row in RESULT_ROWS.items()
                if row['deep_pending'] and (row['deep_eligible']
                                            or row['deep_queue_backend'] == 'rq')]
        elif 'FOR UPDATE' in s:
            # _requeue_claims' eligibility scan
            self.rows = []
            for item_id, row in RESULT_ROWS.items():
                if not row['deep_pending'] or not row['deep_eligible']:
                    continue
                if "deep_claim_state IS DISTINCT FROM 'outcome_unknown'" in s:
                    legacy = (row['deep_claim_id'] is None
                              and row['deep_claim_state'] != 'outcome_unknown')
                    if not legacy and row['deep_claim_state'] != 'failed':
                        continue
                elif ("deep_task_id IS NULL OR deep_claim_state ="
                      " 'outcome_unknown'" in s):
                    if (row['deep_task_id'] is not None
                            and row['deep_claim_state'] != 'outcome_unknown'):
                        continue
                else:
                    continue
                self.rows.append((
                    item_id, row['deep_pending_since'], row['deep_claim_id'],
                    row['deep_claim_state'], row['deep_queue_backend'],
                    row['deep_task_id'], row['deep_claimed_at'],
                    row['deep_claim_detail'], row['deep_completed_claim_id'],
                    row['deep_completed_at']))
        elif 'RETURNING deep_claim_id' in s:
            # _requeue_claims' per-row CAS replacement
            item_id, prior_claim = params[2], params[3]
            row = RESULT_ROWS.get(item_id)
            self.rows = []
            if row is not None and row['deep_claim_id'] == prior_claim:
                claim_id = next_claim_id(item_id)
                row['deep_pending_since'] = DB_NOW
                row['deep_claim_id'] = claim_id
                row['deep_claim_state'] = 'claimed'
                row['deep_claimed_at'] = DB_NOW
                row['deep_queue_backend'] = params[0]
                row['deep_task_id'] = None
                row['deep_completed_claim_id'] = None
                row['deep_completed_at'] = None
                row['deep_claim_detail'] = params[1]
                self.rows = [(claim_id,)]
        elif 'SET deep_pending=TRUE' in s and 'RETURNING item_id, deep_claim_id' in s:
            # _new_deep_claims: one claim id per row
            claimed = []
            for item_id, row in RESULT_ROWS.items():
                if row['deep_pending'] or not row['deep_eligible']:
                    continue
                if 'verdict IS DISTINCT FROM' in s and row['verdict'] == 'CLEAN':
                    continue
                if 'NOT verified' in s and row['verified']:
                    continue
                if 'album = %s' in s and row['album'] != params[-1]:
                    continue
                if 'item_id = ANY(%s)' in s and item_id not in params[-1]:
                    continue
                claim_id = next_claim_id(item_id)
                row['deep_pending'] = True
                row['deep_pending_since'] = DB_NOW
                row['deep_claim_id'] = claim_id
                row['deep_claim_state'] = 'claimed'
                row['deep_claimed_at'] = DB_NOW
                row['deep_queue_backend'] = params[0]
                row['deep_task_id'] = None
                row['deep_completed_claim_id'] = None
                row['deep_completed_at'] = None
                row['deep_claim_detail'] = params[1]
                claimed.append((item_id, claim_id))
            self.rows = claimed
        elif 'SET deep_task_id = %s' in s:
            # _attach_deep_task: never regresses a fast child
            task_id, item_id, claim_id = params
            row = RESULT_ROWS.get(item_id)
            if (row is not None and row['deep_claim_id'] == claim_id
                    and row['deep_pending']):
                row['deep_task_id'] = task_id
                if row['deep_claim_state'] == 'claimed':
                    row['deep_claim_state'] = 'launched'
            self.rows = []
        elif "SET deep_claim_state = 'running'" in s:
            # _deep_claim_start: matching live claim -> running
            item_id, claim_id = params
            row = RESULT_ROWS.get(item_id)
            self.rowcount = 0
            if (row is not None and row['deep_claim_id'] == claim_id
                    and row['deep_pending']
                    and row['deep_claim_state'] in ('claimed', 'launched',
                                                    'running')):
                row['deep_claim_state'] = 'running'
                self.rowcount = 1
            self.rows = []
        elif "SET deep_claim_state = 'failed'" in s:
            # _deep_claim_failed: keep deep_pending, record the failure
            detail, item_id, claim_id = params
            row = RESULT_ROWS.get(item_id)
            if (row is not None and row['deep_claim_id'] == claim_id
                    and row['deep_pending']
                    and row['deep_claim_state'] in ('claimed', 'launched',
                                                    'running')):
                row['deep_claim_state'] = 'failed'
                row['deep_claim_detail'] = detail
            self.rows = []
        elif 'SET deep_completed_claim_id = %s' in s:
            # _deep_claim_done, write one: the claim-specific completion marker
            claim_id, item_id, _ = params
            row = RESULT_ROWS.get(item_id)
            if (row is not None and row['deep_claim_id'] == claim_id
                    and row['deep_pending']):
                row['deep_completed_claim_id'] = claim_id
                row['deep_completed_at'] = DB_NOW
            self.rows = []
        elif 'SET deep_pending=FALSE' in s and 'deep_completed_claim_id = %s' in s:
            # _deep_claim_done, write two: clear only on a matching marker
            item_id, claim_id, marker = params
            row = RESULT_ROWS.get(item_id)
            if (row is not None and row['deep_claim_id'] == claim_id
                    and row['deep_completed_claim_id'] == marker):
                _clear_row(row)
            self.rows = []
        elif 'SET deep_pending=FALSE' in s and 'deep_claim_id IS NULL' in s:
            # _clear_legacy_deep_pending: pre-claim rows only
            item_id = params[0]
            row = RESULT_ROWS.get(item_id)
            if (row is not None and row['deep_claim_id'] is None
                    and row['deep_claim_state'] != 'outcome_unknown'):
                row['deep_pending'] = False
                row['deep_pending_since'] = None
            self.rows = []
        elif 'SET deep_pending=FALSE' in s:
            # cleanup / reconciler clear: fenced on the claim id
            item_id, claim_id = params
            row = RESULT_ROWS.get(item_id)
            if row is not None and row['deep_claim_id'] == claim_id:
                _clear_row(row)
            self.rows = []
        elif 'SET deep_pending_since=%s, deep_claim_id=%s' in s:
            # _restore_deep_claim: CAS on the NEW claim id
            (p_since, p_claim, p_state, p_backend, p_task, p_claimed,
             p_detail, p_marker, p_completed, item_id, new_claim) = params
            row = RESULT_ROWS.get(item_id)
            self.rowcount = 0
            if row is not None and row['deep_claim_id'] == new_claim:
                row['deep_pending_since'] = p_since
                row['deep_claim_id'] = p_claim
                row['deep_claim_state'] = p_state
                row['deep_queue_backend'] = p_backend
                row['deep_task_id'] = p_task
                row['deep_claimed_at'] = p_claimed
                row['deep_claim_detail'] = p_detail
                row['deep_completed_claim_id'] = p_marker
                row['deep_completed_at'] = p_completed
                self.rowcount = 1
            self.rows = []
        elif 'SET deep_claim_state=%s, deep_claim_detail=%s' in s:
            # _reconcile_settle: record failed / outcome_unknown
            state, detail, item_id, claim_id = params
            row = RESULT_ROWS.get(item_id)
            if (row is not None and row['deep_claim_id'] == claim_id
                    and row['deep_pending']
                    and row['deep_claim_state'] in ('claimed', 'launched',
                                                    'running')):
                row['deep_claim_state'] = state
                row['deep_claim_detail'] = detail
            self.rows = []
        else:
            self.rows = []

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def close(self):
        CLOSED_CURSORS.append(self)


class FakeDB:
    def __init__(self):
        self.rollbacks = 0

    def cursor(self):
        return FakeCursor()

    def commit(self):
        pass

    def rollback(self):
        self.rollbacks += 1


class QueuedPluginTask(str):
    """What plugin.api.enqueue returns on a 3.2+ core: the task id, as a str."""

    @property
    def id(self):
        return str(self)


def _fake_enqueue(func, *args, queue='default', **kwargs):
    task_id = 'q%d' % len(ENQUEUED)
    ENQUEUED.append({'func': getattr(func, '__name__', str(func)), 'args': args,
                     'kwargs': kwargs, 'queue': queue, 'task_id': task_id})
    TASK_ROWS[task_id] = 'NEW'
    return QueuedPluginTask(task_id)


def _fake_save_task_status(task_id, task_type, status, **kwargs):
    TASK_ROWS[task_id] = status
    if 'parent_task_id' in kwargs:
        TASK_PARENT[task_id] = kwargs['parent_task_id']
    if 'details' in kwargs:
        TASK_DETAILS[task_id] = kwargs['details']


def _run_one_child():
    """Stand-in for a worker: finish the oldest child still sitting queued."""
    for task_id, status in TASK_ROWS.items():
        if task_id.startswith('q') and status == 'NEW':
            TASK_ROWS[task_id] = 'SUCCESS'
            return True
    return False


def _install_core_stubs():
    api = types.ModuleType('plugin.api')
    api.config = types.SimpleNamespace(MAX_QUEUED_ANALYSIS_JOBS=MAX_IN_FLIGHT,
                                       QUEUE_INLINE_STALE_SECONDS=1800.0)
    api.enqueue = _fake_enqueue
    api.get_db = lambda: FakeDB()
    api.get_setting = lambda key, default=None: default
    api.table = lambda name: 'plugin_spectrum_analyzer__' + name
    api.logger = types.SimpleNamespace(info=lambda *a, **k: None,
                                       warning=lambda *a, **k: None,
                                       exception=lambda *a, **k: None)
    api.save_task_status = _fake_save_task_status
    api.active_server_id = lambda: CORE['active_server']
    api.list_servers = lambda: [{'server_id': s} for s in CORE['servers']]

    def use_server(server_id):
        CORE['bound'].append(server_id)
        return contextlib.nullcontext()

    api.use_server = use_server
    # the 3.2+ spellings, which config still aliases the old names onto
    api.TASK_STATUS_PENDING = 'NEW'
    api.TASK_STATUS_STARTED = 'RUNNING'
    api.TASK_STATUS_PROGRESS = 'RUNNING'
    api.TASK_STATUS_SUCCESS = 'SUCCESS'
    api.TASK_STATUS_FAILURE = 'FAIL'
    api.TASK_STATUS_REVOKED = 'REVOKED'

    plugin = types.ModuleType('plugin')
    plugin.__path__ = []
    sys.modules['plugin'] = plugin
    sys.modules['plugin.api'] = api

    # the core's queue module: the job reads its own task id from here
    taskqueue = types.ModuleType('taskqueue')
    taskqueue.current_task_id = lambda: CORE['current_task']
    sys.modules['taskqueue'] = taskqueue

    def get_recent_albums(n):
        return list(MEDIA['albums'])

    def get_tracks_from_album(album_id):
        return list(MEDIA['tracks'].get(album_id, []))

    def download_track(directory, track):
        DOWNLOADS.append((track.get('Id') or track.get('id'), directory))
        return None

    mediaserver = types.ModuleType('tasks.mediaserver')
    mediaserver.get_recent_albums = get_recent_albums
    mediaserver.get_tracks_from_album = get_tracks_from_album
    mediaserver.download_track = download_track
    registry = types.ModuleType('tasks.mediaserver.registry')
    registry.reverse_translate_ids = _reverse_translate_ids
    registry.translate_ids = _translate_ids
    tasks = types.ModuleType('tasks')
    tasks.__path__ = []
    sys.modules['tasks'] = tasks
    sys.modules['tasks.mediaserver'] = mediaserver
    sys.modules['tasks.mediaserver.registry'] = registry

    pkg = types.ModuleType('sa')
    pkg.__path__ = [PKG_DIR]
    sys.modules['sa'] = pkg
    dsp = types.ModuleType('sa.dsp')
    dsp.analysis_rev = lambda *a: 'r1'
    sys.modules['sa.dsp'] = dsp

    spec = importlib.util.spec_from_file_location(
        'sa.jobs', os.path.join(PKG_DIR, 'jobs.py'))
    module = importlib.util.module_from_spec(spec)
    sys.modules['sa.jobs'] = module
    spec.loader.exec_module(module)
    # the throttle's wait is where a worker gets to make progress;
    # strftime/gmtime feed _claim_detail's recorded_at stamp
    module.time = types.SimpleNamespace(
        sleep=lambda s: _run_one_child(),
        monotonic=lambda: 0.0,
        strftime=lambda fmt, t: '2026-09-02T12:00:00Z',
        gmtime=lambda: (0,))
    return module


jobs = _install_core_stubs()
_reset_stub_controls()


def _reset(n_tracks=8, verdict='FAKE_SUSPECT'):
    TASK_ROWS.clear()
    TASK_PARENT.clear()
    TASK_DETAILS.clear()
    RESULT_ROWS.clear()
    SCORE_IDS.clear()
    del ENQUEUED[:]
    del DOWNLOADS[:]
    del SQL_LOG[:]
    del CLOSED_CURSORS[:]
    CLAIM_SEQ[0] = 0
    _reset_stub_controls()
    for i in range(n_tracks):
        RESULT_ROWS['t%d' % i] = {
            'album': 'A', 'verdict': verdict, 'verified': False,
            'deep_pending': False, 'deep_eligible': True,
            'deep_pending_since': None, 'deep_claim_id': None,
            'deep_claim_state': None, 'deep_queue_backend': None,
            'deep_task_id': None, 'deep_claimed_at': None,
            'deep_completed_claim_id': None, 'deep_completed_at': None,
            'deep_claim_detail': None,
            'title': 'Track %d' % i, 'artist': 'Artist', 'album_id': None,
            'file_path': None, 'suffix': 'flac', 'bitrate': None,
            'provider_track_id': None, 'server_id': None,
            'meta_fp': None, 'audio_md5': None, 'analysis_rev': None}


def _tag(item_id, state='claimed', task_id=None, claim_id=AUTO,
         since=None, marker=None, backend='postgres'):
    """Put a row into a given claim state (test setup helper).

    claim_id=None builds a truly claimless row (NULL deep_claim_id), which is
    what a pre-claim-model row looks like; the default generates a fresh id.
    """
    row = RESULT_ROWS[item_id]
    row['deep_pending'] = True
    row['deep_pending_since'] = since if since is not None else DB_NOW
    row['deep_claim_id'] = (next_claim_id(item_id) if claim_id is AUTO
                            else claim_id)
    row['deep_claim_state'] = state
    row['deep_queue_backend'] = backend
    row['deep_task_id'] = task_id
    row['deep_claimed_at'] = since if since is not None else DB_NOW
    row['deep_completed_claim_id'] = marker
    row['deep_claim_detail'] = None
    return row['deep_claim_id']


def _live_children():
    return [t for t, s in TASK_ROWS.items() if t.startswith('q') and s == 'NEW']


class TestPruneFinished(unittest.TestCase):
    def test_only_live_rows_hold_a_slot(self):
        # a vanished row counts as finished: the core's retention sweep only
        # ever deletes TERMINAL rows, and every root enqueue triggers it
        TASK_ROWS.clear()
        TASK_ROWS.update({'a': 'NEW', 'b': 'RUNNING', 'c': 'SUCCESS',
                          'd': 'FAIL', 'e': 'REVOKED'})
        handles = {k: QueuedPluginTask(k)
                   for k in ('a', 'b', 'c', 'd', 'e', 'swept-away')}
        self.assertEqual(jobs._prune_finished(handles), 2)
        self.assertEqual(sorted(handles), ['a', 'b'])


class TestLaunchThrottled(unittest.TestCase):
    def test_never_exceeds_the_cap_and_really_waits(self):
        _reset(8)
        in_flight_after_each = []
        real_enqueue = jobs.enqueue

        def watched(func, *args, **kwargs):
            # nothing finishes in here on purpose: only the throttle's own wait
            # runs a child, so in-flight can fall only when it genuinely waited
            handle = real_enqueue(func, *args, **kwargs)
            in_flight_after_each.append(len(_live_children()))
            return handle

        jobs.enqueue = watched
        try:
            launched, unlaunched = jobs._launch_throttled(
                ['t%d' % i for i in range(8)], deep=True)
        finally:
            jobs.enqueue = real_enqueue

        self.assertEqual(len(launched), 8)
        self.assertEqual(unlaunched, [])
        self.assertEqual(max(in_flight_after_each), MAX_IN_FLIGHT)
        self.assertLessEqual(min(in_flight_after_each[MAX_IN_FLIGHT:]), MAX_IN_FLIGHT)

    def test_enqueue_failure_raises_launch_failed(self):
        _reset(4)
        real_enqueue = jobs.enqueue

        def boom(func, *args, **kwargs):
            raise RuntimeError('queue down')

        jobs.enqueue = boom
        try:
            with self.assertRaises(jobs._LaunchFailed) as cm:
                jobs._launch_throttled(['t0', 't1'], deep=True)
        finally:
            jobs.enqueue = real_enqueue
        self.assertIsInstance(cm.exception.cause, RuntimeError)
        self.assertEqual(cm.exception.launched, [])
        self.assertEqual(cm.exception.unlaunched, ['t0', 't1'])

    def test_status_read_failure_fails_the_parent(self):
        # a should_stop() that RAISES is an infrastructure failure, not a
        # cancellation: the parent row must end in FAILURE and every claim
        # that never reached the queue must be cleaned up
        _reset(4)
        real_state = jobs._task_state

        def boom(task_id):
            raise RuntimeError('db down')

        jobs._task_state = boom
        try:
            with self.assertRaises(RuntimeError):
                jobs.bulk_analyze_job(select='non_clean', deep=True,
                                      task_id='p8')
        finally:
            jobs._task_state = real_state
        self.assertEqual(TASK_ROWS['p8'], 'FAIL')
        self.assertEqual(ENQUEUED, [])
        for i in range(4):
            self.assertFalse(RESULT_ROWS['t%d' % i]['deep_pending'])
            self.assertIsNone(RESULT_ROWS['t%d' % i]['deep_claim_id'])


class TestClaimModel(unittest.TestCase):
    def test_claim_ids_are_distinct_per_track(self):
        _reset(3)
        claims = jobs._new_deep_claims(item_ids=['t0', 't1', 't2'])
        self.assertEqual(len(claims), 3)
        claim_ids = [c for _i, c in claims]
        self.assertEqual(len(set(claim_ids)), 3)
        for item_id, claim_id in claims:
            row = RESULT_ROWS[item_id]
            self.assertEqual(row['deep_claim_id'], claim_id)
            self.assertEqual(row['deep_claim_state'], 'claimed')
            self.assertEqual(row['deep_queue_backend'], 'postgres')
            self.assertIsNone(row['deep_task_id'])

    def test_attach_stores_task_and_does_not_regress_running(self):
        _reset(2)
        claim0 = _tag('t0', state='claimed')
        claim1 = _tag('t1', state='running')
        jobs._attach_deep_task('t0', claim0, QueuedPluginTask('q9'))
        self.assertEqual(RESULT_ROWS['t0']['deep_task_id'], 'q9')
        self.assertEqual(RESULT_ROWS['t0']['deep_claim_state'], 'launched')
        # a fast child already running keeps its state
        jobs._attach_deep_task('t1', claim1, QueuedPluginTask('q10'))
        self.assertEqual(RESULT_ROWS['t1']['deep_task_id'], 'q10')
        self.assertEqual(RESULT_ROWS['t1']['deep_claim_state'], 'running')
        # a stale attach (wrong claim id) changes nothing
        jobs._attach_deep_task('t0', 'claim-other', QueuedPluginTask('q11'))
        self.assertEqual(RESULT_ROWS['t0']['deep_task_id'], 'q9')

    def test_completion_is_claim_fenced(self):
        _reset(2)
        claim = _tag('t0', state='running', task_id='q1')
        jobs._deep_claim_done('t0', claim)
        self.assertFalse(RESULT_ROWS['t0']['deep_pending'])
        self.assertIsNone(RESULT_ROWS['t0']['deep_claim_id'])
        # a stale completion (wrong claim id) cannot clear the row
        claim2 = _tag('t1', state='running', task_id='q2')
        jobs._deep_claim_done('t1', 'claim-stale')
        self.assertTrue(RESULT_ROWS['t1']['deep_pending'])
        self.assertEqual(RESULT_ROWS['t1']['deep_claim_id'], claim2)

    def test_completion_marker_is_durable_before_the_clear(self):
        _reset(1)
        claim = _tag('t0', state='running', task_id='q1')
        # the marker write is committed before the clear, so a crash between
        # the two commits leaves the marker behind for the reconciler
        real_execute = FakeCursor.execute

        def flaky_execute(self, sql, params=()):
            if 'SET deep_pending=FALSE' in sql:
                raise OSError('crash between commits')
            return real_execute(self, sql, params)

        FakeCursor.execute = flaky_execute
        try:
            with self.assertRaises(OSError):
                jobs._deep_claim_done('t0', claim)
        finally:
            FakeCursor.execute = real_execute
        row = RESULT_ROWS['t0']
        # the clear never landed, but the marker did
        self.assertTrue(row['deep_pending'])
        self.assertEqual(row['deep_claim_id'], claim)
        self.assertEqual(row['deep_completed_claim_id'], claim)
        # and the reconciler's gone-row branch clears it from the marker
        counts = jobs.reconcile_deep_pending_task()
        self.assertFalse(RESULT_ROWS['t0']['deep_pending'])
        self.assertEqual(counts['settled'], 1)

    def test_failure_keeps_pending_and_is_claim_fenced(self):
        _reset(2)
        claim = _tag('t0', state='running', task_id='q1')
        jobs._deep_claim_failed('t0', claim, RuntimeError('decode died'))
        row = RESULT_ROWS['t0']
        self.assertTrue(row['deep_pending'])
        self.assertEqual(row['deep_claim_state'], 'failed')
        self.assertIn('decode died', row['deep_claim_detail'])
        claim2 = _tag('t1', state='running', task_id='q2')
        jobs._deep_claim_failed('t1', 'claim-stale', RuntimeError('x'))
        self.assertEqual(RESULT_ROWS['t1']['deep_claim_state'], 'running')

    def test_restore_is_cas_on_the_new_claim(self):
        _reset(1)
        prior_since = DB_NOW - timedelta(hours=5)
        _tag('t0', state='failed', since=prior_since, claim_id='claim-old')
        claims, restore_map = jobs._requeue_claims('requeue_all')
        self.assertEqual(len(claims), 1)
        item_id, new_claim = claims[0]
        # a concurrent re-claim already replaced the claim: restore is a no-op
        RESULT_ROWS['t0']['deep_claim_id'] = 'claim-newer'
        self.assertFalse(jobs._restore_deep_claim(item_id, new_claim,
                                                  restore_map[item_id]))
        self.assertEqual(RESULT_ROWS['t0']['deep_claim_id'], 'claim-newer')
        # and it lands when the new claim still matches
        RESULT_ROWS['t0']['deep_claim_id'] = new_claim
        self.assertTrue(jobs._restore_deep_claim(item_id, new_claim,
                                                 restore_map[item_id]))
        row = RESULT_ROWS['t0']
        self.assertEqual(row['deep_claim_id'], 'claim-old')
        self.assertEqual(row['deep_claim_state'], 'failed')
        self.assertEqual(row['deep_pending_since'], prior_since)

    def test_restore_preserves_the_completion_marker(self):
        _reset(1)
        prior_since = DB_NOW - timedelta(hours=5)
        _tag('t0', state='failed', since=prior_since, claim_id='claim-old')
        # a partially-settled record still carries a completion marker
        RESULT_ROWS['t0']['deep_completed_claim_id'] = 'claim-old'
        RESULT_ROWS['t0']['deep_completed_at'] = DB_NOW - timedelta(hours=4)
        claims, restore_map = jobs._requeue_claims('requeue_all')
        item_id, new_claim = claims[0]
        # the fresh claim cleared the marker...
        self.assertIsNone(RESULT_ROWS['t0']['deep_completed_claim_id'])
        # ...and a failed-launch restore brings the whole prior record back
        self.assertTrue(jobs._restore_deep_claim(item_id, new_claim,
                                                 restore_map[item_id]))
        row = RESULT_ROWS['t0']
        self.assertEqual(row['deep_completed_claim_id'], 'claim-old')
        self.assertEqual(row['deep_completed_at'], DB_NOW - timedelta(hours=4))

    def test_requeue_all_selects_failed_and_legacy_only(self):
        _reset(4)
        _tag('t0', state='failed')
        _tag('t1', state='running', task_id='q1')
        _tag('t2', state='outcome_unknown', task_id='q2')
        # t3 is legacy: pending, no claim metadata at all
        RESULT_ROWS['t3']['deep_pending'] = True
        RESULT_ROWS['t3']['deep_pending_since'] = DB_NOW
        claims, restore_map = jobs._requeue_claims('requeue_all')
        self.assertEqual(sorted(i for i, _c in claims), ['t0', 't3'])
        for item_id, _c in claims:
            row = RESULT_ROWS[item_id]
            self.assertEqual(row['deep_claim_state'], 'claimed')
            self.assertIsNotNone(row['deep_claim_id'])

    def test_requeue_unknown_selects_taskless_and_unknown_only(self):
        _reset(4)
        _tag('t0', state='claimed')                 # task-less
        _tag('t1', state='outcome_unknown', task_id='q1')
        _tag('t2', state='failed', task_id='q2')
        _tag('t3', state='running', task_id='q3')
        claims, _restore = jobs._requeue_claims('requeue_unknown')
        self.assertEqual(sorted(i for i, _c in claims), ['t0', 't1'])


class TestAnalyzeTrackJob(unittest.TestCase):
    def setUp(self):
        _reset(2)
        self.real_analyze = jobs._analyze_track
        self.analyzed = []
        jobs._analyze_track = (
            lambda item_id, deep: self.analyzed.append((item_id, deep))
            or 'ok')

    def tearDown(self):
        jobs._analyze_track = self.real_analyze

    def test_matching_claim_runs_and_completes(self):
        claim = _tag('t0', state='claimed', task_id='q1')
        jobs.analyze_track_job('t0', deep=True, claim_id=claim)
        self.assertEqual(self.analyzed, [('t0', True)])
        self.assertFalse(RESULT_ROWS['t0']['deep_pending'])
        self.assertIsNone(RESULT_ROWS['t0']['deep_claim_id'])

    def test_stale_child_skips_analysis(self):
        _tag('t0', state='running', task_id='q1', claim_id='claim-new')
        jobs.analyze_track_job('t0', deep=True, claim_id='claim-old')
        self.assertEqual(self.analyzed, [])
        self.assertTrue(RESULT_ROWS['t0']['deep_pending'])

    def test_reclaim_redelivery_reenters_a_running_claim(self):
        # the core's worker-loss reclaim redelivers the same queue row with
        # the same claim id; the replacement must run the analysis
        claim = _tag('t0', state='running', task_id='q1')
        jobs.analyze_track_job('t0', deep=True, claim_id=claim)
        self.assertEqual(self.analyzed, [('t0', True)])
        self.assertFalse(RESULT_ROWS['t0']['deep_pending'])

    def test_failure_marks_claim_failed_and_reraises(self):
        claim = _tag('t0', state='claimed', task_id='q1')
        jobs._analyze_track = lambda item_id, deep: (_ for _ in ()).throw(
            RuntimeError('decode died'))
        with self.assertRaises(RuntimeError):
            jobs.analyze_track_job('t0', deep=True, claim_id=claim)
        row = RESULT_ROWS['t0']
        self.assertTrue(row['deep_pending'])
        self.assertEqual(row['deep_claim_state'], 'failed')

    def test_failure_state_write_failure_propagates_chained(self):
        # if the failure-state write itself fails, it propagates (chained to
        # the original analysis error) so the task is not recorded successful
        claim = _tag('t0', state='claimed', task_id='q1')
        jobs._analyze_track = lambda item_id, deep: (_ for _ in ()).throw(
            RuntimeError('decode died'))
        real_failed = jobs._deep_claim_failed

        def boom(item_id, claim_id, exc):
            raise OSError('db down')

        jobs._deep_claim_failed = boom
        try:
            with self.assertRaises(OSError) as cm:
                jobs.analyze_track_job('t0', deep=True, claim_id=claim)
        finally:
            jobs._deep_claim_failed = real_failed
        self.assertIsInstance(cm.exception.__cause__, RuntimeError)

    def test_plain_reanalysis_clears_only_legacy_pending(self):
        # a legacy (pre-claim) pending row is cleared by a plain re-analysis
        RESULT_ROWS['t0']['deep_pending'] = True
        RESULT_ROWS['t0']['deep_pending_since'] = DB_NOW
        # and an owned claim is left alone
        _tag('t1', state='claimed')
        jobs.analyze_track_job('t0', deep=False)
        jobs.analyze_track_job('t1', deep=False)
        self.assertFalse(RESULT_ROWS['t0']['deep_pending'])
        self.assertTrue(RESULT_ROWS['t1']['deep_pending'])

    def test_preupgrade_deep_child_clears_only_legacy_pending(self):
        # a deep child queued by a pre-claim version carries no claim id
        RESULT_ROWS['t0']['deep_pending'] = True
        RESULT_ROWS['t0']['deep_pending_since'] = DB_NOW
        _tag('t1', state='claimed')
        jobs.analyze_track_job('t0', deep=True)
        self.assertEqual(self.analyzed, [('t0', True)])
        self.assertFalse(RESULT_ROWS['t0']['deep_pending'])
        self.assertTrue(RESULT_ROWS['t1']['deep_pending'])


class TestBulkAnalyzeJob(unittest.TestCase):
    def test_non_clean_claims_tags_and_fans_out(self):
        _reset(5)
        RESULT_ROWS['t4']['verdict'] = 'CLEAN'   # skipped: already clean
        RESULT_ROWS['t3']['verified'] = True     # skipped: manually verified
        result = jobs.bulk_analyze_job(select='non_clean', deep=True,
                                       task_id='parent')
        self.assertEqual(result['tracks_total'], 3)
        self.assertEqual(result['tracks_launched'], 3)
        self.assertEqual(TASK_ROWS['parent'], 'SUCCESS')
        # children share the default queue so every worker can take one
        self.assertEqual({e['queue'] for e in ENQUEUED}, {'default'})
        self.assertTrue(all(e['kwargs'].get('deep') for e in ENQUEUED))
        # every launched child carries its claim, and the claim is attached
        for e in ENQUEUED:
            item_id = e['args'][0]
            self.assertEqual(e['kwargs'].get('claim_id'),
                             RESULT_ROWS[item_id]['deep_claim_id'])
            self.assertEqual(RESULT_ROWS[item_id]['deep_task_id'],
                             e['task_id'])
        self.assertFalse(RESULT_ROWS['t4']['deep_pending'])
        self.assertFalse(RESULT_ROWS['t3']['deep_pending'])

    def test_claiming_is_idempotent(self):
        _reset(5)
        jobs.bulk_analyze_job(select='non_clean', deep=True, task_id='p1')
        del ENQUEUED[:]
        again = jobs.bulk_analyze_job(select='non_clean', deep=True, task_id='p2')
        self.assertEqual(again['tracks_total'], 0)
        self.assertEqual(ENQUEUED, [])

    def test_cancelling_untags_only_what_it_never_launched(self):
        _reset(6)
        TASK_ROWS['p3'] = 'NEW'
        writes = {'n': 0}
        real_save = jobs.save_task_status

        def revoke_mid_launch(task_id, task_type, status, **kwargs):
            if task_id == 'p3':
                writes['n'] += 1
                if writes['n'] > 3:   # let a few launches happen, then cancel
                    TASK_ROWS['p3'] = 'REVOKED'
                    return
            real_save(task_id, task_type, status, **kwargs)

        jobs.save_task_status = revoke_mid_launch
        try:
            jobs.bulk_analyze_job(select='non_clean', deep=True, task_id='p3')
        finally:
            jobs.save_task_status = real_save

        launched = [e['args'][0] for e in ENQUEUED]
        still_tagged = sorted(i for i, r in RESULT_ROWS.items() if r['deep_pending'])
        self.assertLess(len(launched), 6)          # it really did stop early
        self.assertEqual(still_tagged, sorted(launched))
        # the unlaunched claims are gone entirely, the launched ones intact
        for item_id in launched:
            self.assertIsNotNone(RESULT_ROWS[item_id]['deep_claim_id'])

    def test_enqueue_failure_fails_parent_and_clears_unlaunched(self):
        _reset(4)
        real_enqueue = jobs.enqueue

        def flaky(func, *args, **kwargs):
            if len(ENQUEUED) == 1:   # the second launch blows up
                raise RuntimeError('queue down')
            return real_enqueue(func, *args, **kwargs)

        jobs.enqueue = flaky
        try:
            with self.assertRaises(RuntimeError):
                jobs.bulk_analyze_job(select='non_clean', deep=True,
                                      task_id='p6')
        finally:
            jobs.enqueue = real_enqueue
        self.assertEqual(TASK_ROWS['p6'], 'FAIL')
        launched = [e['args'][0] for e in ENQUEUED]
        self.assertEqual(launched, ['t0'])
        # the launched claim stays pending; the never-launched ones are cleared
        self.assertTrue(RESULT_ROWS['t0']['deep_pending'])
        for i in (1, 2, 3):
            self.assertFalse(RESULT_ROWS['t%d' % i]['deep_pending'])
            self.assertIsNone(RESULT_ROWS['t%d' % i]['deep_claim_id'])

    def test_requeue_all_partial_failure_restores_unlaunched(self):
        _reset(3)
        prior_since = DB_NOW - timedelta(hours=5)
        for i in range(3):
            _tag('t%d' % i, state='failed', since=prior_since,
                 claim_id='claim-old-%d' % i)
        real_enqueue = jobs.enqueue

        def flaky(func, *args, **kwargs):
            if len(ENQUEUED) == 1:
                raise RuntimeError('queue down')
            return real_enqueue(func, *args, **kwargs)

        jobs.enqueue = flaky
        try:
            with self.assertRaises(RuntimeError):
                jobs.bulk_analyze_job(select='requeue_all', deep=True,
                                      task_id='p7')
        finally:
            jobs.enqueue = real_enqueue
        self.assertEqual(TASK_ROWS['p7'], 'FAIL')
        # the launched row keeps its fresh claim and bumped timestamp
        self.assertEqual(RESULT_ROWS['t0']['deep_claim_state'], 'launched')
        self.assertEqual(RESULT_ROWS['t0']['deep_pending_since'], DB_NOW)
        # the unlaunched rows are restored to their exact prior record
        for i in (1, 2):
            row = RESULT_ROWS['t%d' % i]
            self.assertEqual(row['deep_claim_id'], 'claim-old-%d' % i)
            self.assertEqual(row['deep_claim_state'], 'failed')
            self.assertEqual(row['deep_pending_since'], prior_since)

    def test_plain_bulk_uses_ids_verbatim_and_tags_nothing(self):
        _reset(4)
        result = jobs.bulk_analyze_job(item_ids=['t0', 't1'], deep=False,
                                       task_id='p4')
        self.assertEqual(result['tracks_total'], 2)
        self.assertEqual([i for i, r in RESULT_ROWS.items() if r['deep_pending']], [])
        self.assertFalse(any(e['kwargs'].get('deep') for e in ENQUEUED))

    def test_nothing_to_do_succeeds_without_queueing(self):
        _reset(3, verdict='CLEAN')
        result = jobs.bulk_analyze_job(select='non_clean', deep=True, task_id='p5')
        self.assertEqual(result['tracks_total'], 0)
        self.assertEqual(TASK_ROWS['p5'], 'SUCCESS')
        self.assertEqual(ENQUEUED, [])


class TestReconciler(unittest.TestCase):
    def test_terminal_queue_rows_settle_the_claim(self):
        _reset(4)
        c0 = _tag('t0', state='running', task_id='q0')
        c1 = _tag('t1', state='launched', task_id='q1')
        c2 = _tag('t2', state='running', task_id='q2')
        c3 = _tag('t3', state='running', task_id='q3')
        TASK_ROWS.update({'q0': 'SUCCESS', 'q1': 'REVOKED', 'q2': 'FAIL',
                          'q3': 'RUNNING'})
        counts = jobs.reconcile_deep_pending_task()
        self.assertFalse(RESULT_ROWS['t0']['deep_pending'])
        self.assertFalse(RESULT_ROWS['t1']['deep_pending'])
        row2 = RESULT_ROWS['t2']
        self.assertTrue(row2['deep_pending'])
        self.assertEqual(row2['deep_claim_state'], 'failed')
        self.assertTrue(RESULT_ROWS['t3']['deep_pending'])
        self.assertEqual(RESULT_ROWS['t3']['deep_claim_state'], 'running')
        self.assertEqual(counts['settled'], 3)
        self.assertEqual(counts['live'], 1)
        # and nothing was ever enqueued by the reconciler
        self.assertEqual(ENQUEUED, [])

    def test_gone_row_with_completion_marker_clears(self):
        _reset(1)
        claim = _tag('t0', state='running', task_id='qgone',
                     marker='x')  # marker set below to the real claim
        RESULT_ROWS['t0']['deep_completed_claim_id'] = claim
        counts = jobs.reconcile_deep_pending_task()
        self.assertFalse(RESULT_ROWS['t0']['deep_pending'])
        self.assertEqual(counts['settled'], 1)

    def test_gone_row_without_marker_becomes_unknown_past_grace(self):
        _reset(1)
        _tag('t0', state='running', task_id='qgone',
             since=DB_NOW - timedelta(hours=3))   # past the 2h grace
        counts = jobs.reconcile_deep_pending_task()
        row = RESULT_ROWS['t0']
        self.assertTrue(row['deep_pending'])
        self.assertEqual(row['deep_claim_state'], 'outcome_unknown')
        self.assertEqual(counts['unknown'], 1)

    def test_gone_row_within_grace_stays_live(self):
        _reset(1)
        _tag('t0', state='running', task_id='qgone',
             since=DB_NOW - timedelta(hours=1))   # inside the 2h grace
        counts = jobs.reconcile_deep_pending_task()
        self.assertTrue(RESULT_ROWS['t0']['deep_pending'])
        self.assertEqual(RESULT_ROWS['t0']['deep_claim_state'], 'running')
        self.assertEqual(counts['live'], 1)

    def test_taskless_claim_becomes_unknown_past_grace(self):
        _reset(1)
        _tag('t0', state='claimed', since=DB_NOW - timedelta(hours=3))
        counts = jobs.reconcile_deep_pending_task()
        self.assertEqual(RESULT_ROWS['t0']['deep_claim_state'], 'outcome_unknown')
        self.assertEqual(counts['unknown'], 1)

    def test_unreadable_queue_state_makes_no_mutation(self):
        _reset(1)
        _tag('t0', state='running', task_id='q0',
             since=DB_NOW - timedelta(hours=9))
        real_states = jobs._queue_states

        def unreadable(task_ids):
            return None

        jobs._queue_states = unreadable
        try:
            counts = jobs.reconcile_deep_pending_task()
        finally:
            jobs._queue_states = real_states
        self.assertEqual(counts['unreadable'], 1)
        self.assertEqual(RESULT_ROWS['t0']['deep_claim_state'], 'running')
        self.assertTrue(RESULT_ROWS['t0']['deep_pending'])

    def test_legacy_rows_are_counted_but_untouched(self):
        _reset(1)
        RESULT_ROWS['t0']['deep_pending'] = True
        RESULT_ROWS['t0']['deep_pending_since'] = DB_NOW - timedelta(hours=9)
        counts = jobs.reconcile_deep_pending_task()
        self.assertEqual(counts['legacy'], 1)
        self.assertTrue(RESULT_ROWS['t0']['deep_pending'])
        self.assertIsNone(RESULT_ROWS['t0']['deep_claim_id'])

class TestRqQuarantine(unittest.TestCase):
    """Claims recorded against the legacy RQ queue are never read back.

    The reconciler quarantines them as outcome_unknown instead: the queue
    behind them no longer exists, so no outcome can be confirmed. The fence on
    the backend and the claim id keeps any other claim out of reach.
    """

    def setUp(self):
        _reset(6)

    def test_live_claim_states_are_quarantined(self):
        for i, state in enumerate([None, 'claimed', 'launched', 'running',
                                   'failed']):
            _tag('t%d' % i, state=state, task_id='rq%d' % i, backend='rq')
        counts = jobs.reconcile_deep_pending_task()
        self.assertEqual(counts['unknown'], 5)
        for i in range(5):
            row = RESULT_ROWS['t%d' % i]
            self.assertTrue(row['deep_pending'])
            self.assertEqual(row['deep_claim_state'], 'outcome_unknown')
            self.assertIn('outcome_unknown', row['deep_claim_detail'])
        self.assertEqual(ENQUEUED, [])
        sql, params = [(q, p) for q, p in SQL_LOG
                       if 'IS NOT DISTINCT FROM' in q][-1]
        self.assertIn('deep_queue_backend=%s', sql)
        self.assertIn("('claimed', 'launched', 'running', 'failed')", sql)
        self.assertIn('deep_claim_state IS NULL', sql)
        self.assertEqual(params[2], 'rq')

    def test_already_unknown_row_keeps_its_original_detail(self):
        _tag('t0', state='outcome_unknown', task_id='rq0', backend='rq')
        RESULT_ROWS['t0']['deep_claim_detail'] = 'original detail'
        counts = jobs.reconcile_deep_pending_task()
        self.assertEqual(counts['unknown'], 0)
        self.assertEqual(RESULT_ROWS['t0']['deep_claim_detail'], 'original detail')

    def test_cleared_rq_row_is_not_selected(self):
        _tag('t0', state='running', task_id='rq0', backend='rq')
        RESULT_ROWS['t0']['deep_pending'] = False
        counts = jobs.reconcile_deep_pending_task()
        self.assertEqual(counts['unknown'], 0)
        self.assertEqual(RESULT_ROWS['t0']['deep_claim_state'], 'running')

    def test_claimless_rq_row_is_quarantined(self):
        _tag('t0', state='claimed', claim_id=None, backend='rq')
        counts = jobs.reconcile_deep_pending_task()
        self.assertEqual(counts['unknown'], 1)
        self.assertEqual(RESULT_ROWS['t0']['deep_claim_state'], 'outcome_unknown')

    def test_taskless_rq_row_is_quarantined_whatever_its_age(self):
        # a fresh claim, well inside the warning grace: still no outcome to wait for
        _tag('t0', state='claimed', backend='rq', since=DB_NOW)
        counts = jobs.reconcile_deep_pending_task()
        self.assertEqual(counts['unknown'], 1)
        self.assertEqual(RESULT_ROWS['t0']['deep_claim_state'], 'outcome_unknown')

    def test_completion_marker_does_not_protect_an_rq_row(self):
        # a deliberate override: the marker would clear a postgres claim, but
        # for RQ nothing can prove the marker is the job's own last word
        claim = _tag('t0', state='running', task_id='rq0', backend='rq')
        RESULT_ROWS['t0']['deep_completed_claim_id'] = claim
        counts = jobs.reconcile_deep_pending_task()
        self.assertEqual(counts['unknown'], 1)
        row = RESULT_ROWS['t0']
        self.assertTrue(row['deep_pending'])
        self.assertEqual(row['deep_claim_state'], 'outcome_unknown')
        self.assertEqual(row['deep_completed_claim_id'], claim)

    def test_ineligible_rq_row_is_still_quarantined(self):
        RESULT_ROWS['t0']['deep_eligible'] = False
        _tag('t0', state='running', task_id='rq0', backend='rq')
        counts = jobs.reconcile_deep_pending_task()
        self.assertEqual(counts['unknown'], 1)
        self.assertEqual(RESULT_ROWS['t0']['deep_claim_state'], 'outcome_unknown')

    def test_fresh_postgres_claim_is_never_touched(self):
        # the same item was re-claimed on postgres: the RQ fence must not match
        claim = _tag('t0', state='claimed', task_id='q0', backend='postgres')
        TASK_ROWS['q0'] = 'NEW'
        self.assertFalse(jobs._quarantine_legacy_backend('t0', claim, 'rq'))
        # and a row whose claim id moved on is not matched either
        _tag('t1', state='claimed', task_id='rq1', backend='rq',
             claim_id='claim-a')
        self.assertFalse(jobs._quarantine_legacy_backend('t1', 'claim-b', 'rq'))
        row = RESULT_ROWS['t0']
        self.assertEqual(row['deep_claim_state'], 'claimed')
        self.assertEqual(row['deep_queue_backend'], 'postgres')
        self.assertEqual(RESULT_ROWS['t1']['deep_claim_state'], 'claimed')

    def test_quarantine_sql_carries_every_fence(self):
        claim = _tag('t0', state='running', task_id='rq0', backend='rq')
        jobs._quarantine_legacy_backend('t0', claim, 'rq')
        sql, params = [(q, p) for q, p in SQL_LOG
                       if 'IS NOT DISTINCT FROM' in q][-1]
        for fragment in ('deep_pending', 'deep_queue_backend=%s',
                         'deep_claim_id IS NOT DISTINCT FROM %s',
                         'deep_claim_state IS NULL',
                         "('claimed', 'launched', 'running', 'failed')"):
            self.assertIn(fragment, sql)
        self.assertEqual(params[1:], ('t0', 'rq', claim))

    def test_quarantine_reports_whether_it_did_the_write(self):
        claim = _tag('t0', state='failed', task_id='rq0', backend='rq')
        self.assertTrue(jobs._quarantine_legacy_backend('t0', claim, 'rq'))
        self.assertFalse(jobs._quarantine_legacy_backend('t0', claim, 'rq'))

    def test_null_backend_is_a_postgres_claim(self):
        # rows from before the backend column are postgres: they are settled
        # from the queue, not quarantined
        _tag('t0', state='running', task_id='q0', backend=None)
        TASK_ROWS['q0'] = 'SUCCESS'
        counts = jobs.reconcile_deep_pending_task()
        self.assertEqual(counts['settled'], 1)
        self.assertFalse(RESULT_ROWS['t0']['deep_pending'])


class TestQuarantineRollback(unittest.TestCase):
    """What the fake can show about a failing quarantine write: the helper asks
    the connection to roll back, closes its cursor, and returns False, and the
    next call runs normally on the same connection object. The fake does not
    model an aborted transaction, so this is not evidence of Postgres recovery."""

    def setUp(self):
        _reset(1)
        self.conn = FakeDB()
        self.real_get_db = jobs.get_db
        self.real_execute = FakeCursor.execute
        jobs.get_db = lambda: self.conn

    def tearDown(self):
        jobs.get_db = self.real_get_db
        FakeCursor.execute = self.real_execute

    def test_failed_write_rolls_back_closes_its_cursor_then_the_next_call_runs(self):
        claim = _tag('t0', state='running', task_id='rq0', backend='rq')
        real_execute = self.real_execute
        failed = []

        def flaky_execute(cursor, sql, params=()):
            if 'outcome_unknown' in sql and not failed:
                failed.append(cursor)
                raise OSError('write failed')
            return real_execute(cursor, sql, params)

        FakeCursor.execute = flaky_execute
        self.assertFalse(jobs._quarantine_legacy_backend('t0', claim, 'rq'))
        self.assertEqual(self.conn.rollbacks, 1)
        self.assertIn(failed[0], CLOSED_CURSORS)
        self.assertEqual(RESULT_ROWS['t0']['deep_claim_state'], 'running')
        self.assertTrue(jobs._quarantine_legacy_backend('t0', claim, 'rq'))
        self.assertEqual(RESULT_ROWS['t0']['deep_claim_state'], 'outcome_unknown')


class TestQuarantinedClaimless(unittest.TestCase):
    """A claimless row quarantined as outcome_unknown is reachable only through
    the confirmed requeue_unknown select; a genuine legacy row is unchanged."""

    def setUp(self):
        _reset(2)

    def test_quarantined_claimless_row_is_not_picked_by_requeue_all(self):
        _tag('t0', state='claimed', claim_id=None, backend='rq')
        self.assertTrue(jobs._quarantine_legacy_backend('t0', None, 'rq'))
        claims, _restore = jobs._requeue_claims('requeue_all')
        self.assertEqual(claims, [])
        sql = [q for q, _p in SQL_LOG if 'FOR UPDATE' in q][-1]
        self.assertIn("deep_claim_state IS DISTINCT FROM 'outcome_unknown'", sql)

    def test_quarantined_claimless_row_is_picked_by_requeue_unknown(self):
        _tag('t0', state='claimed', claim_id=None, backend='rq')
        jobs._quarantine_legacy_backend('t0', None, 'rq')
        claims, _restore = jobs._requeue_claims('requeue_unknown')
        self.assertEqual([i for i, _c in claims], ['t0'])

    def test_quarantined_claimless_row_survives_a_plain_reanalysis(self):
        _tag('t0', state='claimed', claim_id=None, backend='rq')
        jobs._quarantine_legacy_backend('t0', None, 'rq')
        real_analyze = jobs._analyze_track
        jobs._analyze_track = lambda item_id, deep: 'ok'
        try:
            jobs.analyze_track_job('t0', deep=False)
        finally:
            jobs._analyze_track = real_analyze
        self.assertTrue(RESULT_ROWS['t0']['deep_pending'])
        self.assertEqual(RESULT_ROWS['t0']['deep_claim_state'], 'outcome_unknown')

    def test_genuine_legacy_claimless_row_is_still_requeued_and_cleared(self):
        RESULT_ROWS['t0']['deep_pending'] = True
        RESULT_ROWS['t0']['deep_pending_since'] = DB_NOW
        claims, _restore = jobs._requeue_claims('requeue_all')
        self.assertEqual([i for i, _c in claims], ['t0'])
        RESULT_ROWS['t1']['deep_pending'] = True
        RESULT_ROWS['t1']['deep_pending_since'] = DB_NOW
        real_analyze = jobs._analyze_track
        jobs._analyze_track = lambda item_id, deep: 'ok'
        try:
            jobs.analyze_track_job('t1', deep=False)
        finally:
            jobs._analyze_track = real_analyze
        self.assertFalse(RESULT_ROWS['t1']['deep_pending'])


class TestNativeIdTranslation(unittest.TestCase):
    def setUp(self):
        _reset(1)

    def test_default_server_maps_ids_to_themselves(self):
        self.assertEqual(jobs._native_to_fp(['n1', 'n2']), {'n1': 'n1', 'n2': 'n2'})
        self.assertEqual(jobs._fp_to_native(['n1']), {'n1': 'n1'})

    def test_default_server_reverse_is_identity_forward_drops_canonical_ids(self):
        self.assertEqual(jobs._native_to_fp(['n1', 'fp_x']),
                         {'n1': 'n1', 'fp_x': 'fp_x'})
        self.assertEqual(jobs._fp_to_native(['n1']), {'n1': 'n1'})
        self.assertEqual(jobs._fp_to_native(['fp_x']), {})

    def test_secondary_miss_is_absent_not_identity_keyed(self):
        CORE['secondary'] = {'n1': 'canon1'}
        self.assertEqual(jobs._native_to_fp(['n1', 'n2'], 'srv2'),
                         {'n1': 'canon1'})
        self.assertEqual(jobs._fp_to_native(['canon1', 'canon2'], 'srv2'),
                         {'canon1': 'n1'})

    def test_registry_failure_raises_in_both_directions(self):
        CORE['translate_fail'] = True
        with self.assertRaises(RuntimeError):
            jobs._native_to_fp(['n1'], 'srv2')
        with self.assertRaises(RuntimeError):
            jobs._fp_to_native(['canon1'], 'srv2')

    def test_unmapped_native_id_gives_no_item_id(self):
        track = {'Id': 'n2', 'Name': 'Lost'}
        info = jobs._info_from_track(track, None, {'n1': 'canon1'}, 'srv2')
        self.assertIsNone(info['item_id'])
        self.assertEqual(info['provider_track_id'], 'n2')

    def test_mapped_native_id_gives_canonical_item_id(self):
        track = {'Id': 'n1', 'Name': 'Found'}
        info = jobs._info_from_track(track, None, {'n1': 'canon1'}, 'srv2')
        self.assertEqual(info['item_id'], 'canon1')


class TestScanAlbumUnmapped(unittest.TestCase):
    def setUp(self):
        _reset(1)
        SCORE_IDS.clear()
        MEDIA['tracks'] = {'alb1': [{'Id': 'n1', 'Name': 'Mapped'},
                                    {'Id': 'n2', 'Name': 'Unmapped'}]}
        CORE['secondary'] = {'n1': 'canon1'}

    def test_unmapped_track_is_counted_and_skipped(self):
        counters = jobs.scan_album_job('alb1', 'One', mode='changed',
                                       task_id='c1', server_id='srv2')
        self.assertEqual(counters['unmapped'], 1)
        # the mapped track is simply not analyzed yet (not in score)
        self.assertEqual(counters['not_in_score'], 1)
        self.assertEqual(counters['error'], 0)
        self.assertEqual(TASK_DETAILS['c1']['unmapped'], 1)
        self.assertEqual(TASK_ROWS['c1'], 'SUCCESS')
        self.assertEqual(CORE['bound'], ['srv2'])
        self.assertEqual(DOWNLOADS, [])

    def test_track_without_an_id_is_skipped_silently(self):
        MEDIA['tracks']['alb1'] = [{'Name': 'No id'}]
        counters = jobs.scan_album_job('alb1', 'One', mode='changed',
                                       task_id='c3', server_id='srv2')
        self.assertEqual(counters['unmapped'], 0)
        self.assertEqual(counters['not_in_score'], 0)

    def test_registry_failure_fails_the_album(self):
        CORE['translate_fail'] = True
        with self.assertRaises(RuntimeError):
            jobs.scan_album_job('alb1', 'One', mode='changed', task_id='c2',
                                server_id='srv2')
        self.assertEqual(TASK_ROWS['c2'], 'FAIL')


class TestScanLibraryUnmapped(unittest.TestCase):
    def setUp(self):
        _reset(1)
        SCORE_IDS.clear()
        CORE['servers'] = ['srv2']
        CORE['secondary'] = {'n1': 'canon1'}
        MEDIA['albums'] = [{'Id': 'alb1', 'Name': 'One'}]
        MEDIA['tracks'] = {'alb1': [{'Id': 'n1', 'Name': 'Mapped'},
                                    {'Id': 'n2', 'Name': 'Unmapped'}]}

    def test_settled_album_counts_unmapped_once_on_the_parent(self):
        # nothing in the album is scored, so the parent settles it itself
        result = jobs.scan_library_job(mode='changed', task_id='L1',
                                       all_servers=True)
        self.assertEqual(result['unmapped'], 1)
        self.assertEqual(result['not_in_score'], 1)
        self.assertEqual(result['albums_launched'], 0)
        self.assertEqual(ENQUEUED, [])
        self.assertEqual(TASK_ROWS['L1'], 'SUCCESS')

    def test_launched_album_counts_unmapped_only_in_its_child(self):
        # the mapped track is scored and changed, so the album gets a child;
        # the child reports the unmapped track, the parent must not add it too
        SCORE_IDS.add('canon1')
        real_enqueue = jobs.enqueue

        def finish_child(func, *args, **kwargs):
            handle = _fake_enqueue(func, *args, **kwargs)
            _fake_save_task_status(kwargs['task_id'], 'child', 'SUCCESS',
                                   details={'unmapped': 1})
            return handle

        jobs.enqueue = finish_child
        try:
            result = jobs.scan_library_job(mode='changed', task_id='L2',
                                           all_servers=True)
        finally:
            jobs.enqueue = real_enqueue
        self.assertEqual(result['albums_launched'], 1)
        self.assertEqual(result['unmapped'], 1)

    def test_track_without_an_id_is_not_counted_as_unmapped(self):
        MEDIA['tracks']['alb1'] = [{'Name': 'No id'}]
        result = jobs.scan_library_job(mode='changed', task_id='L4',
                                       all_servers=True)
        self.assertEqual(result['unmapped'], 0)
        self.assertEqual(result['not_in_score'], 0)

    def test_translation_failure_skips_each_album_and_continues(self):
        MEDIA['albums'].append({'Id': 'alb2', 'Name': 'Two'})
        MEDIA['tracks']['alb2'] = [{'Id': 'n3', 'Name': 'Three'}]
        CORE['translate_fail'] = True
        result = jobs.scan_library_job(mode='changed', task_id='L3',
                                       all_servers=True)
        self.assertEqual(result['error'], 2)
        self.assertEqual(result['albums_no_work'], 2)
        self.assertEqual(ENQUEUED, [])
        self.assertEqual(TASK_ROWS['L3'], 'SUCCESS')


class TestAnalyzeTrackResolution(unittest.TestCase):
    def setUp(self):
        _reset(1)
        RESULT_ROWS['t0']['server_id'] = 'srv2'

    def test_unresolvable_native_id_raises_before_any_download(self):
        with self.assertRaises(RuntimeError):
            jobs._analyze_track('t0', deep=False)
        self.assertEqual(DOWNLOADS, [])

    def test_stored_provider_id_is_used_as_is(self):
        RESULT_ROWS['t0']['provider_track_id'] = 'n0'
        CORE['translate_fail'] = True   # must not be consulted at all
        jobs._analyze_track('t0', deep=False)
        self.assertEqual([d[0] for d in DOWNLOADS], ['n0'])

    def test_missing_provider_id_is_translated_on_its_server(self):
        CORE['secondary'] = {'n0': 't0'}
        jobs._analyze_track('t0', deep=False)
        self.assertEqual([d[0] for d in DOWNLOADS], ['n0'])
        self.assertEqual(CORE['bound'], ['srv2'])

    def test_info_item_id_is_the_stored_item_id(self):
        CORE['secondary'] = {'n0': 't0'}
        captured = []
        real_download = jobs._analyze_download
        jobs._analyze_download = (
            lambda track, info, *args, **kwargs: captured.append(info) or 'error')
        try:
            jobs._analyze_track('t0', deep=False)
        finally:
            jobs._analyze_download = real_download
        self.assertEqual(captured[0]['item_id'], 't0')
        self.assertEqual(captured[0]['provider_track_id'], 'n0')

    def test_unmapped_canonical_id_on_default_is_never_downloaded(self):
        RESULT_ROWS['fp_x'] = dict(RESULT_ROWS['t0'], provider_track_id=None,
                                   server_id=None)
        with self.assertRaises(RuntimeError):
            jobs._analyze_track('fp_x', deep=False)
        self.assertEqual(DOWNLOADS, [])

    def test_album_lookup_matches_only_the_native_id(self):
        RESULT_ROWS['t0']['album_id'] = 'alb9'
        CORE['secondary'] = {'n0': 't0'}
        MEDIA['tracks'] = {'alb9': [{'Id': 't0', 'Name': 'Canonical-looking'},
                                    {'Id': 'n0', 'Name': 'Native'}]}
        jobs._analyze_track('t0', deep=False)
        self.assertEqual([d[0] for d in DOWNLOADS], ['n0'])


class TestOnSongAnalyzedIds(unittest.TestCase):
    def setUp(self):
        _reset(1)
        fd, self.path = tempfile.mkstemp(suffix='.flac')
        os.write(fd, b'not really audio')
        os.close(fd)
        self.upserts = []
        self.real_upsert = jobs._upsert
        self.real_analyze = getattr(jobs.dsp, 'analyze_file', None)
        jobs._upsert = lambda item_id, info, result, meta_fp, md5: \
            self.upserts.append((item_id, info))
        jobs.dsp.analyze_file = lambda *a, **k: {}

    def tearDown(self):
        jobs._upsert = self.real_upsert
        if self.real_analyze is None:
            del jobs.dsp.analyze_file
        else:
            jobs.dsp.analyze_file = self.real_analyze
        os.remove(self.path)

    def _song(self, native, server_id=None, media_item=None):
        return {'item_id': native, 'audio_path': self.path,
                'server_id': server_id, 'metadata': {'title': 'T'},
                'media_item': ({'Id': native} if media_item is None
                               else media_item)}

    def test_core_catalog_id_is_used_as_is(self):
        jobs.on_song_analyzed(self._song(
            'n1', 'srv2', {'Id': 'n1', '_catalog_item_id': 'canon1'}))
        [(item_id, info)] = self.upserts
        self.assertEqual(item_id, 'canon1')
        self.assertEqual(info['server_id'], 'srv2')

    def test_catalog_id_without_any_server_is_still_used(self):
        jobs.on_song_analyzed(self._song(
            'n1', None, {'Id': 'n1', '_catalog_item_id': 'canon1'}))
        self.assertEqual([i for i, _info in self.upserts], ['canon1'])

    def test_known_default_server_maps_native_id_to_itself(self):
        jobs.on_song_analyzed(self._song('n1', 'srv1'))
        [(item_id, info)] = self.upserts
        self.assertEqual(item_id, 'n1')
        self.assertEqual(info['server_id'], 'srv1')

    def test_unknown_server_is_skipped_without_asking_the_registry(self):
        asked = []
        real_native_to_fp = jobs._native_to_fp

        def spy(native_ids, server_id=None):
            asked.append((list(native_ids), server_id))
            return real_native_to_fp(native_ids, server_id)

        jobs._native_to_fp = spy
        try:
            jobs.on_song_analyzed(self._song('n1', None))
        finally:
            jobs._native_to_fp = real_native_to_fp
        self.assertEqual(asked, [])
        self.assertEqual(self.upserts, [])

    def test_secondary_server_translates_through_the_registry(self):
        CORE['secondary'] = {'n1': 'canon1'}
        jobs.on_song_analyzed(self._song('n1', 'srv2'))
        [(item_id, info)] = self.upserts
        self.assertEqual(item_id, 'canon1')
        self.assertEqual(info['provider_track_id'], 'n1')

    def test_unmapped_secondary_track_is_skipped(self):
        CORE['secondary'] = {}
        jobs.on_song_analyzed(self._song('n1', 'srv2'))
        self.assertEqual(self.upserts, [])

    def test_active_server_fallback_is_what_gets_stored(self):
        CORE['active_server'] = 'srv2'
        CORE['secondary'] = {'n1': 'canon1'}
        jobs.on_song_analyzed(self._song('n1', None))
        [(item_id, info)] = self.upserts
        self.assertEqual(item_id, 'canon1')
        self.assertEqual(info['server_id'], 'srv2')
        self.assertEqual(info['provider_track_id'], 'n1')

    def test_registry_failure_skips_without_writing(self):
        CORE['translate_fail'] = True
        jobs.on_song_analyzed(self._song('n1', 'srv2'))
        self.assertEqual(self.upserts, [])

    def test_unmapped_canonical_looking_id_on_default_maps_to_itself(self):
        jobs.on_song_analyzed(self._song('fp_x', 'srv1'))
        [(item_id, info)] = self.upserts
        self.assertEqual(item_id, 'fp_x')
        self.assertEqual(info['server_id'], 'srv1')


class TestTaskIdContract(unittest.TestCase):
    """A job that owns a task_status row keys it by the queue's id for the run,
    or by an explicit id a parent passed. With neither it refuses to run:
    inventing a uuid would leave a row nothing polls."""

    def setUp(self):
        _reset(1)

    def test_scan_library_job_keys_by_the_queue_id(self):
        CORE['current_task'] = 'queue-7'
        jobs.scan_library_job(mode='changed')
        self.assertEqual(TASK_ROWS['queue-7'], 'SUCCESS')

    def test_explicit_id_wins_over_the_queue_id(self):
        CORE['current_task'] = 'queue-7'
        jobs.scan_library_job(mode='changed', task_id='explicit')
        self.assertEqual(TASK_ROWS['explicit'], 'SUCCESS')
        self.assertNotIn('queue-7', TASK_ROWS)

    def test_scan_library_job_without_any_id_refuses(self):
        with self.assertRaises(RuntimeError):
            jobs.scan_library_job(mode='changed')
        self.assertEqual(TASK_ROWS, {})

    def test_bulk_analyze_job_keys_by_the_queue_id(self):
        CORE['current_task'] = 'queue-b'
        jobs.bulk_analyze_job(item_ids=[], deep=False)
        self.assertEqual(TASK_ROWS['queue-b'], 'SUCCESS')

    def test_bulk_analyze_job_without_any_id_refuses(self):
        with self.assertRaises(RuntimeError):
            jobs.bulk_analyze_job(select='non_clean', deep=True)
        self.assertEqual(TASK_ROWS, {})

    def test_root_album_scan_keys_by_the_queue_id(self):
        CORE['current_task'] = 'queue-a'
        jobs.scan_album_job('alb1', 'One', mode='force')
        self.assertEqual(TASK_ROWS['queue-a'], 'SUCCESS')

    def test_root_album_scan_without_any_id_refuses(self):
        with self.assertRaises(RuntimeError):
            jobs.scan_album_job('alb1', 'One', mode='force')
        self.assertEqual(TASK_ROWS, {})

    def test_parent_launched_album_keeps_its_bookkeeping_id(self):
        CORE['current_task'] = 'queue-a'
        jobs.scan_album_job('alb1', 'One', mode='changed', task_id='child-1',
                            parent_task_id='parent-1')
        self.assertEqual(TASK_ROWS['child-1'], 'SUCCESS')
        self.assertEqual(TASK_PARENT['child-1'], 'parent-1')
        self.assertNotIn('queue-a', TASK_ROWS)


if __name__ == '__main__':
    unittest.main()
