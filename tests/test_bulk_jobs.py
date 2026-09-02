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
import importlib.util
import os
import sys
import types
import unittest
from datetime import datetime, timedelta

_HERE = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.join(os.path.dirname(_HERE), 'plugins', 'SpectrumAnalyzer')

MAX_IN_FLIGHT = 3

# a fixed "database now" so grace-period arithmetic is deterministic
DB_NOW = datetime(2026, 9, 2, 12, 0, 0)

TASK_ROWS = {}       # task_id -> status
RESULT_ROWS = {}     # item_id -> column dict
ENQUEUED = []        # every enqueue() the code under test made
RQ_JOBS = {}         # job_id -> RQ state ('missing'/'error' are special)
CLAIM_SEQ = [0]      # claim-id counter


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
        if s.startswith('SELECT task_id, status FROM task_status'):
            self.rows = [(t, TASK_ROWS[t]) for t in params[0] if t in TASK_ROWS]
        elif s.startswith('SELECT status FROM task_status'):
            self.rows = [(TASK_ROWS[params[0]],)] if params[0] in TASK_ROWS else []
        elif s == 'SELECT now()':
            self.rows = [(DB_NOW,)]
        elif 'COALESCE(deep_claimed_at, deep_pending_since)' in s:
            # the reconciler's pending-claim scan
            self.rows = [
                (item_id, row['deep_claim_id'], row['deep_task_id'],
                 row['deep_queue_backend'], row['deep_claim_state'],
                 row['deep_completed_claim_id'], row['deep_claim_detail'],
                 row['deep_claimed_at'] or row['deep_pending_since'])
                for item_id, row in RESULT_ROWS.items()
                if row['deep_pending'] and row['deep_eligible']]
        elif 'FOR UPDATE' in s:
            # _requeue_claims' eligibility scan
            self.rows = []
            for item_id, row in RESULT_ROWS.items():
                if not row['deep_pending'] or not row['deep_eligible']:
                    continue
                if "deep_claim_id IS NULL OR deep_claim_state = 'failed'" in s:
                    if (row['deep_claim_id'] is not None
                            and row['deep_claim_state'] != 'failed'):
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
            if row is not None and row['deep_claim_id'] is None:
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
        pass


class FakeDB:
    def cursor(self):
        return FakeCursor()

    def commit(self):
        pass


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


def _run_one_child():
    """Stand-in for a worker: finish the oldest child still sitting queued."""
    for task_id, status in TASK_ROWS.items():
        if task_id.startswith('q') and status == 'NEW':
            TASK_ROWS[task_id] = 'SUCCESS'
            return True
    return False


class _FakeRQJob:
    def __init__(self, state):
        self._state = state

    def get_status(self, refresh=False):
        return self._state


def _install_rq():
    """A controllable rq stand-in for the legacy-queue adapter tests.

    Mirrors the real API the adapter actually calls: rq.job.Job.fetch(id,
    connection=...) raising NoSuchJobError when the job key is gone, plus
    redis.Redis() for the connection. (rq.Queue has no .fetch() method in any
    version, so the fake must not invent one.)
    """
    rq = types.ModuleType('rq')
    exceptions = types.ModuleType('rq.exceptions')
    jobmod = types.ModuleType('rq.job')

    class NoSuchJobError(Exception):
        pass

    exceptions.NoSuchJobError = NoSuchJobError
    rq.exceptions = exceptions

    class Job:
        @classmethod
        def fetch(cls, job_id, connection=None, serializer=None):
            state = RQ_JOBS.get(job_id)
            if state is None:
                raise NoSuchJobError(job_id)
            if state == 'error':
                raise ConnectionError('redis transport error')
            return _FakeRQJob(state)

    jobmod.Job = Job
    rq.job = jobmod

    redis = types.ModuleType('redis')

    class Redis:
        def __init__(self, *a, **k):
            pass

    redis.Redis = Redis

    sys.modules['rq'] = rq
    sys.modules['rq.exceptions'] = exceptions
    sys.modules['rq.job'] = jobmod
    sys.modules['redis'] = redis
    return rq


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

    # the 3.2+ queue module: its mere presence makes _queue_backend()
    # classify this install as 'postgres'
    taskqueue = types.ModuleType('taskqueue')
    taskqueue.current_task_id = lambda: None
    sys.modules['taskqueue'] = taskqueue

    mediaserver = types.ModuleType('tasks.mediaserver')
    mediaserver.get_recent_albums = lambda n: []
    mediaserver.get_tracks_from_album = lambda album_id: []
    mediaserver.download_track = lambda d, t: None
    tasks = types.ModuleType('tasks')
    tasks.__path__ = []
    sys.modules['tasks'] = tasks
    sys.modules['tasks.mediaserver'] = mediaserver

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
_install_rq()


def _reset(n_tracks=8, verdict='FAKE_SUSPECT'):
    TASK_ROWS.clear()
    RESULT_ROWS.clear()
    del ENQUEUED[:]
    RQ_JOBS.clear()
    CLAIM_SEQ[0] = 0
    for i in range(n_tracks):
        RESULT_ROWS['t%d' % i] = {
            'album': 'A', 'verdict': verdict, 'verified': False,
            'deep_pending': False, 'deep_eligible': True,
            'deep_pending_since': None, 'deep_claim_id': None,
            'deep_claim_state': None, 'deep_queue_backend': None,
            'deep_task_id': None, 'deep_claimed_at': None,
            'deep_completed_claim_id': None, 'deep_completed_at': None,
            'deep_claim_detail': None}


def _tag(item_id, state='claimed', task_id=None, claim_id=None,
         since=None, marker=None, backend='postgres'):
    """Put a row into a given claim state (test setup helper)."""
    row = RESULT_ROWS[item_id]
    row['deep_pending'] = True
    row['deep_pending_since'] = since if since is not None else DB_NOW
    row['deep_claim_id'] = claim_id or next_claim_id(item_id)
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
        claims = jobs._new_deep_claims(item_ids=['t0', 't1', 't2'],
                                       queue='default')
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

    def test_rq_finished_clears_and_failed_marks_failed(self):
        _reset(2)
        _tag('t0', state='running', task_id='rq0', backend='rq')
        _tag('t1', state='running', task_id='rq1', backend='rq')
        RQ_JOBS.update({'rq0': 'finished', 'rq1': 'failed'})
        counts = jobs.reconcile_deep_pending_task()
        self.assertFalse(RESULT_ROWS['t0']['deep_pending'])
        row1 = RESULT_ROWS['t1']
        self.assertTrue(row1['deep_pending'])
        self.assertEqual(row1['deep_claim_state'], 'failed')
        self.assertEqual(counts['settled'], 2)

    def test_rq_missing_with_marker_clears_without_marker_unknown(self):
        _reset(2)
        c0 = _tag('t0', state='running', task_id='rq0', backend='rq',
                  since=DB_NOW - timedelta(hours=3))
        RESULT_ROWS['t0']['deep_completed_claim_id'] = c0
        _tag('t1', state='running', task_id='rq1', backend='rq',
             since=DB_NOW - timedelta(hours=3))
        # neither job is in RQ_JOBS -> both 'missing'
        counts = jobs.reconcile_deep_pending_task()
        self.assertFalse(RESULT_ROWS['t0']['deep_pending'])
        self.assertEqual(RESULT_ROWS['t1']['deep_claim_state'], 'outcome_unknown')
        self.assertEqual(counts['settled'], 1)
        self.assertEqual(counts['unknown'], 1)

    def test_rq_transport_error_is_unreadable_not_missing(self):
        _reset(1)
        _tag('t0', state='running', task_id='rq0', backend='rq',
             since=DB_NOW - timedelta(hours=9))
        RQ_JOBS['rq0'] = 'error'
        counts = jobs.reconcile_deep_pending_task()
        self.assertEqual(counts['unreadable'], 1)
        self.assertEqual(RESULT_ROWS['t0']['deep_claim_state'], 'running')


if __name__ == '__main__':
    unittest.main()
