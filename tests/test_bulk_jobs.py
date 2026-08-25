"""Bulk fan-out orchestration (jobs.bulk_analyze_job), on a stubbed core.

jobs.py talks to AudioMuse-AI, so the rest of the suite leaves it alone. What
is faked here is deliberately only the plumbing — plugin.api, plus enough of a
task_status / results table to answer the handful of statements these functions
run — so that the ORCHESTRATION being exercised is the real code: the claim,
the throttle, the in-flight bookkeeping and the cancellation path.

What this pins down: the throttle never exceeds MAX_QUEUED_ANALYSIS_JOBS and
does actually wait; a finished (or retention-deleted) queue row frees its slot;
the deep claim skips CLEAN/verified/already-tagged tracks and is idempotent;
cancelling untags exactly the tracks claimed but never launched.

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

_HERE = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.join(os.path.dirname(_HERE), 'plugins', 'SpectrumAnalyzer')

MAX_IN_FLIGHT = 3

TASK_ROWS = {}       # task_id -> status
RESULT_ROWS = {}     # item_id -> column dict
ENQUEUED = []        # every enqueue() the code under test made


class FakeCursor:
    """Answers only the statements jobs.py actually runs here."""

    def __init__(self):
        self.rows = []

    def execute(self, sql, params=()):
        s = ' '.join(sql.split())
        if s.startswith('SELECT task_id, status FROM task_status'):
            self.rows = [(t, TASK_ROWS[t]) for t in params[0] if t in TASK_ROWS]
        elif s.startswith('SELECT status FROM task_status'):
            self.rows = [(TASK_ROWS[params[0]],)] if params[0] in TASK_ROWS else []
        elif 'SET deep_pending=TRUE' in s:
            claimed = []
            for item_id, row in RESULT_ROWS.items():
                if row['deep_pending'] or not row['deep_eligible']:
                    continue
                if 'verdict IS DISTINCT FROM' in s and row['verdict'] == 'CLEAN':
                    continue
                if 'NOT verified' in s and row['verified']:
                    continue
                if 'album = %s' in s and row['album'] != params[0]:
                    continue
                if 'item_id = ANY(%s)' in s and item_id not in params[-1]:
                    continue
                row['deep_pending'] = True
                claimed.append((item_id,))
            self.rows = claimed
        elif 'SET deep_pending=FALSE' in s:
            for item_id in params[0]:
                if item_id in RESULT_ROWS:
                    RESULT_ROWS[item_id]['deep_pending'] = False
            self.rows = []
        elif 'SET deep_pending_since = now()' in s:
            self.rows = [(i,) for i, r in RESULT_ROWS.items()
                         if r['deep_pending'] and r['deep_eligible']]
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
    # the throttle's wait is where a worker gets to make progress
    module.time = types.SimpleNamespace(sleep=lambda s: _run_one_child(),
                                        monotonic=lambda: 0.0)
    return module


jobs = _install_core_stubs()


def _reset(n_tracks=8, verdict='FAKE_SUSPECT'):
    TASK_ROWS.clear()
    RESULT_ROWS.clear()
    del ENQUEUED[:]
    for i in range(n_tracks):
        RESULT_ROWS['t%d' % i] = {'album': 'A', 'verdict': verdict,
                                  'verified': False, 'deep_pending': False,
                                  'deep_eligible': True}


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


if __name__ == '__main__':
    unittest.main()
