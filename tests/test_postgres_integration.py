"""The plugin's own SQL, run against a real Postgres.

test_bulk_jobs.py covers the orchestration on a fake cursor that matches
statement fragments, so it cannot tell whether a statement is valid Postgres.
This file loads the real package (migrate + jobs) on top of a real database and
a minimal copy of the core tables, and exercises the statements that matter:
the migration, the deep-claim lifecycle, the legacy-RQ quarantine and the
reconciler.

It is skipped unless SA_TEST_PG_DSN points at a throwaway database (the public
schema is DROPPED on every test):

    podman run -d --name sa-pg -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=audiomuse \\
        -p 55432:5432 docker.io/library/postgres:16
    SA_TEST_PG_DSN='host=localhost port=55432 dbname=audiomuse user=postgres password=pw' \\
        python -I -m unittest tests.test_postgres_integration

What it does NOT cover: the core itself. plugin.api, the task queue and the
registry are stubs; the core tables are hand-copied from core v3.6.4.
"""
import contextlib
import importlib.util
import os
import sys
import types
import unittest

try:
    import psycopg2
except ImportError:  # pragma: no cover
    psycopg2 = None

_HERE = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.join(os.path.dirname(_HERE), 'plugins', 'SpectrumAnalyzer')
DSN = os.environ.get('SA_TEST_PG_DSN')
TBL = 'plugin_spectrum_analyzer__results'

# the core tables the plugin touches, as core v3.6.4 declares them
CORE_DDL = """
CREATE TABLE score (item_id TEXT PRIMARY KEY, title TEXT, author TEXT,
                    album TEXT);
CREATE TABLE task_status (id SERIAL PRIMARY KEY, task_id TEXT UNIQUE NOT NULL,
    parent_task_id TEXT, task_type TEXT NOT NULL, sub_type_identifier TEXT,
    status TEXT, progress INTEGER DEFAULT 0, details TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE cron (id SERIAL PRIMARY KEY, name TEXT, task_type TEXT NOT NULL,
    cron_expr TEXT NOT NULL, enabled BOOLEAN DEFAULT FALSE,
    last_run DOUBLE PRECISION, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
CREATE UNIQUE INDEX idx_cron_task_type ON cron (task_type);
CREATE TABLE music_servers (server_id TEXT PRIMARY KEY);
CREATE TABLE track_server_map (
    item_id TEXT NOT NULL REFERENCES score (item_id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    server_id TEXT NOT NULL REFERENCES music_servers (server_id)
        ON DELETE CASCADE,
    provider_track_id TEXT NOT NULL, match_tier TEXT, file_path TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (item_id, server_id));
"""

STATE = {'conn': None}


def _stub(name, **attrs):
    mod = types.ModuleType(name)
    mod.__dict__.update(attrs)
    sys.modules[name] = mod
    return mod


def _load_plugin():
    quiet = types.SimpleNamespace(info=lambda *a, **k: None,
                                  warning=lambda *a, **k: None,
                                  exception=lambda *a, **k: None)
    _stub('flask', Blueprint=lambda *a, **k: types.SimpleNamespace(
        route=lambda *a, **k: (lambda f: f)),
        request=None, redirect=None, url_for=None)
    pkg = _stub('plugin', __path__=[])
    api = _stub(
        'plugin.api',
        config=types.SimpleNamespace(MAX_QUEUED_ANALYSIS_JOBS=3,
                                     QUEUE_INLINE_STALE_SECONDS=1800.0),
        enqueue=None, get_db=lambda: STATE['conn'],
        get_setting=lambda key, default=None: default, set_setting=None,
        table=lambda name: 'plugin_spectrum_analyzer__' + name,
        render_page=None, manage_plugins_url=None, logger=quiet,
        save_task_status=None, active_server_id=lambda: None,
        list_servers=lambda: [],
        use_server=lambda s: contextlib.nullcontext(),
        TASK_STATUS_PENDING='NEW', TASK_STATUS_STARTED='RUNNING',
        TASK_STATUS_PROGRESS='RUNNING', TASK_STATUS_SUCCESS='SUCCESS',
        TASK_STATUS_FAILURE='FAIL', TASK_STATUS_REVOKED='REVOKED')
    pkg.api = api
    _stub('taskqueue', current_task_id=lambda: None)
    _stub('tasks', __path__=[])
    _stub('tasks.mediaserver', get_recent_albums=None,
          get_tracks_from_album=None, download_track=None)
    _stub('tasks.mediaserver.registry', reverse_translate_ids=None,
          translate_ids=None)
    spec = importlib.util.spec_from_file_location(
        'sa_pg', os.path.join(PKG_DIR, '__init__.py'),
        submodule_search_locations=[PKG_DIR])
    module = importlib.util.module_from_spec(spec)
    sys.modules['sa_pg'] = module
    spec.loader.exec_module(module)
    return module


@unittest.skipUnless(DSN and psycopg2, 'set SA_TEST_PG_DSN to run')
class PgCase(unittest.TestCase):
    plugin = None

    @classmethod
    def setUpClass(cls):
        cls.plugin = _load_plugin()
        cls.jobs = sys.modules['sa_pg.jobs']

    def setUp(self):
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public')
        cur.execute(CORE_DDL)
        conn.autocommit = False
        STATE['conn'] = conn
        self.conn = conn
        self.addCleanup(conn.close)

    # helpers
    def q(self, sql, params=()):
        cur = self.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall() if cur.description else None
        self.conn.commit()
        cur.close()
        return rows

    def add_track(self, item_id, **cols):
        """A score row plus a results row with the given column values."""
        self.q('INSERT INTO score (item_id, title) VALUES (%s, %s)',
               (item_id, item_id))
        cols = dict({'verdict': 'FAKE_SUSPECT', 'deep_eligible': True}, **cols)
        names = ['item_id'] + list(cols)
        self.q('INSERT INTO ' + TBL + ' (' + ', '.join(names) + ') VALUES (' +
               ', '.join(['%s'] * len(names)) + ')',
               [item_id] + list(cols.values()))

    def row(self, item_id):
        cur = self.conn.cursor()
        cur.execute('SELECT deep_pending, deep_claim_id, deep_task_id,'
                    ' deep_queue_backend, deep_claim_state FROM ' + TBL +
                    ' WHERE item_id=%s', (item_id,))
        r = cur.fetchone()
        self.conn.commit()
        cur.close()
        return dict(zip(('pending', 'claim', 'task', 'backend', 'state'), r))

    def migrate(self):
        self.plugin.migrate(self.conn)


class TestMigrate(PgCase):
    def test_fresh_install_is_idempotent(self):
        self.migrate()
        self.migrate()
        cols = {r[0] for r in self.q(
            'SELECT column_name FROM information_schema.columns'
            ' WHERE table_name=%s', (TBL,))}
        for c in ('deep_claim_id', 'deep_queue_backend', 'deep_claim_state',
                  'analysis_rev', 'provider_track_id', 'server_id'):
            self.assertIn(c, cols)
        crons = self.q('SELECT task_type, enabled FROM cron ORDER BY 1')
        self.assertEqual(crons, [
            ('plugin.spectrum_analyzer.recover_deep_pending', False),
            ('plugin.spectrum_analyzer.scan_changed', False)])

    def test_check_constraints_reject_bad_values(self):
        self.migrate()
        self.add_track('t1')
        for col, bad in (('deep_queue_backend', 'redis'),
                         ('deep_claim_state', 'bogus')):
            with self.assertRaises(psycopg2.errors.CheckViolation):
                self.q('UPDATE ' + TBL + ' SET ' + col + '=%s', (bad,))
            self.conn.rollback()
        # the retired backend is still accepted: it is what gets quarantined
        self.q('UPDATE ' + TBL + " SET deep_queue_backend='rq'")

    def test_upgrade_from_pre_claim_table_with_native_ids(self):
        """An 0.3.x table (native-keyed, plain FK) is repaired and backfilled."""
        self.q('INSERT INTO music_servers VALUES (%s)', ('srv1',))
        self.q("INSERT INTO score (item_id, title) VALUES ('fp_a', 'A')")
        self.q("INSERT INTO track_server_map (item_id, server_id,"
               " provider_track_id) VALUES ('fp_a', 'srv1', 'native-a')")
        self.q('CREATE TABLE ' + TBL + ' (item_id TEXT PRIMARY KEY'
               ' REFERENCES score (item_id) ON DELETE CASCADE,'
               ' album TEXT, verdict TEXT,'
               ' deep_pending BOOLEAN NOT NULL DEFAULT FALSE)')
        self.q('INSERT INTO ' + TBL + " (item_id, verdict, deep_pending)"
               " VALUES ('fp_a', 'FAKE_SUSPECT', TRUE)")
        self.migrate()
        self.migrate()
        r = self.row('fp_a')
        self.assertTrue(r['pending'])
        self.assertIsNone(r['claim'])   # legacy rows are not backfilled
        self.assertEqual(
            self.q('SELECT provider_track_id, server_id FROM ' + TBL),
            [('native-a', 'srv1')])
        # the repaired FK now cascades the core's in-place relabel
        self.q("UPDATE score SET item_id='fp_b' WHERE item_id='fp_a'")
        self.assertEqual(self.q('SELECT item_id FROM ' + TBL), [('fp_b',)])


class TestClaims(PgCase):
    def setUp(self):
        super().setUp()
        self.migrate()

    def test_new_claims_pick_only_eligible_untagged_tracks(self):
        self.add_track('bad', album='A')
        self.add_track('clean', verdict='CLEAN', album='A')
        self.add_track('verified', verified=True, album='A')
        self.add_track('noelig', deep_eligible=False, album='A')
        self.add_track('other', album='B')
        claims = self.jobs._new_deep_claims(select='non_clean', album='A')
        self.assertEqual([c[0] for c in claims], ['bad'])
        self.assertEqual(self.row('bad')['state'], 'claimed')
        self.assertEqual(self.row('bad')['backend'], 'postgres')
        self.assertEqual(self.row('bad')['claim'], claims[0][1])
        # idempotent: a second click claims nothing new
        self.assertEqual(self.jobs._new_deep_claims(select='non_clean',
                                                    album='A'), [])
        # a batch never shares a claim id
        more = self.jobs._new_deep_claims(select='non_clean')
        self.assertEqual([c[0] for c in more], ['other'])
        ids = self.jobs._new_deep_claims(item_ids=['noelig', 'nosuch'])
        self.assertEqual(ids, [])

    def test_requeue_all_skips_quarantined_and_live_claims(self):
        self.add_track('legacy', deep_pending=True)
        self.add_track('failed', deep_pending=True, deep_claim_id='c1',
                       deep_claim_state='failed', deep_queue_backend='postgres',
                       deep_task_id='t1')
        self.add_track('quarantined', deep_pending=True,
                       deep_queue_backend='rq',
                       deep_claim_state='outcome_unknown')
        self.add_track('live', deep_pending=True, deep_claim_id='c2',
                       deep_claim_state='running', deep_queue_backend='postgres',
                       deep_task_id='t2')
        claims, restore = self.jobs._requeue_claims('requeue_all')
        self.assertEqual(sorted(c[0] for c in claims), ['failed', 'legacy'])
        self.assertEqual(sorted(restore), ['failed', 'legacy'])
        self.assertEqual(self.row('quarantined')['state'], 'outcome_unknown')
        self.assertEqual(self.row('live')['claim'], 'c2')
        # the prior record can be put back (CAS on the new claim id)
        item, new_claim = next(c for c in claims if c[0] == 'failed')
        self.jobs._restore_deep_claim(item, new_claim, restore[item])
        self.assertEqual(self.row('failed')['claim'], 'c1')
        self.assertEqual(self.row('failed')['state'], 'failed')

    def test_requeue_unknown_takes_taskless_and_quarantined(self):
        self.add_track('taskless', deep_pending=True, deep_claim_id='c1',
                       deep_claim_state='claimed', deep_queue_backend='postgres')
        self.add_track('quarantined', deep_pending=True,
                       deep_queue_backend='rq',
                       deep_claim_state='outcome_unknown')
        self.add_track('live', deep_pending=True, deep_claim_id='c2',
                       deep_claim_state='running', deep_queue_backend='postgres',
                       deep_task_id='t2')
        claims, _restore = self.jobs._requeue_claims('requeue_unknown')
        self.assertEqual(sorted(c[0] for c in claims),
                         ['quarantined', 'taskless'])
        self.assertEqual(self.row('quarantined')['backend'], 'postgres')
        self.assertEqual(self.row('live')['claim'], 'c2')

    def test_clear_legacy_leaves_claimed_and_quarantined_rows(self):
        self.add_track('legacy', deep_pending=True)
        self.add_track('claimed', deep_pending=True, deep_claim_id='c1',
                       deep_claim_state='running')
        self.add_track('quarantined', deep_pending=True,
                       deep_claim_state='outcome_unknown',
                       deep_queue_backend='rq')
        for item in ('legacy', 'claimed', 'quarantined'):
            self.jobs._clear_legacy_deep_pending(item)
        self.assertFalse(self.row('legacy')['pending'])
        self.assertTrue(self.row('claimed')['pending'])
        self.assertTrue(self.row('quarantined')['pending'])


class TestReconcile(PgCase):
    def setUp(self):
        super().setUp()
        self.migrate()

    def seed_task(self, task_id, status):
        self.q('INSERT INTO task_status (task_id, task_type, status)'
               ' VALUES (%s, %s, %s)', (task_id, 'plugin.x', status))

    def old(self, item_id):
        self.q('UPDATE ' + TBL + " SET deep_claimed_at=now()-interval '9 days',"
               " deep_pending_since=now()-interval '9 days' WHERE item_id=%s",
               (item_id,))

    def claim(self, item_id, claim, state, task=None, backend='postgres', **kw):
        self.add_track(item_id, deep_pending=True, deep_claim_id=claim,
                       deep_claim_state=state, deep_queue_backend=backend,
                       deep_task_id=task, **kw)
        # stamped by the database, so the grace arithmetic has no clock skew
        self.q('UPDATE ' + TBL + ' SET deep_claimed_at=now(),'
               ' deep_pending_since=now() WHERE item_id=%s', (item_id,))

    def test_settles_from_queue_rows_and_markers(self):
        self.claim('ok', 'c1', 'running', 't1')
        self.seed_task('t1', 'SUCCESS')
        self.claim('bad', 'c2', 'running', 't2')
        self.seed_task('t2', 'FAIL')
        self.claim('live', 'c3', 'running', 't3')
        self.seed_task('t3', 'RUNNING')
        self.claim('marked', 'c4', 'running', 't4', deep_completed_claim_id='c4')
        self.old('marked')
        self.claim('gone', 'c5', 'running', 't5')
        self.old('gone')
        self.claim('fresh_gone', 'c6', 'running', 't6')
        self.claim('taskless', 'c7', 'claimed')
        self.old('taskless')
        counts = self.jobs.reconcile_deep_pending_task()
        self.assertFalse(self.row('ok')['pending'])
        self.assertEqual(self.row('bad')['state'], 'failed')
        self.assertEqual(self.row('live')['state'], 'running')
        self.assertFalse(self.row('marked')['pending'])
        self.assertEqual(self.row('gone')['state'], 'outcome_unknown')
        self.assertEqual(self.row('fresh_gone')['state'], 'running')
        self.assertEqual(self.row('taskless')['state'], 'outcome_unknown')
        self.assertEqual(counts['live'], 2)
        self.assertEqual(counts['settled'], 3)
        self.assertEqual(counts['unknown'], 2)

    def test_quarantines_legacy_rq_claims_even_when_ineligible(self):
        # a claimless rq row, a claimed rq row, an ineligible rq row and a
        # fresh postgres claim that must be left alone
        self.add_track('claimless', deep_pending=True, deep_queue_backend='rq')
        self.claim('claimed', 'c1', 'running', 'rq-job', backend='rq')
        self.claim('inelig', 'c2', 'failed', 'rq-job2', backend='rq',
                   deep_eligible=False)
        self.claim('pg', 'c3', 'running', 't9')
        self.seed_task('t9', 'RUNNING')
        counts = self.jobs.reconcile_deep_pending_task()
        for item in ('claimless', 'claimed', 'inelig'):
            self.assertEqual(self.row(item)['state'], 'outcome_unknown', item)
            self.assertTrue(self.row(item)['pending'], item)
        self.assertEqual(self.row('pg')['state'], 'running')
        self.assertEqual(counts['unknown'], 3)
        # a second run is a no-op: the rows are already quarantined
        self.assertEqual(self.jobs.reconcile_deep_pending_task()['unknown'], 0)
        # and nothing here ever re-claims
        self.assertEqual(self.row('claimed')['task'], 'rq-job')

    def test_quarantine_is_fenced_on_the_claim_id(self):
        self.claim('x', 'new', 'claimed', backend='postgres')
        self.assertFalse(self.jobs._quarantine_legacy_backend('x', 'old', 'rq'))
        self.assertFalse(self.jobs._quarantine_legacy_backend('x', 'new', 'rq'))
        self.assertEqual(self.row('x')['state'], 'claimed')


if __name__ == '__main__':
    unittest.main()
