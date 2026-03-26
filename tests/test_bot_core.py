import importlib.util
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parent.parent
MAIN_PATH = PROJECT_ROOT / "main.py"


def load_main_module(db_path: Path):
    # Hard-stop any attempt to call real Epic API in tests.
    os.environ["DRY_RUN"] = "1"

    os.environ["TELEGRAM_BOT_TOKEN"] = "123456:TEST_TOKEN"
    os.environ["ADMIN_TELEGRAM_ID"] = "1"
    os.environ["DB_URL"] = f"sqlite:///{db_path}"
    os.environ["DB_AUTO_INIT_ON_IMPORT"] = "0"
    os.environ["MAX_TASKS_PER_TICK"] = "50"
    os.environ["PROCESS_TICK_SECONDS"] = "1"
    os.environ["DEFAULT_SEND_JITTER_MIN_SEC"] = "0"
    os.environ["DEFAULT_SEND_JITTER_MAX_SEC"] = "0"

    # Force fresh DB engine per test module load. main.py imports db_models at import-time.
    for mod in [
        "db_models",
        "epic_api_client",
        "epic_device_auth",
        "device_auth_jobs",
        "device_auth_generator",
    ]:
        sys.modules.pop(mod, None)

    module_name = f"main_test_{db_path.stem}_{id(db_path)}"
    spec = importlib.util.spec_from_file_location(module_name, str(MAIN_PATH))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    module.init_db_schema(run_migrations=True)
    return module


class TestBotCore(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp_dir = tempfile.TemporaryDirectory()
        cls.db_path = Path(cls.tmp_dir.name) / "test_bot.db"
        cls.main = load_main_module(cls.db_path)

    @classmethod
    def tearDownClass(cls):
        cls.tmp_dir.cleanup()

    def setUp(self):
        m = self.main
        db = m.SessionLocal()
        try:
            db.query(m.Task).delete()
            db.query(m.Target).delete()
            db.query(m.Account).delete()
            db.query(m.Proxy).delete()
            db.query(m.Setting).delete()
            db.query(m.LogEvent).delete()
            db.commit()
        finally:
            db.close()

    def _create_account(self, **overrides):
        m = self.main
        defaults = {
            "login": "user@example.com",
            "password": "pass",
            "enabled": True,
            "status": m.AccountStatus.ACTIVE.value,
            "daily_limit": 5,
            "today_sent": 0,
            "total_sent": 0,
            "total_failed": 0,
            "total_accepted": 0,
            "active_windows_json": "[]",
            # device_auth by default
            "epic_account_id": "acc123",
            "device_id": "dev123",
            "device_secret": "sec123",
        }
        defaults.update(overrides)
        db = m.SessionLocal()
        try:
            a = m.Account(**defaults)
            db.add(a)
            db.commit()
            db.refresh(a)
            return a.id
        finally:
            db.close()

    def _create_target(self, **overrides):
        m = self.main
        defaults = {"username": "TargetUser", "status": m.TargetStatus.NEW.value, "priority": 100}
        defaults.update(overrides)
        db = m.SessionLocal()
        try:
            t = m.Target(**defaults)
            db.add(t)
            db.commit()
            db.refresh(t)
            return t.id
        finally:
            db.close()

    def test_parse_account_line_supports_multiple_separators(self):
        m = self.main
        self.assertEqual(m._parse_account_line("a:b"), ("a", "b"))
        self.assertEqual(m._parse_account_line("a;b"), ("a", "b"))
        self.assertEqual(m._parse_account_line("a,b"), ("a", "b"))
        self.assertEqual(m._parse_account_line("a\tb"), ("a", "b"))

    def test_create_tasks_for_new_targets_creates_queued_task(self):
        m = self.main
        self._create_account()
        self._create_target(username="u1", status=m.TargetStatus.NEW.value)
        db = m.SessionLocal()
        try:
            created = m.create_tasks_for_new_targets(db, limit=10)
            self.assertEqual(created, 1)
            task = db.query(m.Task).first()
            self.assertEqual(task.task_type, "send_request")
            self.assertEqual(task.status, m.TaskStatus.QUEUED.value)
        finally:
            db.close()

    def test_process_tasks_job_success_flow_creates_check_task(self):
        m = self.main
        acc_id = self._create_account(today_sent=0, daily_limit=10)
        tgt_id = self._create_target(status=m.TargetStatus.PENDING.value, username="u_ok")

        db = m.SessionLocal()
        try:
            db.add(
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.QUEUED.value,
                    account_id=acc_id,
                    target_id=tgt_id,
                    scheduled_for=m.utc_now() - timedelta(seconds=1),
                    max_attempts=3,
                )
            )
            db.commit()
        finally:
            db.close()

        # Even with DRY_RUN=1, process_tasks_job should create check_status task.
        m.process_tasks_job()

        db = m.SessionLocal()
        try:
            send_task = db.query(m.Task).filter(m.Task.task_type == "send_request").first()
            self.assertEqual(send_task.status, m.TaskStatus.DONE.value)

            check_task = db.query(m.Task).filter(m.Task.task_type == "check_status").first()
            self.assertIsNotNone(check_task)
            self.assertEqual(check_task.status, m.TaskStatus.QUEUED.value)

            acc = db.query(m.Account).filter(m.Account.id == acc_id).first()
            self.assertEqual(acc.today_sent, 1)
            self.assertEqual(acc.total_sent, 1)
        finally:
            db.close()

    def test_process_tasks_job_fails_when_device_auth_missing(self):
        m = self.main
        acc_id = self._create_account(epic_account_id=None, device_id=None, device_secret=None)
        tgt_id = self._create_target(status=m.TargetStatus.PENDING.value, username="u2")

        db = m.SessionLocal()
        try:
            db.add(
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.QUEUED.value,
                    account_id=acc_id,
                    target_id=tgt_id,
                    scheduled_for=m.utc_now() - timedelta(seconds=1),
                    max_attempts=3,
                )
            )
            db.commit()
        finally:
            db.close()

        m.process_tasks_job()

        db = m.SessionLocal()
        try:
            acc = db.query(m.Account).filter(m.Account.id == acc_id).first()
            tgt = db.query(m.Target).filter(m.Target.id == tgt_id).first()
            task = db.query(m.Task).filter(m.Task.account_id == acc_id, m.Task.target_id == tgt_id).first()
            self.assertEqual(acc.status, m.AccountStatus.MANUAL.value)
            self.assertEqual(tgt.status, m.TargetStatus.PENDING.value)
            self.assertEqual(task.status, m.TaskStatus.FAILED.value)
            self.assertEqual(task.last_error, "missing_device_auth")
        finally:
            db.close()

    def test_process_tasks_job_reassigns_send_when_account_at_limit(self):
        m = self.main
        acc_limited = self._create_account(login="lim@example.com", daily_limit=1, today_sent=1)
        acc_ok = self._create_account(login="ok@example.com", daily_limit=5, today_sent=0)
        tgt_id = self._create_target(status=m.TargetStatus.PENDING.value, username="u3")

        db = m.SessionLocal()
        try:
            db.add(
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.QUEUED.value,
                    account_id=acc_limited,
                    target_id=tgt_id,
                    scheduled_for=m.utc_now() - timedelta(seconds=1),
                    max_attempts=3,
                )
            )
            db.commit()
        finally:
            db.close()

        # For DRY_RUN it should reassign and still complete.
        m.process_tasks_job()

        db = m.SessionLocal()
        try:
            task = db.query(m.Task).filter(m.Task.task_type == "send_request").first()
            self.assertEqual(task.status, m.TaskStatus.DONE.value)
            self.assertEqual(task.account_id, acc_ok)

            a_lim = db.query(m.Account).filter(m.Account.id == acc_limited).first()
            a_ok = db.query(m.Account).filter(m.Account.id == acc_ok).first()
            self.assertEqual(a_lim.today_sent, 1)
            self.assertEqual(a_ok.today_sent, 1)
        finally:
            db.close()

    def test_process_tasks_job_reassign_skips_already_used_sender(self):
        m = self.main
        acc_limited = self._create_account(login="lim2@example.com", daily_limit=1, today_sent=1)
        acc_used = self._create_account(login="used@example.com", daily_limit=5, today_sent=0)
        tgt_id = self._create_target(
            status=m.TargetStatus.PENDING.value,
            username="u_reassign_guard",
            required_senders=2,
        )

        db = m.SessionLocal()
        try:
            # Sender acc_used already covered this target with real DONE send_request.
            db.add(
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.DONE.value,
                    account_id=acc_used,
                    target_id=tgt_id,
                    scheduled_for=m.utc_now() - timedelta(minutes=10),
                    completed_at=m.utc_now() - timedelta(minutes=9),
                    max_attempts=3,
                )
            )
            db.add(
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.QUEUED.value,
                    account_id=acc_limited,
                    target_id=tgt_id,
                    scheduled_for=m.utc_now() - timedelta(seconds=1),
                    max_attempts=3,
                )
            )
            db.commit()
        finally:
            db.close()

        m.process_tasks_job()

        db = m.SessionLocal()
        try:
            queued = (
                db.query(m.Task)
                .filter(
                    m.Task.task_type == "send_request",
                    m.Task.account_id == acc_limited,
                    m.Task.target_id == tgt_id,
                    m.Task.status == m.TaskStatus.POSTPONED.value,
                )
                .order_by(m.Task.id.desc())
                .first()
            )
            self.assertIsNotNone(queued)
            self.assertEqual(queued.last_error, "sender_daily_limit_no_replacement")

            done_for_used = (
                db.query(m.Task)
                .filter(
                    m.Task.task_type == "send_request",
                    m.Task.account_id == acc_used,
                    m.Task.target_id == tgt_id,
                    m.Task.status == m.TaskStatus.DONE.value,
                )
                .count()
            )
            self.assertEqual(done_for_used, 1)
        finally:
            db.close()

    def test_process_tasks_job_recovers_stale_running_send_task(self):
        m = self.main
        acc_id = self._create_account(login="stale@example.com", daily_limit=5, today_sent=0)
        tgt_id = self._create_target(status=m.TargetStatus.PENDING.value, username="u_stale")

        db = m.SessionLocal()
        try:
            stale = m.Task(
                task_type="send_request",
                status=m.TaskStatus.RUNNING.value,
                account_id=acc_id,
                target_id=tgt_id,
                scheduled_for=m.utc_now() - timedelta(minutes=30),
                started_at=m.utc_now() - timedelta(minutes=30),
                attempt_number=0,
                max_attempts=3,
            )
            db.add(stale)
            db.commit()
            stale_id = int(stale.id)
        finally:
            db.close()

        m.process_tasks_job()

        db = m.SessionLocal()
        try:
            stale = db.query(m.Task).filter(m.Task.id == stale_id).first()
            self.assertEqual(stale.status, m.TaskStatus.POSTPONED.value)
            self.assertEqual(stale.last_error, "running_timeout_recovered")
            self.assertEqual(int(stale.attempt_number or 0), 1)
            self.assertIsNone(stale.started_at)
        finally:
            db.close()

    def test_process_tasks_job_marks_manual_on_password_grant_blocked(self):
        m = self.main

        # Force "real" path but keep everything mocked.
        m.DRY_RUN = False
        m.SEND_REQUESTS_ENABLED = True

        acc_id = self._create_account(
            login="acc1@example.com",
            daily_limit=5,
            today_sent=0,
            epic_account_id="epic-1",
            device_id="dev-1",
            device_secret="sec-1",
        )
        tgt_id = self._create_target(status=m.TargetStatus.PENDING.value, username="u4")

        db = m.SessionLocal()
        try:
            db.add(
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.QUEUED.value,
                    account_id=acc_id,
                    target_id=tgt_id,
                    scheduled_for=m.utc_now() - timedelta(seconds=1),
                    max_attempts=3,
                )
            )
            db.commit()
        finally:
            db.close()

        # Pre-check shouldn't short-circuit.
        m.check_friend_status_with_device = lambda **kw: SimpleNamespace(ok=False, code="x", message="x", data=None)
        # Simulate Epic blocking password grant. This must push account into MANUAL.
        m.send_friend_request_with_device = lambda **kw: SimpleNamespace(ok=False, code="password_grant_blocked", message="blocked", data=None)

        m.process_tasks_job()

        db = m.SessionLocal()
        try:
            acc = db.query(m.Account).filter(m.Account.id == acc_id).first()
            task = db.query(m.Task).filter(m.Task.account_id == acc_id, m.Task.target_id == tgt_id).first()
            tgt = db.query(m.Target).filter(m.Target.id == tgt_id).first()
            self.assertEqual(acc.status, m.AccountStatus.MANUAL.value)
            self.assertEqual(acc.last_error, "password_grant_blocked_use_device_auth")
            self.assertEqual(task.status, m.TaskStatus.FAILED.value)
            self.assertEqual(task.last_error, "password_grant_blocked_use_device_auth")
            self.assertEqual(tgt.status, m.TargetStatus.PENDING.value)
        finally:
            db.close()
            # Restore globals to avoid influencing other tests.
            m.DRY_RUN = True
            m.SEND_REQUESTS_ENABLED = False

    def test_precheck_pending_skips_sender_from_coverage_and_requeues_replacement(self):
        m = self.main
        m.DRY_RUN = False
        m.SEND_REQUESTS_ENABLED = True

        db = m.SessionLocal()
        try:
            camp = m.Campaign(name="precheck_replace", enabled=True, jitter_min_sec=0, jitter_max_sec=0)
            db.add(camp)
            db.flush()

            acc1 = m.Account(
                login="a1@example.com",
                password="x",
                enabled=True,
                status=m.AccountStatus.ACTIVE.value,
                epic_account_id="e1",
                device_id="d1",
                device_secret="s1",
                daily_limit=100,
                today_sent=0,
                active_windows_json="[]",
            )
            acc2 = m.Account(
                login="a2@example.com",
                password="x",
                enabled=True,
                status=m.AccountStatus.ACTIVE.value,
                epic_account_id="e2",
                device_id="d2",
                device_secret="s2",
                daily_limit=100,
                today_sent=0,
                active_windows_json="[]",
            )
            db.add_all([acc1, acc2])
            db.flush()

            tgt = m.Target(
                username="pending_target",
                campaign_id=int(camp.id),
                status=m.TargetStatus.PENDING.value,
                required_senders=1,
                sent_count=0,
            )
            db.add(tgt)
            db.flush()

            original = m.Task(
                task_type="send_request",
                status=m.TaskStatus.QUEUED.value,
                campaign_id=int(camp.id),
                account_id=int(acc1.id),
                target_id=int(tgt.id),
                scheduled_for=m.utc_now() - timedelta(seconds=1),
                max_attempts=3,
            )
            db.add(original)
            db.commit()
            original_id = int(original.id)
            acc1_id = int(acc1.id)
            acc2_id = int(acc2.id)
            tgt_id = int(tgt.id)
            camp_id = int(camp.id)
        finally:
            db.close()

        def _precheck(**kw):
            login = str(kw.get("login") or "")
            if login == "a1@example.com":
                return SimpleNamespace(ok=True, code="pending", message="already pending", data={})
            return SimpleNamespace(ok=False, code="unknown", message="none", data={})

        m.check_friend_status_with_device = _precheck
        m.send_friend_request_with_device = lambda **kw: SimpleNamespace(ok=True, code="request_sent", message="ok", data={})

        try:
            m.process_tasks_job()

            db = m.SessionLocal()
            try:
                orig = db.query(m.Task).filter(m.Task.id == original_id).first()
                self.assertEqual(orig.status, m.TaskStatus.CANCELLED.value)
                self.assertEqual(orig.last_error, "precheck_pending_skip")

                tgt = db.query(m.Target).filter(m.Target.id == tgt_id).first()
                self.assertEqual(int(tgt.sent_count or 0), 0)

                repl = (
                    db.query(m.Task)
                    .filter(
                        m.Task.task_type == "send_request",
                        m.Task.campaign_id == camp_id,
                        m.Task.target_id == tgt_id,
                        m.Task.account_id == acc2_id,
                        m.Task.status == m.TaskStatus.QUEUED.value,
                    )
                    .first()
                )
                self.assertIsNone(repl)
            finally:
                db.close()
        finally:
            m.DRY_RUN = True
            m.SEND_REQUESTS_ENABLED = False

    def test_idempotent_send_skips_coverage_and_requeues_replacement(self):
        m = self.main
        m.DRY_RUN = False
        m.SEND_REQUESTS_ENABLED = True

        db = m.SessionLocal()
        try:
            camp = m.Campaign(name="idempotent_replace", enabled=True, jitter_min_sec=0, jitter_max_sec=0)
            db.add(camp)
            db.flush()

            acc1 = m.Account(
                login="idem_a1@example.com",
                password="x",
                enabled=True,
                status=m.AccountStatus.ACTIVE.value,
                epic_account_id="ie1",
                device_id="id1",
                device_secret="is1",
                daily_limit=100,
                today_sent=0,
                active_windows_json="[]",
            )
            acc2 = m.Account(
                login="idem_a2@example.com",
                password="x",
                enabled=True,
                status=m.AccountStatus.ACTIVE.value,
                epic_account_id="ie2",
                device_id="id2",
                device_secret="is2",
                daily_limit=100,
                today_sent=0,
                active_windows_json="[]",
            )
            db.add_all([acc1, acc2])
            db.flush()

            tgt = m.Target(
                username="idem_target",
                campaign_id=int(camp.id),
                status=m.TargetStatus.PENDING.value,
                required_senders=1,
                sent_count=0,
            )
            db.add(tgt)
            db.flush()

            original = m.Task(
                task_type="send_request",
                status=m.TaskStatus.QUEUED.value,
                campaign_id=int(camp.id),
                account_id=int(acc1.id),
                target_id=int(tgt.id),
                scheduled_for=m.utc_now() - timedelta(seconds=1),
                max_attempts=3,
            )
            db.add(original)
            db.commit()
            original_id = int(original.id)
            acc2_id = int(acc2.id)
            tgt_id = int(tgt.id)
            camp_id = int(camp.id)
        finally:
            db.close()

        # Pre-check doesn't classify as accepted/pending here.
        m.check_friend_status_with_device = lambda **kw: SimpleNamespace(ok=False, code="unknown", message="none", data={})
        # Epic returns idempotent request_sent (already friends/pending).
        m.send_friend_request_with_device = lambda **kw: SimpleNamespace(
            ok=True,
            code="request_sent",
            message="idempotent",
            data={"note": "idempotent_success"},
        )

        try:
            m.process_tasks_job()

            db = m.SessionLocal()
            try:
                orig = db.query(m.Task).filter(m.Task.id == original_id).first()
                self.assertEqual(orig.status, m.TaskStatus.CANCELLED.value)
                self.assertEqual(orig.last_error, "idempotent_request_skip")

                tgt = db.query(m.Target).filter(m.Target.id == tgt_id).first()
                self.assertEqual(int(tgt.sent_count or 0), 0)

                repl = (
                    db.query(m.Task)
                    .filter(
                        m.Task.task_type == "send_request",
                        m.Task.campaign_id == camp_id,
                        m.Task.target_id == tgt_id,
                        m.Task.account_id == acc2_id,
                        m.Task.status == m.TaskStatus.QUEUED.value,
                    )
                    .first()
                )
                self.assertIsNone(repl)
            finally:
                db.close()
        finally:
            m.DRY_RUN = True
            m.SEND_REQUESTS_ENABLED = False
