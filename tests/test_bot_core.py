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
