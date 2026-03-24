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

    module_name = f"main_test_add_{db_path.stem}_{id(db_path)}"
    spec = importlib.util.spec_from_file_location(module_name, str(MAIN_PATH))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class TestMainAdditional(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp_dir = tempfile.TemporaryDirectory()
        cls.db_path = Path(cls.tmp_dir.name) / "test_additional.db"
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

    def _create_account(self, **overrides) -> int:
        m = self.main
        defaults = dict(
            login="a@example.com",
            password="x",
            enabled=True,
            status="active",
            epic_account_id="e",
            device_id="d",
            device_secret="s",
            daily_limit=5,
            today_sent=0,
            active_windows_json="[]",
        )
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

    def test_parse_windows_text_and_check_window(self):
        m = self.main
        windows = m.parse_windows_text("days=1,2,3,4,5,6,7 from=00:00 to=23:59")
        self.assertTrue(m.is_in_window_utc(windows, m.utc_now()))

    def test_create_tasks_spreads_across_accounts_using_reservations(self):
        m = self.main
        db = m.SessionLocal()
        try:
            a1 = m.Account(
                login="a1@example.com",
                password="x",
                enabled=True,
                status="active",
                epic_account_id="e1",
                device_id="d1",
                device_secret="s1",
                daily_limit=5,
                today_sent=0,
                active_windows_json="[]",
            )
            a2 = m.Account(
                login="a2@example.com",
                password="x",
                enabled=True,
                status="active",
                epic_account_id="e2",
                device_id="d2",
                device_secret="s2",
                daily_limit=5,
                today_sent=0,
                active_windows_json="[]",
            )
            db.add_all([a1, a2])
            db.commit()

            for i in range(10):
                db.add(m.Target(username=f"u{i}", status="new", priority=100))
            db.commit()

            created = m.create_tasks_for_new_targets(db, limit=10)
            self.assertEqual(created, 10)

            tasks = db.query(m.Task).filter(m.Task.task_type == "send_request").all()
            acct_ids = {t.account_id for t in tasks}
            self.assertEqual(acct_ids, {a1.id, a2.id})
        finally:
            db.close()

    def test_create_tasks_for_new_targets_does_not_duplicate_active_send(self):
        m = self.main
        acc_id = self._create_account()
        db = m.SessionLocal()
        try:
            tgt = m.Target(username="dup_target", status="new", priority=100)
            db.add(tgt)
            db.commit()
            db.refresh(tgt)

            # Existing active send task -> should not create another.
            db.add(
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.QUEUED.value,
                    account_id=acc_id,
                    target_id=tgt.id,
                    scheduled_for=m.utc_now(),
                    max_attempts=3,
                )
            )
            db.commit()

            created = m.create_tasks_for_new_targets(db, limit=100)
            self.assertEqual(created, 0)
            db.refresh(tgt)
            self.assertEqual(tgt.status, m.TargetStatus.PENDING.value)
        finally:
            db.close()

    def test_create_tasks_respects_target_fanout_setting(self):
        m = self.main
        db = m.SessionLocal()
        try:
            m.set_setting(db, "target_senders_count", "2")
            a1 = m.Account(
                login="f1@example.com",
                password="x",
                enabled=True,
                status="active",
                epic_account_id="e1",
                device_id="d1",
                device_secret="s1",
                daily_limit=5,
                today_sent=0,
                active_windows_json="[]",
            )
            a2 = m.Account(
                login="f2@example.com",
                password="x",
                enabled=True,
                status="active",
                epic_account_id="e2",
                device_id="d2",
                device_secret="s2",
                daily_limit=5,
                today_sent=0,
                active_windows_json="[]",
            )
            db.add_all([a1, a2, m.Target(username="fanout_target", status="new", priority=100)])
            db.commit()

            created = m.create_tasks_for_new_targets(db, limit=10)
            self.assertEqual(created, 2)
            tasks = db.query(m.Task).filter(m.Task.task_type == "send_request").all()
            self.assertEqual(len(tasks), 2)
            self.assertEqual(len({t.account_id for t in tasks}), 2)
        finally:
            db.close()

    def test_enforce_api_rate_limit_blocks_too_fast_requests(self):
        m = self.main
        db = m.SessionLocal()
        try:
            acc = m.Account(
                login="rl@example.com",
                password="x",
                enabled=True,
                status="active",
                epic_account_id="e",
                device_id="d",
                device_secret="s",
                daily_limit=500,
                today_sent=0,
                active_windows_json="[]",
            )
            db.add(acc)
            db.commit()
            db.refresh(acc)

            m.set_setting(db, "min_request_interval_sec", "40")
            m.set_setting(db, "hourly_api_limit", "40")
            m.set_setting(db, "daily_api_limit", "500")

            now = m.utc_now()
            ok1, _, _ = m.enforce_api_rate_limit(db, acc, now, api_cost=3)
            self.assertTrue(ok1)

            ok2, _, reason = m.enforce_api_rate_limit(db, acc, now + timedelta(seconds=10), api_cost=3)
            self.assertFalse(ok2)
            self.assertEqual(reason, "min_interval")
        finally:
            db.close()

    def test_enforce_api_rate_limit_sets_random_next_allowed_window(self):
        m = self.main
        db = m.SessionLocal()
        try:
            acc = m.Account(
                login="rl2@example.com",
                password="x",
                enabled=True,
                status="active",
                epic_account_id="e",
                device_id="d",
                device_secret="s",
                daily_limit=500,
                today_sent=0,
                active_windows_json="[]",
            )
            db.add(acc)
            db.commit()
            db.refresh(acc)

            m.set_setting(db, "min_request_interval_sec", "30")
            m.set_setting(db, "max_request_interval_sec", "40")
            m.set_setting(db, "hourly_api_limit", "40")
            m.set_setting(db, "daily_api_limit", "500")

            now = m.utc_now()
            ok, _, _ = m.enforce_api_rate_limit(db, acc, now, api_cost=1)
            self.assertTrue(ok)
            self.assertIsNotNone(acc.api_next_allowed_at)
            delta = int((acc.api_next_allowed_at - now).total_seconds())
            self.assertGreaterEqual(delta, 30)
            self.assertLessEqual(delta, 40)

            ok2, next_at, reason = m.enforce_api_rate_limit(db, acc, now + timedelta(seconds=1), api_cost=1)
            self.assertFalse(ok2)
            self.assertEqual(reason, "min_interval")
            self.assertIsNotNone(next_at)
        finally:
            db.close()

    def test_import_targets_allows_same_username_in_different_goals(self):
        m = self.main
        db = m.SessionLocal()
        try:
            c1 = m.Campaign(name="goal_dup_1", enabled=True, target_senders_count=1)
            c2 = m.Campaign(name="goal_dup_2", enabled=True, target_senders_count=1)
            db.add_all([c1, c2])
            db.commit()
            db.refresh(c1)
            db.refresh(c2)

            t1 = m.Target(username="same_nick", campaign_id=c1.id, status=m.TargetStatus.NEW.value)
            db.add(t1)
            db.commit()

            t2 = m.Target(username="same_nick", campaign_id=c2.id, status=m.TargetStatus.NEW.value)
            db.add(t2)
            db.commit()

            rows = (
                db.query(m.Target)
                .filter(m.Target.username == "same_nick")
                .order_by(m.Target.campaign_id.asc())
                .all()
            )
            self.assertEqual(len(rows), 2)
            self.assertEqual({int(r.campaign_id) for r in rows}, {int(c1.id), int(c2.id)})
        finally:
            db.close()

    def test_create_tasks_daily_repeat_skips_if_already_created_today(self):
        m = self.main
        db = m.SessionLocal()
        try:
            m.set_setting(db, "daily_repeat_campaign_enabled", "1")
            m.set_setting(db, "target_senders_count", "1")
            acc = m.Account(
                login="dr1@example.com",
                password="x",
                enabled=True,
                status="active",
                epic_account_id="e1",
                device_id="d1",
                device_secret="s1",
                daily_limit=5,
                today_sent=0,
                active_windows_json="[]",
            )
            tgt = m.Target(username="daily_repeat_target", status="sent", priority=100)
            db.add_all([acc, tgt])
            db.commit()
            db.refresh(acc)
            db.refresh(tgt)

            # Simulate already created send task today for this pair.
            db.add(
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.DONE.value,
                    account_id=acc.id,
                    target_id=tgt.id,
                    scheduled_for=m.utc_now(),
                    completed_at=m.utc_now(),
                )
            )
            db.commit()

            created = m.create_tasks_for_new_targets(db, limit=10)
            self.assertEqual(created, 0)
        finally:
            db.close()

    def test_create_tasks_daily_repeat_resets_sender_cap_on_next_day(self):
        m = self.main
        db = m.SessionLocal()
        try:
            m.set_setting(db, "daily_repeat_campaign_enabled", "1")
            m.set_setting(db, "target_senders_count", "1")
            acc = m.Account(
                login="dr2@example.com",
                password="x",
                enabled=True,
                status="active",
                epic_account_id="e2",
                device_id="d2",
                device_secret="s2",
                daily_limit=5,
                today_sent=0,
                active_windows_json="[]",
            )
            tgt = m.Target(username="daily_repeat_target2", status="sent", priority=100)
            db.add_all([acc, tgt])
            db.commit()
            db.refresh(acc)
            db.refresh(tgt)

            yesterday = m.utc_now() - timedelta(days=1, minutes=5)
            db.add(
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.DONE.value,
                    account_id=acc.id,
                    target_id=tgt.id,
                    scheduled_for=yesterday,
                    completed_at=yesterday,
                    created_at=yesterday,
                )
            )
            db.commit()

            created = m.create_tasks_for_new_targets(db, limit=10)
            # Daily repeat cap is per-day: next day planner can assign sender again.
            self.assertEqual(created, 1)
        finally:
            db.close()

    def test_is_in_window_utc_crosses_midnight(self):
        m = self.main
        windows = [{"days": [4], "from": "22:00", "to": "02:00"}]  # Thu

        # Thu 23:00 -> inside
        dt1 = datetime(2026, 2, 12, 23, 0, 0)  # 2026-02-12 is Thursday
        self.assertTrue(m.is_in_window_utc(windows, dt1))

        # Fri 01:00 -> still inside, because it belongs to Thu window
        dt2 = datetime(2026, 2, 13, 1, 0, 0)
        self.assertTrue(m.is_in_window_utc(windows, dt2))

        # Fri 03:00 -> outside
        dt3 = datetime(2026, 2, 13, 3, 0, 0)
        self.assertFalse(m.is_in_window_utc(windows, dt3))

    def test_process_tasks_uses_skip_locked_on_postgres(self):
        m = self.main
        # This test checks that code path calls with_for_update(skip_locked=True) when DB_URL is postgresql.
        fake_q = mock.Mock()
        fake_q.filter.return_value = fake_q
        fake_q.order_by.return_value = fake_q
        fake_q.with_for_update.return_value = fake_q
        fake_q.limit.return_value = fake_q
        fake_q.all.return_value = []

        class FakeSession:
            def query(self, *args, **kwargs):
                return fake_q
            def commit(self):
                return None

        with (
            mock.patch.object(m, "DB_URL", "postgresql+psycopg://x/y"),
            mock.patch.object(m, "get_setting_bool", return_value=True),
            mock.patch.object(m, "db_exec", side_effect=lambda fn: fn(FakeSession())),
        ):
            m.process_tasks_job()

        fake_q.with_for_update.assert_called()

    def test_process_tasks_cancels_newer_duplicate_active_pair_task(self):
        m = self.main
        acc_id = self._create_account()
        db = m.SessionLocal()
        try:
            tgt = m.Target(username="dup_pair_target", status=m.TargetStatus.PENDING.value, priority=100)
            db.add(tgt)
            db.commit()
            db.refresh(tgt)

            older_running = m.Task(
                task_type="send_request",
                status=m.TaskStatus.RUNNING.value,
                account_id=acc_id,
                target_id=tgt.id,
                scheduled_for=m.utc_now() - timedelta(minutes=2),
                started_at=m.utc_now() - timedelta(minutes=1),
                max_attempts=3,
            )
            newer_queued = m.Task(
                task_type="send_request",
                status=m.TaskStatus.QUEUED.value,
                account_id=acc_id,
                target_id=tgt.id,
                scheduled_for=m.utc_now() - timedelta(minutes=1),
                max_attempts=3,
            )
            db.add_all([older_running, newer_queued])
            db.commit()
            newer_id = int(newer_queued.id)
        finally:
            db.close()

        m.process_tasks_job()

        db = m.SessionLocal()
        try:
            newer = db.query(m.Task).filter(m.Task.id == newer_id).first()
            self.assertIsNotNone(newer)
            self.assertEqual(newer.status, m.TaskStatus.CANCELLED.value)
            self.assertEqual(newer.last_error, "duplicate_active_pair_task")
        finally:
            db.close()

    def test_create_recheck_tasks_job_respects_per_goal_daily_limits(self):
        m = self.main
        db = m.SessionLocal()
        try:
            now = m.utc_now()
            acc = m.Account(
                login="recheck@example.com",
                password="x",
                enabled=True,
                status="active",
                epic_account_id="e",
                device_id="d",
                device_secret="s",
                daily_limit=500,
                today_sent=0,
                active_windows_json="[]",
            )
            c1 = m.Campaign(name="goal_r1", enabled=True, recheck_daily_limit=1, jitter_min_sec=0, jitter_max_sec=0)
            c2 = m.Campaign(name="goal_r2", enabled=True, recheck_daily_limit=2, jitter_min_sec=0, jitter_max_sec=0)
            db.add_all([acc, c1, c2])
            db.commit()
            db.refresh(acc)
            db.refresh(c1)
            db.refresh(c2)

            t1 = m.Target(username="g1_u1", campaign_id=c1.id, status=m.TargetStatus.PENDING.value, required_senders=1)
            t2 = m.Target(username="g2_u1", campaign_id=c2.id, status=m.TargetStatus.PENDING.value, required_senders=1)
            t3 = m.Target(username="g2_u2", campaign_id=c2.id, status=m.TargetStatus.SENT.value, required_senders=1)
            db.add_all([t1, t2, t3])
            db.commit()
            db.refresh(t1)
            db.refresh(t2)
            db.refresh(t3)

            c1_id = int(c1.id)
            c2_id = int(c2.id)

            db.add_all([
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.DONE.value,
                    campaign_id=c1_id,
                    account_id=acc.id,
                    target_id=t1.id,
                    scheduled_for=now,
                    completed_at=now,
                ),
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.DONE.value,
                    campaign_id=c2_id,
                    account_id=acc.id,
                    target_id=t2.id,
                    scheduled_for=now,
                    completed_at=now,
                ),
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.DONE.value,
                    campaign_id=c2_id,
                    account_id=acc.id,
                    target_id=t3.id,
                    scheduled_for=now,
                    completed_at=now,
                ),
            ])
            db.commit()
        finally:
            db.close()

        created = m.create_recheck_tasks_job()
        self.assertEqual(created, 3)

        db = m.SessionLocal()
        try:
            checks = db.query(m.Task).filter(m.Task.task_type == "check_status").all()
            self.assertEqual(len(checks), 3)
            self.assertEqual(sum(1 for t in checks if t.campaign_id == c1_id), 1)
            self.assertEqual(sum(1 for t in checks if t.campaign_id == c2_id), 2)

            day1 = m.get_setting(db, f"recheck_counter_day_{c1_id}", "")
            day2 = m.get_setting(db, f"recheck_counter_day_{c2_id}", "")
            self.assertEqual(day1, m.utc_today().isoformat())
            self.assertEqual(day2, m.utc_today().isoformat())
            self.assertEqual(m.get_setting_int(db, f"recheck_counter_value_{c1_id}", 0), 1)
            self.assertEqual(m.get_setting_int(db, f"recheck_counter_value_{c2_id}", 0), 2)
        finally:
            db.close()

        created_second = m.create_recheck_tasks_job()
        self.assertEqual(created_second, 0)

    def test_create_recheck_tasks_job_limit_is_targets_per_day_not_raw_checks(self):
        m = self.main
        db = m.SessionLocal()
        try:
            now = m.utc_now()
            camp = m.Campaign(name="goal_recheck_targets_limit", enabled=True, recheck_daily_limit=1, jitter_min_sec=0, jitter_max_sec=0)
            db.add(camp)
            db.commit()
            db.refresh(camp)
            camp_id = int(camp.id)

            target = m.Target(
                username="goal_recheck_targets_limit_nick",
                campaign_id=camp_id,
                status=m.TargetStatus.PENDING.value,
                required_senders=3,
            )
            db.add(target)
            db.commit()
            db.refresh(target)
            target_id = int(target.id)

            accounts = []
            for idx in range(3):
                acc = m.Account(
                    login=f"recheck_limit_acc_{idx}@example.com",
                    password="x",
                    enabled=True,
                    status="active",
                    epic_account_id=f"e{idx}",
                    device_id=f"d{idx}",
                    device_secret=f"s{idx}",
                    daily_limit=500,
                    today_sent=0,
                    active_windows_json="[]",
                )
                accounts.append(acc)
            db.add_all(accounts)
            db.commit()
            for acc in accounts:
                db.refresh(acc)

            for acc in accounts:
                db.add(
                    m.Task(
                        task_type="send_request",
                        status=m.TaskStatus.DONE.value,
                        campaign_id=camp_id,
                        account_id=int(acc.id),
                        target_id=target_id,
                        scheduled_for=now,
                        completed_at=now,
                    )
                )
            db.commit()
        finally:
            db.close()

        created = m.create_recheck_tasks_job()
        # One nick/day can spawn checks for all its senders.
        self.assertEqual(created, 3)

        db = m.SessionLocal()
        try:
            checks = db.query(m.Task).filter(m.Task.task_type == "check_status", m.Task.campaign_id == camp_id).all()
            self.assertEqual(len(checks), 3)
            self.assertEqual(m.get_setting_int(db, f"recheck_counter_value_{camp_id}", 0), 1)
        finally:
            db.close()

    def test_create_recheck_tasks_job_skips_disabled_goal(self):
        m = self.main
        db = m.SessionLocal()
        try:
            now = m.utc_now()
            acc = m.Account(
                login="recheck2@example.com",
                password="x",
                enabled=True,
                status="active",
                epic_account_id="e",
                device_id="d",
                device_secret="s",
                daily_limit=500,
                today_sent=0,
                active_windows_json="[]",
            )
            c1 = m.Campaign(name="goal_r3", enabled=True, recheck_daily_limit=5, jitter_min_sec=0, jitter_max_sec=0)
            c2 = m.Campaign(name="goal_r4", enabled=False, recheck_daily_limit=5, jitter_min_sec=0, jitter_max_sec=0)
            db.add_all([acc, c1, c2])
            db.commit()
            db.refresh(acc)
            db.refresh(c1)
            db.refresh(c2)

            t1 = m.Target(username="g3_u1", campaign_id=c1.id, status=m.TargetStatus.PENDING.value, required_senders=1)
            t2 = m.Target(username="g4_u1", campaign_id=c2.id, status=m.TargetStatus.PENDING.value, required_senders=1)
            db.add_all([t1, t2])
            db.commit()
            db.refresh(t1)
            db.refresh(t2)

            c1_id = int(c1.id)
            c2_id = int(c2.id)

            db.add_all([
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.DONE.value,
                    campaign_id=c1_id,
                    account_id=acc.id,
                    target_id=t1.id,
                    scheduled_for=now,
                    completed_at=now,
                ),
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.DONE.value,
                    campaign_id=c2_id,
                    account_id=acc.id,
                    target_id=t2.id,
                    scheduled_for=now,
                    completed_at=now,
                ),
            ])
            db.commit()
            c1_id = int(c1.id)
        finally:
            db.close()

        created = m.create_recheck_tasks_job()
        self.assertEqual(created, 1)

        db = m.SessionLocal()
        try:
            checks = db.query(m.Task).filter(m.Task.task_type == "check_status").all()
            self.assertEqual(len(checks), 1)
            self.assertEqual(checks[0].campaign_id, c1_id)
        finally:
            db.close()

    def test_create_recheck_tasks_job_includes_accepted_targets(self):
        m = self.main
        db = m.SessionLocal()
        try:
            now = m.utc_now()
            acc = m.Account(
                login="recheck3@example.com",
                password="x",
                enabled=True,
                status="active",
                epic_account_id="e",
                device_id="d",
                device_secret="s",
                daily_limit=500,
                today_sent=0,
                active_windows_json="[]",
            )
            c = m.Campaign(name="goal_r5", enabled=True, recheck_daily_limit=5, jitter_min_sec=0, jitter_max_sec=0)
            db.add_all([acc, c])
            db.commit()
            db.refresh(acc)
            db.refresh(c)

            t = m.Target(username="g5_u1", campaign_id=c.id, status=m.TargetStatus.ACCEPTED.value, required_senders=1)
            db.add(t)
            db.commit()
            db.refresh(t)

            db.add(
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.DONE.value,
                    campaign_id=int(c.id),
                    account_id=int(acc.id),
                    target_id=int(t.id),
                    scheduled_for=now,
                    completed_at=now,
                )
            )
            db.commit()
            camp_id = int(c.id)
            acc_id = int(acc.id)
            tgt_id = int(t.id)
        finally:
            db.close()

        created = m.create_recheck_tasks_job()
        self.assertEqual(created, 1)

        db = m.SessionLocal()
        try:
            check = db.query(m.Task).filter(
                m.Task.task_type == "check_status",
                m.Task.account_id == acc_id,
                m.Task.target_id == tgt_id,
                m.Task.campaign_id == camp_id,
            ).first()
            self.assertIsNotNone(check)
        finally:
            db.close()

    def test_process_check_status_requeues_send_when_friendship_lost(self):
        m = self.main
        db = m.SessionLocal()
        try:
            acc = m.Account(
                login="lost_friend@example.com",
                password="x",
                enabled=True,
                status="active",
                epic_account_id="e",
                device_id="d",
                device_secret="s",
                daily_limit=500,
                today_sent=0,
                active_windows_json="[]",
            )
            c = m.Campaign(name="goal_lost_friend", enabled=True, jitter_min_sec=0, jitter_max_sec=0)
            db.add_all([acc, c])
            db.commit()
            db.refresh(acc)
            db.refresh(c)

            t = m.Target(
                username="lost_friend_target",
                campaign_id=int(c.id),
                status=m.TargetStatus.ACCEPTED.value,
                required_senders=1,
                accepted_count=1,
            )
            db.add(t)
            db.commit()
            db.refresh(t)

            db.add(
                m.Task(
                    task_type="check_status",
                    status=m.TaskStatus.QUEUED.value,
                    campaign_id=int(c.id),
                    account_id=int(acc.id),
                    target_id=int(t.id),
                    scheduled_for=m.utc_now() - timedelta(seconds=1),
                    max_attempts=3,
                )
            )
            db.commit()
            camp_id = int(c.id)
            acc_id = int(acc.id)
            tgt_id = int(t.id)
        finally:
            db.close()

        with (
            mock.patch.object(m, "DRY_RUN", False),
            mock.patch.object(m, "enforce_api_rate_limit", return_value=(True, None, "")),
            mock.patch.object(
                m,
                "check_friend_status_with_device",
                return_value=SimpleNamespace(ok=True, code="rejected", message="not friends"),
            ),
        ):
            m.process_tasks_job()

        db = m.SessionLocal()
        try:
            resend = db.query(m.Task).filter(
                m.Task.task_type == "send_request",
                m.Task.campaign_id == camp_id,
                m.Task.account_id == acc_id,
                m.Task.target_id == tgt_id,
                m.Task.status == m.TaskStatus.QUEUED.value,
                m.Task.last_error == "recheck_resend",
            ).first()
            tgt = db.query(m.Target).filter(m.Target.id == tgt_id).first()
            self.assertIsNotNone(resend)
            self.assertEqual(tgt.status, m.TargetStatus.PENDING.value)
        finally:
            db.close()
