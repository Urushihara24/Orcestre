from datetime import timedelta


def test_campaign_effective_daily_limit_freezes_until_next_day(fresh_main):
    m = fresh_main
    db = m.SessionLocal()
    try:
        # Use stable in-day timestamp to avoid midnight edge flake.
        day_start, _ = m.local_day_bounds_utc_naive(db, m.utc_now())
        now = day_start + timedelta(hours=12)
        camp = m.Campaign(name="PaceGoal", daily_limit_per_account=10, enabled=True)
        db.add(camp)
        db.commit()
        db.refresh(camp)

        v1 = m._campaign_effective_daily_limit(db, camp, now)
        assert v1 == 10

        camp.daily_limit_per_account = 20
        db.commit()

        v2 = m._campaign_effective_daily_limit(db, camp, now + timedelta(hours=1))
        assert v2 == 10

        v3 = m._campaign_effective_daily_limit(db, camp, now + timedelta(days=1, hours=1))
        assert v3 == 20
    finally:
        db.close()


def test_sender_first_random_keeps_full_sender_layers(fresh_main, monkeypatch):
    m = fresh_main
    db = m.SessionLocal()
    camp_id = 0
    acc_ids = []
    try:
        now = m.utc_now()
        camp = m.Campaign(
            name="SenderRandomLayerGoal",
            enabled=True,
            daily_limit_per_account=10,
            target_senders_count=3,
            jitter_min_sec=5,
            jitter_max_sec=5,
            active_windows_json="[]",
        )
        db.add(camp)
        db.commit()
        db.refresh(camp)
        camp_id = int(camp.id)
        m.set_campaign_send_mode(db, int(camp.id), "sender_first")
        m.set_campaign_sender_pick_mode(db, int(camp.id), "random")

        for i in range(3):
            acc = m.Account(
                login=f"rand_layer_a{i+1}@example.com",
                password="x",
                enabled=True,
                status=m.AccountStatus.ACTIVE.value,
                epic_account_id=f"rle{i+1}",
                device_id=f"rld{i+1}",
                device_secret=f"rls{i+1}",
                daily_limit=10,
                today_sent=0,
                active_windows_json="[]",
                warmup_until=now - timedelta(minutes=1),
            )
            db.add(acc)
            db.flush()
            acc_ids.append(int(acc.id))

        db.add_all(
            [
                m.Target(
                    username="rand_layer_nick_1",
                    campaign_id=int(camp.id),
                    status=m.TargetStatus.NEW.value,
                    required_senders=3,
                ),
                m.Target(
                    username="rand_layer_nick_2",
                    campaign_id=int(camp.id),
                    status=m.TargetStatus.NEW.value,
                    required_senders=3,
                ),
            ]
        )
        db.commit()
    finally:
        db.close()

    # Deterministic "random": reverse sender order.
    monkeypatch.setattr(m.random, "shuffle", lambda arr: arr.reverse(), raising=True)
    created = m.db_exec(lambda db: m.create_tasks_for_new_targets(db, limit=500, campaign_id=int(camp_id)))
    assert created == 6

    db = m.SessionLocal()
    try:
        rows = (
            db.query(m.Task)
            .filter(
                m.Task.task_type == "send_request",
                m.Task.status == m.TaskStatus.QUEUED.value,
                m.Task.campaign_id == int(camp_id),
            )
            .order_by(m.Task.scheduled_for.asc(), m.Task.id.asc())
            .all()
        )
        assert len(rows) == 6
        per_acc = {}
        for t in rows:
            per_acc.setdefault(int(t.account_id), []).append(t.scheduled_for)

        assert set(per_acc.keys()) == set(acc_ids)
        assert all(len(v) == 2 for v in per_acc.values())
        # Sender blocks must not overlap even in random sender order.
        blocks = sorted((min(v), max(v), aid) for aid, v in per_acc.items())
        assert blocks[0][1] <= blocks[1][0]
        assert blocks[1][1] <= blocks[2][0]
    finally:
        db.close()


def test_new_send_planner_does_not_reuse_sender_done_today(fresh_main):
    m = fresh_main
    db = m.SessionLocal()
    camp_id = 0
    acc1_id = 0
    acc2_id = 0
    try:
        now = m.utc_now()
        camp = m.Campaign(
            name="NoReuseSenderTodayGoal",
            enabled=True,
            daily_limit_per_account=10,
            target_senders_count=1,
            jitter_min_sec=0,
            jitter_max_sec=0,
            active_windows_json="[]",
        )
        db.add(camp)
        db.commit()
        db.refresh(camp)
        camp_id = int(camp.id)
        m.set_campaign_send_mode(db, int(camp.id), "sender_first")
        m.set_campaign_sender_pick_mode(db, int(camp.id), "ordered")

        acc1 = m.Account(
            login="noreuse_a1@example.com",
            password="x",
            enabled=True,
            status=m.AccountStatus.ACTIVE.value,
            epic_account_id="nre1",
            device_id="nrd1",
            device_secret="nrs1",
            daily_limit=10,
            today_sent=0,
            active_windows_json="[]",
            warmup_until=now - timedelta(minutes=1),
        )
        acc2 = m.Account(
            login="noreuse_a2@example.com",
            password="x",
            enabled=True,
            status=m.AccountStatus.ACTIVE.value,
            epic_account_id="nre2",
            device_id="nrd2",
            device_secret="nrs2",
            daily_limit=10,
            today_sent=0,
            active_windows_json="[]",
            warmup_until=now - timedelta(minutes=1),
        )
        db.add_all([acc1, acc2])
        db.flush()
        acc1_id = int(acc1.id)
        acc2_id = int(acc2.id)

        old_t = m.Target(
            username="noreuse_old",
            campaign_id=int(camp.id),
            status=m.TargetStatus.ACCEPTED.value,
            required_senders=1,
        )
        t1 = m.Target(
            username="noreuse_new_1",
            campaign_id=int(camp.id),
            status=m.TargetStatus.NEW.value,
            required_senders=1,
        )
        t2 = m.Target(
            username="noreuse_new_2",
            campaign_id=int(camp.id),
            status=m.TargetStatus.NEW.value,
            required_senders=1,
        )
        db.add_all([old_t, t1, t2])
        db.flush()

        day_start, _ = m.local_day_bounds_utc_naive(db, now)
        db.add(
            m.Task(
                task_type="send_request",
                status=m.TaskStatus.DONE.value,
                campaign_id=int(camp.id),
                account_id=int(acc1.id),
                target_id=int(old_t.id),
                scheduled_for=day_start + timedelta(hours=1),
                completed_at=day_start + timedelta(hours=1, minutes=1),
                max_attempts=3,
            )
        )
        db.commit()
    finally:
        db.close()

    created = m.db_exec(lambda db: m.create_tasks_for_new_targets(db, limit=500, campaign_id=int(camp_id)))
    assert created == 2

    db = m.SessionLocal()
    try:
        rows = (
            db.query(m.Task.account_id, m.Task.target_id)
            .join(m.Target, m.Target.id == m.Task.target_id)
            .filter(
                m.Task.task_type == "send_request",
                m.Task.status == m.TaskStatus.QUEUED.value,
                m.Task.campaign_id == int(camp_id),
                m.Target.username.in_(["noreuse_new_1", "noreuse_new_2"]),
            )
            .all()
        )
        assert len(rows) == 2
        assert all(int(acc_id) == int(acc2_id) for acc_id, _ in rows)
        assert all(int(acc_id) != int(acc1_id) for acc_id, _ in rows)
    finally:
        db.close()


def test_campaign_pacing_gate_blocks_if_ahead_of_window(fresh_main):
    m = fresh_main
    db = m.SessionLocal()
    try:
        now_ref = m.utc_now()
        day_start, _ = m.local_day_bounds_utc_naive(db, now_ref)
        now = day_start + timedelta(hours=1)

        camp = m.Campaign(name="PaceGate", enabled=True, active_windows_json="[]")
        db.add(camp)
        db.commit()
        db.refresh(camp)

        # Planned today: 10 sends.
        for i in range(10):
            db.add(
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.QUEUED.value,
                    campaign_id=int(camp.id),
                    account_id=1,
                    target_id=100 + i,
                    scheduled_for=day_start + timedelta(hours=2, minutes=i),
                    max_attempts=3,
                )
            )
        # Already done too many too early.
        for i in range(8):
            db.add(
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.DONE.value,
                    campaign_id=int(camp.id),
                    account_id=1,
                    target_id=200 + i,
                    scheduled_for=day_start + timedelta(minutes=i),
                    completed_at=day_start + timedelta(minutes=30 + i),
                    max_attempts=3,
                )
            )
        db.commit()

        ok, next_at, reason = m._campaign_pacing_gate(db, camp, now)
        assert ok is False
        assert reason == "paced_by_window"
        assert next_at is not None
        assert next_at > now
    finally:
        db.close()


def test_sender_first_auto_raises_layer_limit_to_cover_all_nicks(fresh_main):
    m = fresh_main
    db = m.SessionLocal()
    camp_id = 0
    acc1_id = 0
    try:
        now = m.utc_now()
        camp = m.Campaign(
            name="LayerCoverGoal",
            enabled=True,
            daily_limit_per_account=1,  # intentionally lower than nick count
            target_senders_count=1,
            jitter_min_sec=0,
            jitter_max_sec=0,
            active_windows_json="[]",
        )
        db.add(camp)
        db.commit()
        db.refresh(camp)
        camp_id = int(camp.id)
        # Simulate production-like clamp that previously caused sender overlap.
        m.set_setting(db, "sender_switch_min_sec", "0")
        m.set_setting(db, "sender_switch_max_sec", "60")

        acc1 = m.Account(
            login="layer_a1@example.com",
            password="x",
            enabled=True,
            status=m.AccountStatus.ACTIVE.value,
            epic_account_id="ea1",
            device_id="d1",
            device_secret="s1",
            daily_limit=1,
            today_sent=0,
            active_windows_json="[]",
            warmup_until=now - timedelta(minutes=1),
        )
        acc2 = m.Account(
            login="layer_a2@example.com",
            password="x",
            enabled=True,
            status=m.AccountStatus.ACTIVE.value,
            epic_account_id="ea2",
            device_id="d2",
            device_secret="s2",
            daily_limit=1,
            today_sent=0,
            active_windows_json="[]",
            warmup_until=now - timedelta(minutes=1),
        )
        db.add_all([acc1, acc2])
        db.commit()
        db.refresh(acc1)
        db.refresh(acc2)
        acc1_id = int(acc1.id)

        t1 = m.Target(
            username="layer_nick_1",
            campaign_id=int(camp.id),
            status=m.TargetStatus.NEW.value,
            required_senders=1,
        )
        t2 = m.Target(
            username="layer_nick_2",
            campaign_id=int(camp.id),
            status=m.TargetStatus.NEW.value,
            required_senders=1,
        )
        db.add_all([t1, t2])
        db.commit()
    finally:
        db.close()


def test_sender_first_blocks_do_not_overlap_between_senders(fresh_main):
    m = fresh_main
    db = m.SessionLocal()
    camp_id = 0
    acc_ids = []
    try:
        now = m.utc_now()
        camp = m.Campaign(
            name="SenderBlockOrderGoal",
            enabled=True,
            daily_limit_per_account=10,
            target_senders_count=3,
            jitter_min_sec=7,
            jitter_max_sec=7,
            active_windows_json="[]",
        )
        db.add(camp)
        db.commit()
        db.refresh(camp)
        camp_id = int(camp.id)

        for i in range(3):
            acc = m.Account(
                login=f"order_a{i+1}@example.com",
                password="x",
                enabled=True,
                status=m.AccountStatus.ACTIVE.value,
                epic_account_id=f"oe{i+1}",
                device_id=f"od{i+1}",
                device_secret=f"os{i+1}",
                daily_limit=10,
                today_sent=0,
                active_windows_json="[]",
                warmup_until=now - timedelta(minutes=1),
            )
            db.add(acc)
            db.flush()
            acc_ids.append(int(acc.id))

        db.add_all(
            [
                m.Target(
                    username="order_nick_1",
                    campaign_id=int(camp.id),
                    status=m.TargetStatus.NEW.value,
                    required_senders=3,
                ),
                m.Target(
                    username="order_nick_2",
                    campaign_id=int(camp.id),
                    status=m.TargetStatus.NEW.value,
                    required_senders=3,
                ),
            ]
        )
        db.commit()
    finally:
        db.close()

    created = m.db_exec(lambda db: m.create_tasks_for_new_targets(db, limit=500, campaign_id=int(camp_id)))
    assert created == 6

    db = m.SessionLocal()
    try:
        rows = (
            db.query(m.Task)
            .filter(
                m.Task.task_type == "send_request",
                m.Task.status == m.TaskStatus.QUEUED.value,
                m.Task.campaign_id == int(camp_id),
            )
            .order_by(m.Task.scheduled_for.asc(), m.Task.id.asc())
            .all()
        )
        assert len(rows) == 6

        per_acc = {}
        for t in rows:
            per_acc.setdefault(int(t.account_id), []).append(t.scheduled_for)

        # Sender-first mode should assign one full nick layer per sender (2 nicks per sender here).
        assert set(per_acc.keys()) == set(acc_ids)
        assert all(len(v) == 2 for v in per_acc.values())

        # No overlap between sender blocks:
        # all tasks of sender#1 are scheduled not later than the first task of sender#2, etc.
        a1, a2, a3 = acc_ids
        assert max(per_acc[a1]) <= min(per_acc[a2])
        assert max(per_acc[a2]) <= min(per_acc[a3])
    finally:
        db.close()
