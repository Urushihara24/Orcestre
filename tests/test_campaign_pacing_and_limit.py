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

    created = m.db_exec(lambda db: m.create_tasks_for_new_targets(db, limit=100, campaign_id=int(camp_id)))
    assert created == 2

    db = m.SessionLocal()
    try:
        queued = (
            db.query(m.Task)
            .filter(
                m.Task.task_type == "send_request",
                m.Task.status == m.TaskStatus.QUEUED.value,
                m.Task.campaign_id == int(camp_id),
            )
            .order_by(m.Task.target_id.asc())
            .all()
        )
        assert len(queued) == 2
        # One sender should cover the full nick list in a layer.
        assert len({int(t.account_id) for t in queued}) == 1
        assert int(queued[0].account_id) == int(acc1_id)
    finally:
        db.close()
