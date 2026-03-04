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
