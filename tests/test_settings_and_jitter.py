from datetime import datetime, timezone


def test_set_get_setting_and_jitter_seconds(fresh_main):
    m = fresh_main
    db = m.SessionLocal()
    try:
        m.set_setting(db, "jitter_min_sec", "5")
        m.set_setting(db, "jitter_max_sec", "7")
        j = m.jitter_seconds_with_db(db)
        assert 5 <= j <= 7
    finally:
        db.close()


def test_next_daily_reset_utc(fresh_main):
    m = fresh_main

    # DAILY_RESET_HOUR_UTC is set by env fixture to 0 by default.
    now = datetime(2026, 2, 12, 0, 0, 0)  # exactly at reset
    nxt = m.next_daily_reset_utc(now)
    assert nxt > now

