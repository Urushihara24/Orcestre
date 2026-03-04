from types import SimpleNamespace


def test_send_requests_disabled_gate_prevents_real_send(fresh_main, monkeypatch):
    m = fresh_main

    # Force "real mode" logic but keep gate disabled.
    monkeypatch.setattr(m, "DRY_RUN", False, raising=True)
    monkeypatch.setattr(m, "SEND_REQUESTS_ENABLED", False, raising=True)

    # If code ever tries to touch Epic client, fail the test.
    def _boom(*args, **kwargs):
        raise AssertionError("Epic API should not be called when SEND_REQUESTS_ENABLED=0")

    monkeypatch.setattr(m, "send_friend_request_with_device", _boom, raising=True)
    monkeypatch.setattr(m, "check_friend_status_with_device", _boom, raising=True)

    now = m.utc_now()

    def _seed(db):
        acc = m.Account(
            id=1,
            login="acc1",
            password="pass",
            status=m.AccountStatus.ACTIVE.value,
            enabled=True,
            daily_limit=10,
            today_sent=0,
            epic_account_id="epic-1",
            device_id="dev-1",
            device_secret="sec-1",
        )
        tgt = m.Target(id=1, username="target1", status=m.TargetStatus.NEW.value, max_attempts=3)
        task = m.Task(
            id=1,
            task_type="send_request",
            status=m.TaskStatus.QUEUED.value,
            account_id=1,
            target_id=1,
            scheduled_for=now,
            max_attempts=3,
        )
        db.add_all([acc, tgt, task])
        db.commit()

    m.db_exec(_seed)

    m.process_tasks_job()

    def _check(db):
        t = db.query(m.Task).filter(m.Task.id == 1).first()
        return t.status, t.last_error

    status, last_error = m.db_exec(_check)
    assert status == m.TaskStatus.POSTPONED.value
    assert last_error == "send_requests_disabled"

