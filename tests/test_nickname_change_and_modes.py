from types import SimpleNamespace


def _msg(chat_id=1, user_id=1, text=""):
    return SimpleNamespace(
        chat=SimpleNamespace(id=chat_id),
        from_user=SimpleNamespace(id=user_id),
        message_id=100,
        text=text,
    )


def _doc_msg(chat_id=1, user_id=1, file_name="nicks.txt", file_id="F1"):
    return SimpleNamespace(
        chat=SimpleNamespace(id=chat_id),
        from_user=SimpleNamespace(id=user_id),
        message_id=999,
        document=SimpleNamespace(file_id=file_id, file_name=file_name),
    )


class FakeBot:
    def __init__(self, content: bytes):
        self._content = content

    def delete_message(self, chat_id, message_id):
        return None

    def get_file(self, file_id):
        return SimpleNamespace(file_path=f"files/{file_id}")

    def download_file(self, file_path):
        return self._content


def test_import_nickname_change_tasks_text(fresh_main, tmp_path):
    m = fresh_main

    def _seed(db):
        db.add(
            m.Account(
                login="acc1@example.com",
                password="x",
                status=m.AccountStatus.ACTIVE.value,
                enabled=True,
                epic_account_id="epic-1",
                device_id="dev-1",
                device_secret="sec-1",
            )
        )
        db.commit()

    m.db_exec(_seed)
    p = tmp_path / "nick_change.txt"
    p.write_text("acc1@example.com;NewNick_01\n", encoding="utf-8")
    added, skipped, errors = m.import_nickname_change_tasks(str(p), source_file="nick_change.txt")
    assert added == 1
    assert skipped == 0
    assert errors == 0


def test_process_nickname_change_tasks_job_dry_run_updates_display_name(fresh_main):
    m = fresh_main
    now = m.utc_now()

    def _seed(db):
        acc = m.Account(
            id=1,
            login="acc1@example.com",
            password="x",
            status=m.AccountStatus.ACTIVE.value,
            enabled=True,
            epic_account_id="epic-1",
            device_id="dev-1",
            device_secret="sec-1",
        )
        db.add(acc)
        db.commit()
        db.add(
            m.NicknameChangeTask(
                account_id=1,
                requested_nick="DryRunNick_1",
                status=m.NicknameChangeStatus.QUEUED.value,
                scheduled_for=now,
            )
        )
        db.commit()

    m.db_exec(_seed)
    processed = m.process_nickname_change_tasks_job()
    assert processed >= 1

    def _check(db):
        acc = db.query(m.Account).filter(m.Account.id == 1).first()
        task = db.query(m.NicknameChangeTask).first()
        return acc.epic_display_name, task.status, task.final_nick

    disp, status, final_nick = m.db_exec(_check)
    assert disp == "DryRunNick_1"
    assert status == m.NicknameChangeStatus.DONE.value
    assert final_nick == "DryRunNick_1"


def test_new_send_requests_disabled_blocks_normal_send_but_allows_recheck_resend(fresh_main):
    m = fresh_main
    now = m.utc_now()

    def _seed(db):
        m.set_setting(db, "new_send_requests_enabled", "0")
        m.set_setting(db, "recheck_only_mode_enabled", "1")
        db.add(
            m.Account(
                id=1,
                login="acc1@example.com",
                password="x",
                status=m.AccountStatus.ACTIVE.value,
                enabled=True,
                epic_account_id="epic-1",
                device_id="dev-1",
                device_secret="sec-1",
                daily_limit=100,
            )
        )
        db.add(m.Target(id=1, username="target_1", status=m.TargetStatus.NEW.value))
        db.add(m.Target(id=2, username="target_2", status=m.TargetStatus.NEW.value))
        db.add(
            m.Task(
                id=1,
                task_type="send_request",
                status=m.TaskStatus.QUEUED.value,
                account_id=1,
                target_id=1,
                scheduled_for=now,
                max_attempts=3,
                last_error="",
            )
        )
        db.add(
            m.Task(
                id=2,
                task_type="send_request",
                status=m.TaskStatus.QUEUED.value,
                account_id=1,
                target_id=2,
                scheduled_for=now,
                max_attempts=3,
                last_error="recheck_resend",
            )
        )
        db.commit()

    m.db_exec(_seed)
    m.process_tasks_job()

    def _check(db):
        t1 = db.query(m.Task).filter(m.Task.id == 1).first()
        t2 = db.query(m.Task).filter(m.Task.id == 2).first()
        return (t1.status, t1.last_error), (t2.status, t2.last_error)

    (s1, e1), (s2, e2) = m.db_exec(_check)
    assert s1 == m.TaskStatus.POSTPONED.value
    assert e1 == "new_send_requests_disabled"
    assert s2 == m.TaskStatus.DONE.value


def test_enqueue_goal_friend_presence_checks_and_resend_missing(fresh_main):
    m = fresh_main
    now = m.utc_now()

    def _seed(db):
        camp = m.Campaign(id=1, name="GoalA", enabled=True, target_senders_count=1)
        acc = m.Account(
            id=1,
            login="acc1@example.com",
            password="x",
            status=m.AccountStatus.ACTIVE.value,
            enabled=True,
            epic_account_id="epic-1",
            device_id="dev-1",
            device_secret="sec-1",
        )
        tgt = m.Target(id=1, username="target_1", campaign_id=1, status=m.TargetStatus.SENT.value, required_senders=1)
        db.add_all([camp, acc, tgt])
        db.commit()
        db.add(
            m.Task(
                task_type="send_request",
                status=m.TaskStatus.DONE.value,
                campaign_id=1,
                account_id=1,
                target_id=1,
                scheduled_for=now,
                completed_at=now,
            )
        )
        db.commit()

    m.db_exec(_seed)
    m.set_chat_ui_value(1, "selected_campaign_id", 1)
    queued_check, _, _ = m.enqueue_goal_friend_presence_checks(chat_id=1)
    assert queued_check == 1
    queued_send, _, _ = m.enqueue_goal_resend_missing(chat_id=1)
    assert queued_send == 1


def test_handle_document_routes_to_nickname_change_import_mode(fresh_main, monkeypatch):
    m = fresh_main
    monkeypatch.setattr(m, "bot", FakeBot(content=b"acc@example.com;Nick_123"), raising=True)
    m.set_current_menu(1, "nick_change_import")
    out = {}
    monkeypatch.setattr(m, "import_nickname_change_tasks", lambda path, source_file="": (2, 1, 0), raising=True)
    monkeypatch.setattr(
        m,
        "show_menu_status",
        lambda chat_id, menu_key, status_text: out.update({"menu_key": menu_key, "status_text": status_text}),
        raising=True,
    )
    m.handle_document(_doc_msg(file_name="nick_change.txt"))
    assert out["menu_key"] == "accounts"
    assert "смены ников" in out["status_text"]
