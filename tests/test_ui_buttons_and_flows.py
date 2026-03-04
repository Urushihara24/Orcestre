import re
from pathlib import Path
from types import SimpleNamespace


class FakeBot:
    def __init__(self):
        self._next_id = 100
        self.sent = []
        self.docs = []
        self.answered = []
        self.next_steps = []

    def answer_callback_query(self, callback_query_id, *args, **kwargs):
        self.answered.append((callback_query_id, args, kwargs))

    def send_message(self, chat_id, text, **kwargs):
        self._next_id += 1
        msg = SimpleNamespace(message_id=self._next_id, chat=SimpleNamespace(id=chat_id))
        self.sent.append((chat_id, text, kwargs, msg.message_id))
        return msg

    def send_document(self, chat_id, document, **kwargs):
        self.docs.append((chat_id, kwargs))
        return True

    def delete_message(self, chat_id, message_id):
        return True

    def register_next_step_handler(self, msg, handler):
        # We don't execute step handlers automatically here; just store registration.
        self.next_steps.append((msg.message_id, handler))


class ImmediateThread:
    def __init__(self, target=None, args=(), kwargs=None, daemon=None):
        self._target = target
        self._args = args
        self._kwargs = kwargs or {}
        self.daemon = daemon

    def start(self):
        if self._target:
            self._target(*self._args, **self._kwargs)


class FakeTimer:
    def __init__(self, *args, **kwargs):
        pass

    def start(self):
        return None


def _call(chat_id=1, user_id=1, message_id=10, data=""):
    return SimpleNamespace(
        id="cbq-1",
        data=data,
        from_user=SimpleNamespace(id=user_id),
        message=SimpleNamespace(
            chat=SimpleNamespace(id=chat_id),
            message_id=message_id,
        ),
    )


def _msg(chat_id=1, user_id=1, message_id=10, text=""):
    return SimpleNamespace(
        chat=SimpleNamespace(id=chat_id),
        from_user=SimpleNamespace(id=user_id),
        message_id=message_id,
        text=text,
    )


def test_all_callback_buttons_have_test_dispatch(fresh_main):
    """
    Статический аудит: все callback_data, которые создаются в main.py,
    должны быть либо явными кнопками с тестовым вызовом, либо динамическими префиксами.
    """
    m = fresh_main
    src = Path(m.__file__).read_text(encoding="utf-8")
    raw = set(re.findall(r'callback_data="([^"]+)"', src))

    # normalize dynamic callbacks like acc_device_cancel:{account_id}
    normalized = set()
    dynamic_prefixes = {"acc_device_cancel:"}
    expected_static = {
        "acc_import",
        "acc_list",
        "acc_banned",
        "acc_paused",
        "acc_verify",
        "acc_device_auto",
        "tgt_import",
        "tgt_status",
        "tgt_distribute",
        "set_limit",
        "set_jitter",
        "set_windows",
        "set_target_senders",
        "set_timezone",
        "set_recheck_limit",
        "set_daily_repeat",
        "proxy_add",
        "proxy_list",
        "proxy_delete",
        "manage_tick",
        "manage_stop",
        "manage_start",
        "manage_status",
        "manage_export",
    }
    for x in raw:
        if "{" in x:
            # f-string template; keep prefix before '{'
            normalized.add(x.split("{", 1)[0])
        elif ":" in x and x.split(":", 1)[0] + ":" in dynamic_prefixes:
            normalized.add(x.split(":", 1)[0] + ":")
        else:
            normalized.add(x)
    expected_dynamic = {"acc_device_cancel:"}
    # These are created via f-strings, so static scan sees "acc_device_show_login:{...}" etc.
    expected_dynamic |= {"acc_device_show_login:", "acc_device_show_pass:"}

    unknown = sorted(normalized - expected_static - expected_dynamic)
    assert unknown == [], f"Unhandled callback_data in source: {unknown}"


def test_press_all_buttons_smoke(fresh_main, monkeypatch, tmp_path):
    """
    Дымовой тест: нажать все inline-кнопки и убедиться, что хэндлеры не падают.
    """
    m = fresh_main
    fb = FakeBot()
    monkeypatch.setattr(m, "bot", fb, raising=True)

    # Avoid background concurrency & heavy operations
    monkeypatch.setattr(m.threading, "Thread", ImmediateThread, raising=True)
    monkeypatch.setattr(m.threading, "Timer", FakeTimer, raising=True)
    monkeypatch.setattr(m, "verify_accounts_health_job", lambda: None, raising=True)
    monkeypatch.setattr(m, "process_tasks_job", lambda: None, raising=True)
    monkeypatch.setattr(m, "_start_device_auth_worker", lambda *a, **k: None, raising=True)

    # Export: make a tiny file and don't actually send it to Telegram
    export_path = tmp_path / "results.xlsx"
    export_path.write_bytes(b"x")
    monkeypatch.setattr(m, "export_results_to_excel", lambda: str(export_path), raising=True)

    # Provide one account row so handlers that query accounts have something
    def _seed(db):
        db.add(m.Account(login="user1", password="pass", status=m.AccountStatus.ACTIVE.value))
        db.commit()
    m.db_exec(_seed)

    # Accounts
    m.cb_acc_import(_call(data="acc_import"))
    m.cb_acc_list(_call(data="acc_list"))
    m.cb_acc_banned(_call(data="acc_banned"))
    m.cb_acc_paused(_call(data="acc_paused"))
    m.cb_acc_verify(_call(data="acc_verify"))
    m.cb_acc_device_auto(_call(data="acc_device_auto"))
    m.cb_acc_device_cancel(_call(data="acc_device_cancel:1", message_id=555))
    m.cb_acc_device_show_login(_call(data="acc_device_show_login:1", message_id=556))
    m.cb_acc_device_show_pass(_call(data="acc_device_show_pass:1", message_id=557))

    # Targets
    m.cb_tgt_import(_call(data="tgt_import"))
    m.cb_tgt_status(_call(data="tgt_status"))
    m.cb_tgt_distribute(_call(data="tgt_distribute"))

    # Settings
    m.cb_set_limit(_call(data="set_limit"))
    m.cb_set_jitter(_call(data="set_jitter"))
    m.cb_set_windows(_call(data="set_windows"))
    m.cb_set_target_senders(_call(data="set_target_senders"))
    m.cb_set_timezone(_call(data="set_timezone"))
    m.cb_set_recheck_limit(_call(data="set_recheck_limit"))
    m.cb_set_daily_repeat(_call(data="set_daily_repeat"))

    # Proxies
    m.cb_proxy_add(_call(data="proxy_add"))
    m.cb_proxy_list(_call(data="proxy_list"))
    m.cb_proxy_delete(_call(data="proxy_delete"))

    # Manage
    m.cb_manage_tick(_call(data="manage_tick"))
    m.cb_manage_stop(_call(data="manage_stop"))
    m.cb_manage_start(_call(data="manage_start"))
    m.cb_manage_status(_call(data="manage_status"))
    m.cb_manage_export(_call(data="manage_export"))


def test_step_handlers_basic_flows(fresh_main, monkeypatch):
    """
    Прогон основных step-хендлеров (без Telegram), чтобы проверить сценарии ввода.
    """
    m = fresh_main

    # Replace UI output with no-op
    monkeypatch.setattr(m, "show_menu_status", lambda *a, **k: None, raising=True)
    monkeypatch.setattr(m, "cleanup_step", lambda *a, **k: None, raising=True)

    # Seed account + proxy tables where needed
    def _seed(db):
        db.add(m.Account(id=5, login="acc5", password="pass", status=m.AccountStatus.ACTIVE.value))
        db.commit()
    m.db_exec(_seed)

    # set limit
    m.handle_set_limit(_msg(text="5"))
    m.handle_set_limit(_msg(text="-1"))
    m.handle_set_limit(_msg(text="nope"))

    # jitter
    m.handle_set_jitter(_msg(text="10 20"))
    m.handle_set_jitter(_msg(text="20 10"))
    m.handle_set_jitter(_msg(text="bad"))

    # windows
    m.handle_set_windows(_msg(text="5\ndays=1,2,3 from=10:00 to=22:00"))
    m.handle_set_windows(_msg(text="5\ndays=1,2,3 from=22:00 to=02:00"))
    m.handle_set_windows(_msg(text="bad"))

    # advanced settings
    m.handle_set_target_senders(_msg(text="10"))
    m.handle_set_target_senders(_msg(text="0"))
    m.handle_set_timezone(_msg(text="Europe/Moscow"))
    m.handle_set_timezone(_msg(text="Bad/Timezone"))
    m.handle_set_recheck_limit(_msg(text="200"))
    m.handle_set_recheck_limit(_msg(text="-1"))
    m.handle_set_daily_repeat(_msg(text="1"))
    m.handle_set_daily_repeat(_msg(text="0"))
    m.handle_set_daily_repeat(_msg(text="bad"))

    # proxy add/delete
    m.handle_proxy_add(_msg(text="http://127.0.0.1:8080"))
    m.handle_proxy_add(_msg(text="not-a-url"))
    m.handle_proxy_delete(_msg(text="1"))
    m.handle_proxy_delete(_msg(text="bad"))

    # targets import via step
    m.handle_import_targets(_msg(text="nick1\nnick2\nnick1"))
    m.handle_add_account_single(_msg(text="new@example.com:pass"))
    m.handle_add_account_single(_msg(text="badformat"))
    m.handle_delete_account_single(_msg(text="new@example.com"))
    m.handle_add_target_single(_msg(text="target_new"))
    m.handle_delete_target_single(_msg(text="target_new"))
    m.handle_show_target_senders(_msg(text="unknown_target"))


def test_add_same_nick_in_different_goals_via_ui_handler(fresh_main, monkeypatch):
    m = fresh_main
    monkeypatch.setattr(m, "cleanup_step", lambda *a, **k: None, raising=True)
    out = []
    monkeypatch.setattr(
        m,
        "show_menu_status",
        lambda chat_id, menu_key, status_text: out.append((chat_id, menu_key, status_text)),
        raising=True,
    )

    def _seed(db):
        c1 = m.Campaign(name="ui_goal_1", enabled=True, target_senders_count=1)
        c2 = m.Campaign(name="ui_goal_2", enabled=True, target_senders_count=1)
        db.add_all([c1, c2])
        db.commit()
        db.refresh(c1)
        db.refresh(c2)
        return int(c1.id), int(c2.id)

    c1_id, c2_id = m.db_exec(_seed)
    chat_id = 1001

    m.set_chat_ui_value(chat_id, "selected_campaign_id", c1_id)
    m.handle_add_target_single(_msg(chat_id=chat_id, text="same_ui_nick"))
    assert "✅" in out[-1][2]

    m.set_chat_ui_value(chat_id, "selected_campaign_id", c2_id)
    m.handle_add_target_single(_msg(chat_id=chat_id, text="same_ui_nick"))
    assert "✅" in out[-1][2]

    count = m.db_exec(lambda db: db.query(m.Target).filter(m.Target.username == "same_ui_nick").count())
    assert count == 2


def test_reply_nav_routes_new_pagination_search_and_progress(fresh_main, monkeypatch):
    m = fresh_main

    called = {"acc": [], "tgt": [], "progress": 0, "ask": []}

    monkeypatch.setattr(
        m,
        "show_accounts_list",
        lambda chat_id, page=1, query="": called["acc"].append((chat_id, page, query)),
        raising=True,
    )
    monkeypatch.setattr(
        m,
        "show_targets_status",
        lambda chat_id, page=1, query="": called["tgt"].append((chat_id, page, query)),
        raising=True,
    )
    monkeypatch.setattr(
        m,
        "show_campaign_progress",
        lambda chat_id: called.__setitem__("progress", called["progress"] + 1),
        raising=True,
    )
    monkeypatch.setattr(
        m,
        "ask_step",
        lambda message, prompt_text, next_handler, parse_mode=None: called["ask"].append(
            (message.chat.id, prompt_text, next_handler.__name__)
        ),
        raising=True,
    )

    chat_id = 777
    m.set_chat_ui_value(chat_id, "acc_page", 3)
    m.set_chat_ui_value(chat_id, "acc_query", "mail")
    m.set_chat_ui_value(chat_id, "tgt_page", 4)
    m.set_chat_ui_value(chat_id, "tgt_query", "nick")

    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="◀️ Акк"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="▶️ Акк"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="🔎 Акк поиск"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="◀️ Ники"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="▶️ Ники"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="🔎 Поиск ников"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="📈 Прогресс текущей цели"))

    assert called["acc"] == [(chat_id, 2, "mail"), (chat_id, 4, "mail")]
    assert called["tgt"] == [(chat_id, 3, "nick"), (chat_id, 5, "nick")]
    assert called["progress"] == 1
    assert called["ask"][0][2] == "handle_accounts_search_query"
    assert called["ask"][1][2] == "handle_targets_search_query"


def test_show_accounts_list_pagination_and_filter(fresh_main, monkeypatch):
    m = fresh_main
    fb = FakeBot()
    monkeypatch.setattr(m, "bot", fb, raising=True)
    monkeypatch.setattr(m.threading, "Timer", FakeTimer, raising=True)

    def _seed(db):
        for i in range(1, 61):
            db.add(
                m.Account(
                    login=f"user{i}@example.com",
                    password="pass",
                    status=m.AccountStatus.ACTIVE.value,
                    daily_limit=10,
                    today_sent=0,
                )
            )
        db.commit()

    m.db_exec(_seed)
    chat_id = 900

    m.show_accounts_list(chat_id, page=2, query="user")
    text = fb.sent[-1][1]
    assert "Страница: 2/3" in text
    assert "Фильтр: `user`" in text
    assert "#26" in text
    assert "#50" in text
    assert m.get_chat_ui_int(chat_id, "acc_page", 0) == 2
    assert m.get_chat_ui_value(chat_id, "acc_query", "") == "user"


def test_show_targets_status_and_campaign_progress(fresh_main, monkeypatch):
    m = fresh_main
    fb = FakeBot()
    monkeypatch.setattr(m, "bot", fb, raising=True)
    monkeypatch.setattr(m.threading, "Timer", FakeTimer, raising=True)

    def _seed(db):
        acc1 = m.Account(
            login="a1@example.com",
            password="x",
            enabled=True,
            status=m.AccountStatus.ACTIVE.value,
            epic_account_id="e1",
            device_id="d1",
            device_secret="s1",
            daily_limit=10,
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
            daily_limit=10,
            today_sent=0,
            active_windows_json="[]",
        )
        db.add_all([acc1, acc2])
        db.flush()

        t1 = m.Target(username="alpha", status=m.TargetStatus.PENDING.value, required_senders=2)
        t2 = m.Target(username="beta", status=m.TargetStatus.NEW.value, required_senders=1)
        db.add_all([t1, t2])
        db.flush()

        db.add(
            m.Task(
                task_type="send_request",
                status=m.TaskStatus.DONE.value,
                account_id=acc1.id,
                target_id=t1.id,
                scheduled_for=m.utc_now(),
                completed_at=m.utc_now(),
            )
        )
        db.commit()

    m.db_exec(_seed)
    chat_id = 901

    m.show_targets_status(chat_id, page=1, query="a")
    status_text = fb.sent[-1][1]
    assert "🎯 **Ники цели" in status_text
    assert "Фильтр: `a`" in status_text
    assert "Отправители: 1/2" in status_text

    m.show_campaign_progress(chat_id)
    progress_text = fb.sent[-1][1]
    assert "📈 Прогресс текущей цели" in progress_text
    assert "Целей: 2" in progress_text
    assert "Покрытие отправителей: 1/3" in progress_text


def test_show_targets_receiver_stats_lists_per_nick_metrics(fresh_main, monkeypatch):
    m = fresh_main
    fb = FakeBot()
    monkeypatch.setattr(m, "bot", fb, raising=True)
    monkeypatch.setattr(m.threading, "Timer", FakeTimer, raising=True)

    def _seed(db):
        camp = m.Campaign(name="NickStats", enabled=True)
        db.add(camp)
        db.commit()
        db.refresh(camp)

        t1 = m.Target(
            username="nick_a",
            campaign_id=int(camp.id),
            status=m.TargetStatus.SENT.value,
            sent_count=5,
            accepted_count=2,
        )
        t2 = m.Target(
            username="nick_b",
            campaign_id=int(camp.id),
            status=m.TargetStatus.REJECTED.value,
            sent_count=3,
            accepted_count=0,
        )
        db.add_all([t1, t2])
        db.flush()

        db.add(
            m.Task(
                task_type="send_request",
                status=m.TaskStatus.QUEUED.value,
                campaign_id=int(camp.id),
                account_id=1,
                target_id=int(t1.id),
                scheduled_for=m.utc_now(),
            )
        )
        db.add(
            m.Task(
                task_type="check_status",
                status=m.TaskStatus.QUEUED.value,
                campaign_id=int(camp.id),
                account_id=1,
                target_id=int(t1.id),
                scheduled_for=m.utc_now(),
            )
        )
        db.add(
            m.Task(
                task_type="check_status",
                status=m.TaskStatus.DONE.value,
                campaign_id=int(camp.id),
                account_id=1,
                target_id=int(t1.id),
                scheduled_for=m.utc_now(),
                completed_at=m.utc_now(),
                last_error="friend_status:accepted",
            )
        )
        db.commit()
        return int(camp.id)

    camp_id = m.db_exec(_seed)
    m.set_chat_ui_value(1, "selected_campaign_id", camp_id)
    m.show_targets_receiver_stats(1, page=1, query="")

    txt = fb.sent[-1][1]
    assert "Статусы по никам" in txt
    assert "nick_a" in txt
    assert "5/2/1/0" in txt
    assert "nick_b" in txt
    assert "3/0/0/1" in txt


def test_bulk_delete_accounts_and_targets(fresh_main, monkeypatch):
    m = fresh_main
    monkeypatch.setattr(m, "cleanup_step", lambda *a, **k: None, raising=True)
    captured = {"accounts": None, "targets": None}

    def _capture(chat_id, menu_key, status_text):
        captured[menu_key] = status_text

    monkeypatch.setattr(m, "show_menu_status", _capture, raising=True)

    def _seed(db):
        a1 = m.Account(login="bulk1@example.com", password="x", status=m.AccountStatus.ACTIVE.value)
        a2 = m.Account(login="bulk2@example.com", password="x", status=m.AccountStatus.ACTIVE.value)
        t1 = m.Target(username="bulk_target_1", status=m.TargetStatus.NEW.value)
        t2 = m.Target(username="bulk_target_2", status=m.TargetStatus.NEW.value)
        db.add_all([a1, a2, t1, t2])
        db.commit()
        return a1.id, a2.id, t1.id, t2.id

    ids = m.db_exec(_seed)
    a1_id, _, t1_id, _ = ids

    # account bulk delete: by id + login + one unknown
    m.handle_delete_account_single(
        _msg(
            text=f"{a1_id}\nbulk2@example.com\nunknown@example.com",
        )
    )
    assert "Удалено аккаунтов: 2" in captured["accounts"]
    assert "Не найдено: 1" in captured["accounts"]
    remain_acc = m.db_exec(lambda db: db.query(m.Account).count())
    assert remain_acc == 0

    # target bulk delete: by id + username + one unknown
    m.handle_delete_target_single(
        _msg(
            text=f"{t1_id}, bulk_target_2; missing_target",
        )
    )
    assert "Удалено целей: 2" in captured["targets"]
    assert "Не найдено: 1" in captured["targets"]
    remain_tgt = m.db_exec(lambda db: db.query(m.Target).count())
    assert remain_tgt == 0


def test_show_target_senders_prefers_display_name(fresh_main, monkeypatch):
    m = fresh_main
    monkeypatch.setattr(m, "cleanup_step", lambda *a, **k: None, raising=True)
    captured = {}
    monkeypatch.setattr(
        m,
        "show_menu_status",
        lambda chat_id, menu_key, status_text: captured.update({"menu": menu_key, "text": status_text}),
        raising=True,
    )

    def _seed(db):
        camp = m.Campaign(name="T1", enabled=True)
        db.add(camp)
        db.flush()
        acc = m.Account(
            login="sender_login@example.com",
            epic_display_name="SenderDisplay",
            password="x",
            enabled=True,
            status=m.AccountStatus.ACTIVE.value,
            epic_account_id="e1",
            device_id="d1",
            device_secret="s1",
            daily_limit=10,
            today_sent=0,
            active_windows_json="[]",
        )
        tgt = m.Target(
            username="sender_target",
            campaign_id=int(camp.id),
            status=m.TargetStatus.PENDING.value,
            required_senders=1,
        )
        db.add_all([acc, tgt])
        db.flush()
        db.add(
            m.Task(
                task_type="send_request",
                status=m.TaskStatus.DONE.value,
                campaign_id=int(camp.id),
                account_id=acc.id,
                target_id=tgt.id,
                scheduled_for=m.utc_now(),
                completed_at=m.utc_now(),
            )
        )
        db.commit()

    m.db_exec(_seed)
    m.handle_show_target_senders(_msg(text="sender_target"))
    assert "SenderDisplay (sender_login@example.com)" in captured["text"]
    assert "⏳" in captured["text"]


def test_show_target_senders_icons_for_check_result(fresh_main, monkeypatch):
    m = fresh_main
    monkeypatch.setattr(m, "cleanup_step", lambda *a, **k: None, raising=True)
    captured = {}
    monkeypatch.setattr(
        m,
        "show_menu_status",
        lambda chat_id, menu_key, status_text: captured.update({"menu": menu_key, "text": status_text}),
        raising=True,
    )

    def _seed(db):
        camp = m.Campaign(name="T2", enabled=True)
        db.add(camp)
        db.flush()
        acc1 = m.Account(
            login="a1@example.com",
            epic_display_name="A1",
            password="x",
            enabled=True,
            status=m.AccountStatus.ACTIVE.value,
            epic_account_id="e1",
            device_id="d1",
            device_secret="s1",
            daily_limit=10,
            today_sent=0,
            active_windows_json="[]",
        )
        acc2 = m.Account(
            login="a2@example.com",
            epic_display_name="A2",
            password="x",
            enabled=True,
            status=m.AccountStatus.ACTIVE.value,
            epic_account_id="e2",
            device_id="d2",
            device_secret="s2",
            daily_limit=10,
            today_sent=0,
            active_windows_json="[]",
        )
        tgt = m.Target(
            username="target_icons",
            campaign_id=int(camp.id),
            status=m.TargetStatus.ACCEPTED.value,
            required_senders=2,
        )
        db.add_all([acc1, acc2, tgt])
        db.flush()
        db.add(
            m.Task(
                task_type="send_request",
                status=m.TaskStatus.DONE.value,
                campaign_id=int(camp.id),
                account_id=acc1.id,
                target_id=tgt.id,
                scheduled_for=m.utc_now(),
                completed_at=m.utc_now(),
            )
        )
        db.add(
            m.Task(
                task_type="send_request",
                status=m.TaskStatus.DONE.value,
                campaign_id=int(camp.id),
                account_id=acc2.id,
                target_id=tgt.id,
                scheduled_for=m.utc_now(),
                completed_at=m.utc_now(),
            )
        )
        db.add(
            m.Task(
                task_type="check_status",
                status=m.TaskStatus.DONE.value,
                campaign_id=int(camp.id),
                account_id=acc1.id,
                target_id=tgt.id,
                scheduled_for=m.utc_now(),
                completed_at=m.utc_now(),
            )
        )
        db.commit()

    m.db_exec(_seed)
    m.handle_show_target_senders(_msg(text="target_icons"))
    txt = captured["text"]
    assert "✅ A1 (a1@example.com)" in txt
    assert "⏳ A2 (a2@example.com)" in txt


def test_clear_all_targets_with_confirmation(fresh_main, monkeypatch):
    m = fresh_main
    monkeypatch.setattr(m, "cleanup_step", lambda *a, **k: None, raising=True)
    captured = {}
    monkeypatch.setattr(
        m,
        "show_menu_status",
        lambda chat_id, menu_key, status_text: captured.update({"menu": menu_key, "text": status_text}),
        raising=True,
    )

    def _seed(db):
        acc = m.Account(login="sender@example.com", password="x", status=m.AccountStatus.ACTIVE.value)
        db.add(acc)
        db.flush()
        t = m.Target(username="to_clear", status=m.TargetStatus.NEW.value)
        db.add(t)
        db.flush()
        db.add(
            m.Task(
                task_type="send_request",
                status=m.TaskStatus.QUEUED.value,
                account_id=acc.id,
                target_id=t.id,
                scheduled_for=m.utc_now(),
            )
        )
        db.commit()

    m.db_exec(_seed)

    m.handle_clear_all_targets_confirm(_msg(text="нет"))
    assert "Отменено" in captured["text"]
    assert m.db_exec(lambda db: db.query(m.Target).count()) == 1

    m.handle_clear_all_targets_confirm(_msg(text="ОЧИСТИТЬ"))
    assert "Удалено целей: 1" in captured["text"]
    assert m.db_exec(lambda db: db.query(m.Target).count()) == 0
    assert m.db_exec(lambda db: db.query(m.Task).count()) == 0


def test_set_target_senders_updates_existing_targets_and_creates_missing_tasks(fresh_main, monkeypatch):
    m = fresh_main
    monkeypatch.setattr(m, "cleanup_step", lambda *a, **k: None, raising=True)
    out = {}
    monkeypatch.setattr(
        m,
        "show_menu_status",
        lambda chat_id, menu_key, status_text: out.update({"menu": menu_key, "text": status_text}),
        raising=True,
    )

    def _seed(db):
        # Two ready accounts with device_auth
        a1 = m.Account(
            login="u1@example.com",
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
        a2 = m.Account(
            login="u2@example.com",
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
        db.add_all([a1, a2])
        db.flush()

        tgt = m.Target(username="fanout_target", status=m.TargetStatus.NEW.value, required_senders=1)
        db.add(tgt)
        db.flush()

        # Existing assignment from one account
        db.add(
            m.Task(
                task_type="send_request",
                status=m.TaskStatus.DONE.value,
                account_id=a1.id,
                target_id=tgt.id,
                scheduled_for=m.utc_now(),
                completed_at=m.utc_now(),
            )
        )
        db.commit()

    m.db_exec(_seed)

    m.handle_set_target_senders(_msg(text="2"))
    assert "На 1 ник получателя: 2 аккаунтов" in out["text"]

    def _check(db):
        tgt = db.query(m.Target).filter(m.Target.username == "fanout_target").first()
        tasks = (
            db.query(m.Task)
            .filter(
                m.Task.target_id == tgt.id,
                m.Task.task_type == "send_request",
                m.Task.status.in_(
                    [
                        m.TaskStatus.QUEUED.value,
                        m.TaskStatus.POSTPONED.value,
                        m.TaskStatus.RUNNING.value,
                        m.TaskStatus.DONE.value,
                    ]
                ),
            )
            .all()
        )
        return int(tgt.required_senders), len({t.account_id for t in tasks})

    req, unique_accounts = m.db_exec(_check)
    assert req == 2
    assert unique_accounts == 2


def test_set_goal_send_mode_and_force_cycle(fresh_main, monkeypatch):
    m = fresh_main
    monkeypatch.setattr(m, "cleanup_step", lambda *a, **k: None, raising=True)
    out = {}
    monkeypatch.setattr(
        m,
        "show_menu_status",
        lambda chat_id, menu_key, status_text: out.update({"menu": menu_key, "text": status_text}),
        raising=True,
    )

    def _seed(db):
        camp = m.Campaign(name="force_mode", enabled=True, jitter_min_sec=0, jitter_max_sec=0)
        db.add(camp)
        db.flush()
        acc = m.Account(
            login="force_sender@example.com",
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
        t1 = m.Target(username="fm_1", campaign_id=int(camp.id), status=m.TargetStatus.NEW.value, required_senders=1)
        t2 = m.Target(username="fm_2", campaign_id=int(camp.id), status=m.TargetStatus.NEW.value, required_senders=1)
        db.add_all([acc, t1, t2])
        db.commit()
        return int(camp.id), int(acc.id)

    camp_id, acc_id = m.db_exec(_seed)
    m.set_chat_ui_value(1, "selected_campaign_id", camp_id)

    m.handle_set_goal_send_mode(_msg(text="2"))
    mode = m.db_exec(lambda db: m.get_campaign_send_mode(db, camp_id))
    assert mode == "target_first"

    m.handle_force_cycle_account(_msg(text=str(acc_id)))
    assert "Создано задач: 2" in out.get("text", "")
    send_tasks = m.db_exec(
        lambda db: db.query(m.Task).filter(
            m.Task.task_type == "send_request",
            m.Task.campaign_id == camp_id,
            m.Task.account_id == acc_id,
            m.Task.last_error == "manual_forced_cycle",
        ).count()
    )
    assert send_tasks == 2


def test_tgt_distribute_shows_reason_when_zero_created(fresh_main, monkeypatch):
    m = fresh_main
    captured = []
    monkeypatch.setattr(
        m,
        "show_menu_status",
        lambda chat_id, menu_key, status_text: captured.append((chat_id, menu_key, status_text)),
        raising=True,
    )

    class _ImmediateThread:
        def __init__(self, target=None, args=(), kwargs=None, daemon=None):
            self._target = target
            self._args = args
            self._kwargs = kwargs or {}

        def start(self):
            if self._target:
                self._target(*self._args, **self._kwargs)

    monkeypatch.setattr(m.threading, "Thread", _ImmediateThread, raising=True)
    monkeypatch.setattr(m, "create_tasks_for_new_targets", lambda db, limit=1000, campaign_id=None: 0, raising=True)

    def _seed(db):
        camp = m.Campaign(name="ZeroReasonCamp", enabled=True)
        db.add(camp)
        db.commit()
        db.refresh(camp)
        db.add(m.Target(username="nick_one", campaign_id=int(camp.id), status=m.TargetStatus.PENDING.value))
        db.commit()
        return int(camp.id)

    camp_id = m.db_exec(_seed)
    m.set_chat_ui_value(1, "selected_campaign_id", camp_id)

    call = SimpleNamespace(
        id="cbq-x",
        data="tgt_distribute",
        from_user=SimpleNamespace(id=1),
        message=SimpleNamespace(chat=SimpleNamespace(id=1), message_id=1),
    )
    m.cb_tgt_distribute(call)

    assert any("Создано задач: 0" in x[2] for x in captured)
    assert any("В очереди отправки" in x[2] for x in captured)


def test_reply_back_navigation_by_menu_context(fresh_main, monkeypatch):
    m = fresh_main
    called = []

    monkeypatch.setattr(m, "show_main_menu", lambda chat_id: called.append(("main", chat_id)), raising=True)
    monkeypatch.setattr(m, "show_targets_menu", lambda chat_id: called.append(("targets", chat_id)), raising=True)
    monkeypatch.setattr(m, "show_goal_manager_menu", lambda chat_id: called.append(("goal_manager", chat_id)), raising=True)
    monkeypatch.setattr(m, "show_selected_goal_menu", lambda chat_id: called.append(("goal_selected", chat_id)), raising=True)
    monkeypatch.setattr(m, "show_settings_menu", lambda chat_id: called.append(("settings", chat_id)), raising=True)

    chat_id = 4321

    m.set_current_menu(chat_id, "goal_edit")
    m.cmd_reply_nav(_msg(chat_id=chat_id, text="⬅️ Назад"))
    assert called[-1] == ("goal_selected", chat_id)

    m.set_current_menu(chat_id, "goal_selected")
    m.cmd_reply_nav(_msg(chat_id=chat_id, text="⬅️ Назад"))
    assert called[-1] == ("goal_manager", chat_id)

    m.set_current_menu(chat_id, "goal_manager")
    m.cmd_reply_nav(_msg(chat_id=chat_id, text="⬅️ Назад"))
    assert called[-1] == ("targets", chat_id)

    m.set_current_menu(chat_id, "settings")
    m.cmd_reply_nav(_msg(chat_id=chat_id, text="⬅️ Назад"))
    assert called[-1] == ("main", chat_id)


def test_show_menu_status_preserves_goal_context(fresh_main, monkeypatch):
    m = fresh_main
    fb = FakeBot()
    monkeypatch.setattr(m, "bot", fb, raising=True)
    monkeypatch.setattr(m.threading, "Timer", FakeTimer, raising=True)

    chat_id = 777
    m.set_current_menu(chat_id, "goal_selected")
    m.show_menu_status(chat_id, "targets", "test selected context")
    assert m.get_current_menu(chat_id) == "goal_selected"
    assert "Текущая цель" in fb.sent[-1][1]

    m.set_current_menu(chat_id, "goal_edit")
    m.show_menu_status(chat_id, "targets", "test edit context")
    assert m.get_current_menu(chat_id) == "goal_edit"
    assert "Редактирование цели" in fb.sent[-1][1]


def test_goal_edit_menu_routes_to_goal_specific_handlers(fresh_main, monkeypatch):
    m = fresh_main
    asked = []
    monkeypatch.setattr(
        m,
        "ask_step",
        lambda message, prompt_text, next_handler, parse_mode=None: asked.append((prompt_text, next_handler.__name__)),
        raising=True,
    )

    chat_id = 987
    m.set_current_menu(chat_id, "goal_edit")

    m.cmd_reply_nav(_msg(chat_id=chat_id, text="🔄 Лимит"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, text="⏱️ Джиттер"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, text="🕐 Окна"))

    assert asked[0][1] == "handle_set_goal_daily_limit"
    assert asked[1][1] == "handle_set_goal_jitter"
    assert asked[2][1] == "handle_set_goal_windows"
