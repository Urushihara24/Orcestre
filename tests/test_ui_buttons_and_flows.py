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


def test_inline_action_router_dispatches_to_text_navigation(fresh_main, monkeypatch):
    m = fresh_main
    routed = {}
    monkeypatch.setattr(
        m,
        "cmd_reply_nav",
        lambda msg: routed.update(
            {
                "chat_id": msg.chat.id,
                "user_id": msg.from_user.id,
                "text": msg.text,
            }
        ),
        raising=True,
    )
    m.cb_inline_action_nav(_call(chat_id=77, user_id=1, data="act:main_accounts"))
    assert routed == {"chat_id": 77, "user_id": 1, "text": "👥 Аккаунты"}


def test_main_keyboards_are_inline(fresh_main):
    m = fresh_main
    assert isinstance(m.kb_main_reply(), m.types.InlineKeyboardMarkup)
    assert isinstance(m.kb_auth_operator_reply(), m.types.InlineKeyboardMarkup)
    assert isinstance(m.kb_accounts_reply(), m.types.InlineKeyboardMarkup)
    assert isinstance(m.kb_targets_reply(), m.types.InlineKeyboardMarkup)
    assert isinstance(m.kb_goal_manager_reply(), m.types.InlineKeyboardMarkup)
    assert isinstance(m.kb_goal_selected_reply(), m.types.InlineKeyboardMarkup)
    assert isinstance(m.kb_goal_nicks_reply(), m.types.InlineKeyboardMarkup)
    assert isinstance(m.kb_goal_sending_reply(), m.types.InlineKeyboardMarkup)
    assert isinstance(m.kb_goal_ops_reply(), m.types.InlineKeyboardMarkup)
    assert isinstance(m.kb_goal_edit_reply(), m.types.InlineKeyboardMarkup)
    assert isinstance(m.kb_settings_reply(), m.types.InlineKeyboardMarkup)
    assert isinstance(m.kb_manage_reply(), m.types.InlineKeyboardMarkup)
    assert isinstance(m.kb_proxy_reply(), m.types.InlineKeyboardMarkup)


def test_inline_action_router_smoke_for_all_action_codes(fresh_main, monkeypatch):
    m = fresh_main
    seen = []
    monkeypatch.setattr(m, "cmd_reply_nav", lambda msg: seen.append(msg.text), raising=True)
    for code in sorted(m.INLINE_ACTION_TEXT.keys()):
        m.cb_inline_action_nav(_call(chat_id=1, user_id=1, data=f"act:{code}"))
    assert set(seen) == set(m.INLINE_ACTION_TEXT.values())


def test_auth_operator_act_access_scope(fresh_main):
    m = fresh_main

    def _seed(db):
        m.set_setting(db, m.AUTH_OPERATOR_IDS_SETTING_KEY, "77")

    m.db_exec(_seed)
    assert m._can_use_callback(77, "act:acc_list") is True
    assert m._can_use_callback(77, "act:acc_device") is True
    assert m._can_use_callback(77, "act:main_targets") is False
    assert m._can_use_callback(77, "act:set_api_limits") is False


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

    called = {"acc": [], "tgt": [], "tgt_stats": [], "progress": 0, "ask": []}

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
        "show_targets_receiver_stats",
        lambda chat_id, page=1, query="": called["tgt_stats"].append((chat_id, page, query)),
        raising=True,
    )
    monkeypatch.setattr(
        m,
        "show_campaign_progress",
        lambda *args, **kwargs: called.__setitem__("progress", called["progress"] + 1),
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

    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="◀️ Аккаунты"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="▶️ Аккаунты"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="🔎 Поиск аккаунтов"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="◀️ Ники"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="▶️ Ники"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="🔎 Поиск ников"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="📊 Статистика цели"))

    assert called["acc"] == [(chat_id, 2, "mail"), (chat_id, 4, "mail")]
    assert called["tgt"] == [(chat_id, 3, "nick"), (chat_id, 5, "nick")]
    assert called["tgt_stats"] == []
    assert called["progress"] == 1
    assert called["ask"][0][2] == "handle_accounts_search_query"
    assert called["ask"][1][2] == "handle_targets_search_query"

    # Pagination in receiver stats mode should stay in receiver stats screen.
    m.set_chat_ui_value(chat_id, "tgt_view_mode", "receiver_stats")
    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="◀️ Ники"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="▶️ Ники"))
    assert called["tgt_stats"] == [(chat_id, 3, "nick"), (chat_id, 5, "nick")]


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
    assert "Отправители (сегодня/лимит): 1/2" in status_text

    m.show_campaign_progress(chat_id)
    progress_text = fb.sent[-1][1]
    assert "📈 Прогресс текущей цели" in progress_text
    assert "Целей: 2" in progress_text
    assert "Покрытие отправителей (всего): 1/3" in progress_text
    assert "Самый покрытый ник: 1/2 отправителей" in progress_text
    assert "Осталось до исчерпания пула: 1" in progress_text


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
    assert "5/1/1/0" in txt
    assert "nick_b" in txt
    assert "3/0/0/1" in txt


def test_goal_nicks_keyboard_uses_unified_page_arrows(fresh_main):
    m = fresh_main
    kb = m.kb_goal_nicks_reply()
    texts = []
    for row in kb.keyboard:
        for btn in row:
            texts.append(str(getattr(btn, "text", "")))
    assert "◀️ Страница" in texts
    assert "▶️ Страница" in texts
    assert "◀️ Ники" not in texts
    assert "▶️ Ники" not in texts
    assert "◀️ Отправители" not in texts
    assert "▶️ Отправители" not in texts


def test_goal_selected_keyboard_is_grouped(fresh_main):
    m = fresh_main
    kb = m.kb_goal_selected_reply()
    texts = []
    for row in kb.keyboard:
        for btn in row:
            texts.append(str(getattr(btn, "text", "")))
    assert "👥 Ники" in texts
    assert "🚀 Отправка" in texts
    assert "🧹 Операции" in texts


def test_unified_page_arrows_route_to_senders_context(fresh_main, monkeypatch):
    m = fresh_main
    calls = []
    monkeypatch.setattr(
        m,
        "show_target_senders_page",
        lambda chat_id, target_id, page=1: calls.append((chat_id, target_id, page)),
        raising=True,
    )
    chat_id = 2020
    m.set_chat_ui_value(chat_id, "goal_page_context", "target_senders")
    m.set_chat_ui_value(chat_id, "senders_target_id", 55)
    m.set_chat_ui_value(chat_id, "senders_page", 3)

    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="◀️ Страница"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, user_id=1, text="▶️ Страница"))
    assert calls == [(chat_id, 55, 2), (chat_id, 55, 4)]


def test_handle_targets_search_query_routes_by_current_view_mode(fresh_main, monkeypatch):
    m = fresh_main
    called = {"targets": [], "stats": []}

    monkeypatch.setattr(m, "cleanup_step", lambda *a, **k: None, raising=True)
    monkeypatch.setattr(
        m,
        "show_targets_status",
        lambda chat_id, page=1, query="": called["targets"].append((chat_id, page, query)),
        raising=True,
    )
    monkeypatch.setattr(
        m,
        "show_targets_receiver_stats",
        lambda chat_id, page=1, query="": called["stats"].append((chat_id, page, query)),
        raising=True,
    )

    chat_id = 1234
    msg = _msg(chat_id=chat_id, text="fox")

    m.set_chat_ui_value(chat_id, "tgt_view_mode", "targets")
    m.handle_targets_search_query(msg)
    m.set_chat_ui_value(chat_id, "tgt_view_mode", "receiver_stats")
    m.handle_targets_search_query(msg)

    assert called["targets"] == [(chat_id, 1, "fox")]
    assert called["stats"] == [(chat_id, 1, "fox")]


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
    assert "Удалено ников получателя: 2" in captured["targets"]
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
    assert "Удалено ников получателя: 1" in captured["text"]
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


def test_set_goal_jitter_rebuilds_send_queue_without_stale_tasks(fresh_main, monkeypatch):
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
        camp = m.Campaign(
            name="rebuild_jitter_goal",
            enabled=True,
            target_senders_count=1,
            jitter_min_sec=0,
            jitter_max_sec=0,
        )
        db.add(camp)
        db.flush()
        a1 = m.Account(
            login="rj_1@example.com",
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
            login="rj_2@example.com",
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
        t = m.Target(
            username="rebuild_jitter_target",
            campaign_id=int(camp.id),
            status=m.TargetStatus.NEW.value,
            required_senders=1,
        )
        db.add_all([a1, a2, t])
        db.flush()
        # Stale queue tail: 2 queued tasks for target with required_senders=1
        db.add_all(
            [
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.QUEUED.value,
                    campaign_id=int(camp.id),
                    account_id=int(a1.id),
                    target_id=int(t.id),
                    scheduled_for=m.utc_now(),
                    max_attempts=3,
                ),
                m.Task(
                    task_type="send_request",
                    status=m.TaskStatus.POSTPONED.value,
                    campaign_id=int(camp.id),
                    account_id=int(a2.id),
                    target_id=int(t.id),
                    scheduled_for=m.utc_now(),
                    max_attempts=3,
                ),
            ]
        )
        db.commit()
        return int(camp.id), int(t.id)

    camp_id, target_id = m.db_exec(_seed)
    m.set_chat_ui_value(1, "selected_campaign_id", camp_id)
    m.handle_set_goal_jitter(_msg(text="10 20"))

    assert "Очищено старых задач" in out.get("text", "")
    assert "Добавлено недостающих задач" in out.get("text", "")

    def _check(db):
        active = (
            db.query(m.Task)
            .filter(
                m.Task.task_type == "send_request",
                m.Task.campaign_id == camp_id,
                m.Task.target_id == target_id,
                m.Task.status.in_(
                    [
                        m.TaskStatus.QUEUED.value,
                        m.TaskStatus.POSTPONED.value,
                        m.TaskStatus.RUNNING.value,
                    ]
                ),
            )
            .all()
        )
        return len(active), len({int(x.account_id) for x in active})

    active_count, unique_accounts = m.db_exec(_check)
    assert active_count == 1
    assert unique_accounts == 1


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
    # Goal mode change rebuilds queue; clear it to isolate force-cycle behavior.
    def _clear_send_queue(db):
        db.query(m.Task).filter(
            m.Task.task_type == "send_request",
            m.Task.campaign_id == camp_id,
        ).delete(synchronize_session=False)
        db.commit()
    m.db_exec(_clear_send_queue)

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


def test_set_goal_sender_pick_mode_and_force_cycle_random(fresh_main, monkeypatch):
    m = fresh_main
    monkeypatch.setattr(m, "cleanup_step", lambda *a, **k: None, raising=True)
    monkeypatch.setattr(m.random, "shuffle", lambda arr: arr.reverse(), raising=True)
    out = {}
    monkeypatch.setattr(
        m,
        "show_menu_status",
        lambda chat_id, menu_key, status_text: out.update({"menu": menu_key, "text": status_text}),
        raising=True,
    )

    def _seed(db):
        camp = m.Campaign(name="force_random_mode", enabled=True, jitter_min_sec=0, jitter_max_sec=0)
        db.add(camp)
        db.flush()
        a1 = m.Account(
            login="fr_1@example.com",
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
            login="fr_2@example.com",
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
        t1 = m.Target(username="fr_t1", campaign_id=int(camp.id), status=m.TargetStatus.NEW.value, required_senders=1)
        t2 = m.Target(username="fr_t2", campaign_id=int(camp.id), status=m.TargetStatus.NEW.value, required_senders=1)
        db.add_all([a1, a2, t1, t2])
        db.commit()
        return int(camp.id), int(a1.id), int(a2.id)

    camp_id, _, acc2_id = m.db_exec(_seed)
    m.set_chat_ui_value(1, "selected_campaign_id", camp_id)

    m.handle_set_goal_sender_pick_mode(_msg(text="2"))
    sender_pick = m.db_exec(lambda db: m.get_campaign_sender_pick_mode(db, camp_id))
    assert sender_pick == "random"
    # Sender pick mode change rebuilds queue; clear it to isolate force-cycle behavior.
    def _clear_send_queue(db):
        db.query(m.Task).filter(
            m.Task.task_type == "send_request",
            m.Task.campaign_id == camp_id,
        ).delete(synchronize_session=False)
        db.commit()
    m.db_exec(_clear_send_queue)

    m.handle_force_cycle_random(_msg(text="ok"))
    assert f"Выбран аккаунт: #{acc2_id}" in out.get("text", "")
    send_tasks = m.db_exec(
        lambda db: db.query(m.Task).filter(
            m.Task.task_type == "send_request",
            m.Task.campaign_id == camp_id,
            m.Task.account_id == acc2_id,
            m.Task.last_error == "manual_forced_cycle",
        ).count()
    )
    assert send_tasks == 2


def test_force_cycle_replaces_known_connected_sender(fresh_main):
    m = fresh_main

    def _seed(db):
        camp = m.Campaign(name="force_replace_connected", enabled=True, jitter_min_sec=0, jitter_max_sec=0)
        db.add(camp)
        db.flush()
        a1 = m.Account(
            login="fc1@example.com",
            password="x",
            enabled=True,
            status=m.AccountStatus.ACTIVE.value,
            epic_account_id="fce1",
            device_id="fcd1",
            device_secret="fcs1",
            daily_limit=100,
            today_sent=0,
            active_windows_json="[]",
        )
        a2 = m.Account(
            login="fc2@example.com",
            password="x",
            enabled=True,
            status=m.AccountStatus.ACTIVE.value,
            epic_account_id="fce2",
            device_id="fcd2",
            device_secret="fcs2",
            daily_limit=100,
            today_sent=0,
            active_windows_json="[]",
        )
        t1 = m.Target(
            username="fc_target_1",
            campaign_id=int(camp.id),
            status=m.TargetStatus.PENDING.value,
            required_senders=1,
        )
        db.add_all([a1, a2, t1])
        db.flush()

        # Known connected pair for a1 -> t1.
        db.add(
            m.Task(
                task_type="check_status",
                status=m.TaskStatus.DONE.value,
                campaign_id=int(camp.id),
                account_id=int(a1.id),
                target_id=int(t1.id),
                scheduled_for=m.utc_now(),
                completed_at=m.utc_now(),
                last_error="friend_status:accepted",
            )
        )
        db.commit()
        return int(camp.id), int(a1.id), int(a2.id), int(t1.id)

    camp_id, a1_id, a2_id, tgt_id = m.db_exec(_seed)

    def _run(db):
        camp = db.query(m.Campaign).filter(m.Campaign.id == camp_id).first()
        acc = db.query(m.Account).filter(m.Account.id == a1_id).first()
        result = m._create_manual_force_cycle_for_account(db, camp, acc)
        db.commit()
        return result

    res = m.db_exec(_run)
    assert res.get("ok") is True
    assert int(res.get("created", 0)) == 0
    # Connected sender does not count as covered until real DONE send_request.
    assert int(res.get("replaced_connected", 0)) == 1

    queued = m.db_exec(
        lambda db: db.query(m.Task).filter(
            m.Task.task_type == "send_request",
            m.Task.campaign_id == camp_id,
            m.Task.account_id == a2_id,
            m.Task.target_id == tgt_id,
            m.Task.status == m.TaskStatus.QUEUED.value,
            m.Task.last_error == "replacement_after_manual_connected",
        ).count()
    )
    assert queued == 1


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


def test_recheck_uses_campaign_send_mode_for_planner_strategy(fresh_main, monkeypatch):
    m = fresh_main
    captured = {}

    class _FakePlanner:
        def __init__(self, mode="sender", shuffle_groups=False, shuffle_inside_group=True, seed=None):
            captured["mode"] = mode
            captured["shuffle_groups"] = bool(shuffle_groups)
            captured["shuffle_inside_group"] = bool(shuffle_inside_group)
            self._pairs = []

        def build(self, pairs):
            self._pairs = list(pairs)

        def pop_many(self, limit):
            return list(self._pairs)[: int(limit)]

    monkeypatch.setattr(m, "RecheckQueuePlanner", _FakePlanner, raising=True)

    def _seed(db):
        now = m.utc_now()
        acc = m.Account(
            login="strategy_recheck@example.com",
            password="x",
            enabled=True,
            status=m.AccountStatus.ACTIVE.value,
            epic_account_id="e",
            device_id="d",
            device_secret="s",
            daily_limit=500,
            today_sent=0,
            active_windows_json="[]",
        )
        camp = m.Campaign(name="strategy_recheck_goal", enabled=True, recheck_daily_limit=5, jitter_min_sec=0, jitter_max_sec=0)
        db.add_all([acc, camp])
        db.commit()
        db.refresh(acc)
        db.refresh(camp)

        m.set_campaign_send_mode(db, int(camp.id), "target_first")
        db.commit()

        tgt = m.Target(username="strategy_target", campaign_id=int(camp.id), status=m.TargetStatus.PENDING.value, required_senders=1)
        db.add(tgt)
        db.commit()
        db.refresh(tgt)

        db.add(
            m.Task(
                task_type="send_request",
                status=m.TaskStatus.DONE.value,
                campaign_id=int(camp.id),
                account_id=int(acc.id),
                target_id=int(tgt.id),
                scheduled_for=now,
                completed_at=now,
            )
        )
        db.commit()

    m.db_exec(_seed)
    created = m.create_recheck_tasks_job()
    assert created >= 1
    assert captured["mode"] == "nickname"
    assert captured["shuffle_groups"] is True
    assert captured["shuffle_inside_group"] is False


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


def test_unknown_text_fallback_returns_to_current_menu(fresh_main, monkeypatch):
    m = fresh_main
    called = []
    chat_id = 9901
    monkeypatch.setattr(m, "show_selected_goal_menu", lambda cid: called.append(("goal_selected", cid)), raising=True)
    m.set_current_menu(chat_id, "goal_selected")
    m.cmd_reply_nav(_msg(chat_id=chat_id, text="какой-то текст"))
    assert called[-1] == ("goal_selected", chat_id)


def test_unknown_text_fallback_for_auth_operator(fresh_main, monkeypatch):
    m = fresh_main
    called = []
    monkeypatch.setattr(m, "show_auth_operator_menu", lambda cid: called.append(cid), raising=True)
    m.db_exec(lambda db: m.set_setting(db, m.AUTH_OPERATOR_IDS_SETTING_KEY, "77"))
    m.cmd_reply_nav(_msg(chat_id=7001, user_id=77, text="unknown"))
    assert called and called[-1] == 7001


def test_handle_campaign_select_uses_goal_selected_status_screen(fresh_main, monkeypatch):
    m = fresh_main
    monkeypatch.setattr(m, "cleanup_step", lambda *a, **k: None, raising=True)

    captured = {}
    monkeypatch.setattr(
        m,
        "show_menu_status",
        lambda chat_id, menu_key, status_text: captured.update(
            {"chat_id": chat_id, "menu": menu_key, "text": status_text}
        ),
        raising=True,
    )
    called = []
    monkeypatch.setattr(m, "show_selected_goal_menu", lambda chat_id: called.append(chat_id), raising=True)

    camp_id = m.db_exec(
        lambda db: (
            db.add(m.Campaign(name="select_goal", enabled=False)),
            db.commit(),
            int(db.query(m.Campaign.id).filter(m.Campaign.name == "select_goal").first()[0]),
        )[-1]
    )

    chat_id = 5001
    m.handle_campaign_select(_msg(chat_id=chat_id, text=str(camp_id)))

    assert captured["chat_id"] == chat_id
    assert captured["menu"] == "goal_selected"
    assert f"Выбрана цель #{camp_id}" in captured["text"]
    assert called == []
    assert m.get_chat_ui_int(chat_id, "selected_campaign_id", 0) == int(camp_id)


def test_handle_delete_goal_single_success_uses_goal_manager_status_screen(fresh_main, monkeypatch):
    m = fresh_main
    monkeypatch.setattr(m, "cleanup_step", lambda *a, **k: None, raising=True)

    captured = {}
    monkeypatch.setattr(
        m,
        "show_menu_status",
        lambda chat_id, menu_key, status_text: captured.update(
            {"chat_id": chat_id, "menu": menu_key, "text": status_text}
        ),
        raising=True,
    )
    called = []
    monkeypatch.setattr(m, "show_goal_manager_menu", lambda chat_id: called.append(chat_id), raising=True)

    camp_id = m.db_exec(
        lambda db: (
            db.add(m.Campaign(name="delete_goal", enabled=False)),
            db.commit(),
            int(db.query(m.Campaign.id).filter(m.Campaign.name == "delete_goal").first()[0]),
        )[-1]
    )

    chat_id = 5002
    m.handle_delete_goal_single(_msg(chat_id=chat_id, text=str(camp_id)))

    assert captured["chat_id"] == chat_id
    assert captured["menu"] == "goal_manager"
    assert f"Цель #{camp_id} удалена" in captured["text"]
    assert called == []
    assert m.db_exec(lambda db: db.query(m.Campaign).filter(m.Campaign.id == camp_id).count()) == 0


def test_goal_edit_menu_routes_to_goal_specific_handlers(fresh_main, monkeypatch):
    m = fresh_main
    asked = []
    status_calls = []
    monkeypatch.setattr(
        m,
        "ask_step",
        lambda message, prompt_text, next_handler, parse_mode=None: asked.append((prompt_text, next_handler.__name__)),
        raising=True,
    )
    monkeypatch.setattr(
        m,
        "show_menu_status",
        lambda chat_id, menu_key, text, *a, **k: status_calls.append((chat_id, menu_key, text)),
        raising=True,
    )

    chat_id = 987
    m.set_current_menu(chat_id, "goal_edit")

    m.cmd_reply_nav(_msg(chat_id=chat_id, text="🔄 Лимит"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, text="⏱️ Джиттер"))
    m.cmd_reply_nav(_msg(chat_id=chat_id, text="🕐 Окна"))

    assert len(status_calls) == 1
    assert status_calls[0][1] == "goal_edit"
    assert "ручной" in status_calls[0][2].lower()
    assert asked[0][1] == "handle_set_goal_jitter"
    assert asked[1][1] == "handle_set_goal_windows"
