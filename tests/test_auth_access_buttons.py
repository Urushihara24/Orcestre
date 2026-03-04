from types import SimpleNamespace


def _msg(text: str, chat_id: int = 1, user_id: int = 1, message_id: int = 10):
    return SimpleNamespace(
        chat=SimpleNamespace(id=chat_id),
        from_user=SimpleNamespace(id=user_id),
        message_id=message_id,
        text=text,
    )


def test_settings_auth_access_opens_submenu(fresh_main, monkeypatch):
    m = fresh_main
    called = []
    monkeypatch.setattr(m, "show_auth_access_menu", lambda chat_id: called.append(chat_id), raising=True)
    m.cmd_reply_nav(_msg("👤 Доступ auth"))
    assert called == [1]


def test_auth_access_add_and_remove_id_handlers(fresh_main, monkeypatch):
    m = fresh_main
    statuses = []
    monkeypatch.setattr(
        m,
        "show_menu_status",
        lambda chat_id, menu_key, status_text: statuses.append((chat_id, menu_key, status_text)),
        raising=True,
    )

    candidate = 2000000000
    while m.is_admin(candidate):
        candidate += 1

    m.handle_auth_operator_add_id(_msg(str(candidate)))
    assert candidate in m.get_auth_operator_ids()

    m.handle_auth_operator_remove_id(_msg(str(candidate)))
    assert candidate not in m.get_auth_operator_ids()

    assert any(x[1] == "auth_access" for x in statuses)


def test_auth_access_clear_all_with_confirmation(fresh_main, monkeypatch):
    m = fresh_main
    statuses = []
    monkeypatch.setattr(
        m,
        "show_menu_status",
        lambda chat_id, menu_key, status_text: statuses.append((chat_id, menu_key, status_text)),
        raising=True,
    )

    first = 2000000100
    second = 2000000101
    while m.is_admin(first):
        first += 10
    while m.is_admin(second) or second == first:
        second += 10

    m.handle_auth_operator_add_id(_msg(str(first)))
    m.handle_auth_operator_add_id(_msg(str(second)))
    assert first in m.get_auth_operator_ids()
    assert second in m.get_auth_operator_ids()

    m.handle_auth_operator_clear_all_confirm(_msg("не очищать"))
    assert first in m.get_auth_operator_ids()
    assert second in m.get_auth_operator_ids()

    m.handle_auth_operator_clear_all_confirm(_msg("ОЧИСТИТЬ"))
    assert m.get_auth_operator_ids() == set()
    assert any("очищен" in x[2] for x in statuses)
