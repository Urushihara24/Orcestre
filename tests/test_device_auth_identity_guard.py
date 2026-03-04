from types import SimpleNamespace


def test_validate_device_auth_identity_ok_for_same_login(fresh_main):
    m = fresh_main
    db = m.SessionLocal()
    try:
        acc = m.Account(login="user@example.com", password="p")
        db.add(acc)
        db.commit()
        db.refresh(acc)

        result = SimpleNamespace(
            email="user@example.com",
            epic_account_id="epic-1",
            display_name="UserOne",
        )
        ok, code, matched = m._validate_device_auth_identity(db, acc, result)
        assert ok is True
        assert code == ""
        assert matched is None
    finally:
        db.close()


def test_validate_device_auth_identity_blocks_email_mismatch(fresh_main):
    m = fresh_main
    db = m.SessionLocal()
    try:
        acc = m.Account(login="selected@example.com", password="p")
        db.add(acc)
        db.commit()
        db.refresh(acc)

        result = SimpleNamespace(
            email="other@example.com",
            epic_account_id="epic-2",
            display_name="OtherUser",
        )
        ok, code, matched = m._validate_device_auth_identity(db, acc, result)
        assert ok is False
        assert code == "email_mismatch"
        assert matched is None
    finally:
        db.close()


def test_validate_device_auth_identity_blocks_already_bound_epic_id(fresh_main):
    m = fresh_main
    db = m.SessionLocal()
    try:
        owner = m.Account(login="owner@example.com", password="p", epic_account_id="epic-owner")
        selected = m.Account(login="selected@example.com", password="p")
        db.add_all([owner, selected])
        db.commit()
        db.refresh(selected)

        result = SimpleNamespace(
            email="selected@example.com",
            epic_account_id="epic-owner",
            display_name="OwnerNick",
        )
        ok, code, matched = m._validate_device_auth_identity(db, selected, result)
        assert ok is False
        assert code == "epic_account_already_bound"
        assert matched == owner.id
    finally:
        db.close()


def test_validate_device_auth_identity_blocks_selected_epic_mismatch(fresh_main):
    m = fresh_main
    db = m.SessionLocal()
    try:
        selected = m.Account(login="selected@example.com", password="p", epic_account_id="epic-selected")
        db.add(selected)
        db.commit()
        db.refresh(selected)

        result = SimpleNamespace(
            email="selected@example.com",
            epic_account_id="epic-other",
            display_name="OtherEpic",
        )
        ok, code, matched = m._validate_device_auth_identity(db, selected, result)
        assert ok is False
        assert code == "epic_id_mismatch"
        assert matched is None
    finally:
        db.close()
