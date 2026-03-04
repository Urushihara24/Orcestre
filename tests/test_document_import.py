import os
from types import SimpleNamespace


def _make_message(chat_id=1, user_id=1, file_name="file.txt", file_id="FID"):
    return SimpleNamespace(
        chat=SimpleNamespace(id=chat_id),
        from_user=SimpleNamespace(id=user_id),
        message_id=999,
        document=SimpleNamespace(file_id=file_id, file_name=file_name),
    )


class FakeBot:
    def __init__(self, content: bytes = b""):
        self._content = content

    def delete_message(self, chat_id, message_id):
        return None

    def get_file(self, file_id):
        return SimpleNamespace(file_path=f"files/{file_id}")

    def download_file(self, file_path):
        return self._content


def test_handle_document_rejects_bad_extension(fresh_main, monkeypatch):
    m = fresh_main
    fb = FakeBot()
    monkeypatch.setattr(m, "bot", fb, raising=True)

    called = {}

    def fake_show_screen(chat_id, text, **kwargs):
        called["chat_id"] = chat_id
        called["text"] = text

    monkeypatch.setattr(m, "show_screen", fake_show_screen, raising=True)

    msg = _make_message(file_name="x.exe")
    m.handle_document(msg)
    assert "Поддерживается" in called["text"]


def test_handle_document_txt_routes_accounts_vs_targets(fresh_main, monkeypatch, tmp_path):
    m = fresh_main

    # Accounts file: first line is login:pass
    fb = FakeBot(content=b"login:pass\n")
    monkeypatch.setattr(m, "bot", fb, raising=True)

    out = {}

    def fake_show_menu_status(chat_id, menu_key, status_text):
        out["menu_key"] = menu_key
        out["status_text"] = status_text

    monkeypatch.setattr(m, "show_menu_status", fake_show_menu_status, raising=True)
    monkeypatch.setattr(m, "import_accounts_from_text", lambda path: (1, 0, 0), raising=True)

    msg = _make_message(file_name="a.txt", file_id="A1")
    m.handle_document(msg)
    assert out["menu_key"] == "accounts"

    # Targets file: first line is just nickname
    fb2 = FakeBot(content=b"NickNameOnly\n")
    monkeypatch.setattr(m, "bot", fb2, raising=True)
    out2 = {}
    monkeypatch.setattr(m, "show_menu_status", lambda chat_id, menu_key, status_text: out2.update({"menu_key": menu_key}), raising=True)
    monkeypatch.setattr(m, "import_targets_from_text", lambda path: (2, 0, 0), raising=True)

    msg2 = _make_message(file_name="t.txt", file_id="T1")
    m.handle_document(msg2)
    assert out2["menu_key"] == "targets"

