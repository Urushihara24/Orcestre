from types import SimpleNamespace

import pytest


class FakeBot:
    def __init__(self):
        self._next_id = 100
        self.sent = []
        self.edits = []
        self.deletes = []
        self._fail_edit = False
        self._next_step = []

    def send_message(self, chat_id, text, **kwargs):
        self._next_id += 1
        msg = SimpleNamespace(message_id=self._next_id, chat=SimpleNamespace(id=chat_id))
        self.sent.append((chat_id, text, kwargs, msg.message_id))
        return msg

    def edit_message_text(self, text, chat_id, message_id, **kwargs):
        if self._fail_edit:
            raise RuntimeError("edit failed")
        self.edits.append((chat_id, message_id, text, kwargs))
        return True

    def delete_message(self, chat_id, message_id):
        self.deletes.append((chat_id, message_id))

    def register_next_step_handler(self, msg, handler):
        self._next_step.append((msg.message_id, handler))


class FakeTimer:
    def __init__(self, *args, **kwargs):
        pass

    def start(self):
        return None


def _msg(chat_id=1, user_id=1, message_id=10):
    return SimpleNamespace(
        chat=SimpleNamespace(id=chat_id),
        from_user=SimpleNamespace(id=user_id),
        message_id=message_id,
    )


def test_notify_tracks_and_cleanup_deletes(fresh_main, monkeypatch):
    m = fresh_main
    fb = FakeBot()
    monkeypatch.setattr(m, "bot", fb, raising=True)
    monkeypatch.setattr(m.threading, "Timer", FakeTimer, raising=True)

    m.notify(1, "hello", ttl_sec=999, parse_mode=None)
    assert 1 in m.TRANSIENT_MESSAGES
    assert len(m.TRANSIENT_MESSAGES[1]) == 1

    m.cleanup_transient_messages(1)
    assert 1 not in m.TRANSIENT_MESSAGES
    assert len(fb.deletes) == 1


def test_show_screen_uses_edit_then_fallback_to_send(fresh_main, monkeypatch):
    m = fresh_main
    fb = FakeBot()
    monkeypatch.setattr(m, "bot", fb, raising=True)

    # First call -> sends first screen message
    mid1 = m.show_screen(1, "screen-1", parse_mode=None)
    assert mid1 is not None

    # Second call tries edit
    mid2 = m.show_screen(1, "screen-2", parse_mode=None)
    assert mid2 == mid1
    assert len(fb.edits) == 1

    # Force edit failure -> should send new message and update screen_msg_id
    fb._fail_edit = True
    mid3 = m.show_screen(1, "screen-3", parse_mode=None)
    assert mid3 != mid1


def test_show_screen_force_new_sends_and_deletes_old(fresh_main, monkeypatch):
    m = fresh_main
    fb = FakeBot()
    monkeypatch.setattr(m, "bot", fb, raising=True)

    mid1 = m.show_screen(1, "a", parse_mode=None, force_new=True)
    mid2 = m.show_screen(1, "b", parse_mode=None, force_new=True)
    assert mid2 != mid1
    # old screen should be deleted
    assert (1, mid1) in fb.deletes


def test_ask_step_deletes_previous_prompt(fresh_main, monkeypatch):
    m = fresh_main
    fb = FakeBot()
    monkeypatch.setattr(m, "bot", fb, raising=True)

    msg = _msg(chat_id=1, user_id=7, message_id=42)
    m.ask_step(msg, "q1", lambda x: x)
    m.ask_step(msg, "q2", lambda x: x)

    # second ask_step should delete previous prompt id
    assert len(fb.deletes) == 1


def test_cancel_all_step_prompts_deletes_prompts(fresh_main, monkeypatch):
    m = fresh_main
    fb = FakeBot()
    monkeypatch.setattr(m, "bot", fb, raising=True)

    # Pretend we have two prompts in the same chat for two users.
    m._set_step_prompt(1, 10, 111)
    m._set_step_prompt(1, 11, 222)

    m.cancel_all_step_prompts(1)
    assert (1, 111) in fb.deletes
    assert (1, 222) in fb.deletes
