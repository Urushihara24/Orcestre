import importlib
import os

import pytest


def test_real_smoke_tool_requires_send_requests_enabled(monkeypatch):
    # Import as a module and call main(); it should exit early before importing main.py.
    monkeypatch.setenv("DB_URL", "sqlite:///./dummy.db")
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("SEND_REQUESTS_ENABLED", "0")

    mod = importlib.import_module("tools.real_smoke_send_one")

    monkeypatch.setattr(
        "sys.argv",
        ["tools/real_smoke_send_one.py", "--account-id", "1", "--target", "Nick"],
        raising=False,
    )

    with pytest.raises(SystemExit) as e:
        mod.main()
    assert "SEND_REQUESTS_ENABLED=0" in str(e.value)


def test_real_smoke_tool_requires_dry_run_off(monkeypatch):
    monkeypatch.setenv("DB_URL", "sqlite:///./dummy.db")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("SEND_REQUESTS_ENABLED", "1")

    mod = importlib.import_module("tools.real_smoke_send_one")
    monkeypatch.setattr(
        "sys.argv",
        ["tools/real_smoke_send_one.py", "--account-id", "1", "--target", "Nick"],
        raising=False,
    )

    with pytest.raises(SystemExit) as e:
        mod.main()
    assert "DRY_RUN=1" in str(e.value)

