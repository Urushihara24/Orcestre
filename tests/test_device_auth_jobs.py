import importlib
import os
import sys
from pathlib import Path


def _fresh_import(module_name: str):
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


def test_device_auth_jobs_disabled_returns_zero(tmp_path, monkeypatch):
    db_path = tmp_path / "job.db"
    monkeypatch.setenv("DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("ALLOW_PASSWORD_DEVICE_AUTH_BATCH", "0")

    # ensure DB models are created on this DB
    sys.modules.pop("db_models", None)
    import db_models  # noqa: F401

    jobs = _fresh_import("device_auth_jobs")
    s, f = jobs.generate_device_auth_for_missing_accounts(max_per_run=5)
    assert (s, f) == (0, 0)


def test_device_auth_jobs_writes_device_auth(tmp_path, monkeypatch):
    db_path = tmp_path / "job2.db"
    monkeypatch.setenv("DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("ALLOW_PASSWORD_DEVICE_AUTH_BATCH", "1")

    sys.modules.pop("db_models", None)
    import db_models

    # create one account without device auth
    db = db_models.SessionLocal()
    try:
        a = db_models.Account(login="x@example.com", password="p", enabled=True, status="active")
        db.add(a)
        db.commit()
        db.refresh(a)
        acc_id = a.id
    finally:
        db.close()

    # patch generator
    gen = _fresh_import("device_auth_generator")
    monkeypatch.setattr(
        gen,
        "generate_device_auth_for_account",
        lambda login, password, proxy_url=None: (
            True,
            {"epic_account_id": "EA", "device_id": "D", "device_secret": "S"},
            "ok",
        ),
        raising=True,
    )

    jobs = _fresh_import("device_auth_jobs")
    s, f = jobs.generate_device_auth_for_missing_accounts(max_per_run=5)
    assert s == 1
    assert f == 0

    db = db_models.SessionLocal()
    try:
        a2 = db.query(db_models.Account).filter(db_models.Account.id == acc_id).first()
        assert a2.epic_account_id == "EA"
        assert a2.device_id == "D"
        assert a2.device_secret == "S"
    finally:
        db.close()

