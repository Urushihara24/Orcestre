#!/usr/bin/env python3
"""
Production readiness checks (safe: no friend-request POST).

Checks:
- required env vars present
- DB connectivity + table counts
- at least one active account with device_auth (optional)

Usage:
  ./.venv/bin/python tools/prod_doctor.py
  ./.venv/bin/python tools/prod_doctor.py --allow-empty
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db_models import Account, SessionLocal, Target, Task  # noqa: E402


def _require(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise SystemExit(f"❌ Missing env var: {name}")
    return v


def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--allow-empty", action="store_true", help="Do not fail if no accounts/device_auth yet.")
    args = ap.parse_args()

    _require("TELEGRAM_BOT_TOKEN")
    # Multi-admin supported; at least one is required.
    if not (os.getenv("ADMIN_TELEGRAM_IDS", "").strip() or os.getenv("ADMIN_TELEGRAM_ID", "").strip()):
        raise SystemExit("❌ Missing env var: ADMIN_TELEGRAM_ID or ADMIN_TELEGRAM_IDS")
    _require("DB_URL")

    db = SessionLocal()
    try:
        accounts = db.query(Account).count()
        targets = db.query(Target).count()
        tasks = db.query(Task).count()
        ada = (
            db.query(Account)
            .filter(Account.enabled == True, Account.status == "active")
            .filter(Account.epic_account_id.isnot(None), Account.device_id.isnot(None), Account.device_secret.isnot(None))
            .count()
        )
        print("db_ok", True)
        print("accounts", int(accounts))
        print("targets", int(targets))
        print("tasks", int(tasks))
        print("accounts_with_device_auth_active", int(ada))
        if not args.allow_empty and ada == 0:
            raise SystemExit("❌ No active accounts with device_auth yet")
        print("doctor_ok", True)
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())

