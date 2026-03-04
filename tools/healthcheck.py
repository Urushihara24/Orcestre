#!/usr/bin/env python3
"""
Production-friendly healthcheck (no friend request sending).

Checks:
- DB connectivity
- counts of accounts/targets/tasks
- at least one active enabled account with device_auth (unless --allow-empty)
- optional real Epic API smoke: verify health + friend-status precheck (no POST)

Usage:
  ./.venv/bin/python tools/healthcheck.py
  ./.venv/bin/python tools/healthcheck.py --epic-target SOME_NICK
  ./.venv/bin/python tools/healthcheck.py --account-id 123 --epic-target SOME_NICK
"""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import func, select

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db_models import Account, SessionLocal, Target, Task  # noqa: E402
from epic_api_client import (  # noqa: E402
    check_friend_status_with_device,
    verify_account_health_with_device,
)


def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--allow-empty", action="store_true", help="Do not fail if there are no accounts with device_auth.")
    ap.add_argument("--account-id", type=int, default=None, help="Account id from DB for Epic smoke (optional).")
    ap.add_argument("--epic-target", type=str, default=None, help="Epic display name for Epic smoke (optional).")
    args = ap.parse_args()

    db = SessionLocal()
    try:
        accounts = db.execute(select(func.count()).select_from(Account)).scalar() or 0
        targets = db.execute(select(func.count()).select_from(Target)).scalar() or 0
        tasks = db.execute(select(func.count()).select_from(Task)).scalar() or 0
        print("db_ok", True)
        print("accounts", int(accounts))
        print("targets", int(targets))
        print("tasks", int(tasks))

        ada_q = (
            db.query(Account)
            .filter(Account.enabled == True, Account.status == "active")
            .filter(Account.epic_account_id.isnot(None), Account.device_id.isnot(None), Account.device_secret.isnot(None))
        )
        ada_count = ada_q.count()
        print("accounts_with_device_auth_active", int(ada_count))
        if ada_count == 0 and not args.allow_empty:
            print("healthcheck_fail", "no_active_device_auth_accounts")
            return 2

        if args.epic_target:
            q = ada_q
            if args.account_id is not None:
                q = q.filter(Account.id == int(args.account_id))
            acc = q.order_by(Account.id.asc()).first()
            if not acc:
                print("healthcheck_fail", "epic_smoke_no_matching_account")
                return 3

            print("epic_smoke_account_id", int(acc.id))
            health = verify_account_health_with_device(
                login=acc.login,
                password=acc.password,
                proxy_url=None,
                epic_account_id=acc.epic_account_id,
                device_id=acc.device_id,
                device_secret=acc.device_secret,
            )
            print("epic_health_ok", bool(health.ok))
            print("epic_health_code", health.code)

            status = check_friend_status_with_device(
                login=acc.login,
                password=acc.password,
                target_username=args.epic_target,
                proxy_url=None,
                epic_account_id=acc.epic_account_id,
                device_id=acc.device_id,
                device_secret=acc.device_secret,
            )
            print("epic_friend_status_ok", bool(status.ok))
            print("epic_friend_status_code", status.code)

        print("healthcheck_ok", True)
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())

