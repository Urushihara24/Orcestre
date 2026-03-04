#!/usr/bin/env python3
"""
Seed helper for load-testing (SQLite or Postgres via DB_URL in .env).

Examples:
  python3 tools/seed_data.py --accounts 1000 --targets 5000
  python3 tools/seed_data.py --accounts 1000 --targets 5000 --clear
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db_models import SessionLocal, Account, Target  # noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--accounts", type=int, default=0)
    p.add_argument("--targets", type=int, default=0)
    p.add_argument("--clear", action="store_true", help="Clear existing accounts/targets before seeding")
    p.add_argument("--login-prefix", default="seed_acc")
    p.add_argument("--target-prefix", default="seed_tgt")
    args = p.parse_args()

    db = SessionLocal()
    try:
        if args.clear:
            db.query(Target).delete()
            db.query(Account).delete()
            db.commit()

        if args.accounts > 0:
            # We set fake device_auth fields to pass production filters even in DRY_RUN.
            batch = []
            for i in range(args.accounts):
                login = f"{args.login_prefix}_{i}@example.com"
                batch.append(
                    Account(
                        login=login,
                        password="x",
                        epic_account_id=f"acc_{i}",
                        device_id=f"dev_{i}",
                        device_secret=f"sec_{i}",
                        status="active",
                        enabled=True,
                        daily_limit=5,
                        active_windows_json="[]",
                    )
                )
            db.bulk_save_objects(batch)
            db.commit()

        if args.targets > 0:
            batch = []
            for i in range(args.targets):
                name = f"{args.target_prefix}_{i}"
                batch.append(
                    Target(
                        username=name,
                        status="new",
                        priority=100,
                        max_attempts=3,
                    )
                )
            db.bulk_save_objects(batch)
            db.commit()

        print("SEEDED_OK")
        print("accounts_added", args.accounts)
        print("targets_added", args.targets)
    finally:
        db.close()


if __name__ == "__main__":
    main()
