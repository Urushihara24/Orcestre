import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db_models import Account, SessionLocal  # noqa: E402
from epic_api_client import (  # noqa: E402
    check_friend_status_with_device,
    verify_account_health_with_device,
)


def main() -> int:
    """
    Safe smoke-check for real Epic API using stored device_auth.
    Does NOT send friend requests (only health + friend status pre-check).
    """
    load_dotenv()

    ap = argparse.ArgumentParser(description="Smoke-check Epic device_auth: health + friend-status precheck.")
    ap.add_argument("--account-id", type=int, default=None, help="Account id from DB (optional).")
    ap.add_argument("--target-username", type=str, required=True, help="Epic display name to check.")
    args = ap.parse_args()

    db = SessionLocal()
    try:
        q = db.query(Account).filter(
            Account.enabled == True,
            Account.status == "active",
            Account.epic_account_id.isnot(None),
            Account.device_id.isnot(None),
            Account.device_secret.isnot(None),
        )
        if args.account_id is not None:
            q = q.filter(Account.id == int(args.account_id))
        acc = q.order_by(Account.id.asc()).first()
        if not acc:
            print("❌ No active account with device_auth found (or account-id not found).")
            return 2

        proxy_url = None
        # We keep it simple: proxy can be added here if you wire Proxy relation by id.

        print(f"Using acc#{acc.id} login={acc.login}")
        print("1) verify_account_health_with_device ...")
        health = verify_account_health_with_device(
            login=acc.login,
            password=acc.password,
            proxy_url=proxy_url,
            epic_account_id=acc.epic_account_id,
            device_id=acc.device_id,
            device_secret=acc.device_secret,
        )
        print("  ok:", health.ok, "code:", health.code, "message:", health.message)

        print("2) check_friend_status_with_device (pre-check) ...")
        status = check_friend_status_with_device(
            login=acc.login,
            password=acc.password,
            target_username=args.target_username,
            proxy_url=proxy_url,
            epic_account_id=acc.epic_account_id,
            device_id=acc.device_id,
            device_secret=acc.device_secret,
        )
        print("  ok:", status.ok, "code:", status.code, "message:", status.message)
        # Expected codes:
        # - accepted: already friends
        # - pending: outgoing/incoming already exists
        # - rejected: not friends / no pending

        print("✅ Smoke-check finished (no friend request was sent).")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())

