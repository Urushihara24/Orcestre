import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import select

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db_models import Account, SessionLocal, Setting  # noqa: E402


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def set_setting(db, key: str, value: str):
    obj = db.query(Setting).filter(Setting.key == key).first()
    if obj:
        obj.value = value
        obj.updated_at = utc_now()
    else:
        db.add(Setting(key=key, value=value))


def parse_days(text: str) -> list[int]:
    # ISO weekday: 1..7
    out = []
    for part in (text or "").split(","):
        part = part.strip()
        if not part:
            continue
        n = int(part)
        if n < 1 or n > 7:
            raise ValueError("days must be 1..7 (ISO weekday)")
        out.append(n)
    if not out:
        raise ValueError("days is empty")
    return out


def main():
    load_dotenv()

    ap = argparse.ArgumentParser(description="Bulk configure accounts (daily_limit, windows) and global settings.")
    ap.add_argument("--daily-limit", type=int, default=None, help="Set daily_limit for selected accounts.")
    ap.add_argument("--set-windows", action="store_true", help="Set active windows for selected accounts.")
    ap.add_argument("--days", type=str, default="1,2,3,4,5,6,7", help="ISO weekdays list for windows (1..7).")
    ap.add_argument("--from", dest="from_hhmm", type=str, default="09:00", help="Window start (HH:MM) in UTC.")
    ap.add_argument("--to", dest="to_hhmm", type=str, default="21:00", help="Window end (HH:MM) in UTC.")
    ap.add_argument("--jitter-min-sec", type=int, default=None, help="Set global jitter_min_sec setting.")
    ap.add_argument("--jitter-max-sec", type=int, default=None, help="Set global jitter_max_sec setting.")
    ap.add_argument("--include-disabled", action="store_true", help="Also apply to disabled/non-active accounts.")
    ap.add_argument("--dry-run", action="store_true", help="Do not commit changes.")
    args = ap.parse_args()

    days = parse_days(args.days) if args.set_windows else None
    if args.set_windows:
        windows = [{"days": days, "from": args.from_hhmm, "to": args.to_hhmm}]
        windows_json = json.dumps(windows, ensure_ascii=True)
    else:
        windows_json = None

    db = SessionLocal()
    try:
        q = select(Account)
        if not args.include_disabled:
            q = q.where(Account.enabled == True).where(Account.status == "active")
        accs = db.execute(q).scalars().all()

        updated = 0
        for a in accs:
            changed = False
            if args.daily_limit is not None and int(a.daily_limit or 0) != int(args.daily_limit):
                a.daily_limit = int(args.daily_limit)
                changed = True
            if windows_json is not None and (a.active_windows_json or "[]") != windows_json:
                a.active_windows_json = windows_json
                changed = True
            if changed:
                updated += 1

        if args.jitter_min_sec is not None:
            set_setting(db, "jitter_min_sec", str(int(args.jitter_min_sec)))
        if args.jitter_max_sec is not None:
            set_setting(db, "jitter_max_sec", str(int(args.jitter_max_sec)))

        print("bulk_config_summary")
        print("accounts_selected", len(accs))
        print("accounts_updated", updated)
        if args.set_windows:
            print("windows_json", windows_json)
        if args.daily_limit is not None:
            print("daily_limit", int(args.daily_limit))
        if args.jitter_min_sec is not None or args.jitter_max_sec is not None:
            print("settings_updated", True)

        if args.dry_run:
            db.rollback()
        else:
            db.commit()
    finally:
        db.close()


if __name__ == "__main__":
    main()

