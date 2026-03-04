import argparse
import json
import os
import random
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from sqlalchemy import func

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db_models import DB_URL, Account, SessionLocal, Task  # noqa: E402


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)

def next_daily_reset_utc(now_utc: datetime, reset_hour_utc: int) -> datetime:
    reset_hour_utc = int(reset_hour_utc)
    reset_today = datetime.combine(now_utc.date(), datetime.min.time()).replace(hour=reset_hour_utc, minute=0, second=0)
    if now_utc < reset_today:
        return reset_today
    return reset_today + timedelta(days=1)


def is_in_window_utc(windows: list, dt_utc: datetime) -> bool:
    """
    windows: [{"days":[0..6], "from":"HH:MM", "to":"HH:MM"}]
    dt_utc: naive UTC datetime
    """
    if not windows:
        return True
    weekday = dt_utc.weekday()
    hhmm = dt_utc.strftime("%H:%M")
    for w in windows:
        days = w.get("days") or []
        if days and weekday not in days:
            continue
        t_from = str(w.get("from") or "00:00")
        t_to = str(w.get("to") or "23:59")
        if t_from <= t_to:
            if t_from <= hhmm <= t_to:
                return True
        else:
            # crosses midnight: [from..23:59] or [00:00..to]
            if hhmm >= t_from or hhmm <= t_to:
                return True
    return False


def account_available(a: Account, now: datetime) -> bool:
    if not a.enabled or a.status != "active":
        return False
    if not (a.epic_account_id and a.device_id and a.device_secret):
        return False
    if a.warmup_until and now < a.warmup_until:
        return False
    if a.today_sent is None or a.daily_limit is None:
        return False
    if int(a.today_sent) >= int(a.daily_limit):
        return False
    try:
        windows = json.loads(a.active_windows_json or "[]")
    except Exception:
        windows = []
    if not is_in_window_utc(windows, now):
        return False
    return True


def pick_best_account(accounts: list[Account], reserved: dict[int, int], now: datetime) -> Account | None:
    best = None
    best_key = None
    for a in accounts:
        if not account_available(a, now):
            continue
        r = int(reserved.get(int(a.id), 0))
        effective = int(a.today_sent or 0) + r
        if effective >= int(a.daily_limit or 0):
            continue
        key = (effective, int(a.today_sent or 0), -int(a.total_sent or 0), int(a.id))
        if best is None or key < best_key:
            best = a
            best_key = key
    return best


def main():
    load_dotenv()

    ap = argparse.ArgumentParser(description="Rebalance queued/postponed send_request tasks across available accounts.")
    ap.add_argument("--limit", type=int, default=5000, help="Max tasks to rebalance in one run.")
    ap.add_argument("--min-delay-sec", type=int, default=0, help="Min delay for rescheduled tasks.")
    ap.add_argument("--max-delay-sec", type=int, default=3, help="Max delay for rescheduled tasks.")
    ap.add_argument(
        "--reschedule-if-scheduled-after-sec",
        type=int,
        default=None,
        help="If set: for tasks whose current account is OK but scheduled_for is too far in the future, "
        "reschedule to now+random(min/max).",
    )
    ap.add_argument(
        "--postpone-no-capacity-to-reset",
        action="store_true",
        help="If a task can't be moved due to lack of capacity, postpone it to the next daily reset hour (UTC).",
    )
    ap.add_argument("--dry-run", action="store_true", help="Do not commit changes, only print stats.")
    args = ap.parse_args()

    now = utc_now()
    min_d = max(0, int(args.min_delay_sec))
    max_d = max(min_d, int(args.max_delay_sec))

    db = SessionLocal()
    try:
        accounts = db.query(Account).all()

        reserved_by_account: dict[int, int] = {}
        for acc_id, cnt in (
            db.query(Task.account_id, func.count(Task.id))
            .filter(
                Task.task_type == "send_request",
                Task.status.in_(["queued", "postponed", "running"]),
            )
            .group_by(Task.account_id)
            .all()
        ):
            reserved_by_account[int(acc_id)] = int(cnt)

        # Tasks that are blocked by the current assigned account (limit, disabled, etc.)
        tasks_query = (
            db.query(Task)
            .filter(Task.task_type == "send_request")
            .filter(Task.status.in_(["queued", "postponed"]))
            .order_by(Task.scheduled_for.asc(), Task.id.asc())
        )
        if DB_URL.startswith("postgresql"):
            tasks_query = tasks_query.with_for_update(skip_locked=True)

        tasks = tasks_query.limit(int(args.limit)).all()
        moved = 0
        skipped_ok = 0
        no_capacity = 0
        postponed_to_reset = 0
        rescheduled_far_future = 0

        acc_by_id = {int(a.id): a for a in accounts}

        for t in tasks:
            cur = acc_by_id.get(int(t.account_id))
            if cur and account_available(cur, now):
                if args.reschedule_if_scheduled_after_sec is not None:
                    thr = int(args.reschedule_if_scheduled_after_sec)
                    if t.scheduled_for and t.scheduled_for > (now + timedelta(seconds=thr)):
                        t.status = "queued"
                        t.scheduled_for = now + timedelta(seconds=random.randint(min_d, max_d))
                        rescheduled_far_future += 1
                        continue
                skipped_ok += 1
                continue

            best = pick_best_account(accounts, reserved_by_account, now)
            if not best:
                no_capacity += 1
                if args.postpone_no_capacity_to_reset:
                    reset_hour = int(os.getenv("DAILY_RESET_HOUR_UTC", "0") or "0")
                    reset_at = next_daily_reset_utc(now, reset_hour)
                    t.status = "postponed"
                    t.scheduled_for = reset_at + timedelta(seconds=random.randint(0, 600))
                    postponed_to_reset += 1
                continue

            old_acc_id = int(t.account_id)
            new_acc_id = int(best.id)
            if new_acc_id == old_acc_id:
                # If best==old but old isn't available, treat as no capacity.
                no_capacity += 1
                continue

            t.account_id = new_acc_id
            t.status = "queued"
            t.scheduled_for = now + timedelta(seconds=random.randint(min_d, max_d))

            # update reservations
            reserved_by_account[old_acc_id] = max(0, int(reserved_by_account.get(old_acc_id, 0)) - 1)
            reserved_by_account[new_acc_id] = int(reserved_by_account.get(new_acc_id, 0)) + 1
            moved += 1

        print("rebalance_summary")
        print("total_scanned", len(tasks))
        print("moved", moved)
        print("skipped_ok", skipped_ok)
        print("no_capacity", no_capacity)
        print("postponed_to_reset", postponed_to_reset)
        print("rescheduled_far_future", rescheduled_far_future)

        if args.dry_run:
            db.rollback()
        else:
            db.commit()
    finally:
        db.close()


if __name__ == "__main__":
    main()
