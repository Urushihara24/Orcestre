#!/usr/bin/env python3
"""
Queue audit helper: show duplicates / backlog summary.
Works for SQLite or PostgreSQL (uses DB_URL from .env).

Usage:
  python3 tools/audit_queue.py
"""

from collections import Counter, defaultdict
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db_models import SessionLocal, Account, Target, Task


ACTIVE_SEND_STATUSES = {"queued", "postponed", "running"}


def main():
    db = SessionLocal()
    try:
        tasks = db.query(Task).all()
        targets = {t.id: t for t in db.query(Target).all()}
        accounts = {a.id: a for a in db.query(Account).all()}

        print("TASK_STATUS_COUNTS")
        sc = Counter(t.status for t in tasks)
        for k, v in sorted(sc.items()):
            print(k, v)

        print("\nTASK_TYPE_COUNTS")
        tc = Counter(t.task_type for t in tasks)
        for k, v in sorted(tc.items()):
            print(k, v)

        print("\nACTIVE_SEND_PER_TARGET")
        per_target = Counter()
        per_target_accounts = defaultdict(set)
        per_pair = Counter()
        for t in tasks:
            if t.task_type != "send_request":
                continue
            if t.status not in ACTIVE_SEND_STATUSES:
                continue
            tgt_id = int(t.target_id or 0)
            acc_id = int(t.account_id or 0)
            per_target[tgt_id] += 1
            per_target_accounts[tgt_id].add(acc_id)
            per_pair[(tgt_id, acc_id)] += 1

        if not per_target:
            print("OK (no active send tasks)")
        else:
            for tgt_id in sorted(per_target.keys()):
                tgt = targets.get(tgt_id)
                uname = getattr(tgt, "username", "?")
                required = int(getattr(tgt, "required_senders", 0) or 0)
                active_total = int(per_target[tgt_id])
                active_unique_accounts = int(len(per_target_accounts[tgt_id]))
                print(
                    "target_id", tgt_id,
                    "username", uname,
                    "required", required,
                    "active_send_tasks", active_total,
                    "active_unique_accounts", active_unique_accounts,
                )

        print("\nDUPLICATES_ACTIVE_SEND_SAME_ACCOUNT_TARGET")
        dup_pairs = 0
        for (tgt_id, acc_id), cnt in sorted(per_pair.items(), key=lambda x: (-x[1], x[0][0], x[0][1])):
            if cnt <= 1:
                continue
            dup_pairs += 1
            uname = getattr(targets.get(tgt_id), "username", "?")
            login = getattr(accounts.get(acc_id), "login", "?")
            print(
                "target_id", tgt_id,
                "username", uname,
                "account_id", acc_id,
                "login", login,
                "active_send_tasks", cnt,
            )
        if dup_pairs == 0:
            print("OK (no same-pair active duplicates)")

        print("\nOVER_REQUIRED_ACTIVE_SEND_PER_TARGET")
        over_required = 0
        for tgt_id in sorted(per_target.keys()):
            tgt = targets.get(tgt_id)
            required = int(getattr(tgt, "required_senders", 0) or 0)
            if required <= 0:
                continue
            active_unique_accounts = int(len(per_target_accounts[tgt_id]))
            if active_unique_accounts > required:
                over_required += 1
                uname = getattr(tgt, "username", "?")
                print(
                    "target_id", tgt_id,
                    "username", uname,
                    "required", required,
                    "active_unique_accounts", active_unique_accounts,
                )
        if over_required == 0:
            print("OK (no over-required active sender allocations)")

        print("\nACCOUNTS_WITH_DEVICE_AUTH")
        ada = 0
        for a in accounts.values():
            ok = bool(a.epic_account_id and a.device_id and a.device_secret)
            if ok:
                ada += 1
        print("count", ada, "of", len(accounts))

        print("\nTARGETS_BY_STATUS")
        st = Counter(t.status for t in targets.values())
        for k, v in sorted(st.items()):
            print(k, v)
    finally:
        db.close()


if __name__ == "__main__":
    main()
