#!/usr/bin/env python3
"""
Queue audit helper: show duplicates / backlog summary.
Works for SQLite or PostgreSQL (uses DB_URL from .env).

Usage:
  python3 tools/audit_queue.py
"""

from collections import Counter
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db_models import SessionLocal, Account, Target, Task


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

        print("\nDUPLICATES_ACTIVE_SEND_PER_TARGET")
        dup = 0
        per = Counter()
        for t in tasks:
            if t.task_type != "send_request":
                continue
            if t.status not in ("queued", "postponed", "running"):
                continue
            per[t.target_id] += 1
        for tgt_id, cnt in per.items():
            if cnt > 1:
                dup += 1
                uname = getattr(targets.get(tgt_id), "username", "?")
                print("target_id", tgt_id, "username", uname, "active_send_tasks", cnt)
        if dup == 0:
            print("OK (no duplicates)")

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

