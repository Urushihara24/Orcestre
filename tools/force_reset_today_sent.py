import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db_models import Account, SessionLocal  # noqa: E402


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def main():
    load_dotenv()
    now = utc_now()
    db = SessionLocal()
    try:
        accs = db.query(Account).all()
        for a in accs:
            a.today_sent = 0
            a.last_reset_date = now
        db.commit()
        print("reset_summary")
        print("accounts", len(accs))
        print("today_sent_set_to", 0)
        print("last_reset_date", now)
    finally:
        db.close()


if __name__ == "__main__":
    main()

