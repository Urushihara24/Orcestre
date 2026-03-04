# device_auth_jobs.py

import os
from datetime import datetime
from typing import Tuple

from device_auth_generator import generate_device_auth_for_account
from db_models import Account, SessionLocal


def log_event(level: str, message: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {level.upper()}: {message}")


def generate_device_auth_for_missing_accounts(max_per_run: int = 5) -> Tuple[int, int]:
    """
    Найти аккаунты без device_auth и попытаться сгенерировать для них.
    Ограничиваемся max_per_run за один запуск, чтобы не ловить жёсткий rate-limit.
    Returns:
        (success_count, fail_count)
    """
    allow_password_grant = os.getenv("ALLOW_PASSWORD_DEVICE_AUTH_BATCH", "0").strip().lower() in {"1", "true", "yes"}
    if not allow_password_grant:
        log_event(
            "warning",
            "device_auth_job: disabled (set ALLOW_PASSWORD_DEVICE_AUTH_BATCH=1 to enable password grant batch mode)",
        )
        return 0, 0

    db = SessionLocal()
    success = 0
    fail = 0

    try:
        accs = (
            db.query(Account)
            .filter(
                (Account.epic_account_id == None) |
                (Account.device_id == None) |
                (Account.device_secret == None),
                Account.enabled == True,
            )
            .order_by(Account.id.asc())
            .limit(max_per_run)
            .all()
        )

        if not accs:
            log_event("info", "device_auth_job: no accounts without device_auth")
            return 0, 0

        log_event("info", f"device_auth_job: found {len(accs)} accounts without device_auth")

        for acc in accs:
            log_event("info", f"device_auth_job: generating for acc#{acc.id} {acc.login}")
            proxy_url = None  # при желании можно читать из отдельной таблицы

            ok, device_auth, msg = generate_device_auth_for_account(
                login=acc.login,
                password=acc.password,
                proxy_url=proxy_url,
            )

            if not ok or not device_auth:
                fail += 1
                acc.last_error = f"device_auth_failed: {msg}"
                db.commit()
                log_event("warning", f"device_auth_job: FAIL {acc.login} {msg}")
                # Если клиент не имеет права на password grant, дальнейшие попытки бессмысленны.
                if "unauthorized_client" in msg or "grant type password" in msg:
                    log_event("error", "device_auth_job: stopping early due to unauthorized password grant")
                    break
                continue

            acc.epic_account_id = device_auth["epic_account_id"]
            acc.device_id = device_auth["device_id"]
            acc.device_secret = device_auth["device_secret"]
            acc.last_error = None
            db.commit()

            success += 1
            log_event(
                "info",
                f"device_auth_job: OK acc#{acc.id} {acc.login} "
                f"epic_account_id={acc.epic_account_id}"
            )

    except Exception as e:
        log_event("error", f"device_auth_job: exception {e}")
    finally:
        db.close()

    return success, fail


if __name__ == "__main__":
    s, f = generate_device_auth_for_missing_accounts()
    print(f"Done: success={s}, fail={f}")
