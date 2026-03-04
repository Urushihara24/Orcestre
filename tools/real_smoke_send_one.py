"""
Real-mode smoke test: send (or pre-check) exactly one Epic friend request.

Safety goals:
- Requires explicit interactive confirmation.
- Works with the existing queue logic (process_tasks_job) but never starts Telegram polling.
- Enforces that only ONE account+target are involved.

Usage:
  ./.venv/bin/python tools/real_smoke_send_one.py --account-id 5 --target SomeNick

Prereqs:
- .env must have valid Epic client secrets and tokens.
- Account must already have device_auth saved in DB.
- Set DRY_RUN=0 and SEND_REQUESTS_ENABLED=1 in env for real sending.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import timedelta

from dotenv import load_dotenv


def _bool_env(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes"}


def _require_confirm(prompt: str, expected: str) -> None:
    print(prompt)
    ans = input("> ").strip()
    if ans != expected:
        raise SystemExit("Отменено: подтверждение не совпало.")


def main() -> int:
    load_dotenv()

    p = argparse.ArgumentParser()
    p.add_argument("--account-id", type=int, required=True, help="ID аккаунта в таблице accounts")
    p.add_argument("--target", required=True, help="Epic username (ник) цели")
    p.add_argument(
        "--force",
        action="store_true",
        help="Разрешить продолжить при неидеальных условиях (например daily_limit > 1)",
    )
    args = p.parse_args()

    dry_run = _bool_env("DRY_RUN", "0")
    send_enabled = _bool_env("SEND_REQUESTS_ENABLED", "0")
    db_url = os.getenv("DB_URL", "").strip()

    if not db_url:
        raise SystemExit("❌ DB_URL не задан")

    if dry_run:
        raise SystemExit("❌ DRY_RUN=1. Для боевого смоука нужно DRY_RUN=0.")

    if not send_enabled:
        raise SystemExit("❌ SEND_REQUESTS_ENABLED=0. Для боевого смоука включи SEND_REQUESTS_ENABLED=1.")

    # main.py требует TELEGRAM_BOT_TOKEN/ADMIN_TELEGRAM_ID на import-time.
    # Для инструмента не нужен реальный Telegram, поэтому подставляем заглушки при отсутствии.
    os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:SMOKE_TOOL_DUMMY")
    os.environ.setdefault("ADMIN_TELEGRAM_ID", "1")

    import main as app  # noqa

    target_username = (args.target or "").strip()
    if not target_username:
        raise SystemExit("❌ Пустой --target")

    def load_state(db):
        acc = db.query(app.Account).filter(app.Account.id == args.account_id).first()
        if not acc:
            raise SystemExit(f"❌ Account #{args.account_id} не найден")

        if not (acc.epic_account_id and acc.device_id and acc.device_secret):
            raise SystemExit(f"❌ Account #{acc.id} без device_auth (epic_account_id/device_id/device_secret)")

        if not acc.enabled or acc.status != app.AccountStatus.ACTIVE.value:
            raise SystemExit(f"❌ Account #{acc.id} не ACTIVE/enabled (status={acc.status}, enabled={acc.enabled})")

        tgt = db.query(app.Target).filter(app.Target.username == target_username).first()
        return acc, tgt

    acc, tgt = app.db_exec(load_state)

    print("=== REAL SMOKE SEND ONE ===")
    print(f"DB_URL: {db_url}")
    print(f"Account: #{acc.id} login={acc.login} today_sent={acc.today_sent} daily_limit={acc.daily_limit}")
    print(f"Target: {target_username} (exists_in_db={bool(tgt)})")
    print("Env: DRY_RUN=0 SEND_REQUESTS_ENABLED=1")
    print("")

    if (acc.daily_limit or 0) > 1 and not args.force:
        print("⚠️ daily_limit аккаунта больше 1. Для смоука лучше поставить 1, чтобы избежать случайной рассылки.")
        print("Либо поставь daily_limit=1 и повтори, либо запусти с --force.")
        return 2

    # Check friend status first (anti-spam pre-check).
    proxy_url = app.db_exec(lambda db: app.get_proxy_for_account(db, acc.id))
    print("Pre-check: friend status...")
    pre = app.check_friend_status_with_device(
        login=acc.login,
        password=acc.password,
        target_username=target_username,
        proxy_url=proxy_url,
        epic_account_id=acc.epic_account_id,
        device_id=acc.device_id,
        device_secret=acc.device_secret,
    )
    print(f"pre_check ok={pre.ok} code={pre.code} message={pre.message}")
    print("")

    confirm = f"SEND#{acc.id}->{target_username}"
    _require_confirm(
        "Подтверди реальную попытку отправки заявки.\n"
        f"Введи ровно: {confirm}",
        expected=confirm,
    )

    # Ensure target row exists.
    def ensure_target(db):
        tgt = db.query(app.Target).filter(app.Target.username == target_username).first()
        if tgt:
            return tgt.id
        t = app.Target(username=target_username, status=app.TargetStatus.NEW.value, priority=1)
        db.add(t)
        db.commit()
        return t.id

    target_id = app.db_exec(ensure_target)

    # Ensure there is exactly one send_request task for this target (avoid duplicates).
    now = app.utc_now()

    def ensure_send_task(db):
        existing = (
            db.query(app.Task)
            .filter(
                app.Task.target_id == target_id,
                app.Task.task_type == "send_request",
                app.Task.status.in_(
                    [
                        app.TaskStatus.QUEUED.value,
                        app.TaskStatus.POSTPONED.value,
                        app.TaskStatus.RUNNING.value,
                    ]
                ),
            )
            .first()
        )
        if existing:
            # Make it due now and bind to chosen account to keep it deterministic for smoke.
            existing.account_id = acc.id
            existing.scheduled_for = now - timedelta(seconds=1)
            db.commit()
            return existing.id

        t = db.query(app.Target).filter(app.Target.id == target_id).first()
        if t:
            t.status = app.TargetStatus.PENDING.value

        task = app.Task(
            task_type="send_request",
            status=app.TaskStatus.QUEUED.value,
            account_id=acc.id,
            target_id=target_id,
            scheduled_for=now - timedelta(seconds=1),
            max_attempts=3,
        )
        db.add(task)
        db.commit()
        return task.id

    task_id = app.db_exec(ensure_send_task)

    print(f"Task prepared: send_request task_id={task_id} scheduled_for<=now")
    print("Running one tick (process_tasks_job)...")
    app.process_tasks_job()

    def fetch_after(db):
        a = db.query(app.Account).filter(app.Account.id == acc.id).first()
        t = db.query(app.Target).filter(app.Target.id == target_id).first()
        task = db.query(app.Task).filter(app.Task.id == task_id).first()
        return a, t, task

    acc2, tgt2, task2 = app.db_exec(fetch_after)
    print("")
    print("After tick:")
    print(f"  account today_sent={acc2.today_sent} total_sent={acc2.total_sent} last_error={acc2.last_error}")
    print(f"  target status={tgt2.status} attempts={tgt2.attempt_count} sent={tgt2.sent_count} accepted={tgt2.accepted_count}")
    print(f"  task status={task2.status} attempt_number={task2.attempt_number} last_error={task2.last_error}")
    print("")

    print("Note: check_status задачи (если создалась) выполнится scheduler/worker-ом позже или через ручной Tick.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

