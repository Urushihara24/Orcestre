"""
DB cleanup utility for production readiness.

This script is intentionally conservative:
- Prints what it would delete by default (dry-run).
- Requires --apply to actually delete rows.

Typical usage before production launch:
  ./.venv/bin/python tools/cleanup_db.py --prune-noisy-logs --delete-failed-tasks --apply

If you want a full wipe of runtime artifacts (tasks/logs) but keep accounts/targets:
  ./.venv/bin/python tools/cleanup_db.py --clear-tasks --clear-log-events --apply

If you used seed_data.py and want to remove seed records:
  ./.venv/bin/python tools/cleanup_db.py --delete-seed-accounts --delete-seed-targets --apply
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import and_, func, or_, text

load_dotenv()

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "123456:TOOLS_DUMMY")
os.environ.setdefault("ADMIN_TELEGRAM_ID", "1")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import main  # noqa: E402


@dataclass
class Plan:
    clear_tasks: bool = False
    delete_failed_tasks: bool = False
    clear_targets: bool = False
    clear_accounts: bool = False
    clear_proxies: bool = False
    clear_settings: bool = False
    restart_identity: bool = False
    clear_log_events: bool = False
    prune_noisy_logs: bool = False
    prune_seed_healthcheck_logs: bool = False
    prune_ui_settings: bool = False
    delete_seed_accounts: bool = False
    delete_seed_targets: bool = False
    reset_today_sent: bool = False


def _seed_login_filter():
    # Matches tools/seed_data.py default pattern.
    return or_(
        main.Account.login.like("seed_acc_%@example.com"),
        main.Account.login.like("seed_acc_%"),
        main.Account.login.like("%@example.com"),
    )


def _seed_target_filter():
    return or_(
        main.Target.username.like("seed_%"),
        main.Target.username.like("Seed_%"),
        main.Target.username.like("example_%"),
    )


def run(plan: Plan, apply: bool) -> int:
    def inner(db):
        out = {}

        is_pg = str(main.DB_URL).startswith("postgresql")

        out["counts_before"] = {
            "proxies": db.query(func.count(main.Proxy.id)).scalar() or 0,
            "accounts": db.query(func.count(main.Account.id)).scalar() or 0,
            "targets": db.query(func.count(main.Target.id)).scalar() or 0,
            "tasks": db.query(func.count(main.Task.id)).scalar() or 0,
            "log_events": db.query(func.count(main.LogEvent.id)).scalar() or 0,
            "settings": db.query(func.count(main.Setting.key)).scalar() or 0,
        }

        deleted = {
            "tasks": 0,
            "log_events": 0,
            "accounts": 0,
            "targets": 0,
            "proxies": 0,
        }

        if plan.restart_identity:
            if not is_pg:
                out["restart_identity_note"] = "skip (not postgresql)"
            elif not apply:
                out["would_restart_identity"] = True

        if plan.reset_today_sent and apply:
            db.query(main.Account).update({main.Account.today_sent: 0})

        # For a true "clean slate" on Postgres, TRUNCATE ... RESTART IDENTITY is better than DELETE:
        # it resets sequences, avoids large bloat, and is fast.
        if plan.restart_identity and apply and is_pg:
            # Use CASCADE to handle any future FKs safely.
            db.execute(
                text(
                    "TRUNCATE TABLE "
                    "tasks, targets, accounts, log_events, proxies "
                    "RESTART IDENTITY CASCADE"
                )
            )
            # Settings has no identity sequence; clear separately if requested below.

        if plan.clear_tasks:
            q = db.query(main.Task)
            if apply:
                deleted["tasks"] += q.delete(synchronize_session=False)
            else:
                out["would_delete_tasks"] = q.count()

        if plan.clear_targets:
            q = db.query(main.Target)
            if apply:
                deleted["targets"] += q.delete(synchronize_session=False)
            else:
                out["would_delete_targets"] = q.count()

        if plan.clear_accounts:
            q = db.query(main.Account)
            if apply:
                deleted["accounts"] += q.delete(synchronize_session=False)
            else:
                out["would_delete_accounts"] = q.count()

        if plan.clear_proxies:
            q = db.query(main.Proxy)
            if apply:
                deleted["proxies"] += q.delete(synchronize_session=False)
            else:
                out["would_delete_proxies"] = q.count()

        if plan.clear_settings:
            q = db.query(main.Setting)
            if apply:
                q.delete(synchronize_session=False)
            else:
                out["would_delete_settings"] = q.count()

        if plan.delete_failed_tasks and not plan.clear_tasks:
            q = db.query(main.Task).filter(main.Task.status == main.TaskStatus.FAILED.value)
            if apply:
                deleted["tasks"] += q.delete(synchronize_session=False)
            else:
                out["would_delete_failed_tasks"] = q.count()

        if plan.clear_log_events:
            q = db.query(main.LogEvent)
            if apply:
                deleted["log_events"] += q.delete(synchronize_session=False)
            else:
                out["would_delete_log_events"] = q.count()

        if plan.prune_noisy_logs and not plan.clear_log_events:
            # Remove historical noise we no longer emit.
            noisy = or_(
                main.LogEvent.message.like("delete_message failed %"),
                main.LogEvent.message.like("ui_menu_click %"),
            )
            q = db.query(main.LogEvent).filter(noisy)
            if apply:
                deleted["log_events"] += q.delete(synchronize_session=False)
            else:
                out["would_delete_noisy_logs"] = q.count()

        if plan.prune_seed_healthcheck_logs and not plan.clear_log_events:
            q = db.query(main.LogEvent).filter(
                main.LogEvent.message.like("health_check_failed %"),
                main.LogEvent.message.like("%login=seed_acc_%"),
            )
            if apply:
                deleted["log_events"] += q.delete(synchronize_session=False)
            else:
                out["would_delete_seed_healthcheck_logs"] = q.count()

        if plan.prune_ui_settings:
            # Remove ephemeral UI-related settings, keep operational settings like jitter_*.
            q = db.query(main.Setting).filter(
                or_(
                    main.Setting.key.like("keyboard_holder_msg_id:%"),
                )
            )
            if apply:
                q.delete(synchronize_session=False)
            else:
                out["would_delete_ui_settings"] = q.count()

        if plan.delete_seed_targets:
            q = db.query(main.Target).filter(_seed_target_filter())
            if apply:
                deleted["targets"] += q.delete(synchronize_session=False)
            else:
                out["would_delete_seed_targets"] = q.count()

        if plan.delete_seed_accounts:
            q_acc = db.query(main.Account).filter(_seed_login_filter())
            if apply:
                # Best-effort: also remove tasks tied to these accounts.
                seed_ids = [x[0] for x in q_acc.with_entities(main.Account.id).all()]
                if seed_ids:
                    deleted["tasks"] += (
                        db.query(main.Task)
                        .filter(main.Task.account_id.in_(seed_ids))
                        .delete(synchronize_session=False)
                    )
                deleted["accounts"] += q_acc.delete(synchronize_session=False)
            else:
                out["would_delete_seed_accounts"] = q_acc.count()

        if apply:
            db.commit()
        else:
            db.rollback()

        out["deleted"] = deleted
        out["counts_after"] = {
            "proxies": db.query(func.count(main.Proxy.id)).scalar() or 0,
            "accounts": db.query(func.count(main.Account.id)).scalar() or 0,
            "targets": db.query(func.count(main.Target.id)).scalar() or 0,
            "tasks": db.query(func.count(main.Task.id)).scalar() or 0,
            "log_events": db.query(func.count(main.LogEvent.id)).scalar() or 0,
            "settings": db.query(func.count(main.Setting.key)).scalar() or 0,
        }
        return out

    res = main.db_exec(inner)
    print("DB_URL:", main.DB_URL)
    print("APPLY:", bool(apply))
    print("COUNTS_BEFORE:", res["counts_before"])
    for k in sorted([x for x in res.keys() if x.startswith("would_")]):
        print(k.upper() + ":", res[k])
    print("DELETED:", res["deleted"])
    print("COUNTS_AFTER:", res["counts_after"])
    return 0


def main_cli() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true", help="Actually delete rows (otherwise dry-run).")
    p.add_argument("--clear-tasks", action="store_true", help="Delete all tasks.")
    p.add_argument("--delete-failed-tasks", action="store_true", help="Delete only failed tasks.")
    p.add_argument("--clear-targets", action="store_true", help="Delete all targets.")
    p.add_argument("--clear-accounts", action="store_true", help="Delete all accounts (also clears device_auth stored on rows).")
    p.add_argument("--clear-proxies", action="store_true", help="Delete all proxies.")
    p.add_argument("--clear-settings", action="store_true", help="Delete all settings (including jitter_*).")
    p.add_argument("--restart-identity", action="store_true", help="Postgres only: TRUNCATE tasks/targets/accounts/log_events/proxies with RESTART IDENTITY.")
    p.add_argument("--clear-log-events", action="store_true", help="Delete all log_events.")
    p.add_argument("--prune-noisy-logs", action="store_true", help="Delete historical noisy log_events entries.")
    p.add_argument("--prune-seed-healthcheck-logs", action="store_true", help="Delete health_check_failed log_events for seed_acc_*.")
    p.add_argument("--prune-ui-settings", action="store_true", help="Delete ephemeral UI settings (keyboard holder ids, etc.).")
    p.add_argument("--delete-seed-accounts", action="store_true", help="Delete seed accounts (seed_acc_*/@example.com).")
    p.add_argument("--delete-seed-targets", action="store_true", help="Delete seed targets (seed_*/example_*).")
    p.add_argument("--reset-today-sent", action="store_true", help="Set Account.today_sent=0 for all accounts.")
    args = p.parse_args()

    plan = Plan(
        clear_tasks=args.clear_tasks,
        delete_failed_tasks=args.delete_failed_tasks,
        clear_targets=args.clear_targets,
        clear_accounts=args.clear_accounts,
        clear_proxies=args.clear_proxies,
        clear_settings=args.clear_settings,
        restart_identity=args.restart_identity,
        clear_log_events=args.clear_log_events,
        prune_noisy_logs=args.prune_noisy_logs,
        prune_seed_healthcheck_logs=args.prune_seed_healthcheck_logs,
        prune_ui_settings=args.prune_ui_settings,
        delete_seed_accounts=args.delete_seed_accounts,
        delete_seed_targets=args.delete_seed_targets,
        reset_today_sent=args.reset_today_sent,
    )

    if not any(vars(plan).values()):
        print("Nothing to do. Provide one or more flags (see --help).")
        return 2

    return run(plan, apply=args.apply)


if __name__ == "__main__":
    raise SystemExit(main_cli())
