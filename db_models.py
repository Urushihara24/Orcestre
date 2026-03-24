import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

DB_URL = os.getenv("DB_URL", "sqlite:///./bot_data.db").strip()
DEFAULT_DAILY_LIMIT = int(os.getenv("DEFAULT_DAILY_LIMIT", "10"))

engine_kwargs = {}
if DB_URL.startswith("sqlite"):
    engine_kwargs["connect_args"] = {"check_same_thread": False}
engine = create_engine(DB_URL, **engine_kwargs)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


def utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Proxy(Base):
    __tablename__ = "proxies"
    id = Column(Integer, primary_key=True)
    url = Column(String(256), unique=True, index=True, nullable=False)
    enabled = Column(Boolean, default=True)
    failed_count = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=utc_now_naive)


class Account(Base):
    __tablename__ = "accounts"
    __table_args__ = (
        Index("ix_accounts_enabled_status", "enabled", "status"),
    )
    id = Column(Integer, primary_key=True)
    login = Column(String(256), unique=True, index=True, nullable=False)
    password = Column(String(512), nullable=False)
    epic_account_id = Column(String(64))
    epic_display_name = Column(String(128))
    device_id = Column(String(128))
    device_secret = Column(String(256))
    enabled = Column(Boolean, default=True)
    status = Column(String(32), default="active")
    proxy_id = Column(Integer)
    warmup_until = Column(DateTime)
    daily_limit = Column(Integer, default=DEFAULT_DAILY_LIMIT)
    today_sent = Column(Integer, default=0)
    total_sent = Column(Integer, default=0)
    total_failed = Column(Integer, default=0)
    total_accepted = Column(Integer, default=0)
    active_windows_json = Column(Text, default="[]")
    last_activity_at = Column(DateTime)
    last_error = Column(Text)
    last_reset_date = Column(DateTime)
    created_at = Column(DateTime, default=utc_now_naive)
    last_api_request_at = Column(DateTime)
    api_next_allowed_at = Column(DateTime)
    api_hour_window_start = Column(DateTime)
    api_hour_count = Column(Integer, default=0)
    api_day_window_start = Column(DateTime)
    api_day_count = Column(Integer, default=0)


class Target(Base):
    __tablename__ = "targets"
    __table_args__ = (
        Index("ix_targets_status_priority", "status", "priority", "id"),
        UniqueConstraint("campaign_id", "username", name="uq_targets_campaign_username"),
    )
    id = Column(Integer, primary_key=True)
    username = Column(String(256), index=True, nullable=False)
    campaign_id = Column(Integer, index=True)
    status = Column(String(32), default="new")
    priority = Column(Integer, default=100)
    attempt_count = Column(Integer, default=0)
    max_attempts = Column(Integer, default=3)
    sent_count = Column(Integer, default=0)
    accepted_count = Column(Integer, default=0)
    first_attempt_at = Column(DateTime)
    last_attempt_at = Column(DateTime)
    last_error = Column(Text)
    created_at = Column(DateTime, default=utc_now_naive)
    required_senders = Column(Integer, default=0)


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        Index("ix_tasks_status_scheduled_for", "status", "scheduled_for"),
        Index("ix_tasks_target_type_status", "target_id", "task_type", "status"),
    )
    id = Column(Integer, primary_key=True)
    task_type = Column(String(32), nullable=False)
    status = Column(String(32), default="queued")
    campaign_id = Column(Integer, index=True)
    account_id = Column(Integer, nullable=False)
    target_id = Column(Integer, nullable=False)
    scheduled_for = Column(DateTime, default=utc_now_naive)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    attempt_number = Column(Integer, default=0)
    max_attempts = Column(Integer, default=3)
    last_error = Column(Text)
    created_at = Column(DateTime, default=utc_now_naive)


class NicknameChangeTask(Base):
    __tablename__ = "nickname_change_tasks"
    __table_args__ = (
        Index("ix_nickname_change_tasks_status_scheduled_for", "status", "scheduled_for"),
        Index("ix_nickname_change_tasks_account_status", "account_id", "status"),
    )
    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, nullable=False, index=True)
    requested_nick = Column(String(32), nullable=False)
    final_nick = Column(String(32))
    status = Column(String(32), default="queued")
    scheduled_for = Column(DateTime, default=utc_now_naive)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    attempt_number = Column(Integer, default=0)
    max_attempts = Column(Integer, default=3)
    source_file = Column(String(256))
    last_error = Column(Text)
    created_at = Column(DateTime, default=utc_now_naive)


class LogEvent(Base):
    __tablename__ = "log_events"
    id = Column(Integer, primary_key=True)
    level = Column(String(16), default="info")
    message = Column(Text, nullable=False)
    created_at = Column(DateTime, default=utc_now_naive)


class Setting(Base):
    __tablename__ = "settings"
    key = Column(String(64), primary_key=True)
    value = Column(String(512), nullable=False)
    updated_at = Column(DateTime, default=utc_now_naive)


class Campaign(Base):
    __tablename__ = "campaigns"
    __table_args__ = (
        Index("ix_campaigns_enabled_name", "enabled", "name"),
    )
    id = Column(Integer, primary_key=True)
    name = Column(String(128), unique=True, index=True, nullable=False)
    enabled = Column(Boolean, default=True)
    daily_limit_per_account = Column(Integer, default=10)
    target_senders_count = Column(Integer, default=1)
    active_windows_json = Column(Text, default="[]")
    jitter_min_sec = Column(Integer, default=60)
    jitter_max_sec = Column(Integer, default=600)
    recheck_daily_limit = Column(Integer, default=500)
    daily_repeat_enabled = Column(Boolean, default=False)
    created_at = Column(DateTime, default=utc_now_naive)
    updated_at = Column(DateTime, default=utc_now_naive)


Base.metadata.create_all(bind=engine)


def _ensure_column(table: str, column: str, ddl_suffix: str) -> None:
    cols = {c["name"] for c in inspect(engine).get_columns(table)}
    if column in cols:
        return
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_suffix}"))


def _run_lightweight_migrations() -> None:
    ts_type = "TIMESTAMP" if DB_URL.startswith("postgresql") else "DATETIME"
    _ensure_column("accounts", "last_api_request_at", ts_type)
    _ensure_column("accounts", "api_next_allowed_at", ts_type)
    _ensure_column("accounts", "api_hour_window_start", ts_type)
    _ensure_column("accounts", "api_hour_count", "INTEGER DEFAULT 0")
    _ensure_column("accounts", "api_day_window_start", ts_type)
    _ensure_column("accounts", "api_day_count", "INTEGER DEFAULT 0")
    _ensure_column("targets", "required_senders", "INTEGER DEFAULT 0")
    _ensure_column("targets", "campaign_id", "INTEGER")
    _ensure_column("tasks", "campaign_id", "INTEGER")
    _ensure_column("accounts", "epic_display_name", "VARCHAR(128)")

    # Migrate unique target key from global username to (campaign_id, username).
    # PostgreSQL path: drop old global uniqueness, enforce per-campaign uniqueness.
    if DB_URL.startswith("postgresql"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE targets DROP CONSTRAINT IF EXISTS targets_username_key"))
            conn.execute(text("DROP INDEX IF EXISTS ix_targets_username"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_targets_username ON targets (username)"))
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_targets_campaign_username "
                    "ON targets (campaign_id, username)"
                )
            )
    else:
        # SQLite: keep best-effort compatibility for fresh DBs and future data.
        with engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_targets_campaign_username "
                    "ON targets (campaign_id, username)"
                )
            )

    # Do not auto-create any campaign.
    # Legacy rows with campaign_id NULL stay as-is for backward compatibility.


_run_lightweight_migrations()
