#!/usr/bin/env python3
"""
One-time migration helper: copy all rows from SQLite DB to PostgreSQL DB.

Usage:
  python3 tools/migrate_sqlite_to_postgres.py \
    --sqlite-url sqlite:///./bot_data.db \
    --postgres-url postgresql+psycopg://egs_user:egs_pass@127.0.0.1:5432/egs_bot
"""

import argparse
from typing import Dict, Iterable, List

from sqlalchemy import MetaData, Table, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


TABLE_ORDER: List[str] = [
    "settings",
    "proxies",
    "accounts",
    "targets",
    "tasks",
    "log_events",
]


def chunked(rows: Iterable[Dict], size: int):
    bucket = []
    for row in rows:
        bucket.append(row)
        if len(bucket) >= size:
            yield bucket
            bucket = []
    if bucket:
        yield bucket


def reflect_tables(engine: Engine) -> MetaData:
    md = MetaData()
    md.reflect(bind=engine)
    return md


def copy_table(src_engine: Engine, dst_engine: Engine, table_name: str, batch_size: int = 1000) -> int:
    src_md = reflect_tables(src_engine)
    dst_md = reflect_tables(dst_engine)

    src_key = table_name if table_name in src_md.tables else f"public.{table_name}"
    dst_key = table_name if table_name in dst_md.tables else f"public.{table_name}"

    if src_key not in src_md.tables:
        print(f"[SKIP] source table not found: {table_name}")
        return 0
    if dst_key not in dst_md.tables:
        print(f"[SKIP] destination table not found: {table_name}")
        return 0

    src_table: Table = src_md.tables[src_key]
    dst_table: Table = dst_md.tables[dst_key]

    with src_engine.connect() as src_conn:
        rows = src_conn.execute(select(src_table)).mappings().all()

    if not rows:
        print(f"[OK] {table_name}: 0 rows")
        return 0

    with dst_engine.begin() as dst_conn:
        dst_conn.execute(dst_table.delete())
        inserted = 0
        for batch in chunked((dict(r) for r in rows), batch_size):
            dst_conn.execute(dst_table.insert(), batch)
            inserted += len(batch)

    print(f"[OK] {table_name}: {inserted} rows")
    return inserted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite-url", required=True)
    parser.add_argument("--postgres-url", required=True)
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()

    src_engine = create_engine(args.sqlite_url)
    dst_engine = create_engine(args.postgres_url)

    try:
        total = 0
        for table in TABLE_ORDER:
            total += copy_table(src_engine, dst_engine, table, batch_size=args.batch_size)
        print(f"[DONE] migrated rows total: {total}")
    except SQLAlchemyError as e:
        print(f"[ERROR] migration failed: {e}")
        raise SystemExit(1)
    finally:
        src_engine.dispose()
        dst_engine.dispose()


if __name__ == "__main__":
    main()
