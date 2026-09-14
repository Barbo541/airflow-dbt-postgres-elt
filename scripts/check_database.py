"""Assertions executed inside the disposable Airflow container only."""

import importlib
import json
import os
import sys
from pathlib import Path

from pandas.errors import DatabaseError as PandasDatabaseError
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

sys.path.insert(0, "/opt/airflow/dags")
load_data_python = importlib.import_module("load_data_python")
get_pg_engine = load_data_python.get_pg_engine
load_source_data = load_data_python.load_source_data
refresh_raw_tables = load_data_python.refresh_raw_tables


FORCED_FAILURE_MARKER = "forced_database_failure"


def is_expected_forced_database_failure(exc):
    """Accept SQL wrappers only for the failure intentionally injected below."""
    return FORCED_FAILURE_MARKER in str(exc)


def snapshot(conn):
    return {
        name: conn.execute(
            text(f"SELECT to_jsonb(t) AS payload FROM {name} t ORDER BY payload")
        ).scalars().all()
        for name in (
            "raw.raw_users",
            "raw.raw_pet_health",
            "raw.raw_pet_activity",
            "public_final.final_join_w_users_4",
        )
    }


def check(mode):
    if not os.environ["PET_DB_NAME"].startswith("validation_"):
        raise RuntimeError("This failure test is restricted to disposable validation databases")

    with get_pg_engine(
        "db",
        5432,
        os.environ["PET_DB_NAME"],
        os.environ["PET_DB_USER"],
        os.environ["PET_DB_PASSWORD"],
    ) as engine:
        with engine.connect() as conn:
            before = snapshot(conn)

        counts = {name: len(rows) for name, rows in before.items()}
        assert list(counts.values()) == [522, 940, 938, 1878], counts

        checkpoint = Path("/tmp/business_snapshot.json")
        serialized = json.dumps(before, sort_keys=True)
        if mode == "first":
            checkpoint.write_text(serialized, encoding="utf-8")
        else:
            assert checkpoint.read_text(encoding="utf-8") == serialized, (
                "Second run changed business output"
            )
            data = load_source_data(Path("/opt/airflow/raw_data"))
            # A real PostgreSQL integer conversion error occurs only after users
            # and health inserts; the transaction must restore all prior raw rows.
            data["raw_pet_activity"]["pet_id"] = FORCED_FAILURE_MARKER
            with engine.connect() as conn:
                full_before = snapshot(conn)
            try:
                with engine.begin() as conn:
                    refresh_raw_tables(conn, data)
            except (DBAPIError, PandasDatabaseError) as exc:
                # pandas 3 wraps SQL I/O failures in pandas.errors.DatabaseError.
                # Reject unrelated database errors so this remains a targeted
                # rollback proof rather than a blanket exception swallow.
                if not is_expected_forced_database_failure(exc):
                    raise
            else:
                raise AssertionError("Expected database failure did not occur")
            with engine.connect() as conn:
                assert snapshot(conn) == full_before, "Atomic rollback failed"
            print("SECOND_RUN_STABLE=true ROLLBACK_OK=true")

        print(json.dumps(counts))


if __name__ == "__main__":
    check(sys.argv[1])
