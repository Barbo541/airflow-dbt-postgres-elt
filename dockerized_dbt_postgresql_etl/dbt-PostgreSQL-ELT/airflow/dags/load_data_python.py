import contextlib
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

RAW_SCHEMA = "raw"

SOURCE_FILES = {
    "users": {
        "filename": "users.csv",
        "columns": ["owner_id", "pet_id", "owner_age_group", "pet_type"],
        "integer_columns": ["owner_id", "pet_id"],
        "date_columns": [],
    },
    "health": {
        "filename": "pet_health.csv",
        "columns": ["pet_id", "visit_date", "issue", "resolution"],
        "integer_columns": ["pet_id"],
        "date_columns": ["visit_date"],
    },
    "activity": {
        "filename": "pet_activities.csv",
        "columns": ["pet_id", "date", "activity_type", "duration_minutes"],
        "integer_columns": ["pet_id"],
        "date_columns": ["date"],
    },
}


def required_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is not set: {name}")
    return value


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_source_csv(
    raw_dir,
    filename,
    expected_columns,
    integer_columns=(),
    date_columns=(),
):
    path = raw_dir / filename
    if not path.is_file():
        raise FileNotFoundError(f"Required source file not found: {path}")

    started = time.monotonic()
    # Read source text without silently coercing malformed values. Physical types
    # are converted explicitly below so bad integers/dates fail before DB access.
    dataframe = pd.read_csv(path, dtype="string", keep_default_na=False)

    actual_columns = list(dataframe.columns)
    if actual_columns != expected_columns:
        raise ValueError(
            f"Unexpected columns in {filename}. "
            f"Expected {expected_columns}, got {actual_columns}"
        )

    for column in integer_columns:
        if dataframe[column].str.strip().eq("").any():
            raise ValueError(f"Blank integer value in {filename}.{column}")
        try:
            dataframe[column] = pd.to_numeric(dataframe[column], errors="raise").astype("int64")
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid integer value in {filename}.{column}") from exc

    for column in date_columns:
        if dataframe[column].str.strip().eq("").any():
            raise ValueError(f"Blank date value in {filename}.{column}")
        try:
            dataframe[column] = pd.to_datetime(
                dataframe[column], format="%Y-%m-%d", errors="raise"
            ).dt.date
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid date value in {filename}.{column}") from exc

    dataframe.attrs["source_metadata"] = {
        "source_file": filename,
        "source_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "rows_read": len(dataframe),
        "read_seconds": round(time.monotonic() - started, 6),
    }
    return dataframe


def load_source_data(raw_dir):
    users = read_source_csv(
        raw_dir,
        SOURCE_FILES["users"]["filename"],
        SOURCE_FILES["users"]["columns"],
        SOURCE_FILES["users"]["integer_columns"],
        SOURCE_FILES["users"]["date_columns"],
    )

    health = read_source_csv(
        raw_dir,
        SOURCE_FILES["health"]["filename"],
        SOURCE_FILES["health"]["columns"],
        SOURCE_FILES["health"]["integer_columns"],
        SOURCE_FILES["health"]["date_columns"],
    )
    health_metadata = dict(health.attrs)
    health = health.rename(columns={"visit_date": "date"})
    health.attrs.update(health_metadata)

    activity = read_source_csv(
        raw_dir,
        SOURCE_FILES["activity"]["filename"],
        SOURCE_FILES["activity"]["columns"],
        SOURCE_FILES["activity"]["integer_columns"],
        SOURCE_FILES["activity"]["date_columns"],
    )

    # Source + one-based CSV data-record ordinal identifies an event in this
    # deterministic fixture. This remains independent of PostgreSQL sequences.
    health["source_row_number"] = range(1, len(health) + 1)
    activity["source_row_number"] = range(1, len(activity) + 1)

    return {
        "raw_users": users,
        "raw_pet_health": health,
        "raw_pet_activity": activity,
    }


def create_tables(conn):
    statements = [
        "CREATE SCHEMA IF NOT EXISTS raw;",
        """
        CREATE TABLE IF NOT EXISTS raw.raw_users (
            pet_id INTEGER PRIMARY KEY,
            owner_id INTEGER NOT NULL,
            owner_age_group TEXT NOT NULL,
            pet_type TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS raw.raw_pet_health (
            source_row_number INTEGER PRIMARY KEY,
            pet_id INTEGER NOT NULL,
            date DATE NOT NULL,
            issue TEXT,
            resolution TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS raw.raw_pet_activity (
            source_row_number INTEGER PRIMARY KEY,
            pet_id INTEGER NOT NULL,
            date DATE NOT NULL,
            activity_type TEXT NOT NULL,
            duration_minutes TEXT
        );
        """,
        # Safe in-place compatibility migration for local volumes created by the
        # earlier portfolio baseline. Fresh volumes already have the target shape.
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'raw' AND table_name = 'raw_users'
                  AND column_name = 'user_id'
            ) THEN
                ALTER TABLE raw.raw_users DROP CONSTRAINT IF EXISTS raw_users_pkey;
                ALTER TABLE raw.raw_users DROP COLUMN user_id;
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = 'raw.raw_users'::regclass AND contype = 'p'
            ) THEN
                ALTER TABLE raw.raw_users
                    ADD CONSTRAINT raw_users_pkey PRIMARY KEY (pet_id);
            END IF;
        END $$;
        """,
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'raw' AND table_name = 'raw_pet_health'
                  AND column_name = 'health_id'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'raw' AND table_name = 'raw_pet_health'
                  AND column_name = 'source_row_number'
            ) THEN
                ALTER TABLE raw.raw_pet_health
                    RENAME COLUMN health_id TO source_row_number;
            END IF;
        END $$;
        ALTER TABLE raw.raw_pet_health
            ALTER COLUMN source_row_number DROP DEFAULT;
        """,
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'raw' AND table_name = 'raw_pet_activity'
                  AND column_name = 'health_id'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'raw' AND table_name = 'raw_pet_activity'
                  AND column_name = 'source_row_number'
            ) THEN
                ALTER TABLE raw.raw_pet_activity
                    RENAME COLUMN health_id TO source_row_number;
            END IF;
        END $$;
        ALTER TABLE raw.raw_pet_activity
            ALTER COLUMN source_row_number DROP DEFAULT;
        """,
    ]

    for statement in statements:
        conn.execute(text(statement))


def refresh_raw_tables(conn, datasets):
    table_names = (
        "raw_users",
        "raw_pet_health",
        "raw_pet_activity",
    )

    for table_name in table_names:
        conn.execute(text(f"TRUNCATE TABLE {RAW_SCHEMA}.{table_name};"))

    load_metrics = {}
    for table_name in table_names:
        dataframe = datasets[table_name]
        started = time.monotonic()
        dataframe.to_sql(
            table_name,
            con=conn,
            schema=RAW_SCHEMA,
            index=False,
            if_exists="append",
            chunksize=500,
        )
        load_metrics[table_name] = {
            "rows_loaded": len(dataframe),
            "load_seconds": round(time.monotonic() - started, 6),
        }

    return load_metrics


def emit_commit_metrics(datasets, load_metrics, elapsed_seconds):
    for table_name, dataframe in datasets.items():
        record = {
            "event": "raw_dataset_committed",
            "table": table_name,
            **dataframe.attrs["source_metadata"],
            **load_metrics[table_name],
            "status": "success",
        }
        print(json.dumps(record, sort_keys=True), flush=True)

    print(
        json.dumps(
            {
                "event": "raw_refresh_committed",
                "datasets": len(datasets),
                "rows_loaded": sum(len(frame) for frame in datasets.values()),
                "elapsed_seconds": round(elapsed_seconds, 6),
                "status": "success",
            },
            sort_keys=True,
        ),
        flush=True,
    )


@contextlib.contextmanager
def get_pg_engine(host, port, database, user, password):
    url = URL.create(
        "postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=database,
    )
    engine = create_engine(url, pool_pre_ping=True, future=True)
    try:
        yield engine
    finally:
        engine.dispose()


def main():
    started = time.monotonic()
    try:
        raw_dir = Path(os.getenv("RAW_DIR", "/opt/airflow/raw_data"))

        # Validate and type all source files before modifying PostgreSQL.
        datasets = load_source_data(raw_dir)

        with get_pg_engine(
            host="db",
            port="5432",
            database=required_env("PET_DB_NAME"),
            user=required_env("PET_DB_USER"),
            password=required_env("PET_DB_PASSWORD"),
        ) as engine:
            # PostgreSQL TRUNCATE and all inserts are part of one transaction.
            # Any exception rolls the entire raw refresh back.
            with engine.begin() as conn:
                create_tables(conn)
                load_metrics = refresh_raw_tables(conn, datasets)

        elapsed = time.monotonic() - started
        emit_commit_metrics(datasets, load_metrics, elapsed)
        sys.stdout.write(
            datetime.now(timezone.utc).isoformat()
            + f" Complete. Elapsed time {elapsed:.4f}s\n"
        )
    except Exception:
        sys.stdout.write(datetime.now(timezone.utc).isoformat() + " INCOMPLETE.\n")
        raise


if __name__ == "__main__":
    main()
