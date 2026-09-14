"""Disposable single-node benchmark for the portfolio pipeline.

The benchmark never uses the developer Compose project. It creates a random
project name, generated credentials, temporary raw fixtures, and disposable
volumes, then removes only those resources.
"""

import argparse
import csv
import json
import os
import secrets
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

from generate_data import generate

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "dockerized_dbt_postgresql_etl/dbt-PostgreSQL-ELT"
DAG = "load_raw_data_and_transform"
EVENT_ROWS = 940 + 938


def scale_events(raw_dir: Path, scale: int) -> None:
    generate(raw_dir)
    if scale == 1:
        return
    for filename in ("pet_health.csv", "pet_activities.csv"):
        path = raw_dir / filename
        with path.open(newline="", encoding="utf-8") as stream:
            rows = list(csv.reader(stream))
        header, data = rows[0], rows[1:]
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(header)
            for _ in range(scale):
                writer.writerows(data)


def main(scales, output):
    if any(scale < 1 for scale in scales):
        raise ValueError("Scales must be positive integers")

    project_name = "pet-benchmark-" + uuid.uuid4().hex[:10]
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("COMPOSE_", "POSTGRES_", "PET_DB_", "AIRFLOW_", "DBT_"))
    }

    with tempfile.TemporaryDirectory(prefix="pet-benchmark-") as temporary:
        temporary_path = Path(temporary)
        raw_dir = temporary_path / "raw_data"
        envfile = temporary_path / "benchmark.env"
        admin_user = "benchmark_admin"
        pet_database = "benchmark_pets"
        airflow_database = "benchmark_metadata"
        envfile.write_text(
            f"POSTGRES_ADMIN_USER={admin_user}\n"
            f"POSTGRES_ADMIN_PASSWORD={secrets.token_hex(24)}\n"
            "POSTGRES_HOST_PORT=0\n"
            "PET_DB_USER=benchmark_app\n"
            f"PET_DB_PASSWORD={secrets.token_hex(24)}\n"
            f"PET_DB_NAME={pet_database}\n"
            "AIRFLOW_DB_USER=benchmark_airflow\n"
            f"AIRFLOW_DB_PASS={secrets.token_hex(24)}\n"
            f"AIRFLOW_DB_NAME={airflow_database}\n"
            "AIRFLOW_HOST_PORT=0\n"
            "AIRFLOW_IMAGE=dbt-pet-airflow:3.3.1\n"
            f"RAW_DATA_HOST_DIR={raw_dir.resolve().as_posix()}\n",
            encoding="utf-8",
        )
        base = [
            "docker",
            "compose",
            "--env-file",
            str(envfile),
            "-p",
            project_name,
            "-f",
            str(PROJECT / "compose.yml"),
        ]

        def run(*args, capture=False):
            result = subprocess.run(
                base + list(args),
                env=env,
                check=True,
                text=True,
                capture_output=capture,
            )
            return result.stdout.strip() if capture else ""

        def admin_sql(database, statement):
            return run(
                "exec",
                "-T",
                "db",
                "psql",
                "-U",
                admin_user,
                "-d",
                database,
                "-At",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                statement,
                capture=True,
            )

        results = []
        print(f"Benchmark project: {project_name}", flush=True)
        try:
            scale_events(raw_dir, scales[0])
            run("config", "--quiet")
            run("build", "af")
            run("up", "-d", "--wait", "--wait-timeout", "360")
            run("exec", "-T", "af", "airflow", "dags", "unpause", DAG)

            for scale in scales:
                scale_events(raw_dir, scale)
                run_id = f"benchmark_{scale}x_{uuid.uuid4().hex[:8]}"
                started = time.monotonic()
                run("exec", "-T", "af", "airflow", "dags", "trigger", DAG, "--run-id", run_id)
                deadline = time.monotonic() + 1200
                while time.monotonic() < deadline:
                    state = admin_sql(
                        airflow_database,
                        f"SELECT state FROM dag_run WHERE dag_id='{DAG}' AND run_id='{run_id}'",
                    )
                    if state == "success":
                        break
                    if state == "failed":
                        raise RuntimeError(f"Benchmark DAG failed at {scale}x")
                    time.sleep(5)
                else:
                    raise TimeoutError(f"Benchmark DAG exceeded 20 minutes at {scale}x")
                wall_seconds = round(time.monotonic() - started, 3)

                task_rows = admin_sql(
                    airflow_database,
                    "SELECT task_id || '|' || "
                    "ROUND(EXTRACT(EPOCH FROM (end_date - start_date))::numeric, 3) "
                    "FROM task_instance "
                    f"WHERE dag_id='{DAG}' AND run_id='{run_id}' ORDER BY task_id",
                )
                task_seconds = {}
                for row in task_rows.splitlines():
                    if not row:
                        continue
                    task_id, seconds = row.split("|", 1)
                    task_seconds[task_id] = float(seconds)

                final_count = int(
                    admin_sql(
                        pet_database,
                        "SELECT count(*) FROM public_final.final_join_w_users_4",
                    )
                )
                expected = EVENT_ROWS * scale
                if final_count != expected:
                    raise AssertionError(
                        f"Unexpected final count at {scale}x: {final_count} != {expected}"
                    )

                plan_text = admin_sql(
                    pet_database,
                    "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
                    "SELECT a.event_id, a.event_source, a.pet_id, a.date, "
                    "a.activity_type, a.duration_minutes, a.issue, a.resolution, "
                    "u.owner_id, u.owner_age_group, u.pet_type "
                    "FROM public_raw.clean_fields_3 a "
                    "LEFT JOIN raw.raw_users u ON a.pet_id = u.pet_id",
                )
                plan = json.loads(plan_text)[0]
                results.append(
                    {
                        "scale": scale,
                        "input_event_rows": expected,
                        "final_rows": final_count,
                        "dag_wall_seconds": wall_seconds,
                        "load_raw_data_seconds": task_seconds.get("load_raw_data"),
                        "dbt_build_seconds": task_seconds.get("dbt_build"),
                        "plan_node": plan["Plan"]["Node Type"],
                        "planning_ms": plan.get("Planning Time"),
                        "execution_ms": plan.get("Execution Time"),
                    }
                )
                print(json.dumps(results[-1], sort_keys=True), flush=True)
        finally:
            try:
                run("down", "--volumes", "--remove-orphans")
            except Exception as cleanup_error:
                print(f"Benchmark cleanup warning: {cleanup_error}")

        payload = {
            "project": "pet-event-analytics",
            "scales": scales,
            "results": results,
        }
        if output:
            output_path = Path(output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
            print(f"Wrote benchmark results to {output_path}")

        print("\n| Scale | Events | Load task (s) | dbt task (s) | DAG wall (s) | Plan | Exec (ms) |")
        print("| ---: | ---: | ---: | ---: | ---: | --- | ---: |")
        for row in results:
            print(
                f"| {row['scale']}x | {row['input_event_rows']} | "
                f"{row['load_raw_data_seconds']} | {row['dbt_build_seconds']} | "
                f"{row['dag_wall_seconds']} | {row['plan_node']} | {row['execution_ms']} |"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scales",
        nargs="+",
        type=int,
        default=[1, 100],
        help="Event multipliers. Add 1000 explicitly for a stress run.",
    )
    parser.add_argument(
        "--output",
        help="Optional JSON output path, e.g. .validation/benchmark.json",
    )
    args = parser.parse_args()
    main(args.scales, args.output)
