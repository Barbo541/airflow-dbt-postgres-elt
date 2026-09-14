"""Clean-room E2E validation. Only resources created by this invocation are removed."""

import os
import secrets
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "dockerized_dbt_postgresql_etl/dbt-PostgreSQL-ELT"
DAG = "load_raw_data_and_transform"


def main():
    project = "pet-validation-" + uuid.uuid4().hex[:10]
    # Discard inherited Compose and application config, including developer .env.
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("COMPOSE_", "POSTGRES_", "PET_DB_", "AIRFLOW_", "DBT_"))
    }

    primary_error = None
    with tempfile.TemporaryDirectory(prefix="pet-validation-") as temporary:
        envfile = Path(temporary) / "validation.env"
        admin_user = "validation_admin"
        pet_user = "validation_app"
        pet_database = "validation_pets"
        airflow_user = "validation_airflow"
        airflow_database = "validation_metadata"
        envfile.write_text(
            f"POSTGRES_ADMIN_USER={admin_user}\n"
            f"POSTGRES_ADMIN_PASSWORD={secrets.token_hex(24)}\n"
            "POSTGRES_HOST_PORT=0\n"
            f"PET_DB_USER={pet_user}\n"
            f"PET_DB_PASSWORD={secrets.token_hex(24)}\n"
            f"PET_DB_NAME={pet_database}\n"
            f"AIRFLOW_DB_USER={airflow_user}\n"
            f"AIRFLOW_DB_PASS={secrets.token_hex(24)}\n"
            f"AIRFLOW_DB_NAME={airflow_database}\n"
            "AIRFLOW_HOST_PORT=0\n"
            "AIRFLOW_IMAGE=dbt-pet-airflow:3.3.1\n",
            encoding="utf-8",
        )
        base = [
            "docker",
            "compose",
            "--env-file",
            str(envfile),
            "-p",
            project,
            "-f",
            str(PROJECT / "compose.yml"),
        ]

        def run(*args, capture=False, check=True):
            result = subprocess.run(
                base + list(args),
                env=env,
                check=check,
                text=True,
                capture_output=capture,
            )
            return result.stdout.strip() if capture else ""

        def metadata_sql(statement):
            return run(
                "exec",
                "-T",
                "db",
                "psql",
                "-U",
                admin_user,
                "-d",
                airflow_database,
                "-At",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                statement,
                capture=True,
            )

        def redacted_diagnostics():
            result = run("logs", "--no-color", "--tail", "100", capture=True, check=False)
            diagnostics = result
            for line in envfile.read_text(encoding="utf-8").splitlines():
                if "PASSWORD=" in line or "PASS=" in line:
                    diagnostics = diagnostics.replace(line.split("=", 1)[1], "<redacted>")
            return "\n".join(
                line
                for line in diagnostics.splitlines()
                if not any(word in line.lower() for word in ("password", "secret", "postgresql+"))
            )

        print(f"Validating isolated Compose project: {project}", flush=True)

        try:
            run("config", "--quiet")
            run("build", "af")
            run("up", "-d", "--wait", "--wait-timeout", "360")
            run("exec", "-T", "af", "python", "/opt/airflow/healthcheck.py")
            errors = run(
                "exec",
                "-T",
                "af",
                "airflow",
                "dags",
                "list-import-errors",
                "--output",
                "json",
                capture=True,
            )
            assert errors.rstrip().endswith("[]"), errors

            run("cp", str(ROOT / "scripts/check_database.py"), "af:/tmp/check_database.py")
            run("exec", "-T", "af", "airflow", "dags", "unpause", DAG)

            for iteration in ("first", "second"):
                run_id = "validation_" + iteration
                run("exec", "-T", "af", "airflow", "dags", "trigger", DAG, "--run-id", run_id)
                deadline = time.monotonic() + 600
                while time.monotonic() < deadline:
                    state = metadata_sql(
                        f"SELECT state FROM dag_run WHERE dag_id='{DAG}' AND run_id='{run_id}'"
                    )
                    if state == "success":
                        break
                    if state == "failed":
                        raise RuntimeError(f"DAG {iteration} failed")
                    time.sleep(5)
                else:
                    raise TimeoutError("DAG did not complete in ten minutes")
                run("exec", "-T", "af", "python", "/tmp/check_database.py", iteration)

            run(
                "exec",
                "-T",
                "af",
                "bash",
                "-c",
                "test ! -S /var/run/docker.sock && "
                "test -n \"$(find /opt/airflow/logs -name '*.log' -print -quit)\"",
            )
            run(
                "exec",
                "-T",
                "af",
                "python",
                "-c",
                "import json; p='/opt/airflow/dbt-target/run_results.json'; "
                "r=json.load(open(p)); "
                "assert all(x['status'] in ('success','pass') for x in r['results']); "
                "from collections import Counter; "
                "print('DBT_RESULTS=' + str(dict(Counter("
                "x['unique_id'].split('.')[0] for x in r['results']))))",
            )
            print("CLEAN_ROOM_PASS=true", flush=True)
        except BaseException as exc:
            primary_error = exc
            try:
                diagnostics = redacted_diagnostics()
                if diagnostics:
                    print(diagnostics, file=sys.stderr)
            except Exception as diagnostic_error:
                print(
                    f"Diagnostic collection failed without replacing root error: {diagnostic_error}",
                    file=sys.stderr,
                )
            raise
        finally:
            try:
                # This exact random project name was created above; never use the
                # developer project or prune global Docker resources.
                run("down", "--volumes", "--remove-orphans")
            except Exception as cleanup_error:
                if primary_error is None:
                    raise
                print(
                    f"Cleanup failed without replacing root error: {cleanup_error}",
                    file=sys.stderr,
                )


if __name__ == "__main__":
    main()
