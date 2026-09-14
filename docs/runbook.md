# Local operations runbook

Run Compose commands from:

`dockerized_dbt_postgresql_etl/dbt-PostgreSQL-ELT`

## Start and verify

```sh
docker compose config --quiet
docker compose build af
docker compose up -d --wait --wait-timeout 360
docker compose ps
docker compose exec af python /opt/airflow/healthcheck.py
docker compose exec af airflow dags list-import-errors
```

A healthy local stack has PostgreSQL ready, Airflow API/scheduler/DAG processor
healthy, and no DAG import errors.

## Trigger the pipeline

```sh
docker compose exec af airflow dags unpause load_raw_data_and_transform
docker compose exec af airflow dags trigger load_raw_data_and_transform
docker compose exec af airflow dags list-runs -d load_raw_data_and_transform
```

The DAG is intentionally serialized with `max_active_runs=1`. Do not run the
loader manually while a DAG run is active.

## Inspect a failed run

```sh
docker compose ps
docker compose exec af python /opt/airflow/healthcheck.py
docker compose exec af airflow dags list-import-errors
docker compose exec af airflow dags list-runs -d load_raw_data_and_transform
docker compose exec af ls /opt/airflow/logs
docker compose exec af ls /opt/airflow/logs/dbt
```

Use the Airflow task log for the failing task. Successful ingestion emits structured
JSON records containing source filename, bytes, SHA-256, rows read/loaded, and read/load
timing only after the database transaction has committed.

If the loader fails, all raw-table changes are rolled back. If `dbt_build` fails after
a raw commit, the new raw snapshot can exist while reporting models are incomplete;
downstream consumers must use successful DAG completion as the publication signal.

`/opt/airflow/dbt-target/run_results.json` records dbt model/test outcomes. Airflow
startup logs may expose the generated local Simple Auth Manager password; inspect them
privately and redact credentials before sharing.

## Query the data product

Connect a SQL client to `127.0.0.1:${POSTGRES_HOST_PORT:-5433}` with `PET_DB_USER`,
`PET_DB_PASSWORD`, and `PET_DB_NAME` from the local `.env`.

Useful checks:

```sql
SELECT event_source, count(*)
FROM public_final.final_join_w_users_4
GROUP BY event_source;
-- activity = 938; health = 940 for the committed fixture

SELECT pet_type, activity_type, count(*), avg(duration_minutes)
FROM public_final.final_join_w_users_4
GROUP BY pet_type, activity_type
ORDER BY pet_type, activity_type;
```

Health `duration_minutes = 0` is a modeling convention. Unknown activity duration is
`NULL`; exclude health events when interpreting average activity duration.

## Credentials and role boundary

A fresh PostgreSQL volume uses three credential domains:

- `POSTGRES_ADMIN_*`: cluster bootstrap/health-check role, retained only by the DB service;
- `PET_DB_*`: application role/database used by ingestion and dbt;
- `AIRFLOW_DB_*`: dedicated Airflow metadata role/database.

The Airflow container does not receive the bootstrap/admin credential.

PostgreSQL initialization scripts run only when the data directory is empty. Changing
`.env` does **not** rotate or create roles inside an existing volume. If you are moving
from an earlier portfolio version that used `POSTGRES_USER` as the application role,
first prove the new release with `python scripts/validate.py`. Then plan an explicit
local credential migration or a deliberately approved rebuild of this project's own
synthetic development volume. Never delete volumes as an automatic troubleshooting step.

The loader contains a compatibility migration for the earlier raw-table identity columns
(`user_id` / `health_id`) so an otherwise compatible local database can move toward the
new `pet_id` / `source_row_number` schema during the transactional refresh. Database role
creation still requires fresh-volume initialization or an explicit administrator migration.

## Source contract failures

The loader validates all files before DB access. Failures can include:

- missing required file;
- unexpected column name/order;
- blank or invalid integer identifier;
- blank or invalid `YYYY-MM-DD` date.

Repair/regenerate the source rather than weakening the contract to make a run green.
The deterministic fixture generator is:

```sh
python scripts/generate_data.py
```

## Startup, mounts, and persistence

Port conflicts: change `POSTGRES_HOST_PORT` or `AIRFLOW_HOST_PORT` in local `.env`.
Both published ports bind to `127.0.0.1`.

`postgres_data`, `airflow_logs`, and `airflow_state` persist across container
replacement. The DAG/dbt/source code bind mounts are read-only. dbt writes target artifacts
outside the read-only source tree through `DBT_TARGET_PATH`, and dbt logs go to the
persistent Airflow log volume.

Logs have no automatic retention policy in this small reference implementation. Monitor
disk usage and archive/remove stale logs deliberately. Database backup/restore automation
is outside this local portfolio scope.

## Independent clean-room verification

From the repository root:

```sh
python scripts/validate.py
```

This validator ignores the developer `.env`, generates credentials, creates a random
`pet-validation-*` project and fresh volumes, builds the runtime, checks service health,
executes the real DAG twice, compares the complete business snapshot, forces a database
failure to prove rollback, checks dbt artifacts/logs/socket absence, and removes only its
own resources.

The diagnostic and cleanup paths are best-effort: a cleanup/logging failure is reported
without replacing the original pipeline exception.

If the host process is forcibly killed before cleanup, use the exact random project name
printed at validator startup and remove only that project's resources. Never use a global
Docker prune command.

## Performance study

For an isolated scale test:

```sh
python scripts/benchmark.py --scales 1 100 --output .validation/benchmark.json
```

The release candidate was also stress-tested separately at `1000x` (1,878,000
events). The benchmark uses a random `pet-benchmark-*` Compose project and does
not touch the development project. Measured release-candidate results and tuning
decisions are recorded in `docs/performance.md`; rerun the benchmark after any
change to the ingestion path, dbt SQL/materializations, PostgreSQL version, or
resource allocation before making new performance claims.

## Safe shutdown

```sh
docker compose stop
```

Or remove containers/network while retaining named volumes:

```sh
docker compose down
```

Do not add `--volumes` to normal development shutdown.

## Reference documentation

- Apache Airflow 3.3.1 documentation: https://airflow.apache.org/docs/apache-airflow/3.3.1/
- dbt unit tests: https://docs.getdbt.com/docs/build/unit-tests
- dbt model contracts: https://docs.getdbt.com/docs/mesh/govern/model-contracts
- PostgreSQL 15 documentation: https://www.postgresql.org/docs/15/
