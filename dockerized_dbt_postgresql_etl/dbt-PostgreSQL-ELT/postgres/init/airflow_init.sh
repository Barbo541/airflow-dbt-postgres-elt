#!/usr/bin/env bash
set -euo pipefail

if [[ "$POSTGRES_USER" == "$PET_DB_USER" || "$POSTGRES_USER" == "$AIRFLOW_DB_USER" ]]; then
    echo "Bootstrap role must differ from runtime roles" >&2
    exit 1
fi
if [[ "$PET_DB_USER" == "$AIRFLOW_DB_USER" || "$PET_DB_NAME" == "$AIRFLOW_DB_NAME" ]]; then
    echo "Pet and Airflow roles/databases must be distinct" >&2
    exit 1
fi

# Runs only on an empty PostgreSQL data volume. The bootstrap role owns cluster
# initialization, while application and Airflow receive separate login roles and
# databases. Passwords come from environment variables and are never embedded.
psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --set ON_ERROR_STOP=1 <<'SQL'
\getenv pet_user PET_DB_USER
\getenv pet_password PET_DB_PASSWORD
\getenv pet_database PET_DB_NAME
\getenv airflow_user AIRFLOW_DB_USER
\getenv airflow_password AIRFLOW_DB_PASS
\getenv airflow_database AIRFLOW_DB_NAME

SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'pet_user', :'pet_password') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'pet_database', :'pet_user') \gexec
SELECT format('REVOKE CONNECT ON DATABASE %I FROM PUBLIC', :'pet_database') \gexec
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'airflow_user', :'airflow_password') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'airflow_database', :'airflow_user') \gexec
SELECT format('REVOKE CONNECT ON DATABASE %I FROM PUBLIC', :'airflow_database') \gexec
SQL
