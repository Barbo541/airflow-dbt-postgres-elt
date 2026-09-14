#!/usr/bin/env bash
set -euo pipefail
airflow db migrate
exec airflow standalone
