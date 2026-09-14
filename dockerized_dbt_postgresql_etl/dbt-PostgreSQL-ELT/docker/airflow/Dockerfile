FROM apache/airflow:3.3.1

# The upstream image already runs as the non-root airflow user. Keep the custom
# runtime aligned with the release-matched Airflow constraints.
USER airflow

COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir \
        "apache-airflow==${AIRFLOW_VERSION}" \
        --constraint "${HOME}/constraints.txt" \
        --requirement /opt/airflow/requirements.txt \
    && pip check

COPY --chown=airflow:root requirements-dbt.txt /opt/airflow/requirements-dbt.txt

# dbt remains isolated from Airflow's Python environment. This avoids adapter /
# telemetry dependency coupling while keeping orchestration inside one container.
RUN python -m venv /opt/airflow/dbt-venv \
    && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir \
        --requirement /opt/airflow/requirements-dbt.txt \
    && /opt/airflow/dbt-venv/bin/pip check \
    && ln --symbolic --force /opt/airflow/dbt-venv/bin/dbt "${HOME}/.local/bin/dbt"

COPY --chown=airflow:root start.sh healthcheck.py metadata_url.py /opt/airflow/
RUN mkdir -p /opt/airflow/state
