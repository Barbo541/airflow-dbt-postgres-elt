"""Print the metadata URL only for Airflow's internal configuration command."""
import os

from sqlalchemy.engine import URL

print(URL.create(
    "postgresql+psycopg2",
    username=os.environ["AIRFLOW_DB_USER"],
    password=os.environ["AIRFLOW_DB_PASS"],
    host="db",
    port=5432,
    database=os.environ["AIRFLOW_DB_NAME"],
).render_as_string(hide_password=False), end="")
