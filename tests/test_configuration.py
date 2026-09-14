import runpy
from pathlib import Path

import check_database
import yaml

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "dockerized_dbt_postgresql_etl/dbt-PostgreSQL-ELT"


def test_ci_workflow_shape():
    # BaseLoader follows YAML 1.2's intended treatment of the Actions 'on' key.
    workflow = yaml.load((ROOT / ".github/workflows/ci.yml").read_text(), Loader=yaml.BaseLoader)
    assert set(workflow["on"]) == {"push", "pull_request"}
    steps = workflow["jobs"]["validate"]["steps"]
    assert any(step.get("run") == "python scripts/validate.py" for step in steps)


def test_runtime_security_and_topology():
    compose = yaml.safe_load((PROJECT / "compose.yml").read_text())
    assert set(compose["services"]) == {"db", "af"}
    assert compose["services"]["db"]["image"] == "postgres:15.19"

    for service in compose["services"].values():
        assert all(port.startswith("127.0.0.1:") for port in service["ports"])

    mounts = compose["services"]["af"]["volumes"]
    for mount in mounts:
        if isinstance(mount, str) and mount.startswith("./"):
            assert mount.endswith(":ro")
        if isinstance(mount, dict) and mount.get("type") == "bind":
            assert mount.get("read_only") is True
    assert "docker.sock" not in str(mounts)

    airflow_env = compose["services"]["af"]["environment"]
    assert "POSTGRES_ADMIN_USER" not in airflow_env
    assert "POSTGRES_ADMIN_PASSWORD" not in airflow_env
    assert airflow_env["PET_DB_USER"] == "${PET_DB_USER}"


def test_runtime_versions_are_explicit():
    dockerfile = (PROJECT / "docker/airflow/Dockerfile").read_text()
    dbt_requirements = (PROJECT / "docker/airflow/requirements-dbt.txt").read_text()
    assert dockerfile.startswith("FROM apache/airflow:3.3.1")
    assert "dbt-core==1.11.11" in dbt_requirements
    assert "dbt-postgres==1.11.0" in dbt_requirements


def test_metadata_command_has_no_trailing_newline(monkeypatch, capsys):
    from sqlalchemy.engine import make_url

    monkeypatch.setenv("AIRFLOW_DB_USER", "metadata_user")
    monkeypatch.setenv("AIRFLOW_DB_PASS", "test@:/?#%")
    monkeypatch.setenv("AIRFLOW_DB_NAME", "metadata_db")
    runpy.run_path(str(PROJECT / "docker/airflow/metadata_url.py"))
    output = capsys.readouterr().out
    assert output == output.strip()
    url = make_url(output)
    assert url.database == "metadata_db"
    assert url.password == "test@:/?#%"


def test_forced_rollback_matches_only_the_injected_failure_marker():
    expected = RuntimeError("invalid input syntax: forced_database_failure")
    unrelated = RuntimeError("some other database failure")

    assert check_database.is_expected_forced_database_failure(expected)
    assert not check_database.is_expected_forced_database_failure(unrelated)
