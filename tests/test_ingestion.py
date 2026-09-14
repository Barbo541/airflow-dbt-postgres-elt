from datetime import date
from unittest.mock import Mock

import load_data_python as loader
import pytest
from generate_data import PROJECT, generate


@pytest.mark.parametrize("value", [None, ""])
def test_required_env_rejects_missing(monkeypatch, value):
    monkeypatch.delenv("TEST_REQUIRED", raising=False)
    if value is not None:
        monkeypatch.setenv("TEST_REQUIRED", value)
    with pytest.raises(RuntimeError, match="TEST_REQUIRED"):
        loader.required_env("TEST_REQUIRED")


def test_required_env(monkeypatch):
    monkeypatch.setenv("TEST_REQUIRED", "configured")
    assert loader.required_env("TEST_REQUIRED") == "configured"


def test_missing_source(tmp_path):
    with pytest.raises(FileNotFoundError):
        loader.read_source_csv(tmp_path, "missing.csv", ["pet_id"])


@pytest.mark.parametrize("headers", ["pet_id,extra", "wrong", "owner_id,pet_id"])
def test_rejects_invalid_headers(tmp_path, headers):
    (tmp_path / "bad.csv").write_text(headers + "\n1,2\n")
    with pytest.raises(ValueError, match="Unexpected columns"):
        loader.read_source_csv(tmp_path, "bad.csv", ["pet_id", "owner_id"])


def test_rejects_invalid_integer_before_db(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("pet_id,date\nnot-an-int,2026-01-01\n")
    with pytest.raises(ValueError, match="Invalid integer"):
        loader.read_source_csv(
            tmp_path,
            "bad.csv",
            ["pet_id", "date"],
            integer_columns=["pet_id"],
            date_columns=["date"],
        )


def test_rejects_invalid_date_before_db(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("pet_id,date\n1,not-a-date\n")
    with pytest.raises(ValueError, match="Invalid date"):
        loader.read_source_csv(
            tmp_path,
            "bad.csv",
            ["pet_id", "date"],
            integer_columns=["pet_id"],
            date_columns=["date"],
        )


def test_source_contracts_normalization_and_audit_metadata():
    data = loader.load_source_data(PROJECT / "raw_data")
    assert {key: len(value) for key, value in data.items()} == {
        "raw_users": 522,
        "raw_pet_health": 940,
        "raw_pet_activity": 938,
    }
    assert "date" in data["raw_pet_health"]
    assert "visit_date" not in data["raw_pet_health"]
    assert data["raw_users"].pet_id.is_unique
    assert isinstance(data["raw_pet_health"].date.iloc[0], date)

    for table in ("raw_pet_health", "raw_pet_activity"):
        assert set(data[table].pet_id) <= set(data["raw_users"].pet_id)
        assert data[table].source_row_number.tolist() == list(range(1, len(data[table]) + 1))

    for frame in data.values():
        metadata = frame.attrs["source_metadata"]
        assert metadata["rows_read"] == len(frame)
        assert metadata["source_bytes"] > 0
        assert len(metadata["sha256"]) == 64
        assert metadata["read_seconds"] >= 0


def test_generator_reproduces_committed_bytes(tmp_path):
    generate(tmp_path)
    for spec in loader.SOURCE_FILES.values():
        filename = spec["filename"]
        assert (tmp_path / filename).read_bytes() == (PROJECT / "raw_data" / filename).read_bytes()


def test_url_handles_reserved_password_and_disposes(monkeypatch):
    engine = Mock()
    factory = Mock(return_value=engine)
    monkeypatch.setattr(loader, "create_engine", factory)
    with loader.get_pg_engine("db", "5432", "pets", "app", "p@ss:/?#%") as actual:
        assert actual is engine
    url = factory.call_args.args[0]
    assert url.password == "p@ss:/?#%"
    assert url.host == "db" and url.port == 5432
    engine.dispose.assert_called_once()


def test_validate_all_sources_before_connecting(tmp_path, monkeypatch):
    generate(tmp_path)
    (tmp_path / "pet_activities.csv").write_text("wrong\n1\n")
    factory = Mock()
    monkeypatch.setattr(loader, "get_pg_engine", factory)
    monkeypatch.setenv("RAW_DIR", str(tmp_path))
    with pytest.raises(ValueError):
        loader.main()
    factory.assert_not_called()
