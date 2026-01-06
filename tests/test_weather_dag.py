import datetime
import importlib

import pytest
from airflow.models import Variable


@pytest.fixture
def weather_dag(monkeypatch):
    monkeypatch.setattr(Variable, "get", lambda key: "test_api_key")
    import dags.weather_dag as module

    importlib.reload(module)
    return module


def test_process_weather_maps_expected_fields(weather_dag):
    payload = {
        "data": [
            {
                "dt": 1700000000,
                "temp": 12.3,
                "clouds": 45,
                "humidity": 67,
                "wind_speed": 5.4,
            }
        ]
    }

    class DummyTI:
        def xcom_pull(self, *args, **kwargs):
            return payload

    result = weather_dag.process_weather({"city": "Kyiv"}, DummyTI())

    assert result == {
        "execution_time": 1700000000,
        "temperature": 12.3,
        "humidity": 45,
        "cloudiness": 67,
        "wind_speed": 5.4,
    }


def test_create_table_runs_expected_sql(weather_dag, monkeypatch):
    captured = {}

    class DummyPostgresHook:
        def __init__(self, postgres_conn_id):
            captured["postgres_conn_id"] = postgres_conn_id

        def run(self, sql):
            captured["sql"] = sql

    monkeypatch.setattr(weather_dag, "PostgresHook", DummyPostgresHook)

    weather_dag.create_table()

    assert captured["postgres_conn_id"] == "pg_conn"
    assert "CREATE TABLE IF NOT EXISTS measures" in captured["sql"]


def test_extract_weather_data_builds_request(weather_dag, monkeypatch):
    captured = {}

    class DummyResponse:
        def json(self):
            return {"status": "ok"}

    class DummyHttpHook:
        def __init__(self, method, http_conn_id):
            captured["method"] = method
            captured["http_conn_id"] = http_conn_id

        def run(self, endpoint, data):
            captured["endpoint"] = endpoint
            captured["data"] = data
            return DummyResponse()

    monkeypatch.setattr(weather_dag, "HttpHook", DummyHttpHook)

    execution_date = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    result = weather_dag.extract_weather_data(
        {"lat": "1.23", "lon": "4.56"}, execution_date
    )

    assert result == {"status": "ok"}
    assert captured["method"] == "GET"
    assert captured["http_conn_id"] == "weather_api_conn"
    assert captured["endpoint"] == "data/3.0/onecall/timemachine?"
    assert captured["data"]["appid"] == "test_api_key"
    assert captured["data"]["lat"] == "1.23"
    assert captured["data"]["lon"] == "4.56"
    assert captured["data"]["dt"] == int(execution_date.timestamp())
    assert captured["data"]["units"] == "metric"
