from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from main import (
    create_bigquery_schema,
    fetch_bike_data,
    validate_schema,
    create_spark_schema,
    fetch_existing_station_ids, create_or_update_table,
)

# mock data instead of api call
MOCK_BIKE_DATA = {
    "network": {
        "stations": [
            {
                "id": "0152cd3a3e9e39a478fed3446b0b5eab",
                "timestamp": "2024-09-06T18:25:23.884248Z",
                "free_bikes": 4,
                "empty_slots": 50,
                "latitude": 25.07042,
                "longitude": 121.49713,
                "extra": {
                    "en": {
                        "name": "Station 1",
                        "district": "District 1",
                        "address": "Address 1"
                    }
                }
            },
            {
                "id": "015f3178e7b749936c3b22a7d683584f",
                "timestamp": "2024-09-06T18:25:23.890218Z",
                "free_bikes": 5,
                "empty_slots": 65,
                "latitude": 25.02341,
                "longitude": 121.46857,
                "extra": {
                    "en": {
                        "name": "Station 2",
                        "district": "District 2",
                        "address": "Address 2"
                    }
                }
            }
        ]
    }
}


def assert_schema_fields(schema, expected_fields):
    for i, field_name in enumerate(expected_fields):
        assert schema[i].name == field_name


def test_create_bigquery_schema():
    fact_schema, dim_schema = create_bigquery_schema()
    assert_schema_fields(fact_schema, ["station_id", "timestamp", "free_bikes", "empty_slots"])
    assert_schema_fields(dim_schema, ["station_id", "latitude", "longitude", "name", "district", "address"])


def test_create_spark_schema():
    fact_schema, dim_schema = create_spark_schema()
    assert_schema_fields(fact_schema, ["station_id", "timestamp", "free_bikes", "empty_slots"])
    assert_schema_fields(dim_schema, ["station_id", "latitude", "longitude", "name", "district", "address"])


def test_fetch_bike_data(mocker):
    mock_get = mocker.patch('main.requests.get')
    mock_response = mocker.MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = MOCK_BIKE_DATA
    mock_get.return_value = mock_response

    data = fetch_bike_data()

    assert "stations" in data["network"]
    assert len(data["network"]["stations"]) == len(MOCK_BIKE_DATA["network"]["stations"])


def test_fetch_bike_data_failure(mocker):
    mock_get = mocker.patch('main.requests.get')
    mock_response = mocker.MagicMock()
    mock_response.status_code = 500
    mock_response.json.return_value = {}
    mock_get.return_value = mock_response

    data = fetch_bike_data()

    assert data is None


def test_create_or_update_table(mocker):
    mock_bq_client = mocker.patch('main.bq_client')
    mock_bq_client.get_table.side_effect = NotFound("Table not found")
    mock_bq_client.create_table = mocker.MagicMock()
    schema = [bigquery.SchemaField("station_id", "STRING", mode="REQUIRED")]
    create_or_update_table("test_project.test_dataset.test_table", schema)

    mock_bq_client.create_table.assert_called_once()


def test_validate_schema_mismatch():
    df_schema = ["station_id", "free_bikes"]
    table_schema = ["station_id", "timestamp", "free_bikes", "empty_slots"]
    result = validate_schema(df_schema, table_schema)

    assert result is False


def test_fetch_existing_station_ids(mocker):
    mock_bq_client = mocker.patch('main.bq_client')
    mock_query_job = mocker.MagicMock()
    mock_query_job.result.return_value = [{"station_id": "station_1"}, {"station_id": "station_2"}]
    mock_bq_client.query.return_value = mock_query_job

    station_ids = fetch_existing_station_ids("test_project.test_dataset")

    assert station_ids == {"station_1", "station_2"}
