import pytest
import requests_mock
from datetime import datetime
from lariat_client import (
    configure,
    Field,
    Indicator,
    RawDataset,
    Dataset,
    FilterClause,
    Filter,
    MetricRecord,
    MetricRecordList,
    get_raw_datasets,
    get_dataset,
    get_datasets,
    get_indicators,
    get_indicator,
    query,
    s,
    LARIAT_PUBLIC_API_ENDPOINT,
)


# Mock API responses
def mock_get_raw_datasets(mocker):
    raw_datasets_response = {
        "raw_datasets": [
            {
                "source_id": "123",
                "data_source": "example",
                "name": "Example Dataset",
                "schema": {"example_field": "string"},
            }
        ]
    }
    mocker.get(
        f"{LARIAT_PUBLIC_API_ENDPOINT}/raw-datasets",
        json=raw_datasets_response,
    )


def mock_get_datasets(mocker):
    datasets_response = {
        "computed_datasets": [
            {
                "data_source": "example",
                "source_id": "123",
                "dataset_name": "Example Dataset",
                "id": 1,
                "query": "SELECT * FROM example_dataset",
                "schema": {"example_field": "string"},
            }
        ]
    }
    mocker.get(
        f"{LARIAT_PUBLIC_API_ENDPOINT}/datasets",
        json=datasets_response,
    )


def mock_get_indicators(mocker):
    indicators_response = {
        "indicators": [
            {
                "indicator_id": 1,
                "computed_dataset_id": 1,
                "computed_dataset_name": "Example Dataset",
                "calculation": "COUNT(example_field)",
                "filters": None,
                "group_fields": None,
                "aggregations": None,
                "name": "Example Indicator",
                "tags": [],
            }
        ]
    }
    mocker.get(
        f"{LARIAT_PUBLIC_API_ENDPOINT}/indicators",
        json=indicators_response,
    )


def mock_get_indicator(mocker):
    indicator_response = {
        "indicator": {
            "indicator_id": 1,
            "computed_dataset_id": 1,
            "computed_dataset_name": "Example Dataset",
            "calculation": "COUNT(example_field)",
            "filters": None,
            "group_fields": None,
            "aggregations": None,
            "name": "Example Indicator",
            "tags": [],
        }
    }
    mocker.get(
        f"{LARIAT_PUBLIC_API_ENDPOINT}/indicator",
        json=indicator_response,
    )


def mock_query_metrics(mocker):
    query_metrics_response = {
        "records": [
            {
                "evaluation_time": 1633014000,
                "value": 42.0,
                "dimensions": {"country": "US", "state": "CA"},
            },
            {
                "evaluation_time": 1633017600,
                "value": 45.0,
                "dimensions": {"country": "US", "state": "CA"},
            },
        ]
    }
    mocker.get(
        f"{LARIAT_PUBLIC_API_ENDPOINT}/query-metrics",
        json=query_metrics_response,
    )


# Test configure function
def test_configure():
    configure("test_api_key", "test_application_key")
    assert (
        s.headers["X-Lariat-Api-Key"] == "test_api_key"
        and s.headers["X-Lariat-Application-Key"] == "test_application_key"
    )


# Test get_raw_datasets function
def test_get_raw_datasets():
    with requests_mock.Mocker() as mocker:
        mock_get_raw_datasets(mocker)
        raw_datasets = get_raw_datasets([1])
        assert len(raw_datasets) == 1
        assert raw_datasets[0].source_id == "123"
        assert raw_datasets[0].data_source == "example"
        assert raw_datasets[0].name == "Example Dataset"
        assert raw_datasets[0].schema == {"example_field": "string"}


# Test get_datasets function
def test_get_datasets():
    with requests_mock.Mocker() as mocker:
        mock_get_datasets(mocker)
        datasets = get_datasets([1])
        assert len(datasets) == 1
        assert datasets[0].data_source == "example"
        assert datasets[0].source_id == "123"
        assert datasets[0].name == "Example Dataset"
        assert datasets[0].id == 1
        assert datasets[0].query == "SELECT * FROM example_dataset"
        assert datasets[0].schema == {"example_field": "string"}


# Test get_indicator function
def test_get_indicator():
    with requests_mock.Mocker() as mocker:
        mock_get_indicator(mocker)
        indicator = get_indicator(1)
        assert indicator.id == 1
        assert indicator.dataset_id == 1
        assert indicator.dataset_name == "Example Dataset"
        assert (
            indicator.query
            == 'SELECT COUNT(example_field) AS value FROM "Example Dataset"'
        )
        assert indicator.aggregations == None
        assert indicator.name == "Example Indicator"
        assert indicator.dimensions == None
        assert indicator.tags == []


# Test get_indicators function


def test_get_indicators():
    with requests_mock.Mocker() as mocker:
        mock_get_indicators(mocker)
        indicators = get_indicators()
        assert len(indicators) == 1
        assert indicators[0].id == 1
        assert indicators[0].dataset_id == 1
        assert indicators[0].dataset_name == "Example Dataset"
        assert (
            indicators[0].query
            == 'SELECT COUNT(example_field) AS value FROM "Example Dataset"'
        )
        assert indicators[0].aggregations == None
        assert indicators[0].name == "Example Indicator"
        assert indicators[0].dimensions == None
        assert indicators[0].tags == []


# Test query function
def test_query(monkeypatch):
    with requests_mock.Mocker() as mocker:
        mock_query_metrics(mocker)
        indicator_id = 1
        from_ts = datetime(2021, 9, 30)
        to_ts = datetime(2021, 10, 1)
        group_by = ["country", "state"]
        aggregate = "sum"
        query_filter = Filter(
            clauses=[FilterClause(field="country", operator="eq", values="US")],
            operator="AND",
        )
        output_format = "json"

        result = query(
            indicator_id,
            from_ts,
            to_ts,
            group_by,
            aggregate,
            query_filter,
            output_format,
        )

        assert isinstance(result, MetricRecordList)
        assert len(result.records) == 2
        assert result.records[0].evaluation_time == 1633014000
        assert result.records[0].value == 42.0
        assert result.records[0].dimensions == {"country": "US", "state": "CA"}
        assert result.records[1].evaluation_time == 1633017600
        assert result.records[1].value == 45.0
        assert result.records[1].dimensions == {"country": "US", "state": "CA"}
