"""Tests standard tap features using the built-in SDK tests library."""

import os
from datetime import datetime, timedelta, timezone

import pytest
from singer_sdk.testing import get_standard_tap_tests

from tap_google_analytics.tap import TapGoogleAnalytics
from tap_google_analytics.client import GoogleAnalyticsStream
from tap_google_analytics.tests.utilities import get_secrets_dict

SAMPLE_CONFIG_SERVICE = {
    "view_id": "188392047",
    "end_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    "start_date": (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d"),
    "key_file_location": f"{os.path.dirname(__file__)}/test_data/client_secrets.json",
}

SAMPLE_CONFIG_CLIENT_SECRETS = {
    "view_id": "188392047",
    "end_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    "start_date": (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d"),
    "client_secrets": get_secrets_dict(),
}


@pytest.mark.parametrize(
    "sample_config",
    [
        (SAMPLE_CONFIG_SERVICE),
        (SAMPLE_CONFIG_CLIENT_SECRETS),
    ],
)
Run standard built-in tap tests from the SDK:
def test_standard_tap_tests(config, sample_config):
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapGoogleAnalytics, config=sample_config)
    for test in tests:
        test()


def test_no_credentials():
    """Run standard tap tests from the SDK."""
    SAMPLE_CONFIG_SERVICE2 = {
        "view_id": "188392047",
        "start_date": (datetime.now(timezone.utc) - timedelta(days=2)).strftime(
            "%Y-%m-%d"
        ),
    }
    with pytest.raises(Exception) as e:
        tap = TapGoogleAnalytics(config=SAMPLE_CONFIG_SERVICE2)
        tap.run_connection_test()
        assert e.value == "No valid credentials provided."


def test_report_definition():
    report = {
        "dimensions": ["ga:date", "ga:country"],
        "metrics": ["ga:sessions", "ga:sessionsPerUser", "ga:avgSessionDuration"],
    }
    definition = GoogleAnalyticsStream._generate_report_definition(report)

    assert definition["dimensions"][0]["name"] == "ga:date"
    assert definition["dimensions"][1]["name"] == "ga:country"

    assert definition["metrics"][0]["expression"] == "ga:sessions"

    assert len(definition.keys()) == 2
    assert len(definition["dimensions"]) == 2
    assert len(definition["metrics"]) == 3


def test_report_definition_with_filters():
    report = {
        "dimensions": ["ga:date", "ga:country"],
        "metrics": ["ga:sessions", "ga:sessionsPerUser", "ga:avgSessionDuration"],
        "dimensionFilterClauses": {
            "ga:country": {"operator": "EXACT", "expressions": ["United States"]}
        },
        "metricFilterClauses": {
            "ga:pageviews": {"operator": "GREATER_THAN", "comparisonValue": "2"}
        },
        "orderBys": {"ga:sessions": "DESCENDING", "ga:pageviews": "DESCENDING"},
    }

    definition = GoogleAnalyticsStream._generate_report_definition(report)

    assert definition["dimensions"][0]["name"] == "ga:date"
    assert definition["dimensions"][1]["name"] == "ga:country"

    assert definition["metrics"][0]["expression"] == "ga:sessions"

    assert len(definition.keys()) == 5
    assert len(definition["dimensions"]) == 2
    assert len(definition["metrics"]) == 3
    assert len(definition["orderBys"]) == 2
    assert len(definition["dimensionFilterClauses"][0]["filters"]) == 1
    assert len(definition["metricFilterClauses"][0]["filters"]) == 1

    assert (
        definition["dimensionFilterClauses"][0]["filters"][0]["dimensionName"]
        == "ga:country"
    )
    assert definition["dimensionFilterClauses"][0]["filters"][0]["expressions"] == [
        "United States"
    ]
    assert definition["dimensionFilterClauses"][0]["filters"][0]["operator"] == "EXACT"

    assert (
        definition["metricFilterClauses"][0]["filters"][0]["metricName"]
        == "ga:pageviews"
    )
    assert definition["metricFilterClauses"][0]["filters"][0]["comparisonValue"] == "2"
    assert (
        definition["metricFilterClauses"][0]["filters"][0]["operator"] == "GREATER_THAN"
    )

    assert definition["orderBys"][0]["fieldName"] == "ga:sessions"
    assert definition["orderBys"][0]["sortOrder"] == "DESCENDING"
