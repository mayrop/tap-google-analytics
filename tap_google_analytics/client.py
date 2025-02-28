"""Custom client handling, including GoogleAnalyticsStream base class."""

import copy
import socket
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

import backoff
from googleapiclient.errors import HttpError
from singer_sdk import typing as th
from singer_sdk.streams import Stream

from tap_google_analytics.error import (
    TapGaAuthenticationError,
    TapGaBackendServerError,
    TapGaInvalidArgumentError,
    TapGaQuotaExceededError,
    TapGaRateLimitError,
    TapGaUnknownError,
    error_reason,
    is_fatal_error,
)

class GoogleAnalyticsStream(Stream):
    """Stream class for GoogleAnalytics streams."""

    def __init__(self, *args, **kwargs) -> None:
        """Init GoogleAnalyticsStream."""
        self.report = kwargs.pop("ga_report")
        self.dimensions_ref = kwargs.pop("ga_dimensions_ref")
        self.metrics_ref = kwargs.pop("ga_metrics_ref")
        self.analytics = kwargs.pop("ga_analytics_client")

        super().__init__(*args, **kwargs)

        self.quota_user = self.config.get("quota_user", None)
        self.end_date = self._get_end_date()
        self.view_id = self.config["view_id"]
        self.page_size = self.config.get("page_size", 1000)
        self.max_records = self.config.get("max_records", None)

    def _get_end_date(self):
        end_date = self.config.get("end_date", datetime.utcnow().strftime("%Y-%m-%d"))
        end_date_offset = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=1)

        return end_date_offset.strftime("%Y-%m-%d")

    def _parse_dimension_type(self, attribute, dimensions_ref):
        if attribute == "ga:segment":
            return "string"
        elif attribute.startswith(
            ("ga:dimension", "ga:customVarName", "ga:customVarValue")
        ):
            # Custom Google Analytics Dimensions that are not part of
            #  self.dimensions_ref. They are always strings
            return "string"
        elif attribute in dimensions_ref:
            return self._parse_other_attrb_type(dimensions_ref[attribute])
        else:
            self.logger.critical(f"Unsuported GA type: {type}")
            sys.exit(1)

    def _parse_metric_type(self, attribute, metrics_ref):
        # Custom Google Analytics Metrics {ga:goalXXStarts, ga:metricXX, ... }
        # We always treat them as strings as we can not be sure of
        # their data type
        if attribute.startswith("ga:goal") and attribute.endswith(
            (
                "Starts",
                "Completions",
                "Value",
                "ConversionRate",
                "Abandons",
                "AbandonRate",
            )
        ):
            return "string"
        elif attribute.startswith("ga:searchGoal") and attribute.endswith(
            "ConversionRate"
        ):
            # Custom Google Analytics Metrics ga:searchGoalXXConversionRate
            return "string"
        elif attribute.startswith(("ga:metric", "ga:calcMetric")):
            return "string"
        elif attribute in metrics_ref:
            return self._parse_other_attrb_type(metrics_ref[attribute])
        else:
            self.logger.critical(f"Unsuported GA type: {type}")
            sys.exit(1)

    def _parse_other_attrb_type(self, attr_type):
        data_type = "string"

        if attr_type == "INTEGER":
            data_type = "integer"
        elif attr_type == "FLOAT" or attr_type == "PERCENT" or attr_type == "TIME":
            data_type = "number"

        return data_type

    def _lookup_data_type(self, type, attribute, dimensions_ref, metrics_ref):
        """Get the data type of a metric or a dimension."""
        if type == "dimension":
            return self._parse_dimension_type(attribute, dimensions_ref)
        elif type == "metric":
            return self._parse_metric_type(attribute, metrics_ref)
        else:
            self.logger.critical(f"Unsuported GA type: {type}")
            sys.exit(1)

    @staticmethod
    def _generate_report_definition(report_def_raw):
        report_definition = {"metrics": [], "dimensions": []}

        for dimension in report_def_raw["dimensions"]:
            report_definition["dimensions"].append(
                {"name": dimension.replace("ga_", "ga:")}
            )

        for metric in report_def_raw["metrics"]:
            report_definition["metrics"].append(
                {"expression": metric.replace("ga_", "ga:")}
            )

        # Add segmentIds to the request if the stream contains them
        if "segments" in report_def_raw:
            report_definition["segments"] = []
            for segment_id in report_def_raw["segments"]:
                report_definition["segments"].append({"segmentId": segment_id})

        if "samplingLevel" in report_def_raw:
            report_definition["samplingLevel"] = report_def_raw["samplingLevel"]

        for key in ["dimension", "metric"]:
            # https://stackoverflow.com/questions/38727095/core-reporting-api-how-to-use-multiple-dimensionfilterclauses-filters
            if not f"{key}FilterClauses" in report_def_raw:
                continue

            filters = []

            for clause in report_def_raw[f"{key}FilterClauses"].keys():
                filters.append({
                    "filters": {
                        f"{key}Name": clause,
                        **report_def_raw[f"{key}FilterClauses"][clause],
                    }
                })

            report_definition[f"{key}FilterClauses"] = filters
            
        if "orderBys" in report_def_raw:
            report_definition["orderBys"] = []
            for clause, sort_order in report_def_raw["orderBys"].items():
                report_definition["orderBys"].append(
                    {
                        "fieldName": clause.replace("ga_", "ga:"),
                        "sortOrder": sort_order,
                    }
                )

        for key in ["page_size", "max_records"]:
            if key in report_def_raw:
                report_definition[key] = report_def_raw[key]

        return report_definition

    def _request_data(
        self, api_report_def, state_filter: str, next_page_token: Optional[Any]
    ) -> dict:
        try:
            return self._query_api(api_report_def, state_filter, next_page_token)
        except HttpError as e:
            # Process API errors
            # Use list of errors defined in:
            # https://developers.google.com/analytics/devguides/reporting/core/v4/errors

            reason = error_reason(e)
            if reason == "userRateLimitExceeded" or reason == "rateLimitExceeded":
                self.logger.error(
                    f"Skipping stream: '{self.name}' due to Rate Limit Errors."
                )
                raise TapGaRateLimitError(e._get_reason())
            elif reason == "quotaExceeded":
                self.logger.error(
                    f"Skipping stream: '{self.name}' due to Quota Exceeded Errors."
                )
                raise TapGaQuotaExceededError(e._get_reason())
            elif e.resp.status == 400:
                self.logger.error(
                    f"Stream: '{self.name}' failed due to invalid report definition."
                )
                raise TapGaInvalidArgumentError(e._get_reason())
            elif e.resp.status in [401, 402]:
                self.logger.error(
                    f"Stopping execution while processing '{self.name}' due to \
                        Authentication Errors."
                )
                raise TapGaAuthenticationError(e._get_reason())
            elif e.resp.status in [500, 503]:
                raise TapGaBackendServerError(e._get_reason())
            else:
                self.logger.error(
                    f"Stopping execution while processing '{self.name}' due to Unknown \
                        Errors."
                )
                raise TapGaUnknownError(e._get_reason())

    def _get_state_filter(self, context: Optional[dict]) -> str:
        state = self.get_context_state(context)
        state_bookmark = state.get("replication_key_value") or self.config["start_date"]
        try:
            parsed = datetime.strptime(state_bookmark, "%Y%m%d")
        except ValueError:
            parsed = datetime.strptime(state_bookmark, "%Y-%m-%d")
        # state bookmarks need to be reformatted for API requests
        return datetime.strftime(parsed, "%Y-%m-%d")

    def _request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records from REST endpoint(s), returning response records.

        If pagination is detected, pages will be recursed automatically.

        Args:
            context: Stream partition or context dictionary.

        Yields
        ------
            An item for every record in the response.

        Raises
        ------
            RuntimeError: If a loop in pagination is detected. That is, when two
                consecutive pagination tokens are identical.

        """
        next_page_token: Any = None
        finished = False
        total_records = 0

        state_filter = self._get_state_filter(context)
        api_report_def = self._generate_report_definition(self.report)
        
        page_size = self.report.get("page_size", self.page_size)
        max_records = self.report.get("max_records", self.max_records)

        while not finished:
            resp = self._request_data(
                api_report_def,
                state_filter=state_filter,
                next_page_token=next_page_token,
            )
            for row in self._parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self._get_next_page_token(response=resp)
            total_records += page_size
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )

            # Cycle until get_next_page_token() no longer returns a value
            finished = self._is_finished(next_page_token, total_records, max_records)

            self.logger.info(f"Total records: {total_records}")
            self.logger.info(f"Max records: {max_records}")
            self.logger.info(f"Finished: {finished}")


    def _is_finished(self, next_page_token: Any, total_records: int, max_records: int | None) -> bool:
        # if there's not an additional page, no need to check anything else
        if not next_page_token:
            return True

        # If we don't have a setting for max records, it means we need to keep checking
        if max_records is None:
            return False

        # If we have more records than the max, we're done
        return total_records >= max_records

    def _get_next_page_token(self, response: dict) -> Any:  # noqa: D417
        """Return token identifying next page or None if all records have been read.

        Args:
        ----
            response: A dict object.

        Return:
        ------
            Reference value to retrieve next page.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response

        """
        report = response.get("reports", [])
        if report:
            return report[0].get("nextPageToken")

    def _normalize_colname(self, colname: str) -> str:
        colname = colname.replace("ga:", "ga_")

        # https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
        return "".join(
            ["_" + c.lower() if c.isupper() else c for c in colname]
        ).lstrip("_")

    def _parse_response(self, response):
        report = response.get("reports", [])[0]
        if report:
            columnHeader = report.get("columnHeader", {})
            dimensionHeaders = columnHeader.get("dimensions", [])
            metricHeaders = columnHeader.get("metricHeader", {}).get(
                "metricHeaderEntries", []
            )

            for row in report.get("data", {}).get("rows", []):
                record = {}

                record["view_id"] = self.view_id
                record["stream"] = self.name

                dimensions = row.get("dimensions", [])
                dateRangeValues = row.get("metrics", [])

                for header, dimension in zip(dimensionHeaders, dimensions):
                    data_type = self._lookup_data_type(
                        "dimension", header, self.dimensions_ref, self.metrics_ref
                    )

                    if data_type == "integer":
                        value = int(dimension)
                    elif data_type == "number":
                        value = round(float(dimension), 10)
                    else:
                        value = dimension

                    record[self._normalize_colname(header)] = value

                    # appending ga_date with a diff date format
                    if self._normalize_colname(header) == "ga_date":
                        record["ga_date_dt"] = datetime.strptime(
                            dimension, "%Y%m%d"
                        ).strftime("%Y-%m-%d")

                    # appending ga_date if yearMonth present (grouping by month)
                    if self._normalize_colname(header) == "ga_year_month":
                        record["ga_date"] = datetime.strptime(
                            dimension, "%Y%m"
                        ).strftime("%Y%m01")

                        record["ga_date_dt"] = datetime.strptime(
                            record["ga_date"], "%Y%m%d"
                        ).strftime("%Y-%m-%d")

                for i, values in enumerate(dateRangeValues):
                    for metricHeader, value in zip(metricHeaders, values.get("values")):
                        metric_name = metricHeader.get("name")
                        metric_type = self._lookup_data_type(
                            "metric", metric_name, self.dimensions_ref, self.metrics_ref
                        )

                        if metric_type == "integer":
                            value = int(value)
                        elif metric_type == "number":
                            value = round(float(value), 10)

                        record[self._normalize_colname(metric_name)] = value

                # Also add the [start_date,end_date) used for the report
                record["report_start_date"] = self.config.get("start_date")
                record["report_end_date"] = self.end_date

                yield record

    @backoff.on_exception(
        backoff.expo, (HttpError, socket.timeout), max_tries=9, giveup=is_fatal_error
    )
    def _query_api(self, report_definition, state_filter, pageToken=None) -> dict:
        """Query the Analytics Reporting API V4.

        Returns
        -------
            The Analytics Reporting API V4 response.

        """
        body = {
            "reportRequests": [
                {
                    "viewId": self.view_id,
                    "dateRanges": [
                        {"startDate": state_filter, "endDate": self.end_date}
                    ],
                    "pageSize": report_definition.get("page_size", self.page_size),
                    "pageToken": pageToken,
                    "metrics": report_definition["metrics"],
                    "dimensions": report_definition["dimensions"],
                }
            ]
        }

        for key in [
            "segments",
            "dimensionFilterClauses",
            "metricFilterClauses",
            "orderBys",
        ]:
            if key in report_definition:
                body["reportRequests"][0][key] = report_definition[key]
                
        if "samplingLevel" in report_definition:
            body["reportRequests"][0]["samplingLevel"] = report_definition["samplingLevel"]

        return (
            self.analytics.reports()
            .batchGet(body=body, quotaUser=self.quota_user)
            .execute()
        )

    @staticmethod
    def _get_datatype(string_type):
        mapping = {
            "string": th.StringType(),
            "integer": th.IntegerType(),
            "number": th.NumberType(),
        }
        return mapping.get(string_type, th.StringType())

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Each row emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields
        ------
            One item per (possibly processed) record in the API.

        """
        for record in self._request_records(context):
            yield record

    @property
    def schema(self) -> dict:
        """Return dictionary of record schema.

        Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """
        properties: List[th.Property] = []
        primary_keys = []
        primary_keys.append("view_id")
        # : List[th.StringType] = []

        # Track if there is a date set as one of the Dimensions
        date_dimension_included = False

        # Append {view_id} params for the report query
        properties.append(th.Property("view_id", th.StringType(), required=True))
        properties.append(th.Property("stream", th.StringType(), required=True))

        # Add the dimensions to the schema and as key_properties
        for dimension in self.report["dimensions"]:
            if dimension == "ga:date":
                date_dimension_included = True
                self.replication_key = "ga_date"

                # add ga_date as date, cause the PR is added as string
                # don't want to modify that
                properties.append(
                    th.Property("ga_date_dt", th.DateType(), required=True)
                )

            # for the sake of grouping by month (lots of data)
            # hardcoding this solution
            if dimension == "ga:yearMonth":
                date_dimension_included = True
                self.replication_key = "ga_date"

                # add ga_date as date, cause the PR is added as string
                primary_keys.append("ga_date")
                
                properties.append(
                    th.Property("ga_date", th.StringType(), required=True)
                )
                
                properties.append(
                    th.Property("ga_date_dt", th.DateType(), required=True)
                )

            data_type = self._lookup_data_type(
                "dimension", dimension, self.dimensions_ref, self.metrics_ref
            )
            dimension = self._normalize_colname(dimension)
            properties.append(
                th.Property(dimension, self._get_datatype(data_type), required=True)
            )
            primary_keys.append(dimension)

        # Add the metrics to the schema
        for metric in self.report["metrics"]:
            data_type = self._lookup_data_type(
                "metric", metric, self.dimensions_ref, self.metrics_ref
            )
            metric = self._normalize_colname(metric)
            properties.append(th.Property(metric, self._get_datatype(data_type)))

        properties.append(
            th.Property("report_start_date", th.DateType(), required=True)
        )
        properties.append(
            th.Property("report_end_date", th.DateType(), required=True)
        )

        # If 'ga:date' has not been added as a Dimension, add the
        #  {start_date, end_date} params as keys
        if not date_dimension_included:
            self.logger.warn(
                f"Incremental sync not supported for stream {self.tap_stream_id}, \
                    'ga.date' is the only supported replication key at this time."
            )
            primary_keys.append("report_start_date")
            primary_keys.append("report_end_date")

        self.primary_keys = primary_keys

        return th.PropertiesList(*properties).to_dict()
