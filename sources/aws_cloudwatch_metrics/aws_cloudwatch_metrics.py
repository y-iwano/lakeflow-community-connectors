# pylint: disable=too-many-lines
from datetime import datetime, timedelta
from typing import Iterator, Any, Dict, List
import json
import time

import boto3
from botocore.exceptions import ClientError
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    MapType,
)


class LakeflowConnect:
    def __init__(self, options: dict[str, str]) -> None:
        """
        Initialize the AWS CloudWatch Metrics connector with connection-level options.

        Expected options:
            - aws_access_key_id: AWS Access Key ID for authentication
            - aws_secret_access_key: AWS Secret Access Key for authentication
            - region: AWS region where CloudWatch metrics are stored
            - cloudwatch_metrics_namespace: CloudWatch namespace for metrics
        """
        aws_access_key_id = options.get("aws_access_key_id")
        aws_secret_access_key = options.get("aws_secret_access_key")
        region = options.get("region")
        self.namespace = options.get("cloudwatch_metrics_namespace")

        if not aws_access_key_id:
            raise ValueError("AWS CloudWatch connector requires 'aws_access_key_id' in options")
        if not aws_secret_access_key:
            raise ValueError("AWS CloudWatch connector requires 'aws_secret_access_key' in options")
        if not region:
            raise ValueError("AWS CloudWatch connector requires 'region' in options")
        if not self.namespace:
            raise ValueError(
                "AWS CloudWatch connector requires 'cloudwatch_metrics_namespace' "
                "in options"
            )

        # Initialize boto3 CloudWatch client
        self.client = boto3.client(
            'cloudwatch',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region
        )
        self.region = region

        # Cache for discovered metrics (namespace + metric_name + dimensions combinations)
        self._discovered_metrics: List[Dict[str, Any]] | None = None

    def list_tables(self) -> list[str]:
        """
        List names of all tables supported by this connector.

        CloudWatch connector supports a single table: 'metrics'
        """
        return ["metrics"]

    def _discover_metrics(self, table_options: dict[str, str]) -> List[Dict[str, Any]]:
        """
        Discover all metrics and dimension combinations using ListMetrics API.

        Args:
            table_options: Table options that may include dimension filters

        Returns:
            List of metric dictionaries, each containing:
            - Namespace: CloudWatch namespace
            - MetricName: Name of the metric
            - Dimensions: List of dimension dictionaries with Name and Value keys
        """
        # If already discovered and no dimension filter, return cached
        if self._discovered_metrics is not None and "dimensions" not in table_options:
            return self._discovered_metrics

        # Check if dimensions filter is specified
        filter_dimensions = None
        if "dimensions" in table_options:
            filter_dimensions = table_options["dimensions"]
            if isinstance(filter_dimensions, str):
                # Parse JSON string if provided as string
                try:
                    filter_dimensions = json.loads(filter_dimensions)
                except json.JSONDecodeError:
                    raise ValueError(f"Invalid dimensions format: {filter_dimensions}")

        # Build ListMetrics request
        list_params = {"Namespace": self.namespace}

        # If dimensions filter is specified, convert to DimensionFilters format
        if filter_dimensions:
            dimension_filters = []
            for key, value in filter_dimensions.items():
                dimension_filters.append({
                    "Name": key,
                    "Value": value
                })
            list_params["Dimensions"] = dimension_filters

        all_metrics = []
        next_token = None

        # Paginate through ListMetrics results
        while True:
            if next_token:
                list_params["NextToken"] = next_token

            try:
                response = self.client.list_metrics(**list_params)
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code == "Throttling":
                    # Retry with exponential backoff
                    # (simplified - in production use retry decorator)
                    time.sleep(1)
                    continue
                raise RuntimeError(f"Failed to list metrics: {e}")

            metrics = response.get("Metrics", [])
            all_metrics.extend(metrics)

            next_token = response.get("NextToken")
            if not next_token:
                break

        # Cache if no dimension filter
        if "dimensions" not in table_options:
            self._discovered_metrics = all_metrics

        return all_metrics

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        """
        Fetch the schema of the metrics table.

        Schema includes:
            - namespace: CloudWatch namespace
            - metric_name: Name of the metric
            - timestamp: Timestamp of the metric data point (UTC)
            - value: Metric value (double)
            - statistic: Statistic type (Average, Sum, Maximum, Minimum, SampleCount)
            - unit: Unit of the metric
            - dimensions: Dimensions as a map
            - region: AWS region (connector-derived)
        """
        if table_name != "metrics":
            raise ValueError(f"Unsupported table: {table_name!r}")

        return StructType(
            [
                StructField("namespace", StringType(), False),
                StructField("metric_name", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("value", DoubleType(), True),
                StructField("statistic", StringType(), False),
                StructField("unit", StringType(), True),
                StructField("dimensions", MapType(StringType(), StringType(), True), True),
                StructField("region", StringType(), False),
            ]
        )

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        """
        Fetch metadata for the metrics table.

        Returns:
            - primary_keys: ["namespace", "metric_name", "timestamp", "dimensions"]
            - cursor_field: "timestamp"
            - ingestion_type: "append"
        """
        if table_name != "metrics":
            raise ValueError(f"Unsupported table: {table_name!r}")

        return {
            "primary_keys": ["namespace", "metric_name", "timestamp", "dimensions"],
            "cursor_field": "timestamp",
            "ingestion_type": "append",
        }

    def _parse_table_options(self, table_options: dict[str, str]) -> Dict[str, Any]:
        """
        Parse and validate table options.

        Args:
            table_options: Table options dictionary

        Returns:
            Dictionary with parsed options:
            - period: Period in seconds (multiple of 60)
            - statistics: List of statistics
        """
        # Parse period
        try:
            period = int(table_options.get("period", 300))
        except (TypeError, ValueError):
            period = 300
        # Ensure period is a multiple of 60
        if period % 60 != 0:
            period = (period // 60) * 60
            if period == 0:
                period = 60

        # Parse statistics
        statistics = table_options.get("statistics", ["Average"])
        if isinstance(statistics, str):
            # Parse JSON string if provided as string
            try:
                statistics = json.loads(statistics)
            except json.JSONDecodeError:
                statistics = [statistics]

        if not isinstance(statistics, list):
            statistics = ["Average"]

        return {
            "period": period,
            "statistics": statistics,
        }

    def _determine_time_range(self, start_offset: dict) -> tuple[datetime, datetime]:
        """
        Determine the time range for metric data retrieval.

        Args:
            start_offset: Start offset dictionary that may contain a cursor

        Returns:
            Tuple of (start_time, end_time) as UTC naive datetime objects
        """
        end_time = datetime.utcnow()

        if start_offset and isinstance(start_offset, dict):
            cursor = start_offset.get("cursor")
            if cursor:
                try:
                    # Parse cursor as ISO 8601 timestamp
                    # Handle both 'Z' suffix and timezone-aware formats
                    cursor_clean = cursor.replace('Z', '+00:00')
                    if '+' not in cursor_clean and cursor_clean.count('-') > 2:
                        # Assume UTC if no timezone info
                        cursor_clean = cursor_clean + '+00:00'
                    start_time = datetime.fromisoformat(cursor_clean)
                    # Convert to UTC naive datetime if timezone-aware
                    if start_time.tzinfo is not None:
                        start_time = start_time.replace(tzinfo=None)
                    # Apply 5-minute lookback window
                    start_time = start_time - timedelta(minutes=5)
                except (ValueError, AttributeError):
                    # If parsing fails, default to 60 minutes ago
                    start_time = end_time - timedelta(minutes=60)
            else:
                # No cursor, first run - default to 60 minutes ago
                start_time = end_time - timedelta(minutes=60)
        else:
            # No start_offset, first run - default to 60 minutes ago
            start_time = end_time - timedelta(minutes=60)

        return start_time, end_time

    def _convert_dimensions(
        self, dimensions: List[Dict[str, str]]
    ) -> tuple[List[Dict[str, str]], Dict[str, str]]:
        """
        Convert dimensions from ListMetrics format to GetMetricData format.

        Args:
            dimensions: List of dimension dicts with Name/Value keys

        Returns:
            Tuple of (dims_for_query, dims_dict):
            - dims_for_query: List of dicts for GetMetricData API
            - dims_dict: Dict mapping dimension names to values
        """
        dims_for_query = []
        dims_dict = {}
        for dim in dimensions:
            dim_name = dim.get("Name")
            dim_value = dim.get("Value")
            if dim_name and dim_value:
                dims_for_query.append({"Name": dim_name, "Value": dim_value})
                dims_dict[dim_name] = dim_value
        return dims_for_query, dims_dict

    def _build_metric_queries(
        self,
        discovered_metrics: List[Dict[str, Any]],
        period: int,
        statistics: List[str],
    ) -> tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """
        Build MetricDataQueries and query metadata from discovered metrics.

        Args:
            discovered_metrics: List of metrics from ListMetrics API
            period: Period in seconds for metric aggregation
            statistics: List of statistics to retrieve

        Returns:
            Tuple of (metric_queries, query_metadata):
            - metric_queries: List of MetricDataQuery dictionaries
            - query_metadata: Dictionary mapping query ID to metric metadata
        """
        metric_queries = []
        query_metadata = {}
        query_id_counter = 1

        for metric in discovered_metrics:
            metric_name = metric.get("MetricName")
            dimensions = metric.get("Dimensions", [])
            dims_for_query, dims_dict = self._convert_dimensions(dimensions)

            # Create a query for each statistic
            for stat in statistics:
                query_id = f"m{query_id_counter}"
                query_id_counter += 1

                metric_queries.append({
                    "Id": query_id,
                    "MetricStat": {
                        "Metric": {
                            "Namespace": self.namespace,
                            "MetricName": metric_name,
                            "Dimensions": dims_for_query
                        },
                        "Period": period,
                        "Stat": stat
                    },
                    "ReturnData": True
                })

                query_metadata[query_id] = {
                    "namespace": self.namespace,
                    "metric_name": metric_name,
                    "dimensions": dims_dict,
                    "statistic": stat,
                }

        return metric_queries, query_metadata

    def _process_metric_data_results(
        self,
        metric_data_results: List[Dict[str, Any]],
        batch_metadata: Dict[str, Dict[str, Any]],
    ) -> tuple[List[dict], str | None]:
        """
        Process GetMetricData response results into records.

        Args:
            metric_data_results: List of MetricDataResult dictionaries from GetMetricData API
            batch_metadata: Dictionary mapping query ID to metric metadata

        Returns:
            Tuple of (records, max_timestamp):
            - records: List of record dictionaries
            - max_timestamp: Maximum timestamp string (ISO 8601) or None
        """
        records = []
        max_timestamp = None

        for result in metric_data_results:
            query_id = result.get("Id")
            metadata = batch_metadata.get(query_id)
            if not metadata:
                continue

            timestamps = result.get("Timestamps", [])
            values = result.get("Values", [])

            # Skip if no data points
            if not timestamps or not values:
                continue

            # Get unit from result (API returns unit in response)
            result_unit = result.get("Unit")

            # Create a record for each timestamp-value pair
            for ts, val in zip(timestamps, values):
                # Convert timestamp to ISO 8601 string if it's a datetime object
                if isinstance(ts, datetime):
                    ts_iso = ts.isoformat()
                else:
                    ts_iso = str(ts)

                record = {
                    "namespace": metadata["namespace"],
                    "metric_name": metadata["metric_name"],
                    "timestamp": ts_iso,
                    "value": float(val) if val is not None else None,
                    "statistic": metadata["statistic"],
                    "unit": result_unit,
                    "dimensions": metadata["dimensions"],
                    "region": self.region,
                }
                records.append(record)

                # Track max timestamp for cursor (compare as strings in ISO format)
                if max_timestamp is None or ts_iso > max_timestamp:
                    max_timestamp = ts_iso

        return records, max_timestamp

    def _fetch_metric_data_batch(
        self,
        batch: List[Dict[str, Any]],
        batch_metadata: Dict[str, Dict[str, Any]],
        start_time: datetime,
        end_time: datetime,
    ) -> tuple[List[dict], str | None]:
        """
        Fetch metric data for a single batch, splitting time range into 1-hour chunks.

        Args:
            batch: List of MetricDataQuery dictionaries (up to 500)
            batch_metadata: Dictionary mapping query ID to metric metadata
            start_time: Start time for data retrieval
            end_time: End time for data retrieval

        Returns:
            Tuple of (all_records, max_timestamp):
            - all_records: List of all records from this batch
            - max_timestamp: Maximum timestamp string (ISO 8601) or None
        """
        all_records = []
        max_timestamp = None

        # Split time range into 1-hour chunks to avoid timeouts
        current_start = start_time
        while current_start < end_time:
            current_end = min(current_start + timedelta(hours=1), end_time)

            try:
                response = self.client.get_metric_data(
                    MetricDataQueries=batch,
                    StartTime=current_start,
                    EndTime=current_end
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code == "Throttling":
                    # Retry with exponential backoff
                    # (simplified - in production use retry decorator)
                    time.sleep(1)
                    continue
                raise RuntimeError(f"Failed to get metric data: {e}")

            # Process response
            metric_data_results = response.get("MetricDataResults", [])
            batch_records, batch_max_ts = self._process_metric_data_results(
                metric_data_results, batch_metadata
            )
            all_records.extend(batch_records)

            # Update max timestamp
            if batch_max_ts:
                if max_timestamp is None or batch_max_ts > max_timestamp:
                    max_timestamp = batch_max_ts

            # Handle pagination if NextToken is present
            # Note: GetMetricData pagination via NextToken requires making additional requests
            # For simplicity, we process all results in the current response
            # If NextToken is present, it means there are more results but we've hit the limit
            # In production, you might want to handle this by making additional requests

            current_start = current_end

        return all_records, max_timestamp

    def _compute_next_cursor(
        self, max_timestamp: str | None, start_time: datetime
    ) -> str:
        """
        Compute the next cursor from the maximum timestamp.

        Args:
            max_timestamp: Maximum timestamp string (ISO 8601) or None
            start_time: Start time used for this sync (fallback if no max_timestamp)

        Returns:
            Next cursor as ISO 8601 string
        """
        if max_timestamp:
            try:
                # Parse max timestamp and subtract 5 minutes for lookback
                if isinstance(max_timestamp, str):
                    # Handle both 'Z' suffix and timezone-aware formats
                    ts_clean = max_timestamp.replace('Z', '+00:00')
                    if '+' not in ts_clean and ts_clean.count('-') > 2:
                        # Assume UTC if no timezone info
                        ts_clean = ts_clean + '+00:00'
                    dt = datetime.fromisoformat(ts_clean)
                    # Convert to UTC naive datetime if timezone-aware
                    if dt.tzinfo is not None:
                        dt = dt.replace(tzinfo=None)
                else:
                    dt = max_timestamp
                dt_with_lookback = dt - timedelta(minutes=5)
                return dt_with_lookback.isoformat()
            except Exception:
                # Fallback: use max_timestamp as-is
                return max_timestamp
        else:
            # No records, reuse start_time as cursor
            return start_time.isoformat()

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> (Iterator[dict], dict):
        """
        Read metric data points from CloudWatch.

        This method:
            - Discovers all metrics using ListMetrics API
            - Batches metrics into GetMetricData requests (up to 500 per request)
            - Automatically manages time ranges:
              - First run: 60 minutes ago to current time
              - Subsequent runs: from stored cursor (with 5-minute lookback) to current time
            - Returns records with namespace, metric_name, timestamp, value,
              statistic, unit, dimensions, region

        Optional table_options:
            - period: Period in seconds for metric aggregation
              (default: 300, must be multiple of 60)
            - statistics: List of statistics to retrieve (default: ["Average"])
            - dimensions: Dimensions filter as dict (optional)
        """
        if table_name != "metrics":
            raise ValueError(f"Unsupported table: {table_name!r}")

        # Parse table options and discover metrics
        parsed_options = self._parse_table_options(table_options)
        discovered_metrics = self._discover_metrics(table_options)

        if not discovered_metrics:
            return iter([]), start_offset if start_offset else {}

        # Determine time range and build queries
        start_time, end_time = self._determine_time_range(start_offset)
        metric_queries, query_metadata = self._build_metric_queries(
            discovered_metrics,
            parsed_options["period"],
            parsed_options["statistics"]
        )

        # Fetch data in batches
        all_records, max_timestamp = self._fetch_all_batches(
            metric_queries, query_metadata, start_time, end_time
        )

        # Compute next cursor
        next_cursor = self._compute_next_cursor(max_timestamp, start_time)
        if not all_records and start_offset:
            next_offset = start_offset
        else:
            next_offset = {"cursor": next_cursor}

        return iter(all_records), next_offset

    def _fetch_all_batches(
        self,
        metric_queries: List[Dict[str, Any]],
        query_metadata: Dict[str, Dict[str, Any]],
        start_time: datetime,
        end_time: datetime,
    ) -> tuple[List[dict], str | None]:
        """
        Fetch metric data for all queries in batches.

        Args:
            metric_queries: List of MetricDataQuery dictionaries
            query_metadata: Dictionary mapping query ID to metadata
            start_time: Start time for data retrieval
            end_time: End time for data retrieval

        Returns:
            Tuple of (all_records, max_timestamp):
            - all_records: List of all metric records
            - max_timestamp: Maximum timestamp across all batches
        """
        batch_size = 500
        all_records = []
        max_timestamp = None

        for i in range(0, len(metric_queries), batch_size):
            batch = metric_queries[i:i + batch_size]
            batch_ids = [q["Id"] for q in batch]
            batch_metadata = {qid: query_metadata[qid] for qid in batch_ids}

            batch_records, batch_max_ts = self._fetch_metric_data_batch(
                batch, batch_metadata, start_time, end_time
            )
            all_records.extend(batch_records)

            if batch_max_ts and (max_timestamp is None or batch_max_ts > max_timestamp):
                max_timestamp = batch_max_ts

        return all_records, max_timestamp
