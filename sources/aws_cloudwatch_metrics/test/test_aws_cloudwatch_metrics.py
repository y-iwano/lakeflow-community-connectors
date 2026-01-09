"""Unit tests for AWS CloudWatch Metrics connector."""
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch
import json
import pytest
from pyspark.sql.types import StructType

from sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics import LakeflowConnect


def get_test_options():
    """Get test options from configs/dev_config.json or use defaults for mocking."""
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"

    if config_path.exists():
        with open(config_path, "r") as f:
            config = json.load(f)
        return {
            "aws_access_key_id": config.get("aws_access_key_id", "test_key"),
            "aws_secret_access_key": config.get("aws_secret_access_key", "test_secret"),
            "region": config.get("region", "us-east-1"),
            "cloudwatch_metrics_namespace": config.get("cloudwatch_metrics_namespace", "AWS/EC2"),
        }
    else:
        # Fallback to defaults if config file doesn't exist
        return {
            "aws_access_key_id": "test_key",
            "aws_secret_access_key": "test_secret",
            "region": "us-east-1",
            "cloudwatch_metrics_namespace": "AWS/EC2",
        }


class TestLakeflowConnectInit:
    """Test initialization of LakeflowConnect."""

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_init_success(self, mock_boto3):
        """Test successful initialization with all required options."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        options = get_test_options()

        connector = LakeflowConnect(options)

        expected_options = get_test_options()

        assert connector.namespace == expected_options["cloudwatch_metrics_namespace"]
        assert connector.region == expected_options["region"]
        assert connector.client == mock_client
        mock_boto3.client.assert_called_once_with(
            "cloudwatch",
            aws_access_key_id=expected_options["aws_access_key_id"],
            aws_secret_access_key=expected_options["aws_secret_access_key"],
            region_name=expected_options["region"],
        )

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_init_missing_access_key(self, mock_boto3):
        """Test initialization fails when aws_access_key_id is missing."""
        test_opts = get_test_options()
        options = {
            "aws_secret_access_key": test_opts["aws_secret_access_key"],
            "region": test_opts["region"],
            "cloudwatch_metrics_namespace": test_opts["cloudwatch_metrics_namespace"],
        }

        with pytest.raises(ValueError, match="aws_access_key_id"):
            LakeflowConnect(options)

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_init_missing_secret_key(self, mock_boto3):
        """Test initialization fails when aws_secret_access_key is missing."""
        test_opts = get_test_options()
        options = {
            "aws_access_key_id": test_opts["aws_access_key_id"],
            "region": test_opts["region"],
            "cloudwatch_metrics_namespace": test_opts["cloudwatch_metrics_namespace"],
        }

        with pytest.raises(ValueError, match="aws_secret_access_key"):
            LakeflowConnect(options)

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_init_missing_region(self, mock_boto3):
        """Test initialization fails when region is missing."""
        test_opts = get_test_options()
        options = {
            "aws_access_key_id": test_opts["aws_access_key_id"],
            "aws_secret_access_key": test_opts["aws_secret_access_key"],
            "cloudwatch_metrics_namespace": test_opts["cloudwatch_metrics_namespace"],
        }

        with pytest.raises(ValueError, match="region"):
            LakeflowConnect(options)

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_init_missing_namespace(self, mock_boto3):
        """Test initialization fails when cloudwatch_metrics_namespace is missing."""
        test_opts = get_test_options()
        options = {
            "aws_access_key_id": test_opts["aws_access_key_id"],
            "aws_secret_access_key": test_opts["aws_secret_access_key"],
            "region": test_opts["region"],
        }

        with pytest.raises(ValueError, match="cloudwatch_metrics_namespace"):
            LakeflowConnect(options)


class TestListTables:  # pylint: disable=too-few-public-methods
    """Test list_tables method."""

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_list_tables(self, mock_boto3):
        """Test list_tables returns correct table name."""
        mock_boto3.client.return_value = Mock()
        options = get_test_options()

        connector = LakeflowConnect(options)
        tables = connector.list_tables()

        assert tables == ["metrics"]


class TestDiscoverMetrics:
    """Test _discover_metrics method."""

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_discover_metrics_success(self, mock_boto3):
        """Test successful metric discovery."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        mock_client.list_metrics.return_value = {
            "Metrics": [
                {
                    "MetricName": "CPUUtilization",
                    "Dimensions": [
                        {"Name": "InstanceId", "Value": "i-1234567890abcdef0"}
                    ],
                }
            ]
        }

        options = get_test_options()

        connector = LakeflowConnect(options)
        metrics = connector._discover_metrics({})

        assert len(metrics) == 1
        assert metrics[0]["MetricName"] == "CPUUtilization"
        mock_client.list_metrics.assert_called_once_with(Namespace="AWS/EC2")

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_discover_metrics_with_dimension_filter(self, mock_boto3):
        """Test metric discovery with dimension filter."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        mock_client.list_metrics.return_value = {"Metrics": []}

        options = get_test_options()

        connector = LakeflowConnect(options)
        table_options = {"dimensions": {"InstanceId": "i-1234567890abcdef0"}}
        connector._discover_metrics(table_options)

        mock_client.list_metrics.assert_called_once_with(
            Namespace="AWS/EC2",
            Dimensions=[{"Name": "InstanceId", "Value": "i-1234567890abcdef0"}],
        )

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_discover_metrics_with_pagination(self, mock_boto3):
        """Test metric discovery with pagination."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        mock_client.list_metrics.side_effect = [
            {
                "Metrics": [{"MetricName": "CPUUtilization", "Dimensions": []}],
                "NextToken": "token123",
            },
            {"Metrics": [{"MetricName": "NetworkIn", "Dimensions": []}]},
        ]

        options = get_test_options()

        connector = LakeflowConnect(options)
        metrics = connector._discover_metrics({})

        assert len(metrics) == 2
        assert mock_client.list_metrics.call_count == 2

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_discover_metrics_caching(self, mock_boto3):
        """Test that discovered metrics are cached."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        mock_client.list_metrics.return_value = {
            "Metrics": [{"MetricName": "CPUUtilization", "Dimensions": []}]
        }

        options = get_test_options()

        connector = LakeflowConnect(options)
        connector._discover_metrics({})
        connector._discover_metrics({})  # Second call should use cache

        # Should only be called once due to caching
        assert mock_client.list_metrics.call_count == 1


class TestParseTableOptions:
    """Test _parse_table_options method."""

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_parse_table_options_defaults(self, mock_boto3):
        """Test parsing with default values."""
        mock_boto3.client.return_value = Mock()
        options = get_test_options()

        connector = LakeflowConnect(options)
        parsed = connector._parse_table_options({})

        assert parsed["period"] == 300
        assert parsed["statistics"] == ["Average"]

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_parse_table_options_custom(self, mock_boto3):
        """Test parsing with custom values."""
        mock_boto3.client.return_value = Mock()
        options = get_test_options()

        connector = LakeflowConnect(options)
        table_options = {
            "period": "600",
            "statistics": '["Average", "Sum"]',
        }
        parsed = connector._parse_table_options(table_options)

        assert parsed["period"] == 600
        assert parsed["statistics"] == ["Average", "Sum"]

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_parse_table_options_period_rounding(self, mock_boto3):
        """Test that period is rounded to nearest multiple of 60."""
        mock_boto3.client.return_value = Mock()
        options = get_test_options()

        connector = LakeflowConnect(options)
        parsed = connector._parse_table_options({"period": "125"})

        assert parsed["period"] == 120  # Rounded down to 120


class TestDetermineTimeRange:
    """Test _determine_time_range method."""

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_determine_time_range_first_run(self, mock_boto3):
        """Test time range determination for first run (no start_offset)."""
        mock_boto3.client.return_value = Mock()
        options = get_test_options()

        connector = LakeflowConnect(options)
        start_time, end_time = connector._determine_time_range({})

        # Should default to 60 minutes ago
        assert (end_time - start_time).total_seconds() == pytest.approx(3600, abs=1)

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_determine_time_range_with_cursor(self, mock_boto3):
        """Test time range determination with cursor."""
        mock_boto3.client.return_value = Mock()
        options = get_test_options()

        connector = LakeflowConnect(options)
        base_time = datetime.utcnow() - timedelta(hours=2)
        start_offset = {"cursor": base_time.isoformat()}
        start_time, end_time = connector._determine_time_range(start_offset)

        # Should start from cursor minus 5 minutes lookback
        expected_start = base_time - timedelta(minutes=5)
        assert abs((start_time - expected_start).total_seconds()) < 1


class TestBuildMetricQueries:  # pylint: disable=too-few-public-methods
    """Test _build_metric_queries method."""

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_build_metric_queries(self, mock_boto3):
        """Test building metric queries."""
        mock_boto3.client.return_value = Mock()
        options = get_test_options()

        connector = LakeflowConnect(options)
        discovered_metrics = [
            {
                "MetricName": "CPUUtilization",
                "Dimensions": [{"Name": "InstanceId", "Value": "i-123"}],
            }
        ]

        queries, metadata = connector._build_metric_queries(
            discovered_metrics, period=300, statistics=["Average"]
        )

        assert len(queries) == 1
        assert queries[0]["Id"] == "m1"
        assert queries[0]["MetricStat"]["Period"] == 300
        assert queries[0]["MetricStat"]["Stat"] == "Average"
        assert "m1" in metadata
        assert metadata["m1"]["metric_name"] == "CPUUtilization"


class TestProcessMetricDataResults:  # pylint: disable=too-few-public-methods
    """Test _process_metric_data_results method."""

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_process_metric_data_results(self, mock_boto3):
        """Test processing metric data results."""
        mock_boto3.client.return_value = Mock()
        options = get_test_options()

        connector = LakeflowConnect(options)
        metric_data_results = [
            {
                "Id": "m1",
                "Timestamps": [
                    datetime(2024, 1, 1, 0, 0, 0),
                    datetime(2024, 1, 1, 0, 5, 0),
                ],
                "Values": [45.5, 52.3],
                "Unit": "Percent",
            }
        ]
        batch_metadata = {
            "m1": {
                "namespace": "AWS/EC2",
                "metric_name": "CPUUtilization",
                "dimensions": {"InstanceId": "i-123"},
                "statistic": "Average",
            }
        }

        records, max_timestamp = connector._process_metric_data_results(
            metric_data_results, batch_metadata
        )

        assert len(records) == 2
        assert records[0]["value"] == 45.5
        assert records[0]["metric_name"] == "CPUUtilization"
        assert records[0]["statistic"] == "Average"
        assert records[0]["unit"] == "Percent"
        assert max_timestamp is not None


class TestComputeNextCursor:
    """Test _compute_next_cursor method."""

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_compute_next_cursor_with_timestamp(self, mock_boto3):
        """Test computing next cursor from timestamp."""
        mock_boto3.client.return_value = Mock()
        options = get_test_options()

        connector = LakeflowConnect(options)
        max_timestamp = "2024-01-01T12:00:00"
        start_time = datetime(2024, 1, 1, 11, 0, 0)

        cursor = connector._compute_next_cursor(max_timestamp, start_time)

        # Should subtract 5 minutes from max_timestamp
        assert cursor is not None
        assert isinstance(cursor, str)

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_compute_next_cursor_no_timestamp(self, mock_boto3):
        """Test computing next cursor when no timestamp."""
        mock_boto3.client.return_value = Mock()
        options = get_test_options()

        connector = LakeflowConnect(options)
        start_time = datetime(2024, 1, 1, 11, 0, 0)

        cursor = connector._compute_next_cursor(None, start_time)

        assert cursor == start_time.isoformat()


class TestGetTableSchema:
    """Test get_table_schema method."""

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_get_table_schema(self, mock_boto3):
        """Test getting table schema."""
        mock_boto3.client.return_value = Mock()
        options = get_test_options()

        connector = LakeflowConnect(options)
        schema = connector.get_table_schema("metrics", {})

        assert isinstance(schema, StructType)
        field_names = [field.name for field in schema.fields]
        assert "namespace" in field_names
        assert "metric_name" in field_names
        assert "timestamp" in field_names
        assert "value" in field_names
        assert "statistic" in field_names
        assert "unit" in field_names
        assert "dimensions" in field_names
        assert "region" in field_names

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_get_table_schema_invalid_table(self, mock_boto3):
        """Test getting schema for invalid table raises error."""
        mock_boto3.client.return_value = Mock()
        options = get_test_options()

        connector = LakeflowConnect(options)
        with pytest.raises(ValueError, match="Unsupported table"):
            connector.get_table_schema("invalid_table", {})


class TestReadTableMetadata:  # pylint: disable=too-few-public-methods
    """Test read_table_metadata method."""

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_read_table_metadata(self, mock_boto3):
        """Test reading table metadata."""
        mock_boto3.client.return_value = Mock()
        options = get_test_options()

        connector = LakeflowConnect(options)
        metadata = connector.read_table_metadata("metrics", {})

        assert metadata["primary_keys"] == [
            "namespace",
            "metric_name",
            "timestamp",
            "dimensions",
        ]
        assert metadata["cursor_field"] == "timestamp"
        assert metadata["ingestion_type"] == "append"


class TestReadTable:
    """Test read_table method."""

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_read_table_no_metrics(self, mock_boto3):
        """Test read_table when no metrics are discovered."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client
        mock_client.list_metrics.return_value = {"Metrics": []}

        options = get_test_options()

        connector = LakeflowConnect(options)
        records, offset = connector.read_table("metrics", {}, {})

        assert not list(records)
        assert offset == {}

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_read_table_success(self, mock_boto3):
        """Test successful read_table execution."""
        mock_client = Mock()
        mock_boto3.client.return_value = mock_client

        # Mock ListMetrics response
        mock_client.list_metrics.return_value = {
            "Metrics": [
                {
                    "MetricName": "CPUUtilization",
                    "Dimensions": [{"Name": "InstanceId", "Value": "i-123"}],
                }
            ]
        }

        # Mock GetMetricData response
        mock_client.get_metric_data.return_value = {
            "MetricDataResults": [
                {
                    "Id": "m1",
                    "Timestamps": [datetime(2024, 1, 1, 0, 0, 0)],
                    "Values": [45.5],
                    "Unit": "Percent",
                }
            ]
        }

        options = get_test_options()

        connector = LakeflowConnect(options)
        records, offset = connector.read_table("metrics", {}, {})

        records_list = list(records)
        assert len(records_list) > 0
        assert "cursor" in offset

    @patch("sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics.boto3")
    def test_read_table_invalid_table(self, mock_boto3):
        """Test read_table with invalid table name."""
        mock_boto3.client.return_value = Mock()
        options = get_test_options()

        connector = LakeflowConnect(options)
        with pytest.raises(ValueError, match="Unsupported table"):
            connector.read_table("invalid_table", {}, {})
