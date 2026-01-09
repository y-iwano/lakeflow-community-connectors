"""Integration tests for AWS CloudWatch Metrics connector."""
from pathlib import Path
import pytest

from sources.aws_cloudwatch_metrics.aws_cloudwatch_metrics import LakeflowConnect
from tests import test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config


def test_aws_cloudwatch_metrics_connector():
    """Test the AWS CloudWatch Metrics connector using the test suite."""
    # Inject the LakeflowConnect class into test_suite module's namespace
    # This is required because test_suite.py expects LakeflowConnect to be available
    test_suite.LakeflowConnect = LakeflowConnect

    # Load configuration
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"
    table_config_path = parent_dir / "configs" / "dev_table_config.json"

    # Skip if dev_config.json doesn't exist
    if not config_path.exists():
        pytest.skip(
            "Skipping integration test: dev_config.json not found. "
            "Create configs/dev_config.json to run integration tests."
        )

    config = load_config(config_path)

    # Load table config
    if not table_config_path.exists():
        pytest.skip(
            "Skipping integration test: dev_table_config.json not found. "
            "Create configs/dev_table_config.json to run integration tests."
        )

    table_config = load_config(table_config_path)

    # Create tester with the config
    tester = LakeflowConnectTester(config, table_config)

    # Run all tests
    report = tester.run_all_tests()

    # Print the report
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, "
        f"{report.error_tests} errors"
    )
