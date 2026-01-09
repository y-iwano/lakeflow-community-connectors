# Lakeflow CloudWatch Community Connector

This documentation provides setup instructions and reference information for the AWS CloudWatch Metrics source connector.

## Prerequisites

- **AWS Account**: You need an AWS account with access to CloudWatch Metrics.
- **AWS IAM Credentials**: 
  - AWS Access Key ID and Secret Access Key with appropriate permissions
  - The IAM user or role must have `cloudwatch:GetMetricData` and `cloudwatch:ListMetrics` permissions
- **Network access**: The environment running the connector must be able to reach AWS CloudWatch API endpoints (typically `monitoring.<region>.amazonaws.com`).
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `aws_access_key_id` | string | yes | AWS Access Key ID for authentication. | `-` |
| `aws_secret_access_key` | string | yes | AWS Secret Access Key for authentication. | `-` |
| `region` | string | yes | AWS region where CloudWatch metrics are stored. | `us-east-1` |
| `cloudwatch_metrics_namespace` | string | yes | CloudWatch namespace for metrics. The connector will automatically discover all metrics and dimension combinations within this namespace. | `AWS/EC2` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names that are allowed to be passed through to the connector. This connector requires table-specific options, so this parameter must be set. | `period,statistics,dimensions` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`period,statistics,dimensions`

> **Note**: `start_time` and `end_time` are automatically managed by the connector based on incremental cursors and are not configurable as table options.

> **Note**: Table-specific options such as `namespace`, `metric_name`, `dimensions`, etc. are **not** connection parameters. They are provided per-table via table options in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Obtaining the Required Parameters

- **AWS Access Key ID and Secret Access Key**:
  1. Sign in to the AWS Management Console.
  2. Navigate to **IAM** → **Users** (or **Roles** if using IAM roles).
  3. Create a new user or select an existing one.
  4. Go to the **Security credentials** tab.
  5. Create a new access key pair (Access Key ID and Secret Access Key).
  6. Store these credentials securely. Use them as `aws_access_key_id` and `aws_secret_access_key` connection options.
  7. Ensure the IAM user/role has the following permissions:
     - `cloudwatch:GetMetricData` (required for retrieving metric data)
     - `cloudwatch:ListMetrics` (required for automatic metric discovery)

- **AWS Region**:
  - Determine which AWS region contains the CloudWatch metrics you want to ingest.
  - Common regions include: `us-east-1`, `us-west-2`, `eu-west-1`, `ap-northeast-1`, etc.
  - Use this as the `region` connection option.

- **CloudWatch Metrics Namespace**:
  - Determine which CloudWatch namespace contains the metrics you want to ingest.
  - Common AWS namespaces include: `AWS/EC2`, `AWS/S3`, `AWS/Lambda`, `AWS/RDS`, `AWS/ApplicationELB`, `AWS/ECS`, etc.
  - Custom namespaces can also be used for application-specific metrics.
  - Use this as the `cloudwatch_metrics_namespace` connection option.
  - The connector will automatically discover all metrics and their dimension combinations within this namespace.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `period,statistics,dimensions` (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The CloudWatch connector automatically discovers all metrics and their dimension combinations within the configured namespace. Each unique combination of metric name and dimensions is treated as a separate time-series.

### Object summary, primary keys, and ingestion mode

The connector defines the ingestion mode and primary key for metrics:

| Table Type | Description | Ingestion Type | Primary Key | Incremental Cursor (if any) |
|------------|-------------|----------------|-------------|----------------------------|
| `metrics` | CloudWatch metric data points | `append` | `["namespace", "metric_name", "timestamp", "dimensions"]` | `timestamp` |

### Required and optional table options

Table-specific options are passed via the pipeline spec under `table` in `objects`. The namespace is specified at the connection level, and the connector automatically discovers all metrics and dimension combinations within that namespace.

**Time range management**:
- `start_time` and `end_time` are **automatically managed** by the connector and cannot be configured as table options.
- On the **first run**, the connector automatically retrieves data from 60 minutes ago to the current time.
- On **subsequent runs**, the connector uses stored cursors (based on `timestamp`) to incrementally retrieve only new data points.
- The connector tracks the latest `timestamp` for each metric-dimension combination and automatically determines the appropriate time range for each sync.

**Configurable table options**:
- **`period`** (integer, optional): Period in seconds for metric aggregation. Must be a multiple of 60. Defaults to 300 (5 minutes).
- **`statistics`** (array of strings, optional): List of statistics to retrieve. Valid values: `Average`, `Sum`, `SampleCount`, `Maximum`, `Minimum`. Defaults to `["Average"]`.
- **`dimensions`** (map/object, optional): Dimensions to filter metrics (e.g., `{"InstanceId": "i-1234567890abcdef0"}`). If not specified, retrieves data for all dimension combinations discovered within the namespace.

**Note on unit**: The `unit` field in the response is automatically determined by CloudWatch API based on the metric type. It cannot be specified as a table option and is always returned by the API in the response.

**How automatic discovery works**:
- The namespace is specified at the connection level via the `cloudwatch_metrics_namespace` parameter.
- The connector calls `ListMetrics` API with the namespace specified in the connection parameters.
- For each metric returned by `ListMetrics`, the connector extracts the `metric_name` and `dimensions` combination.
- Each unique combination of `namespace`, `metric_name`, and `dimensions` is treated as a separate time-series.
- The connector batches multiple metrics into a single `GetMetricData` request (up to 500 metrics per request) for efficiency.
- If `ListMetrics` returns more than 500 metrics, the connector automatically splits them into multiple batches and makes separate `GetMetricData` requests.
- Example: If the connection is configured with `cloudwatch_metrics_namespace: "AWS/EC2"` and the namespace contains `CPUUtilization` metric with 10 EC2 instances, the connector will discover 10 dimension combinations and retrieve data for all 10 instances in a single batch request.

**Note on dimensions**:
- If `dimensions` is not specified in table options, the connector retrieves data for all dimension combinations discovered within the namespace.
- If `dimensions` is specified, the connector filters to only retrieve data for that specific dimension combination.

### Schema highlights

Full schemas are defined by the connector and align with CloudWatch Metrics API:

- **`metrics`**:
  - `namespace` (string): CloudWatch namespace.
  - `metric_name` (string): Name of the metric.
  - `timestamp` (timestamp): Timestamp of the metric data point.
  - `value` (double): Metric value (varies by statistic).
  - `statistic` (string): Statistic type (Average, Sum, Maximum, Minimum, SampleCount).
  - `unit` (string): Unit of the metric.
  - `dimensions` (map): Dimensions associated with the metric (e.g., InstanceId, FunctionName).
  - Additional fields may be included based on the specific metric and dimensions.

You usually do not need to customize the schema; it is driven by the connector implementation and CloudWatch API responses.

## Data Type Mapping

CloudWatch metric fields are mapped to logical types as follows (and then to Spark types in the implementation):

| CloudWatch Type | Example Fields | Connector Logical Type | Notes |
|-----------------|----------------|------------------------|-------|
| double | `value`, aggregated statistics | double (`DoubleType`) | Metric values are stored as double precision floating point. |
| string | `namespace`, `metric_name`, `unit`, dimension keys/values | string (`StringType`) | UTF-8 text. |
| timestamp (ISO 8601) | `timestamp` | timestamp (`TimestampType`) | Stored as UTC timestamps. |
| map/object | `dimensions` | map (`MapType`) | Dimensions are stored as key-value pairs. |

The connector is designed to:
- Preserve metric timestamps as UTC timestamps.
- Store metric values as double precision numbers to maintain accuracy.
- Preserve dimension information as maps for flexible filtering and grouping.
- Handle multiple statistics per metric by creating separate rows or columns.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the CloudWatch connector source in your workspace. This will typically place the connector code (for example, `aws_cloudwatch_metrics.py`) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline code (e.g. `ingestion_pipeline.py` or a similar entrypoint), you will configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this CloudWatch connector.
- One or more **tables** to ingest, each with optional `table_options`.

Example `pipeline_spec` snippet:

**Example: Automatic discovery of all metrics**
This configuration will discover all metrics and dimension combinations within the namespace specified in the connection parameters. Time ranges are automatically managed by the connector:

```json
{
  "pipeline_spec": {
    "connection_name": "cloudwatch_connection",
    "object": [
      {
        "table": {
          "source_table": "metrics",
          "period": 300,
          "statistics": ["Average"]
        }
      }
    ]
  }
}
```

**Example: Filtering by dimensions**
This configuration retrieves data for all metrics, but filters to a specific dimension combination. Time ranges are automatically managed by the connector:

```json
{
  "pipeline_spec": {
    "connection_name": "cloudwatch_connection",
    "object": [
      {
        "table": {
          "source_table": "metrics",
          "period": 300,
          "statistics": ["Average", "Maximum"],
          "dimensions": {
            "InstanceId": "i-1234567890abcdef0"
          }
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your AWS credentials (`aws_access_key_id`, `aws_secret_access_key`, `region`, `cloudwatch_metrics_namespace`).
- The namespace is specified at the connection level via the `cloudwatch_metrics_namespace` parameter.
- For each `table`:
  - `source_table` must be `metrics`.
  - The connector automatically discovers all metrics and dimension combinations within the namespace specified in the connection parameters.
  - Time ranges (`start_time` and `end_time`) are automatically managed by the connector based on incremental cursors and cannot be configured.
  - Optionally specify `dimensions` to filter to a specific dimension combination, or omit `dimensions` to retrieve data for all dimension combinations.
  - Table options such as `period`, `statistics`, and `dimensions` are passed directly to the connector and used to control how metric data is retrieved.

You can ingest additional metrics by adding more `table` entries with different configurations (e.g., different time ranges, periods, or dimension filters).

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow). For incremental ingestion:

- On the **first run**:
  - The connector automatically retrieves data from 60 minutes ago to the current time.
  - The connector retrieves data for all discovered metrics in batches (up to 500 metrics per `GetMetricData` request).
  - Time ranges are automatically managed; no configuration is required.
- On **subsequent runs**:
  - The connector uses the stored `cursor` (based on `timestamp`) to pick up new metric data points incrementally.
  - The connector automatically tracks the latest `timestamp` for each metric and only retrieves new data points.
  - Multiple metrics are retrieved in batches for efficiency.
  - Time ranges are automatically determined based on stored cursors.

#### Best Practices

- **Start small**:
  - Begin with a single metric (e.g., one EC2 instance's CPU utilization) to validate configuration and data shape.
- **Use appropriate periods**:
  - Use `period=300` (5 minutes) for most use cases to balance granularity and API efficiency.
  - Use `period=60` (1 minute) for high-frequency metrics if needed.
  - Note: CloudWatch charges based on the number of API requests, so larger periods reduce costs.
- **Filter with dimensions**:
  - Use `dimensions` to filter metrics to specific resources (e.g., specific EC2 instances, Lambda functions) to reduce data volume and API calls.
- **Respect API limits**:
  - CloudWatch has rate limits (typically 400 GetMetricData requests per second per account).
  - Consider staggering syncs or using batch requests if you encounter rate limiting.
- **Optimize statistics**:
  - Only request the statistics you need (e.g., `["Average"]` instead of `["Average", "Sum", "Maximum", "Minimum", "SampleCount"]`) to reduce API calls and data volume.

#### Troubleshooting

Common issues and how to address them:

- **Authentication failures (`403 Forbidden` or `InvalidClientTokenId`)**:
  - Verify that `aws_access_key_id` and `aws_secret_access_key` are correct and not expired.
  - Ensure the IAM user/role has the required CloudWatch permissions (`cloudwatch:GetMetricData`, `cloudwatch:ListMetrics`).
- **Region errors (`InvalidParameterValue`)**:
  - Check that the `region` parameter matches the region where your metrics are stored.
  - Verify that the region name is correct (e.g., `us-east-1`, not `us-east-1a`).
- **Metric not found (`InvalidParameterValue`)**:
  - Verify that `cloudwatch_metrics_namespace` in connection parameters is spelled correctly.
  - Ensure the IAM user/role has `cloudwatch:ListMetrics` permission to discover available metrics.
  - Check that metrics exist in the specified namespace and region.
  - Verify that the namespace contains metrics in the specified time range.
- **Rate limiting (`Throttling`)**:
  - Reduce the number of concurrent metric requests.
  - Increase the `period` to reduce the number of data points requested.
  - GetMetricData supports batch requests (multiple metrics in a single request), which can help reduce API calls.
- **Missing data points**:
  - CloudWatch metrics may have gaps if no data was reported during a period.
  - Verify that the time range (`start_time` to `end_time`) contains data.
  - Check that the metric is actively being published (e.g., EC2 instance is running, Lambda function is being invoked).

## References

- Connector implementation: `sources/aws_cloudwatch_metrics/aws_cloudwatch_metrics.py`
- Connector API documentation and schemas: `sources/aws_cloudwatch_metrics/aws_cloudwatch_metrics_api_doc.md`
- Official AWS CloudWatch Metrics API documentation:
  - `https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_GetMetricData.html`
  - `https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_ListMetrics.html`
  - `https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html`

