# **CloudWatch Metrics API Documentation**

## **Authorization**

- **Chosen method**: AWS Signature Version 4 (SigV4) authentication using AWS Access Keys or temporary credentials.
- **Base URL**: `https://monitoring.<region>.amazonaws.com` (region-specific endpoint)
- **Auth placement**:
  - AWS Signature Version 4 is used to sign all API requests.
  - Credentials are provided via:
    - `aws_access_key_id`: AWS Access Key ID
    - `aws_secret_access_key`: AWS Secret Access Key
  - The signature is included in the `Authorization` header of each request.
- **IAM Permissions required**:
  - `cloudwatch:GetMetricData` - For retrieving metric data using the GetMetricData API
  - `cloudwatch:ListMetrics` - Required, for discovering available metrics and dimension combinations

Example authenticated request using AWS SDK (Python boto3):

```python
import boto3
from datetime import datetime

client = boto3.client(
    'cloudwatch',
    aws_access_key_id='YOUR_ACCESS_KEY_ID',
    aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
    region_name='us-east-1'
)

response = client.get_metric_data(
    MetricDataQueries=[
        {
            'Id': 'm1',
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/EC2',
                    'MetricName': 'CPUUtilization',
                    'Dimensions': [
                        {
                            'Name': 'InstanceId',
                            'Value': 'i-1234567890abcdef0'
                        }
                    ]
                },
                'Period': 300,
                'Stat': 'Average'
            },
            'ReturnData': True
        }
    ],
    StartTime=datetime(2024, 1, 1),
    EndTime=datetime(2024, 1, 2)
)
```

Notes:
- AWS SDKs (boto3 for Python, AWS SDK for Java, etc.) handle SigV4 signing automatically.
- Rate limiting: CloudWatch has default limits of 400 GetMetricData requests per second per account.
- Requests are signed using AWS Signature Version 4, which includes timestamp and region information in the signature.


## **Object List**

For connector purposes, CloudWatch Metrics are treated as **objects/tables**.  
The object list is **dynamic** (discovered via API or configured per metric), not static.

| Object Name | Description | Primary Endpoint | Ingestion Type (planned) |
|------------|-------------|------------------|--------------------------|
| `metrics` | CloudWatch metric data points | `GetMetricData` | `append` (time-series data) |

**Object discovery**:
- CloudWatch metrics are organized by **namespace** (e.g., `AWS/EC2`, `AWS/S3`, `AWS/Lambda`).
- Each namespace contains multiple **metric names** (e.g., `CPUUtilization`, `NetworkIn`, `Duration`).
- Metrics can have **dimensions** (key-value pairs) that further identify specific resources (e.g., `InstanceId`, `FunctionName`).
- The connector uses **automatic discovery** mode:
  - The namespace is specified at the connection level via the `cloudwatch_metrics_namespace` connection parameter.
  - The connector uses the `ListMetrics` API to automatically discover all available metrics and their dimension combinations within the specified namespace.
  - The connector then retrieves data for all discovered metric-dimension combinations.

**Using ListMetrics API for automatic discovery**:
- The `ListMetrics` API is called with the namespace specified in the connection parameters.
- For each metric, the API returns all available dimension combinations (e.g., different `InstanceId` values for EC2 metrics).
- Each unique combination of `namespace`, `metric_name`, and `dimensions` represents a distinct time-series that can be ingested.
- Example: If the connection is configured with `cloudwatch_metrics_namespace: "AWS/EC2"` and the namespace contains `CPUUtilization` metric with 10 EC2 instances, `ListMetrics` will return 10 dimension combinations (one per instance), and the connector will retrieve data for all 10 combinations.

**Common AWS namespaces**:
- `AWS/EC2` - Amazon EC2 metrics
- `AWS/S3` - Amazon S3 metrics
- `AWS/Lambda` - AWS Lambda metrics
- `AWS/RDS` - Amazon RDS metrics
- `AWS/ApplicationELB` - Application Load Balancer metrics
- `AWS/ECS` - Amazon ECS metrics
- Custom namespaces can also be used for application-specific metrics.

**Connector scope for initial implementation**:
- The connector will support reading metric data using `GetMetricData` API.
- The connector will use automatic discovery via `ListMetrics` API with the namespace specified in connection parameters.
- Each metric configuration (namespace + metric_name + dimensions) discovered via `ListMetrics` is treated as a separate logical table or data stream.
- The connector will support incremental reads based on timestamp cursors for all discovered metrics.


## **Object Schema**

### General notes

- CloudWatch Metrics API returns time-series data with timestamps and values.
- For the connector, we define **tabular schemas** for metric data points.
- Each metric data point includes: timestamp, value(s), statistic type, unit, and dimensions.

### `metrics` object (primary table)

**Source endpoint**:  
- `GetMetricData` - For retrieving metric data with advanced querying capabilities

**Key behavior**:
- Returns time-series data points for one or more metrics in a single request.
- Supports filtering by dimensions, time range, and period.
- Supports multiple statistics per metric (Average, Sum, Maximum, Minimum, SampleCount).
- Supports batch queries (multiple metrics in a single request) for improved efficiency.

**High-level schema (connector view)**:

Top-level fields (all from the CloudWatch Metrics API unless marked as "connector-derived"):

| Column Name | Type | Description |
|------------|------|-------------|
| `namespace` | string | CloudWatch namespace (e.g., `AWS/EC2`, `AWS/Lambda`). |
| `metric_name` | string | Name of the CloudWatch metric (e.g., `CPUUtilization`, `Duration`). |
| `timestamp` | timestamp | Timestamp of the metric data point (UTC). |
| `value` | double | Metric value. The meaning depends on the statistic type. |
| `statistic` | string | Statistic type: `Average`, `Sum`, `SampleCount`, `Maximum`, `Minimum`. |
| `unit` | string | Unit of the metric (e.g., `Seconds`, `Bytes`, `Percent`, `Count`, `None`). |
| `dimensions` | map<string, string> | Dimensions associated with the metric (e.g., `{"InstanceId": "i-1234567890abcdef0"}`). |
| `region` | string (connector-derived) | AWS region where the metric is stored. |

**Example request** (GetMetricData):

```python
import boto3
from datetime import datetime

client = boto3.client('cloudwatch', region_name='us-east-1')

response = client.get_metric_data(
    MetricDataQueries=[
        {
            'Id': 'm1',
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/EC2',
                    'MetricName': 'CPUUtilization',
                    'Dimensions': [
                        {
                            'Name': 'InstanceId',
                            'Value': 'i-1234567890abcdef0'
                        }
                    ]
                },
                'Period': 300,
                'Stat': 'Average'
            },
            'ReturnData': True
        }
    ],
    StartTime=datetime(2024, 1, 1),
    EndTime=datetime(2024, 1, 2)
)
```

**Example response** (GetMetricData):

```json
{
    "MetricDataResults": [
        {
            "Id": "m1",
            "Label": "CPUUtilization",
            "Timestamps": [
                "2024-01-01T00:00:00Z",
                "2024-01-01T00:05:00Z"
            ],
            "Values": [
                45.5,
                52.3
            ],
            "StatusCode": "Complete"
        }
    ],
    "Messages": []
}
```

> The columns listed above define the **complete connector schema** for the `metrics` table.  
> If additional CloudWatch metric fields are needed in the future, they must be added as new columns here so the documentation continues to reflect the full table schema.


## **Get Object Primary Keys**

There is no dedicated metadata endpoint to get the primary key for CloudWatch metrics.  
Instead, the primary key is defined **statically** based on the metric data structure.

- **Primary key for `metrics`**: `["namespace", "metric_name", "timestamp", "dimensions"]`  
  - `namespace` (string): CloudWatch namespace
  - `metric_name` (string): Metric name
  - `timestamp` (timestamp): Timestamp of the data point (UTC)
  - `dimensions` (map): Dimensions as a map/struct (e.g., `{"InstanceId": "i-1234567890abcdef0"}`)
  - Type: Composite key combining namespace, metric name, timestamp, and dimensions
  - Property: Unique for each metric data point

The connector will:
- Read the `namespace`, `metric_name`, `timestamp`, and `dimensions` fields from each metric data point.
- Use the composite key as the primary key for deduplication and upserts.
- Handle multiple statistics per metric by creating separate rows or columns.

Example showing primary key components in response:

```json
{
    "Namespace": "AWS/EC2",
    "MetricName": "CPUUtilization",
    "Dimensions": [
        {
            "Name": "InstanceId",
            "Value": "i-1234567890abcdef0"
        }
    ],
    "Datapoints": [
        {
            "Timestamp": "2024-01-01T00:00:00Z",
            "Value": 45.5,
            "Unit": "Percent"
        }
    ]
}
```

Primary key: `("AWS/EC2", "CPUUtilization", "2024-01-01T00:00:00Z", {"InstanceId": "i-1234567890abcdef0"})`


## **Object's ingestion type**

Supported ingestion types (framework-level definitions):
- `cdc`: Change data capture; supports upserts and/or deletes incrementally.
- `snapshot`: Full replacement snapshot; no inherent incremental support.
- `append`: Incremental but append-only (no updates/deletes).

Planned ingestion types for CloudWatch objects:

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `metrics` | `append` | CloudWatch metrics are time-series data that are append-only. Each data point has a unique timestamp and dimensions combination. Historical data points are immutable once published. New data points are added over time, but existing data points are never updated or deleted. |

For `metrics`:
- **Primary key**: `["namespace", "metric_name", "timestamp", "dimensions"]`
- **Cursor field**: `timestamp`
- **Sort order**: Ascending by `timestamp`
- **Deletes**: CloudWatch metrics are immutable; data points are never deleted. The connector treats all reads as append-only.
- **Incremental strategy**: 
  - Time ranges (`StartTime` and `EndTime`) are automatically managed by the connector.
  - On first run, the connector automatically retrieves data from 60 minutes ago to the current time.
  - On subsequent runs, the connector uses the maximum `timestamp` from the previous sync as the new `StartTime` (with optional lookback window).
  - The connector automatically sets `EndTime` to the current time.
  - Only retrieve new data points that have timestamps greater than the stored cursor.


## **Read API for Data Retrieval**

### Discovery endpoint for `metrics`

#### ListMetrics API

- **HTTP method**: `POST`
- **Endpoint**: `https://monitoring.<region>.amazonaws.com/`
- **Action**: `ListMetrics`

**Purpose**: Discover all available metrics and their dimension combinations within a namespace.

**Required parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `Namespace` | string | yes | CloudWatch namespace (e.g., `AWS/EC2`, `AWS/Lambda`). |

**Optional parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `MetricName` | string | no | none | Filter by specific metric name. If omitted, returns all metrics in the namespace. |
| `Dimensions` | array of DimensionFilter objects | no | none | Filter by dimensions. Each filter can use exact match or wildcards. |
| `NextToken` | string | no | none | Token for pagination (if response is truncated). |

**Response structure**:
- Returns an array of `Metric` objects, each containing:
  - `Namespace`: The namespace of the metric
  - `MetricName`: The name of the metric
  - `Dimensions`: Array of dimension key-value pairs that uniquely identify this metric time-series

**Key behavior**:
- Each `Metric` object in the response represents a unique time-series identified by the combination of `Namespace`, `MetricName`, and `Dimensions`.
- If a metric has multiple dimension combinations (e.g., multiple EC2 instances), `ListMetrics` returns separate `Metric` objects for each combination.
- Example: For `AWS/EC2` namespace with `CPUUtilization` metric, if there are 3 EC2 instances (`i-123`, `i-456`, `i-789`), `ListMetrics` returns 3 `Metric` objects, each with different `InstanceId` dimension values.

**Pagination strategy**:
- `ListMetrics` supports pagination via `NextToken`.
- If the response includes `NextToken`, the connector should make additional requests with the token to retrieve all metrics.
- Maximum response size: 500 metrics per request (default limit).

**Example request** (discover all metrics in a namespace):

```python
import boto3

client = boto3.client('cloudwatch', region_name='us-east-1')

# Discover all metrics in AWS/EC2 namespace
response = client.list_metrics(Namespace='AWS/EC2')

# Process pagination if needed
metrics = response['Metrics']
while 'NextToken' in response:
    response = client.list_metrics(
        Namespace='AWS/EC2',
        NextToken=response['NextToken']
    )
    metrics.extend(response['Metrics'])

# Each metric in 'metrics' contains:
# - Namespace: 'AWS/EC2'
# - MetricName: e.g., 'CPUUtilization', 'NetworkIn', etc.
# - Dimensions: e.g., [{'Name': 'InstanceId', 'Value': 'i-1234567890abcdef0'}]
```

**Example response**:

```json
{
    "Metrics": [
        {
            "Namespace": "AWS/EC2",
            "MetricName": "CPUUtilization",
            "Dimensions": [
                {
                    "Name": "InstanceId",
                    "Value": "i-1234567890abcdef0"
                }
            ]
        },
        {
            "Namespace": "AWS/EC2",
            "MetricName": "CPUUtilization",
            "Dimensions": [
                {
                    "Name": "InstanceId",
                    "Value": "i-0987654321fedcba0"
                }
            ]
        },
        {
            "Namespace": "AWS/EC2",
            "MetricName": "NetworkIn",
            "Dimensions": [
                {
                    "Name": "InstanceId",
                    "Value": "i-1234567890abcdef0"
                }
            ]
        }
    ],
    "NextToken": null
}
```

**Connector workflow with automatic discovery**:
1. User specifies `cloudwatch_metrics_namespace` in connection parameters (e.g., `cloudwatch_metrics_namespace: "AWS/EC2"`).
2. Connector calls `ListMetrics` API with the namespace from connection parameters to discover all available metrics and dimension combinations.
3. The connector groups discovered `Metric` objects into batches for efficient retrieval:
   - Each `Metric` object represents a unique time-series (namespace + metric_name + dimensions combination).
   - The connector batches multiple metrics into a single `GetMetricData` request (up to 500 MetricDataQuery objects per request).
   - If `ListMetrics` returns more than 500 metrics, the connector splits them into multiple batches and makes separate `GetMetricData` requests.
4. For each batch, the connector:
   - Extracts `MetricName` and `Dimensions` from each `Metric` object in the batch.
   - Constructs a `GetMetricData` request with multiple `MetricDataQuery` objects (one per metric-dimension combination).
   - Calls `GetMetricData` to retrieve actual metric data for all metrics in the batch simultaneously.
5. The connector tracks cursors (timestamps) independently for each metric-dimension combination.

**Handling dimension combinations**:
- Each `Metric` object returned by `ListMetrics` represents a complete dimension combination.
- The connector should preserve the exact dimension structure when making subsequent `GetMetricData` calls.
- Example: If `ListMetrics` returns `{"Name": "InstanceId", "Value": "i-123"}`, the connector should use the exact same dimension structure when calling `GetMetricData`.

**Rate limits**:
- `ListMetrics`: 150 requests per second per account (default limit).
- The connector should implement retry logic with exponential backoff for rate limit errors (`Throttling`).

### Primary read endpoint for `metrics`

#### GetMetricData API

- **HTTP method**: `POST`
- **Endpoint**: `https://monitoring.<region>.amazonaws.com/`
- **Action**: `GetMetricData`

**Key advantages**:
- Supports multiple metrics in a single request (batch queries) for improved efficiency.
- More efficient for retrieving multiple metrics compared to individual requests.
- Supports math expressions and metric math for advanced queries.
- Better suited for large-scale metric retrieval scenarios.

**Request limits**:
- Maximum 500 `MetricDataQuery` objects per `GetMetricData` request.
- Maximum 100,800 data points per request (across all queries).
- If `ListMetrics` returns more than 500 metrics, the connector must split them into multiple batches.
- Each batch should contain up to 500 metrics to stay within API limits.

**Required parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `MetricDataQueries` | array of MetricDataQuery objects | yes | Array of metric queries to execute. |
| `StartTime` | timestamp | yes | Start time for metric data retrieval. **Note**: This is automatically managed by the connector based on incremental cursors and is not configurable as a table option. |
| `EndTime` | timestamp | yes | End time for metric data retrieval. **Note**: This is automatically managed by the connector and is not configurable as a table option. |

**Optional parameters**:

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `NextToken` | string | no | none | Token for pagination (if response is truncated). |
| `MaxDatapoints` | integer | no | none | Maximum number of data points to return per query. |

**Pagination strategy**:
- GetMetricData supports pagination via `NextToken`.
- If the response includes `NextToken`, the connector should make additional requests with the token to retrieve all data.
- Maximum response size: 1,440 data points per query.

**Example GetMetricData request** (batch query with multiple metrics):

```python
from datetime import datetime

# Example: Batch query for multiple metrics discovered via ListMetrics
response = client.get_metric_data(
    MetricDataQueries=[
        {
            'Id': 'm1',
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/EC2',
                    'MetricName': 'CPUUtilization',
                    'Dimensions': [
                        {'Name': 'InstanceId', 'Value': 'i-1234567890abcdef0'}
                    ]
                },
                'Period': 300,
                'Stat': 'Average'
            },
            'ReturnData': True
        },
        {
            'Id': 'm2',
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/EC2',
                    'MetricName': 'CPUUtilization',
                    'Dimensions': [
                        {'Name': 'InstanceId', 'Value': 'i-0987654321fedcba0'}
                    ]
                },
                'Period': 300,
                'Stat': 'Average'
            },
            'ReturnData': True
        },
        {
            'Id': 'm3',
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/EC2',
                    'MetricName': 'NetworkIn',
                    'Dimensions': [
                        {'Name': 'InstanceId', 'Value': 'i-1234567890abcdef0'}
                    ]
                },
                'Period': 300,
                'Stat': 'Sum'
            },
            'ReturnData': True
        }
        # ... up to 500 MetricDataQuery objects per request
    ],
    StartTime=datetime(2024, 1, 1),
    EndTime=datetime(2024, 1, 2)
)
```

**Batch processing and request splitting**:
- The connector should batch multiple metrics into a single `GetMetricData` request whenever possible.
- If `ListMetrics` returns more than 500 metrics, the connector must split them into batches:
  - Batch 1: Metrics 1-500
  - Batch 2: Metrics 501-1000
  - Batch 3: Metrics 1001-1500
  - And so on...
- Each batch is processed in a separate `GetMetricData` request.
- This approach maximizes efficiency while staying within API limits.

**Handling missing data points**:
- CloudWatch metrics may have gaps if no data was reported during a period.
- Missing data points are simply not included in the response; the connector should handle this gracefully.
- The connector should not insert null values for missing timestamps unless explicitly configured to do so.

**Time range splitting**:
- For large time ranges, the connector should split requests into smaller chunks (e.g., 1-hour windows) to:
  - Avoid timeouts
  - Stay within API response size limits
  - Improve reliability and error handling

**Incremental strategy and automatic time range management**:
- `StartTime` and `EndTime` are **automatically managed** by the connector and cannot be configured as table options.
- On the **first run**:
  - The connector automatically sets `StartTime` to 60 minutes ago and `EndTime` to the current time.
  - The connector retrieves data for all discovered metrics in batches.
  - No user configuration is required for time ranges.
- On **subsequent runs**:
  - The connector uses the maximum `timestamp` value (plus a small lookback window, e.g., 5 minutes) from the previous sync as the new `StartTime`.
  - The connector automatically sets `EndTime` to the current time.
  - The connector retrieves only new data points that have timestamps greater than the stored cursor.
  - CloudWatch metrics are published with some delay (typically 1-2 minutes), so a lookback window helps ensure no data is missed.

**Example incremental read**:

```python
from datetime import datetime, timedelta

# Namespace is obtained from connection parameters (cloudwatch_metrics_namespace)
namespace = 'AWS/EC2'  # From connection parameter

# First run - batch multiple metrics discovered via ListMetrics
# Assume ListMetrics returned 3 metrics (in practice, could be hundreds)
metrics = [
    {'metric_name': 'CPUUtilization', 'dimensions': [{'Name': 'InstanceId', 'Value': 'i-1234567890abcdef0'}]},
    {'metric_name': 'CPUUtilization', 'dimensions': [{'Name': 'InstanceId', 'Value': 'i-0987654321fedcba0'}]},
    {'metric_name': 'NetworkIn', 'dimensions': [{'Name': 'InstanceId', 'Value': 'i-1234567890abcdef0'}]}
]

# First run: default to 60 minutes ago if start_time not specified
end_time = datetime.now()
start_time = end_time - timedelta(minutes=60)  # Default: 60 minutes ago

# Build MetricDataQueries for batch request (up to 500 per request)
metric_queries = []
for idx, metric in enumerate(metrics):
    metric_queries.append({
        'Id': f'm{idx + 1}',
        'MetricStat': {
            'Metric': {
                'Namespace': namespace,
                'MetricName': metric['metric_name'],
                'Dimensions': metric['dimensions']
            },
            'Period': 300,
            'Stat': 'Average'
        },
        'ReturnData': True
    })

# If more than 500 metrics, split into batches
batch_size = 500
for i in range(0, len(metric_queries), batch_size):
    batch = metric_queries[i:i + batch_size]
    response = client.get_metric_data(
        MetricDataQueries=batch,
        StartTime=start_time,
        EndTime=end_time
    )
    # Process response...

# Subsequent run (using stored cursor)
last_timestamp = datetime(2024, 1, 2)  # From previous sync
lookback_window = timedelta(minutes=5)
start_time = last_timestamp - lookback_window
end_time = datetime.now()

# Use same batching logic for subsequent runs
for i in range(0, len(metric_queries), batch_size):
    batch = metric_queries[i:i + batch_size]
    response = client.get_metric_data(
        MetricDataQueries=batch,
        StartTime=start_time,
        EndTime=end_time
    )
    # Process response...
```

**Rate limits**:
- GetMetricData: 400 requests per second per account (default limit).
- The connector should implement retry logic with exponential backoff for rate limit errors (`Throttling`).


## **Field Type Mapping**

### General mapping (CloudWatch API → connector logical types)

| CloudWatch API Type | Example Fields | Connector Logical Type | Notes |
|---------------------|----------------|------------------------|-------|
| double | `Value`, `Average`, `Sum`, `Maximum`, `Minimum` | `double` / `DoubleType` | Metric values are stored as double precision floating point numbers. |
| string | `Namespace`, `MetricName`, `Unit`, dimension keys/values | `string` / `StringType` | UTF-8 text. |
| timestamp (ISO 8601) | `Timestamp` | `timestamp` / `TimestampType` | Stored as UTC timestamps. CloudWatch returns timestamps in ISO 8601 format. |
| array of Dimension objects | `Dimensions` | `map<string, string>` / `MapType` | Dimensions are converted to a map/struct with dimension names as keys and values as values. |
| array of strings | `Statistics` | `array<string>` / `ArrayType` | Statistics are stored as an array or as separate columns/rows. |

### Special behaviors and constraints

- **Metric values**: Stored as `double` to maintain precision. CloudWatch supports very large numbers (e.g., bytes, counts).
- **Timestamps**: CloudWatch returns timestamps in ISO 8601 format (e.g., `"2024-01-01T00:00:00Z"`). The connector should parse these as UTC timestamps.
- **Dimensions**: Dimensions are key-value pairs that identify specific resources. The connector should preserve them as a map/struct rather than flattening them.
- **Statistics**: Multiple statistics can be requested per metric. The connector can either:
  - Create separate rows for each statistic (one row per timestamp + statistic combination).
  - Create separate columns for each statistic (one row per timestamp with multiple value columns).
- **Units**: Units are strings (e.g., `Seconds`, `Bytes`, `Percent`, `Count`, `None`). The connector should preserve them as strings.
- **Missing data**: CloudWatch does not return data points for periods with no data. The connector should not insert null values unless explicitly configured.
- **Period constraints**: The `Period` parameter must be a multiple of 60 seconds. Common values: 60, 300, 900, 3600.

### Example type mappings

**GetMetricData response**:
```json
{
    "MetricDataResults": [
        {
            "Id": "m1",
            "Label": "CPUUtilization",
            "Timestamps": [
                "2024-01-01T00:00:00Z"
            ],
            "Values": [
                45.5
            ],
            "StatusCode": "Complete"
        }
    ],
    "Messages": []
}
```

**Connector schema**:
- `namespace`: string → `"AWS/EC2"` (from MetricDataQuery)
- `metric_name`: string → `"CPUUtilization"` (from MetricDataQuery)
- `timestamp`: timestamp → `2024-01-01T00:00:00Z` (from Timestamps array, parsed as UTC timestamp)
- `value`: double → `45.5` (from Values array)
- `statistic`: string → `"Average"` (from MetricStat.Stat)
- `unit`: string → `"Percent"` (from API response)
- `dimensions`: map<string, string> → `{"InstanceId": "i-1234567890abcdef0"}` (from MetricDataQuery)


## **Sources and References**

- **Official AWS CloudWatch Metrics API documentation** (highest confidence)
  - `https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_GetMetricData.html`
  - `https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_ListMetrics.html`
  - `https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html`
  - `https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/working_with_metrics.html`
- **AWS SDK documentation** (high confidence)
  - `https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch.html`
  - `https://docs.aws.amazon.com/sdk-for-python/latest/reference/services/cloudwatch.html`
- **AWS CloudWatch pricing and limits** (high confidence)
  - `https://aws.amazon.com/cloudwatch/pricing/`
  - `https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html`

When conflicts arise, **official AWS CloudWatch documentation** is treated as the source of truth.

