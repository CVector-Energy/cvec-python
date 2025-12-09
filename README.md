# CVec Client Library

The "cvec" package is the Python SDK for CVector Energy.

# Getting Started

## Installation

Assuming that you have a supported version of Python installed, you can first create a venv with:

```
python -m venv .venv
```

Then, activate the venv:

```
. .venv/bin/activate
```

Then, you can install cvec from PyPI with:

```
pip install cvec
```

## Using cvec

Import the cvec package. We will also use the datetime module.

```
import cvec
from datetime import datetime
```

Construct the CVec client. The host and api_key can be given through parameters to the constructor or from the environment variables CVEC_HOST and CVEC_API_KEY:

```
cvec = cvec.CVec()
```

### Spans

A span is a period of interest, such as an experiment, a baseline recording session, or an alarm. The initial state of a Span is implicitly defined by a period where a given metric has a constant value.

The newest span for a metric does not have an end time, since it has not ended yet (or has not ended by the finish of the queried period).

To get the spans on `my_tag_name` since 2025-05-14 10am, run:

```
for span in cvec.get_spans("mygroup/myedge/node", start_at=datetime(2025, 5, 14, 10, 0, 0)):
    print("%s\t%s" % (span.value, span.raw_start_at))
```

The output will be like:

```
offline   2025-05-19 16:28:02.130000+00:00
starting  2025-05-19 16:28:01.107000+00:00
running   2025-05-19 15:29:28.795000+00:00
stopping  2025-05-19 15:29:27.788000+00:00
offline   2025-05-19 14:14:43.752000+00:00
```

### Metrics

A metric is a named set of time-series data points pertaining to a particular resource (for example, the value reported by a sensor). Metrics can have numeric or string values. Boolean values are mapped to 0 and 1. The get_metrics function returns a list of metric metadata.

To get all of the metrics that changed value at 10am on 2025-05-14, run:

```
for item in cvec.get_metrics(start_at=datetime(2025, 5, 14, 10, 0, 0), end_at=datetime(2025, 5, 14, 11, 0, 0)):
  print(item.name)
```

Example output:

```
mygroup/myedge/compressor01/status
mygroup/myedge/compressor01/interlocks/emergency_stop
mygroup/myedge/compressor01/stage1/pressure_out/psig
mygroup/myedge/compressor01/stage1/temp_out/c
mygroup/myedge/compressor01/stage2/pressure_out/psig
mygroup/myedge/compressor01/stage2/temp_out/c
mygroup/myedge/compressor01/motor/current/a
mygroup/myedge/compressor01/motor/power_kw
```

### Metric Data

The main content for a metric is a set of points where the metric value changed. These are returned with columns for name, time, value_double, value_string.

To get all of the value changes for all metrics at 10am on 2025-05-14, run:

```
cvec.get_metric_data(start_at=datetime(2025, 5, 14, 10, 0, 0), end_at=datetime(2025, 5, 14, 11, 0, 0))
```

Example output:

```
                                                        name                             time  value_double value_string
0      mygroup/myedge/mode                                   2025-05-14 10:10:41.949000+00:00     24.900000     starting
1      mygroup/myedge/compressor01/interlocks/emergency_stop 2025-05-14 10:27:24.899000+00:00     0.0000000         None
2      mygroup/myedge/compressor01/stage1/pressure_out/psig  2025-05-14 10:43:38.282000+00:00     123.50000         None
3      mygroup/myedge/compressor01/stage1/temp_out/c         2025-05-14 10:10:41.948000+00:00     24.900000         None
4      mygroup/myedge/compressor01/motor/current/a           2025-05-14 10:27:24.897000+00:00     12.000000         None
...                                   ...                              ...           ...          ...
46253  mygroup/myedge/compressor01/stage1/temp_out/c         2025-05-14 10:59:55.725000+00:00     25.300000         None
46254  mygroup/myedge/compressor01/stage2/pressure_out/psig  2025-05-14 10:59:56.736000+00:00     250.00000         None
46255  mygroup/myedge/compressor01/stage2/temp_out/c         2025-05-14 10:59:57.746000+00:00     12.700000         None
46256  mygroup/myedge/compressor01/motor/current/a           2025-05-14 10:59:58.752000+00:00     11.300000         None
46257  mygroup/myedge/compressor01/motor/power_kw            2025-05-14 10:59:59.760000+00:00     523.40000         None

[46257 rows x 4 columns]
```
#### Pandas Data Frames

Use the `get_metric_arrow` function to efficiently load data into a pandas DataFrame like this:

```python
import pandas as pd
import pyarrow as pa

reader = pa.ipc.open_file(cvec.get_metric_arrow(names=["tag1", "tag2"]))
df = reader.read_pandas()
```

### Adding Metric Data

To add new metric data points, you create a list of `MetricDataPoint` objects and pass them to `add_metric_data`. Each `MetricDataPoint` should have a `name`, a `time`, and either a `value_double` (for numeric values) or a `value_string` (for string values).

```python
from datetime import datetime
from cvec.models import MetricDataPoint

# Assuming 'cvec' client is already initialized

# Create some data points
data_points = [
    MetricDataPoint(
        name="mygroup/myedge/compressor01/stage1/temp_out/c",
        time=datetime(2025, 7, 29, 10, 0, 0),
        value_double=25.5,
    ),
    MetricDataPoint(
        name="mygroup/myedge/compressor01/status",
        time=datetime(2025, 7, 29, 10, 0, 5),
        value_string="running",
    ),
]

# Add the data points to CVec
cvec.add_metric_data(data_points)
```

## CSV Import Tool

The repository includes a command-line script for importing CSV data into CVec. The script is located at `scripts/csv_import.py`.

### Usage

```bash
python scripts/csv_import.py [options] csv_file
```

### Options

- `csv_file`: Path to the CSV file to import (required)
- `--prefix PREFIX`: Prefix to add to metric names (separated by '/')
- `--host HOST`: CVec host URL (overrides CVEC_HOST environment variable)
- `--api-key API_KEY`: CVec API key (overrides CVEC_API_KEY environment variable)

### CSV Format

The CSV file must have:
- A header row with column names
- A timestamp column (case-insensitive: "timestamp", "Timestamp", etc.)
- One or more metric columns

Example CSV:
```csv
timestamp,rain_rate,actual_inflow,predicted_inflow
2025-01-01 00:00:00,0.5,100.2,95.8
2025-01-01 01:00:00,1.2,150.5,145.3
2025-01-01 02:00:00,0.8,120.1,118.7
```

### Examples

```bash
# Basic import
python scripts/csv_import.py data.csv

# Add prefix to metric names (rain_rate becomes "weather/rain_rate")
python scripts/csv_import.py data.csv --prefix "weather"

# Specify CVec connection details
python scripts/csv_import.py data.csv --host "https://your-cvec-host.com" --api-key "your-api-key"
```

The script automatically:
- Detects numeric vs string values
- Supports multiple timestamp formats
- Provides detailed progress information
- Handles errors gracefully

# CVec Class

The SDK provides an API client class named `CVec` with the following functions.

## `__init__(?host, ?api_key, ?default_start_at, ?default_end_at)`

Setup the SDK with the given host and API Key. The host and API key are loaded from environment variables CVEC_HOST and CVEC_API_KEY if they are not given as arguments to the constructor. The tenant ID is automatically fetched from the host's `/config` endpoint. The `default_start_at` and `default_end_at` can provide a default query time interval for API methods.

## `get_spans(name, ?start_at, ?end_at, ?limit)`

Return time spans for a metric. Spans are generated from value changes that occur after `start_at` (if specified) and before `end_at` (if specified).
If `start_at` is `None` (e.g., not provided as an argument and no class default `default_start_at` is set), the query for value changes is unbounded at the start. Similarly, if `end_at` is `None`, the query is unbounded at the end.

Each `Span` object in the returned list represents a period where the metric's value is constant and has the following attributes:
- `value`: The metric's value during the span.
- `name`: The name of the metric.
- `raw_start_at`: The timestamp of the value change that initiated this span's value. This will be greater than or equal to the query's `start_at` if one was specified.
- `raw_end_at`: The timestamp marking the end of this span's constant value. For the newest span, the value is `None`. For other spans, it's the raw_start_at of the immediately newer data point, which is next span in the list.
- `id`: Currently `None`. In a future version of the SDK, this will be the span's unique identifier.
- `metadata`: Currently `None`. In a future version, this can be used to store annotations or other metadata related to the span.

Returns a list of `Span` objects, sorted in descending chronological order (newest span first).
If no relevant value changes are found, an empty list is returned.

## `get_metric_data(?names, ?start_at, ?end_at)`

Return all data-points within a given [`start_at`, `end_at`) interval, optionally selecting a given list of metric names. The return value is a Pandas DataFrame with four columns: name, time, value_double, value_string. One row is returned for each metric value transition.

## `add_metric_data(data_points, ?use_arrow)`

Add multiple metric data points to the database.

- `data_points`: A list of `MetricDataPoint` objects to add.
- `use_arrow`: An optional boolean. If `True`, data is sent to the server using the more efficient Apache Arrow format. This is recommended for large datasets. Defaults to `False`.

## `get_metrics(?start_at, ?end_at)`

Return a list of metrics that had at least one transition in the given [`start_at`, `end_at`) interval. All metrics are returned if no `start_at` and `end_at` are given.

## `get_modeling_metrics(?start_at, ?end_at)`

Fetch modeling metrics from the modeling database. This method returns a list of available modeling metrics that had transitions in the specified time range.

- `start_at`: Optional start date for the query range (uses class default if not specified)
- `end_at`: Optional end date for the query range (uses class default if not specified)

Returns a list of `Metric` objects containing modeling metrics.

## `get_modeling_metrics_data(?names, ?start_at, ?end_at)`

Fetch actual data values from modeling metrics within a time range. This method returns the actual data points (values) for the specified modeling metrics, similar to `get_metric_data()` but for the modeling database.

- `names`: Optional list of modeling metric names to filter by
- `start_at`: Optional start time for the query (uses class default if not specified)
- `end_at`: Optional end time for the query (uses class default if not specified)

Returns a list of `MetricDataPoint` objects containing the actual data values.

## `get_modeling_metrics_data_arrow(?names, ?start_at, ?end_at)`

Fetch actual data values from modeling metrics within a time range in Apache Arrow format. This method returns the actual data points (values) for the specified modeling metrics in Arrow IPC format, which is more efficient for large datasets.

- `names`: Optional list of modeling metric names to filter by
- `start_at`: Optional start time for the query (uses class default if not specified)
- `end_at`: Optional end time for the query (uses class default if not specified)

Returns Arrow IPC format data that can be read using `pyarrow.ipc.open_file()`.

## `get_eav_tables()`

Get all EAV (Entity-Attribute-Value) tables for the tenant. EAV tables store semi-structured data where each row represents an entity with flexible attributes.

Returns a list of `EAVTable` objects, each containing:
- `id`: The table's UUID
- `tenant_id`: The tenant ID
- `name`: Human-readable table name
- `created_at`: When the table was created
- `updated_at`: When the table was last updated

Example:
```python
tables = cvec.get_eav_tables()
for table in tables:
    print(f"{table.name} (id: {table.id})")
```

## `get_eav_columns(table_id)`

Get all columns for a specific EAV table.

- `table_id`: The UUID of the EAV table

Returns a list of `EAVColumn` objects, each containing:
- `eav_table_id`: The parent table's UUID
- `eav_column_id`: The column's ID (used for queries)
- `name`: Human-readable column name
- `type`: Data type (`"number"`, `"string"`, or `"boolean"`)
- `created_at`: When the column was created

Example:
```python
columns = cvec.get_eav_columns("00000000-0000-0000-0000-000000000000")
for column in columns:
    print(f"  {column.name} ({column.type}, id: {column.eav_column_id})")
```

## `select_from_eav(table_name, ?column_names, ?filters)`

Query pivoted data from EAV tables using human-readable names. This is the recommended method for most use cases as it allows you to work with table and column names instead of UUIDs.

- `table_name`: Name of the EAV table to query
- `column_names`: Optional list of column names to include. If `None`, all columns are returned.
- `filters`: Optional list of `EAVFilter` objects to filter results

Each `EAVFilter` must use `column_name` and can specify:
- `column_name`: The column name to filter on (required)
- `numeric_min`: Minimum numeric value (inclusive)
- `numeric_max`: Maximum numeric value (exclusive)
- `string_value`: Exact string value to match
- `boolean_value`: Boolean value to match

Returns a list of dictionaries (maximum 1000 rows), each representing a row with an `id` field and fields for each requested column.

Example:
```python
from cvec import CVec, EAVFilter

# Query with filters
filters = [
    EAVFilter(column_name="Weight", numeric_min=100, numeric_max=200),
    EAVFilter(column_name="Status", string_value="ACTIVE"),
]

rows = cvec.select_from_eav(
    table_name="Production Data",
    column_names=["Date", "Weight", "Status"],
    filters=filters,
)

for row in rows:
    print(row)
```

## `select_from_eav_id(table_id, ?column_ids, ?filters)`

Query pivoted data from EAV tables using table and column IDs directly. This is a lower-level method for cases where you already have the UUIDs and want to avoid name lookups.

- `table_id`: UUID of the EAV table to query
- `column_ids`: Optional list of column IDs to include. If `None`, all columns are returned.
- `filters`: Optional list of `EAVFilter` objects to filter results

Each `EAVFilter` must use `column_id` and can specify:
- `column_id`: The column ID to filter on (required)
- `numeric_min`: Minimum numeric value (inclusive)
- `numeric_max`: Maximum numeric value (exclusive)
- `string_value`: Exact string value to match
- `boolean_value`: Boolean value to match

Returns a list of dictionaries (up to 1000), each representing a row. Each row has a field for each column plus an `id` (the "row ID").

Example:
```python
from cvec import EAVFilter

filters = [
    EAVFilter(column_id="abcd", numeric_min=100, numeric_max=200),
    EAVFilter(column_id="efgh", string_value="ACTIVE"),
]

rows = cvec.select_from_eav_id(
    table_id="00000000-0000-0000-0000-000000000000",
    column_ids=["abcd", "efgh", "ijkl"],
    filters=filters,
)

for row in rows:
    print(f"ID: {row['id']}, Values: {row}")
```
