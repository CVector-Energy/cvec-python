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

Construct the CVec client. The host, tenant, and api_key can be given through parameters to the constructor or from the environment variables CVEC_HOST, CVEC_TENANT, and CVEC_API_KEY:

```
client = cvec.CVec()
```

### Spans

A span is a period of interest, such as an experiment, a baseline recording session, or an alarm. The initial state of a Span is implicitly defined by a period where a given metric has a constant value.

The newest span for a metric does not have an end time, since it has not ended yet (or has not ended by the finish of the queried period).

To get the spans on `my_tag_name` since 2025-05-14 10am, run:

```
spans = client.get_spans("my_tag_name", start_at=datetime(2025, 5, 14, 10, 0, 0))
```

### Metrics

A metric is a named set of time-series data points pertaining to a particular resource (for example, the value reported by a sensor). A metric has a lifecycle of being activated or added to the system (birth_at) and later removed from the system (death_at). Metrics can have numeric or string values. Boolean values are mapped to 0 and 1.

### Metric Data

# CVec Class

The SDK provides an API client class named `CVec` with the following functions.

## `__init__(?host, ?tenant, ?api_key, ?default_start_at, ?default_end_at)`

Setup the SDK with the given host and API Key. The host and API key are loaded from environment variables CVEC_HOST, CVEC_TENANT, CVEC_API_KEY, if they are not given as arguments to the constructor. The `default_start_at` and `default_end_at` constrain most API calls, and can be overridden by the `start_at` and `end_at` arguments to each API function.

## `get_spans(name, ?start_at, ?end_at, ?limit)`

Return time spans for a metric. Spans are generated from value changes that occur after `start_at` (if specified) and before `end_at` (if specified).
If `start_at` is `None` (e.g., not provided as an argument and no class default `default_start_at` is set), the query for value changes is unbounded at the start. Similarly, if `end_at` is `None`, the query is unbounded at the end.

Each `Span` object in the returned list represents a period where the metric's value is constant and has the following attributes:
- `value`: The metric's value during the span.
- `name`: The name of the metric.
- `raw_start_at`: The timestamp of the value change that initiated this span's value. This will be greater than or equal to the query's `start_at` if one was specified.
- `raw_end_at`: The timestamp marking the end of this span's constant value. For the newest span, the value is `None`. For other spans, it's the `raw_start_at` of the chronologically newer preceding span in the list.
- `id`: Currently `None`. In a future version of the SDK, this will be the span's unique identifier.
- `metadata`: Currently `None`. In a future version, this can be used to store annotations or other metadata related to the span.

Returns a list of `Span` objects, sorted in descending chronological order (newest span first).
If no relevant value changes are found, an empty list is returned.

## `get_metric_data(?names, ?start_at, ?end_at)`

Return all data-points within a given [`start_at`, `end_at`) interval, optionally selecting a given list of metric names. The return value is a Pandas DataFrame with four columns: name, time, value_double, value_string. One row is returned for each tag value transition.

## `get_metrics(?start_at, ?end_at)`

Return a list of metrics that had at least one transition in the given [`start_at`, `end_at`) interval. All metrics are returned if no `start_at` and `end_at` are given. Each metric has {id, name, birth_at, death_at}.
