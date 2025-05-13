# CVec Client Library

# Data Model

This SDK integrates directly with CVector's database. Each tenant has a schema and a database user, both named for the tenant. The API Key is the password of the user. The database user is restricted to only have access to the tenant's schema. Here are the available database tables:

## tag_data

The tag_data table is a Timescale hypertable. Boolean tags are represented within this table using value 0 and 1. The table uses a "report by exception" approach; a row is inserted only when the value of a metric changes.

```sql
CREATE TABLE tag_data (
    tag_name_id INTEGER NOT NULL,
    tag_value_changed_at TIMESTAMP WITH TIME ZONE,
    tag_value DOUBLE PRECISION
)

SELECT create_hypertable(
    '${schema_name}.tag_data',
    'tag_value_changed_at',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);
```

## tag_data_str

The tag_data_str table is a Timescale hypertable, similar to tag_data for string-valued tags.

```sql
CREATE TABLE tag_data_str (
    tag_name_id INTEGER NOT NULL,
    tag_value_changed_at timestamptz NOT NULL,
    tag_value text
);

SELECT create_hypertable(
    '${schema_name}.tag_data_str',
    'tag_value_changed_at',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);
```

## tag_names

```sql
CREATE TABLE tag_names (
    id SERIAL PRIMARY KEY,
    normalized_name VARCHAR NOT NULL,
    birth_at TIMESTAMPTZ NULL,
    death_at TIMESTAMPTZ NULL
);
```

## metrics

```sql
CREATE VIEW metrics AS
 SELECT td.tag_value AS value,
    td.tag_value_changed_at AS "time",
    tn.normalized_name AS metric
   FROM tag_data td
     JOIN tag_names tn ON td.tag_name_id = tn.id;
```

# CVec Class

The SDK provides an API client class named `CVec` with the following functions.

## `__init__(?host, ?tenant, ?api_key, ?default_start_at, ?default_end_at)`

Setup the SDK with the given host and API Key. The host and API key are loaded from environment variables CVEC_HOST, CVEC_TENANT, CVEC_API_KEY, if they are not given as arguments to the constructor. The `default_start_at` and `default_end_at` constrain most API calls, and can be overridden by the `start_at` and `end_at` arguments to each API function.

## `get_spans(tag_name, ?start_at, ?end_at, ?limit)`

Return time spans for a tag. Spans are generated from value changes that occur after `start_at` (if specified) and before `end_at` (if specified).
If `start_at` is `None` (e.g., not provided as an argument and no class default `default_start_at` is set), the query for value changes is unbounded at the start. Similarly, if `end_at` is `None`, the query is unbounded at the end.

Each span in the returned list represents a period where the tag's value is constant:
- `value`: The tag's value during the span.
- `tag_name`: The name of the tag.
- `start_at`: The timestamp of the value change that initiated this span's value. This will be greater than or equal to the query's `start_at` if one was specified.
- `raw_start_at`: Same as `start_at`.
- `end_at`: The timestamp of the next value change for this tag, or the query's `end_at` parameter, whichever is earlier. If the query's `end_at` is not specified (i.e., `None`) and there is no subsequent value change found by the query, this field will be `None`, indicating the span continues indefinitely.
- `raw_end_at`: The timestamp of the next value change for this tag found by the query. This will be `None` if no subsequent change is found within the query window (e.g., before the query's `end_at` or indefinitely if `end_at` is `None`).

Returns a list of spans. Each span has the following fields: {id, tag_name, value, start_at, end_at, raw_start_at, raw_end_at, metadata}. In a future version of the SDK, spans can be annotated, edited, and deleted.
If no relevant value changes are found, an empty list is returned.

## `get_metric_data(?tag_names, ?start_at, ?end_at)`

Return all data-points within a given [`start_at`, `end_at`) interval, optionally selecting a given list of tags. The return value is a Pandas DataFrame with three columns: tag_name, time, value. One row is returned for each tag value transition.

## `get_tags(?start_at, ?end_at)`

Return a list of tags that had at least one transition in the given [`start_at`, `end_at`) interval. All tags are returned if no `start_at` and `end_at` are given. Each tag has {id, name, birth_at, death_at}.
