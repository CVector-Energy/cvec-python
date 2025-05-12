# CVec Client Library

# Data Model

This SDK integrates directly with CVector's database. Each tenant has a schema and a database user, both named for the tenant. The API Key is the password of the user. The database user is restricted to only have access to the tenant's schema. Here are the available database tables:

## tag_data

The tag_data table is a Timescale hypertable. Boolean tags are represented within this table using value 0 and 1.

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

The tag_data_str table is a Timescale hypertable.

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

Return all of the time spans where a tag has a constant value within the specified [`start_at`, `end_at`) interval. The function returns a list of time-ranges with the value for each time-range. Returns a list of spans. Each span has the following fields: {id, tag_name, value, start_at, end_at, raw_start_at, raw_end_at, metadata}. In a future version of the SDK, spans can be annotated, edited, and deleted.

## `get_metric_data(?tag_names, ?start_at, ?end_at)`

Return all data-points within a given [`start_at`, `end_at`) interval, optionally selecting a given list of tags. The return value is a Pandas DataFrame with three columns: tag_name, time, value. One row is returned for each tag value transition.

## `get_tags(?start_at, ?end_at)`

Return a list of tags that had at least one transition in the given [`start_at`, `end_at`) interval. All tags are returned if no `start_at` and `end_at` are given. Each tag has {id, name, birth_at, death_at}.
