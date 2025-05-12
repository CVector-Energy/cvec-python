# CVec Client Library

# Data Model

This SDK integrates directly with CVector's database. Each tenant has a schema and a database user, both named for the tenant. The API Key is the password of the user. The database user is restricted to only have access to the tenant's schema. Here are the available database tables:

```sql
CREATE TABLE tag_data (
    tag_name_id INTEGER NOT NULL,
    tag_value_changed_at TIMESTAMP WITH TIME ZONE,
    tag_value DOUBLE PRECISION
)
```

```sql
CREATE TABLE tag_data_str (
    tag_name_id INTEGER NOT NULL,
    tag_value_changed_at timestamptz NOT NULL,
    tag_value text
);
```

```sql
CREATE TABLE tag_names (
    id SERIAL PRIMARY KEY,
    normalized_name VARCHAR NOT NULL,
    birth_at TIMESTAMPTZ NULL,
    death_at TIMESTAMPTZ NULL
);
```

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

## `__init__(?host, ?tenant, ?api_key, ?default_time_range)`

Setup the SDK with the given host and API Key. The host and API key are loaded from environment variables CVEC_HOST, CVEC_TENANT, CVEC_API_KEY, if they are not given as arguments to the constructor. The default_time_range constrains most API keys, and can be overridden by the time_range argument to each API function.

## `get_spans(tag_name, ?time_range, ?limit)`

Return all of the time spans where a tag has a constant value. The function returns a list of time-ranges with the value for each time-range. Returns a list of spans. Each span has the following fields: {id, tag_name, value, begin_at, end_at, raw_begin_at, raw_end_at, metadata}. In a future version of the SDK, spans can be annotated, edited, and deleted.

## `get_metric_data(?tag_names, ?time_range)`

Return all data-points within a given time-range, optionally selecting a given list of tags. The return value is a Pandas DataFrame with three columns: tag_name, time, value. One row is returned for each tag value transition.

## `get_tags(?time_range)`

Return a list of tags that had at least one transition in the given time range. All tags are returned if no time_range is given. Each tag has {id, name, birth_at, death_at}.
