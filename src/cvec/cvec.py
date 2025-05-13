import os

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from .span import Span


class CVec:
    """
    CVec API Client
    """

    def __init__(
        self,
        host=None,
        tenant=None,
        api_key=None,
        default_start_at=None,
        default_end_at=None,
    ):
        """
        Setup the SDK with the given host and API Key.
        The host and API key are loaded from environment variables CVEC_HOST,
        CVEC_TENANT, CVEC_API_KEY, if they are not given as arguments to the constructor.
        The default_start_at and default_end_at constrain most API keys, and can be overridden
        by the start_at and end_at arguments to each API function.
        """
        self.host = host or os.environ.get("CVEC_HOST")
        self.tenant = tenant or os.environ.get("CVEC_TENANT")
        self.api_key = api_key or os.environ.get("CVEC_API_KEY")
        self.default_start_at = default_start_at
        self.default_end_at = default_end_at

        if not self.host:
            raise ValueError(
                "CVEC_HOST must be set either as an argument or environment variable"
            )
        if not self.tenant:
            raise ValueError(
                "CVEC_TENANT must be set either as an argument or environment variable"
            )
        if not self.api_key:
            raise ValueError(
                "CVEC_API_KEY must be set either as an argument or environment variable"
            )

    def _get_db_connection(self):
        """Helper method to establish a database connection."""
        return psycopg.connect(
            user=self.tenant,
            password=self.api_key,
            host=self.host,
            dbname=self.tenant,
            row_factory=dict_row,
        )

    def get_spans(self, tag_name, start_at=None, end_at=None, limit=None):
        """
        Return time spans for a tag. Spans are generated from value changes
        that occur after `start_at` (if specified) and before `end_at` (if specified).
        If `start_at` is `None` (e.g., not provided via argument or class default),
        the query is unbounded at the start. If `end_at` is `None`, it's unbounded at the end.

        Each span represents a period where the tag's value is constant.
        - `value`: The tag's value during the span.
        - `tag_name`: The name of the tag.
        - `start_at`: The timestamp of the value change that initiated this span's value.
          This will be >= `_start_at` if `_start_at` was specified.
        - `raw_start_at`: Same as `start_at`.
        - `end_at`: The timestamp of the next value change for this tag, or the
          query's `_end_at` parameter, whichever is earlier. If the query's `_end_at`
          is `None` and there is no subsequent value change, this field will be `None`,
          indicating the span continues indefinitely.
        - `raw_end_at`: The timestamp of the next value change for this tag found by
          the query. `None` if no subsequent change is found within the query window.
        - `id`: Currently `None`.
        - `metadata`: Currently `None`.

        Returns a list of Span objects. Each Span object has attributes corresponding
        to the fields listed above.
        If no relevant value changes are found, an empty list is returned.
        The `limit` parameter restricts the number of spans returned.
        """
        _start_at = start_at or self.default_start_at
        _end_at = end_at or self.default_end_at

        conn = None
        try:
            conn = self._get_db_connection()
            with conn.cursor() as cur:
                query_params = {
                    "tag_name": tag_name,
                    "start_at": _start_at,
                    "end_at": _end_at,
                    # Fetch limit + 1 points to correctly determine the end_at for the limit-th
                    # span. If limit is None or negative, sql_limit will be None (LIMIT NULL in
                    # PostgreSQL, meaning no limit).
                    "limit": limit + 1 if limit is not None and limit >= 0 else None,
                }

                combined_query = f"""
                SELECT
                    td.tag_value_changed_at,
                    td.tag_value AS value_double,
                    NULL::text AS value_string
                FROM tag_data td
                JOIN tag_names tn ON td.tag_name_id = tn.id
                WHERE tn.normalized_name = %(tag_name)s AND (td.tag_value_changed_at >= %(start_at)s OR %(start_at)s IS NULL) AND (td.tag_value_changed_at < %(end_at)s OR %(end_at)s IS NULL)
                UNION ALL
                SELECT
                    tds.tag_value_changed_at,
                    NULL::double precision AS value_double,
                    tds.tag_value AS value_string
                FROM tag_data_str tds
                JOIN tag_names tn ON tds.tag_name_id = tn.id
                WHERE tn.normalized_name = %(tag_name)s AND (tds.tag_value_changed_at >= %(start_at)s OR %(start_at)s IS NULL) AND (tds.tag_value_changed_at < %(end_at)s OR %(end_at)s IS NULL)
                ORDER BY tag_value_changed_at ASC
                LIMIT %(limit)s
                """
                cur.execute(combined_query, query_params)
                all_points = [
                    {
                        "time": row["tag_value_changed_at"],
                        "value": (
                            row["value_double"]
                            if row["value_double"] is not None
                            else row["value_string"]
                        ),
                    }
                    for row in cur.fetchall()
                ]

                spans = [
                    Span(
                        id=None,
                        tag_name=tag_name,
                        value=point["value"],
                        start_at=point["time"],  # TODO: lookup span override start_at
                        end_at=(
                            all_points[i + 1]["time"]
                            if i + 1 < len(all_points)
                            else None
                        ),  # TODO: lookup span override end_at
                        raw_start_at=point["time"],
                        raw_end_at=(
                            all_points[i + 1]["time"]
                            if i + 1 < len(all_points)
                            else None
                        ),
                        metadata=None,
                    )
                    for i, point in enumerate(all_points)
                ]
                return spans
        finally:
            if conn:
                conn.close()

    def get_metric_data(self, tag_names=None, start_at=None, end_at=None):
        """
        Return all data-points within a given [start_at, end_at) interval,
        optionally selecting a given list of tags.
        The return value is a Pandas DataFrame with three columns: tag_name, time, value.
        One row is returned for each tag value transition.
        """
        # Implementation to be added
        return pd.DataFrame(columns=["tag_name", "time", "value"])

    def get_tags(self, start_at=None, end_at=None):
        """
        Return a list of tags that had at least one transition in the given [start_at, end_at) interval.
        All tags are returned if no start_at and end_at are given.
        Each tag has {id, name, birth_at, death_at}.
        """
        # Implementation to be added
        return []
