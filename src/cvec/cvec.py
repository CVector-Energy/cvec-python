import os
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor


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
        try:
            # psycopg2 defaults to using the username as dbname if not specified.
            # Here, self.tenant is used for both user and (implicitly) dbname.
            conn = psycopg2.connect(
                user=self.tenant,
                password=self.api_key,
                host=self.host,
                # dbname=self.tenant # Implicitly self.tenant if not provided
            )
            return conn
        except psycopg2.Error as e:
            # Consider logging this error or raising a custom exception
            print(f"Database connection error: {e}")
            raise

    def get_spans(self, tag_name, start_at=None, end_at=None, limit=None):
        """
        Return time spans for a tag, where each span's value is initiated by a
        value change occurring within the specified [start_at, end_at) interval.

        This function identifies all `tag_value_changed_at` timestamps for the
        given `tag_name` that are greater than or equal to `start_at` and less
        than `end_at`. For each such timestamp (let's call it `event_time`),
        a span is generated:
        - `value`: The tag's value that was set at `event_time`.
        - `tag_name`: The name of the tag.
        - `start_at`: Equal to `event_time`.
        - `raw_start_at`: Equal to `event_time`.
        - `end_at`: The timestamp of the next value change for this tag, or the
          query's `end_at` parameter, whichever is earlier. If there is no
          subsequent value change up to the query's `end_at`, this will be the
          query's `end_at`.
        - `raw_end_at`: The timestamp of the next value change for this tag, if
          another change is found by the query (i.e., before `end_at`).
          Otherwise, `None`.
        - `id`: Currently `None`.
        - `metadata`: Currently `None`.

        Returns a list of dictionaries, where each dictionary represents a span.
        If no value changes occur for the tag within the specified interval, an
        empty list is returned.
        The `limit` parameter restricts the number of spans returned.
        """
        _start_at = start_at or self.default_start_at
        _end_at = end_at or self.default_end_at

        if not _start_at or not _end_at:
            raise ValueError(
                "Effective start_at and end_at must be provided either as arguments or class defaults."
            )

        conn = None
        try:
            conn = self._get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 1. Get tag_name_id
                query_tag_id = (
                    f"SELECT id FROM {self.tenant}.tag_names WHERE normalized_name = %s"
                )
                cur.execute(query_tag_id, (tag_name,))
                tag_row = cur.fetchone()
                if not tag_row:
                    return []  # Tag not found
                tag_name_id = tag_row["id"]

                # 2. Fetch data points from tag_data (numeric) and tag_data_str (text)
                all_points = []

                # Query for numeric data
                query_numeric = f"""
                SELECT tag_value_changed_at, tag_value
                 FROM {self.tenant}.tag_data
                 WHERE tag_name_id = %s AND tag_value_changed_at >= %s AND tag_value_changed_at < %s
                 ORDER BY tag_value_changed_at ASC
                """
                cur.execute(
                    query_numeric,
                    (tag_name_id, _start_at, _end_at),
                )
                for row in cur.fetchall():
                    all_points.append(
                        {
                            "time": row["tag_value_changed_at"],
                            "value": float(row["tag_value"]),
                        }
                    )

                # Query for string data
                query_string = f"""
                SELECT tag_value_changed_at, tag_value
                 FROM {self.tenant}.tag_data_str
                 WHERE tag_name_id = %s AND tag_value_changed_at >= %s AND tag_value_changed_at < %s
                 ORDER BY tag_value_changed_at ASC
                """
                cur.execute(
                    query_string,
                    (tag_name_id, _start_at, _end_at),
                )
                for row in cur.fetchall():
                    all_points.append(
                        {
                            "time": row["tag_value_changed_at"],
                            "value": str(row["tag_value"]),
                        }
                    )

                # Sort all collected points by time
                all_points.sort(key=lambda p: p["time"])

                if not all_points:
                    return []

                spans = []
                # 3. Construct spans
                for i, point in enumerate(all_points):
                    current_raw_start_at = point["time"]
                    current_value = point["value"]

                    span_actual_start = current_raw_start_at # Query now ensures current_raw_start_at >= _start_at

                    next_raw_event_at = None
                    if i + 1 < len(all_points):
                        next_raw_event_at = all_points[i + 1]["time"]
                        span_actual_end = min(next_raw_event_at, _end_at)
                    else:
                        span_actual_end = _end_at

                    if (
                        span_actual_start < span_actual_end
                    ):  # Ensure span has positive duration
                        spans.append(
                            {
                                "id": None,
                                "tag_name": tag_name,
                                "value": current_value,
                                "start_at": span_actual_start,
                                "end_at": span_actual_end,
                                "raw_start_at": current_raw_start_at,
                                "raw_end_at": next_raw_event_at,
                                "metadata": None,
                            }
                        )

                if (
                    limit is not None and limit >= 0
                ):  # allow limit=0 to return empty list
                    spans = spans[:limit]

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
