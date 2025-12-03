import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

from cvec.models.agent_post import AgentPost, AgentPostRecommendation, AgentPostTag
from cvec.models.eav_column import EAVColumn
from cvec.models.eav_filter import EAVFilter
from cvec.models.eav_table import EAVTable
from cvec.models.metric import Metric, MetricDataPoint
from cvec.models.span import Span
from cvec.utils.arrow_converter import (
    arrow_to_metric_data_points,
    metric_data_points_to_arrow,
)

logger = logging.getLogger(__name__)


class CVec:
    """
    CVec API Client
    """

    host: Optional[str]
    default_start_at: Optional[datetime]
    default_end_at: Optional[datetime]
    # Supabase authentication
    _access_token: Optional[str]
    _refresh_token: Optional[str]
    _publishable_key: Optional[str]
    _api_key: Optional[str]
    _tenant_id: int

    def __init__(
        self,
        host: Optional[str] = None,
        default_start_at: Optional[datetime] = None,
        default_end_at: Optional[datetime] = None,
        api_key: Optional[str] = None,
    ) -> None:
        self.host = host or os.environ.get("CVEC_HOST")
        self.default_start_at = default_start_at
        self.default_end_at = default_end_at

        # Supabase authentication
        self._access_token = None
        self._refresh_token = None
        self._publishable_key = None
        self._api_key = api_key or os.environ.get("CVEC_API_KEY")

        if not self.host:
            raise ValueError(
                "CVEC_HOST must be set either as an argument or environment variable"
            )

        # Add https:// scheme if not provided
        if not self.host.startswith("http://") and not self.host.startswith("https://"):
            self.host = f"https://{self.host}"
        if not self._api_key:
            raise ValueError(
                "CVEC_API_KEY must be set either as an argument or environment variable"
            )

        # Fetch config (publishable key and tenant ID)
        self._publishable_key = self._fetch_config()

        # Handle authentication
        email = self._construct_email_from_api_key()
        self._login_with_supabase(email, self._api_key)

    def _construct_email_from_api_key(self) -> str:
        """
        Construct email from API key using the pattern cva+<keyId>@cvector.app

        Returns:
            The constructed email address

        Raises:
            ValueError: If the API key doesn't match the expected pattern
        """
        if not self._api_key:
            raise ValueError("API key is not set")

        if not self._api_key.startswith("cva_"):
            raise ValueError("API key must start with 'cva_'")

        if len(self._api_key) != 40:  # cva_ + 36 62-base encoded symbols
            raise ValueError("API key invalid length. Expected cva_ + 36 symbols.")

        # Extract 4 characters after "cva_"
        key_id = self._api_key[4:8]
        return f"cva+{key_id}@cvector.app"

    def _get_headers(self) -> Dict[str, str]:
        """Helper method to get request headers."""
        if not self._access_token:
            raise ValueError("No access token available. Please login first.")

        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        """Helper method to make HTTP requests."""
        url = urljoin(self.host or "", endpoint)

        if params:
            filtered_params = {k: v for k, v in params.items() if v is not None}
            if filtered_params:
                url = f"{url}?{urlencode(filtered_params)}"

        request_headers = self._get_headers()
        if headers:
            request_headers.update(headers)

        request_body = None
        if json_data is not None:
            request_body = json.dumps(json_data).encode("utf-8")
        elif data is not None:
            request_body = data

        def make_http_request() -> Any:
            """Inner function to make the actual HTTP request."""
            req = Request(
                url, data=request_body, headers=request_headers, method=method
            )
            with urlopen(req) as response:
                response_data = response.read()
                content_type = response.headers.get("content-type", "")

                if content_type == "application/vnd.apache.arrow.stream":
                    return response_data
                return json.loads(response_data.decode("utf-8"))

        try:
            return make_http_request()
        except HTTPError as e:
            # Handle 401 Unauthorized with token refresh
            if e.code == 401 and self._access_token and self._refresh_token:
                try:
                    self._refresh_supabase_token()
                    # Update headers with new token
                    request_headers = self._get_headers()
                    if headers:
                        request_headers.update(headers)

                    # Retry the request
                    req = Request(
                        url, data=request_body, headers=request_headers, method=method
                    )
                    with urlopen(req) as response:
                        response_data = response.read()
                        content_type = response.headers.get("content-type", "")

                        if content_type == "application/vnd.apache.arrow.stream":
                            return response_data
                        return json.loads(response_data.decode("utf-8"))
                except (HTTPError, URLError, ValueError, KeyError) as refresh_error:
                    logger.warning(
                        "Token refresh failed, continuing with original request: %s",
                        refresh_error,
                        exc_info=True,
                    )
                    # If refresh fails, re-raise the original 401 error
                    raise e
            raise

    def get_spans(
        self,
        name: str,
        start_at: Optional[datetime] = None,
        end_at: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[Span]:
        """
        Return time spans for a metric. Spans are generated from value changes
        that occur after `start_at` (if specified) and before `end_at` (if specified).
        If `start_at` is `None` (e.g., not provided via argument or class default),
        the query is unbounded at the start. If `end_at` is `None`, it's unbounded at the end.

        Each span represents a period where the metric's value is constant.
        - `value`: The metric's value during the span.
        - `name`: The name of the metric.
        - `raw_start_at`: The timestamp of the value change that initiated this span's value.
          This will be >= `_start_at` if `_start_at` was specified.
        - `raw_end_at`: The timestamp marking the end of this span's constant value.
          For the newest span, the value is `None`. For other spans, it's the raw_start_at of the immediately newer data point, which is next span in the list.
        - `id`: Currently `None`.
        - `metadata`: Currently `None`.

        Returns a list of Span objects, sorted in descending chronological order (newest span first).
        Each Span object has attributes corresponding to the fields listed above.
        If no relevant value changes are found, an empty list is returned.
        The `limit` parameter restricts the number of spans returned.
        """
        _start_at = start_at or self.default_start_at
        _end_at = end_at or self.default_end_at

        params: Dict[str, Any] = {
            "start_at": _start_at.isoformat() if _start_at else None,
            "end_at": _end_at.isoformat() if _end_at else None,
            "limit": limit,
        }

        response_data = self._make_request(
            "GET", f"/api/metrics/spans/{name}", params=params
        )
        return [Span.model_validate(span_data) for span_data in response_data]

    def get_metric_data(
        self,
        names: Optional[List[str]] = None,
        start_at: Optional[datetime] = None,
        end_at: Optional[datetime] = None,
        use_arrow: bool = False,
    ) -> List[MetricDataPoint]:
        """
        Return all data-points within a given [start_at, end_at) interval,
        optionally selecting a given list of metric names.
        Returns a list of MetricDataPoint objects, one for each metric value transition.

        Args:
            names: Optional list of metric names to filter by
            start_at: Optional start time for the query
            end_at: Optional end time for the query
            use_arrow: If True, uses Arrow format for data transfer (more efficient for large datasets)
        """
        _start_at = start_at or self.default_start_at
        _end_at = end_at or self.default_end_at

        params: Dict[str, Any] = {
            "start_at": _start_at.isoformat() if _start_at else None,
            "end_at": _end_at.isoformat() if _end_at else None,
            "names": ",".join(names) if names else None,
        }

        endpoint = "/api/metrics/data/arrow" if use_arrow else "/api/metrics/data"
        response_data = self._make_request("GET", endpoint, params=params)

        if use_arrow:
            return arrow_to_metric_data_points(response_data)
        return [
            MetricDataPoint.model_validate(point_data) for point_data in response_data
        ]

    def get_metric_arrow(
        self,
        names: Optional[List[str]] = None,
        start_at: Optional[datetime] = None,
        end_at: Optional[datetime] = None,
    ) -> bytes:
        """
        Return all data-points within a given [start_at, end_at) interval,
        optionally selecting a given list of metric names.
        Returns Arrow IPC format data that can be read using pyarrow.ipc.open_file.

        Args:
            names: Optional list of metric names to filter by
            start_at: Optional start time for the query
            end_at: Optional end time for the query
        """
        _start_at = start_at or self.default_start_at
        _end_at = end_at or self.default_end_at

        params: Dict[str, Any] = {
            "start_at": _start_at.isoformat() if _start_at else None,
            "end_at": _end_at.isoformat() if _end_at else None,
            "names": ",".join(names) if names else None,
        }

        endpoint = "/api/metrics/data/arrow"
        result = self._make_request("GET", endpoint, params=params)
        assert isinstance(result, bytes)
        return result

    def get_metrics(
        self,
        start_at: Optional[datetime] = None,
        end_at: Optional[datetime] = None,
    ) -> List[Metric]:
        """
        Return a list of metrics that had at least one transition in the given [start_at, end_at) interval.
        All metrics are returned if no start_at and end_at are given.
        """
        _start_at = start_at or self.default_start_at
        _end_at = end_at or self.default_end_at

        params: Dict[str, Any] = {
            "start_at": _start_at.isoformat() if _start_at else None,
            "end_at": _end_at.isoformat() if _end_at else None,
        }

        response_data = self._make_request("GET", "/api/metrics/", params=params)
        return [Metric.model_validate(metric_data) for metric_data in response_data]

    def add_metric_data(
        self,
        data_points: List[MetricDataPoint],
        use_arrow: bool = False,
    ) -> None:
        """
        Add multiple metric data points to the database.

        Args:
            data_points: List of MetricDataPoint objects to add
            use_arrow: If True, uses Arrow format for data transfer (more efficient for large datasets)
        """
        endpoint = "/api/metrics/data/arrow" if use_arrow else "/api/metrics/data"

        if use_arrow:
            arrow_data = metric_data_points_to_arrow(data_points)
            self._make_request(
                "POST",
                endpoint,
                data=arrow_data,
                headers={"Content-Type": "application/vnd.apache.arrow.stream"},
            )
        else:
            data_dicts: List[Dict[str, Any]] = [
                point.model_dump(mode="json") for point in data_points
            ]
            self._make_request("POST", endpoint, json_data=data_dicts)  # type: ignore[arg-type]

    def get_modeling_metrics(
        self,
        start_at: Optional[datetime] = None,
        end_at: Optional[datetime] = None,
    ) -> List[Metric]:
        """
        Return a list of modeling metrics that had at least one transition in the given [start_at, end_at) interval.
        All metrics are returned if no start_at and end_at are given.

        Args:
            start_at: Optional start time for the query (uses class default if not specified)
            end_at: Optional end time for the query (uses class default if not specified)

        Returns:
            List of Metric objects containing modeling metrics
        """
        _start_at = start_at or self.default_start_at
        _end_at = end_at or self.default_end_at

        params: Dict[str, Any] = {
            "start_at": _start_at.isoformat() if _start_at else None,
            "end_at": _end_at.isoformat() if _end_at else None,
        }

        response_data = self._make_request(
            "GET", "/api/modeling/metrics", params=params
        )
        return [Metric.model_validate(metric_data) for metric_data in response_data]

    def get_modeling_metrics_data(
        self,
        names: Optional[List[str]] = None,
        start_at: Optional[datetime] = None,
        end_at: Optional[datetime] = None,
    ) -> List[MetricDataPoint]:
        """
        Return all data-points within a given [start_at, end_at) interval,
        optionally selecting a given list of modeling metric names.
        Returns a list of MetricDataPoint objects, one for each metric value transition.

        Args:
            names: Optional list of modeling metric names to filter by
            start_at: Optional start time for the query
            end_at: Optional end time for the query
        """
        _start_at = start_at or self.default_start_at
        _end_at = end_at or self.default_end_at

        params: Dict[str, Any] = {
            "start_at": _start_at.isoformat() if _start_at else None,
            "end_at": _end_at.isoformat() if _end_at else None,
            "names": ",".join(names) if names else None,
        }

        response_data = self._make_request(
            "GET", "/api/modeling/metrics/data", params=params
        )
        return [
            MetricDataPoint.model_validate(point_data) for point_data in response_data
        ]

    def get_modeling_metrics_data_arrow(
        self,
        names: Optional[List[str]] = None,
        start_at: Optional[datetime] = None,
        end_at: Optional[datetime] = None,
    ) -> bytes:
        """
        Return all data-points within a given [start_at, end_at) interval,
        optionally selecting a given list of modeling metric names.
        Returns Arrow IPC format data that can be read using pyarrow.ipc.open_file.

        Args:
            names: Optional list of modeling metric names to filter by
            start_at: Optional start time for the query
            end_at: Optional end time for the query
        """
        _start_at = start_at or self.default_start_at
        _end_at = end_at or self.default_end_at

        params: Dict[str, Any] = {
            "start_at": _start_at.isoformat() if _start_at else None,
            "end_at": _end_at.isoformat() if _end_at else None,
            "names": ",".join(names) if names else None,
        }

        endpoint = "/api/modeling/metrics/data/arrow"
        result = self._make_request("GET", endpoint, params=params)
        assert isinstance(result, bytes)
        return result

    def add_agent_post(
        self,
        title: str,
        author: str,
        image_id: Optional[str] = None,
        content: Optional[str] = None,
        recommendations: Optional[List[AgentPostRecommendation]] = None,
        tags: Optional[List[AgentPostTag]] = None,
    ) -> None:
        """
        Add an agent post.

        Note: If image_id is provided, the image must be uploaded to S3 beforehand.
        The image_id should be the UUID used as the filename (without .png extension)
        in the S3 bucket at the tenant's path.
        """

        post = AgentPost(
            title=title,
            author=author,
            image_id=image_id,
            content=content,
            recommendations=recommendations,
            tags=tags,
        )
        payload = post.model_dump(mode="json", exclude_none=True)

        self._make_request("POST", "/api/agent_posts/add", json_data=payload)

    def _login_with_supabase(self, email: str, password: str) -> None:
        """
        Login to Supabase and get access/refresh tokens.

        Args:
            email: User email
            password: User password
        """
        if not self._publishable_key:
            raise ValueError("Publishable key not available")

        supabase_url = f"{self.host}/supabase/auth/v1/token?grant_type=password"

        payload = {"email": email, "password": password}

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "apikey": self._publishable_key,
        }

        request_body = json.dumps(payload).encode("utf-8")
        req = Request(supabase_url, data=request_body, headers=headers, method="POST")

        with urlopen(req) as response:
            response_data = response.read()
            data = json.loads(response_data.decode("utf-8"))

        self._access_token = data["access_token"]
        self._refresh_token = data["refresh_token"]

    def _refresh_supabase_token(self) -> None:
        """
        Refresh the Supabase access token using the refresh token.
        """
        if not self._refresh_token:
            raise ValueError("No refresh token available")
        if not self._publishable_key:
            raise ValueError("Publishable key not available")

        supabase_url = f"{self.host}/supabase/auth/v1/token?grant_type=refresh_token"

        payload = {"refresh_token": self._refresh_token}

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "apikey": self._publishable_key,
        }

        request_body = json.dumps(payload).encode("utf-8")
        req = Request(supabase_url, data=request_body, headers=headers, method="POST")

        with urlopen(req) as response:
            response_data = response.read()
            data = json.loads(response_data.decode("utf-8"))

        self._access_token = data["access_token"]
        self._refresh_token = data["refresh_token"]

    def _fetch_config(self) -> str:
        """
        Fetch configuration from the host's config endpoint.

        Sets the tenant_id on the instance and returns the publishable key.

        Returns:
            The publishable key from the config response

        Raises:
            ValueError: If the config endpoint is not accessible or doesn't contain required fields
        """
        try:
            config_url = f"{self.host}/config"
            req = Request(config_url, method="GET")

            with urlopen(req) as response:
                response_data = response.read()
                config_data = json.loads(response_data.decode("utf-8"))

            publishable_key = config_data.get("supabasePublishableKey")
            tenant_id = config_data.get("tenantId")

            if not publishable_key:
                raise ValueError(f"Configuration fetched from {config_url} is invalid")
            if tenant_id is None:
                raise ValueError(f"tenantId not found in config from {config_url}")

            self._tenant_id = int(tenant_id)
            return str(publishable_key)

        except (HTTPError, URLError) as e:
            raise ValueError(f"Failed to fetch config from {self.host}/config: {e}")
        except (KeyError, ValueError) as e:
            raise ValueError(f"Invalid config response: {e}")

    def _call_rpc(
        self,
        function_name: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Call a Supabase RPC function.

        Args:
            function_name: The name of the RPC function to call
            params: Optional dictionary of parameters to pass to the function

        Returns:
            The response data from the RPC call
        """
        if not self._access_token:
            raise ValueError("No access token available. Please login first.")
        if not self._publishable_key:
            raise ValueError("Publishable key not available")

        url = f"{self.host}/supabase/rest/v1/rpc/{function_name}"

        headers = {
            "Accept": "application/json",
            "Apikey": self._publishable_key,
            "Authorization": f"Bearer {self._access_token}",
            "Content-Profile": "app_data",
            "Content-Type": "application/json",
        }

        request_body = json.dumps(params or {}).encode("utf-8")

        def make_rpc_request() -> Any:
            """Inner function to make the actual RPC request."""
            req = Request(url, data=request_body, headers=headers, method="POST")
            with urlopen(req) as response:
                response_data = response.read()
                return json.loads(response_data.decode("utf-8"))

        try:
            return make_rpc_request()
        except HTTPError as e:
            # Handle 401 Unauthorized with token refresh
            if e.code == 401 and self._access_token and self._refresh_token:
                try:
                    self._refresh_supabase_token()
                    # Update headers with new token
                    headers["Authorization"] = f"Bearer {self._access_token}"

                    # Retry the request
                    req = Request(
                        url, data=request_body, headers=headers, method="POST"
                    )
                    with urlopen(req) as response:
                        response_data = response.read()
                        return json.loads(response_data.decode("utf-8"))
                except (HTTPError, URLError, ValueError, KeyError) as refresh_error:
                    logger.warning(
                        "Token refresh failed, continuing with original request: %s",
                        refresh_error,
                        exc_info=True,
                    )
                    # If refresh fails, re-raise the original 401 error
                    raise e
            raise

    def _query_table(
        self,
        table_name: str,
        query_params: Optional[Dict[str, str]] = None,
    ) -> Any:
        """
        Query a Supabase table via PostgREST.

        Args:
            table_name: The name of the table to query
            query_params: Optional dict of PostgREST query parameters
                         (e.g., {"name": "eq.foo", "order": "name"})

        Returns:
            The response data from the query
        """
        if not self._access_token:
            raise ValueError("No access token available. Please login first.")
        if not self._publishable_key:
            raise ValueError("Publishable key not available")

        url = f"{self.host}/supabase/rest/v1/{table_name}"
        if query_params:
            encoded_params = urlencode(query_params)
            url = f"{url}?{encoded_params}"

        headers = {
            "Accept": "application/json",
            "Accept-Profile": "app_data",
            "Apikey": self._publishable_key,
            "Authorization": f"Bearer {self._access_token}",
        }

        def make_query_request() -> Any:
            """Inner function to make the actual query request."""
            req = Request(url, headers=headers, method="GET")
            with urlopen(req) as response:
                response_data = response.read()
                return json.loads(response_data.decode("utf-8"))

        try:
            return make_query_request()
        except HTTPError as e:
            # Handle 401 Unauthorized with token refresh
            if e.code == 401 and self._access_token and self._refresh_token:
                try:
                    self._refresh_supabase_token()
                    # Update headers with new token
                    headers["Authorization"] = f"Bearer {self._access_token}"

                    # Retry the request
                    req = Request(url, headers=headers, method="GET")
                    with urlopen(req) as response:
                        response_data = response.read()
                        return json.loads(response_data.decode("utf-8"))
                except (HTTPError, URLError, ValueError, KeyError) as refresh_error:
                    logger.warning(
                        "Token refresh failed, continuing with original request: %s",
                        refresh_error,
                        exc_info=True,
                    )
                    # If refresh fails, re-raise the original 401 error
                    raise e
            raise

    def get_eav_tables(self) -> List[EAVTable]:
        """
        Get all EAV tables for the tenant.

        Returns:
            List of EAVTable objects
        """
        response_data = self._query_table(
            "eav_tables",
            {"tenant_id": f"eq.{self._tenant_id}", "order": "name"},
        )
        return [EAVTable.model_validate(table) for table in response_data]

    def get_eav_columns(self, table_id: str) -> List[EAVColumn]:
        """
        Get all columns for an EAV table.

        Args:
            table_id: The UUID of the EAV table

        Returns:
            List of EAVColumn objects
        """
        response_data = self._query_table(
            "eav_columns",
            {"eav_table_id": f"eq.{table_id}", "order": "name"},
        )
        return [EAVColumn.model_validate(column) for column in response_data]

    def select_from_eav_id(
        self,
        table_id: str,
        column_ids: Optional[List[str]] = None,
        filters: Optional[List[EAVFilter]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query pivoted data from EAV tables using table and column IDs directly.

        This is the lower-level method that works with IDs. For a more user-friendly
        interface using names, see select_from_eav().

        Args:
            table_id: The UUID of the EAV table to query
            column_ids: Optional list of column IDs to include in the result.
                       If None, all columns are returned.
            filters: Optional list of EAVFilter objects to filter the results.
                    Each filter must use column_id (not column_name) and can specify:
                    - column_id: The EAV column ID to filter on (required)
                    - numeric_min: Minimum numeric value (inclusive)
                    - numeric_max: Maximum numeric value (exclusive)
                    - string_value: Exact string value to match
                    - boolean_value: Boolean value to match

        Returns:
            List of dictionaries, each representing a row with column values.
            Each row contains an 'id' field plus fields for each column_id
            with their corresponding values (number, string, or boolean).

        Example:
            >>> filters = [
            ...     EAVFilter(column_id="MTnaC", numeric_min=100, numeric_max=200),
            ...     EAVFilter(column_id="z09PL", string_value="ACTIVE"),
            ... ]
            >>> rows = client.select_from_eav_id(
            ...     table_id="550e8400-e29b-41d4-a716-446655440000",
            ...     column_ids=["MTnaC", "z09PL", "ZNAGI"],
            ...     filters=filters,
            ... )
        """
        # Convert EAVFilter objects to dictionaries
        filters_json: List[Dict[str, Any]] = []
        if filters:
            for f in filters:
                if f.column_id is None:
                    raise ValueError(
                        "Filters for select_from_eav_id must use column_id, "
                        "not column_name"
                    )
                filter_dict: Dict[str, Any] = {"column_id": f.column_id}
                if f.numeric_min is not None:
                    filter_dict["numeric_min"] = f.numeric_min
                if f.numeric_max is not None:
                    filter_dict["numeric_max"] = f.numeric_max
                if f.string_value is not None:
                    filter_dict["string_value"] = f.string_value
                if f.boolean_value is not None:
                    filter_dict["boolean_value"] = f.boolean_value
                filters_json.append(filter_dict)

        params: Dict[str, Any] = {
            "tenant_id": self._tenant_id,
            "table_id": table_id,
            "column_ids": column_ids,
            "filters": filters_json,
        }

        response_data = self._call_rpc("select_from_eav", params)
        return list(response_data) if response_data else []

    def select_from_eav(
        self,
        table_name: str,
        column_names: Optional[List[str]] = None,
        filters: Optional[List[EAVFilter]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query pivoted data from EAV tables using human-readable names.

        This method looks up table and column IDs from names, then calls
        select_from_eav_id(). For direct ID access, use select_from_eav_id().

        Args:
            table_name: The name of the EAV table to query
            column_names: Optional list of column names to include in the result.
                         If None, all columns are returned.
            filters: Optional list of EAVFilter objects to filter the results.
                    Each filter must use column_name (not column_id) and can specify:
                    - column_name: The EAV column name to filter on (required)
                    - numeric_min: Minimum numeric value (inclusive)
                    - numeric_max: Maximum numeric value (exclusive)
                    - string_value: Exact string value to match
                    - boolean_value: Boolean value to match

        Returns:
            List of dictionaries, each representing a row with column values.
            Each row contains an 'id' field plus fields for each column name
            with their corresponding values (number, string, or boolean).

        Example:
            >>> filters = [
            ...     EAVFilter(column_name="Weight", numeric_min=100, numeric_max=200),
            ...     EAVFilter(column_name="Status", string_value="ACTIVE"),
            ... ]
            >>> rows = client.select_from_eav(
            ...     table_name="BT/Scrap Entry",
            ...     column_names=["Weight", "Status", "Is Verified"],
            ...     filters=filters,
            ... )
        """
        # Look up the table ID from the table name
        tables_response = self._query_table(
            "eav_tables",
            {
                "tenant_id": f"eq.{self._tenant_id}",
                "name": f"eq.{table_name}",
                "limit": "1",
            },
        )
        if not tables_response:
            raise ValueError(f"Table '{table_name}' not found")
        table_id = tables_response[0]["id"]

        # Get all columns for the table to build name <-> id mappings
        columns = self.get_eav_columns(table_id)
        column_name_to_id = {col.name: col.eav_column_id for col in columns}
        column_id_to_name = {col.eav_column_id: col.name for col in columns}

        # Convert column names to column IDs
        column_ids: Optional[List[str]] = None
        if column_names:
            column_ids = []
            for name in column_names:
                if name not in column_name_to_id:
                    raise ValueError(
                        f"Column '{name}' not found in table '{table_name}'"
                    )
                column_ids.append(column_name_to_id[name])

        # Convert filters with column_name to filters with column_id
        id_filters: Optional[List[EAVFilter]] = None
        if filters:
            id_filters = []
            for f in filters:
                if f.column_name is None:
                    raise ValueError(
                        "Filters for select_from_eav must use column_name, "
                        "not column_id"
                    )
                if f.column_name not in column_name_to_id:
                    raise ValueError(
                        f"Filter column '{f.column_name}' not found in table "
                        f"'{table_name}'"
                    )
                id_filters.append(
                    EAVFilter(
                        column_id=column_name_to_id[f.column_name],
                        numeric_min=f.numeric_min,
                        numeric_max=f.numeric_max,
                        string_value=f.string_value,
                        boolean_value=f.boolean_value,
                    )
                )

        # Call the ID-based method
        response_data = self.select_from_eav_id(
            table_id=table_id,
            column_ids=column_ids,
            filters=id_filters,
        )

        # Convert column IDs back to names in the response
        result: List[Dict[str, Any]] = []
        for row in response_data:
            converted_row: Dict[str, Any] = {}
            for key, value in row.items():
                if key == "id":
                    converted_row[key] = value
                elif key in column_id_to_name:
                    converted_row[column_id_to_name[key]] = value
                else:
                    converted_row[key] = value
            result.append(converted_row)

        return result
