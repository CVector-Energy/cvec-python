import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

from cvec.models.eav_filter import EAVFilter
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

        # Fetch publishable key from host config
        self._publishable_key = self._fetch_publishable_key()

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

    def _fetch_publishable_key(self) -> str:
        """
        Fetch the publishable key from the host's config endpoint.

        Returns:
            The publishable key from the config response

        Raises:
            ValueError: If the config endpoint is not accessible or doesn't contain the key
        """
        try:
            config_url = f"{self.host}/config"
            req = Request(config_url, method="GET")

            with urlopen(req) as response:
                response_data = response.read()
                config_data = json.loads(response_data.decode("utf-8"))

            publishable_key = config_data.get("supabasePublishableKey")

            if not publishable_key:
                raise ValueError(f"Configuration fetched from {config_url} is invalid")

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

    def select_from_eav(
        self,
        tenant_id: int,
        table_id: str,
        column_ids: Optional[List[str]] = None,
        filters: Optional[List[EAVFilter]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query pivoted data from EAV (Entity-Attribute-Value) tables.

        This method calls the app_data.select_from_eav Supabase function to retrieve
        data from EAV tables with optional column selection and filtering.

        Args:
            tenant_id: The tenant ID to query data for
            table_id: The UUID of the EAV table to query
            column_ids: Optional list of column IDs to include in the result.
                       If None, all columns are returned.
            filters: Optional list of EAVFilter objects to filter the results.
                    Each filter can specify:
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
            ...     EAVFilter(column_id="timestamp", numeric_min=100, numeric_max=200),
            ...     EAVFilter(column_id="status", string_value="ACTIVE"),
            ... ]
            >>> rows = client.select_from_eav(
            ...     tenant_id=123,
            ...     table_id="73d3845f-5c0e-4d20-8df7-6f8880c24eb4",
            ...     column_ids=["timestamp", "status", "voltage"],
            ...     filters=filters,
            ... )
        """
        # Convert EAVFilter objects to dictionaries, excluding None values
        filters_json: List[Dict[str, Any]] = []
        if filters:
            for f in filters:
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
            "tenant_id": tenant_id,
            "table_id": table_id,
            "column_ids": column_ids,
            "filters": filters_json,
        }

        response_data = self._call_rpc("select_from_eav", params)
        return list(response_data) if response_data else []
