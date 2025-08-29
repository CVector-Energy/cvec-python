import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests  # type: ignore[import-untyped]

from cvec.models.metric import Metric, MetricDataPoint
from cvec.models.span import Span
from cvec.utils.arrow_converter import (
    arrow_to_metric_data_points,
    metric_data_points_to_arrow,
)
from cvec.models.modeling import (
    FetchModelingReadingsRequest,
    ModelingReadingsDataResponse,
    LatestReadingsRequest,
    LatestReadingsResponse,
)


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
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        """Helper method to make HTTP requests."""
        url = urljoin(self.host or "", endpoint)
        request_headers = self._get_headers()
        if headers:
            request_headers.update(headers)

        response = requests.request(
            method=method,
            url=url,
            headers=request_headers,
            params=params,
            json=json,
            data=data,
        )

        # If we get a 401 and we have Supabase tokens, try to refresh and retry
        if response.status_code == 401 and self._access_token and self._refresh_token:
            try:
                self._refresh_supabase_token()
                # Update headers with new token
                request_headers = self._get_headers()
                if headers:
                    request_headers.update(headers)

                # Retry the request
                response = requests.request(
                    method=method,
                    url=url,
                    headers=request_headers,
                    params=params,
                    json=json,
                    data=data,
                )
            except Exception:
                print("Token refresh failed")
                # If refresh fails, continue with the original error
                pass

        response.raise_for_status()

        if (
            response.headers.get("content-type")
            == "application/vnd.apache.arrow.stream"
        ):
            return response.content
        return response.json()

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
            self._make_request("POST", endpoint, json=data_dicts)  # type: ignore[arg-type]

    def get_modeling_readings(
        self,
        tag_ids: List[int],
        start_at: Optional[datetime] = None,
        end_at: Optional[datetime] = None,
        desired_points: int = 10000,
    ) -> ModelingReadingsDataResponse:
        """
        Fetch modeling readings for chosen tags.
        Perform aggregation to desired count and downsampling via TimescaleDB mechanisms.

        Args:
            tag_ids: Tag ID list to fetch data for
            start_at: Optional start time for the query (uses class default if not specified)
            end_at: Optional end time for the query (uses class default if not specified)
            desired_points: Number of data points to return (default: 10000)

        Returns:
            ModelingReadingsDataResponse containing readings for each tag
        """
        _start_at = start_at or self.default_start_at
        _end_at = end_at or self.default_end_at

        request_data = FetchModelingReadingsRequest(
            tag_ids=tag_ids,
            start_date=_start_at,
            end_date=_end_at,
            desired_points=desired_points,
        )

        response_data = self._make_request(
            "POST", "/api/modeling/fetch_modeling_data", json=request_data.model_dump(mode="json")
        )
        print(response_data)
        return ModelingReadingsDataResponse.model_validate(response_data)

    def get_modeling_latest_readings(self, tag_ids: List[int]) -> LatestReadingsResponse:
        """
        Fetch single latest reading for each of specified tag IDs from the modeling database.

        Args:
            tag_ids: List of tag IDs to fetch data for

        Returns:
            LatestReadingsResponse containing latest readings
        """
        request_data = LatestReadingsRequest(tag_ids=tag_ids)

        response_data = self._make_request(
            "POST", "/api/modeling/fetch_latest_readings", json=request_data.model_dump(mode="json")
        )
        return LatestReadingsResponse.model_validate(response_data)

    def _login_with_supabase(self, email: str, password: str) -> None:
        """
        Login to Supabase and get access/refresh tokens.

        Args:
            email: User email
            password: User password
        """
        supabase_url = f"{self.host}/supabase/auth/v1/token?grant_type=password"

        payload = {"email": email, "password": password}

        headers = {
            "Content-Type": "application/json",
            "apikey": self._publishable_key,
        }

        response = requests.post(supabase_url, json=payload, headers=headers)
        response.raise_for_status()

        data = response.json()

        self._access_token = data["access_token"]
        self._refresh_token = data["refresh_token"]

    def _refresh_supabase_token(self) -> None:
        """
        Refresh the Supabase access token using the refresh token.
        """
        if not self._refresh_token:
            raise ValueError("No refresh token available")

        supabase_url = f"{self.host}/supabase/auth/v1/token?grant_type=refresh_token"

        payload = {"refresh_token": self._refresh_token}

        headers = {
            "Content-Type": "application/json",
            "apikey": self._publishable_key,
        }

        response = requests.post(supabase_url, json=payload, headers=headers)
        response.raise_for_status()

        data = response.json()
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
            response = requests.get(config_url)
            response.raise_for_status()

            config_data = response.json()
            publishable_key = config_data.get("supabasePublishableKey")

            if not publishable_key:
                raise ValueError(f"Configuration fetched from {config_url} is invalid")

            return str(publishable_key)

        except requests.RequestException as e:
            raise ValueError(f"Failed to fetch config from {self.host}/config: {e}")
        except (KeyError, ValueError) as e:
            raise ValueError(f"Invalid config response: {e}")
