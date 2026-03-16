"""Tests for HTTP caching (Cache-Control, ETag, If-None-Match)."""

import json
import time
from typing import Any
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from cvec import CVec
from cvec.http_cache import CacheEntry, parse_max_age


def mock_fetch_config_side_effect(instance: CVec) -> str:
    """Side effect for _fetch_config mock that sets tenant_id."""
    instance._tenant_id = 1
    return "test_publishable_key"


def _make_mock_response(
    body: bytes,
    content_type: str = "application/json",
    etag: str = "",
    cache_control: str = "",
) -> Mock:
    """Create a mock HTTP response."""
    mock_response = Mock()
    mock_response.read.return_value = body

    headers: dict[str, str] = {"content-type": content_type}
    if etag:
        headers["ETag"] = etag
    if cache_control:
        headers["Cache-Control"] = cache_control
    mock_response.headers = headers
    mock_response.__enter__ = Mock(return_value=mock_response)
    mock_response.__exit__ = Mock(return_value=False)
    return mock_response


def _create_client() -> CVec:
    """Create a CVec client with mocked auth."""
    client = CVec(
        host="https://test.example.com",
        api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
    )
    client._access_token = "test_token"
    return client


class TestParseMaxAge:
    """Tests for parse_max_age helper."""

    def test_basic(self) -> None:
        assert parse_max_age("max-age=300") == 300

    def test_with_other_directives(self) -> None:
        assert parse_max_age("public, max-age=600, must-revalidate") == 600

    def test_none_header(self) -> None:
        assert parse_max_age(None) is None

    def test_no_max_age_directive(self) -> None:
        assert parse_max_age("public, no-cache") is None

    def test_zero_max_age(self) -> None:
        assert parse_max_age("max-age=0") == 0


class TestCacheEntry:
    """Tests for CacheEntry dataclass."""

    def test_creation(self) -> None:
        entry = CacheEntry(
            data={"key": "value"},
            etag='"abc123"',
            max_age=300,
            stored_at=100.0,
        )
        assert entry.data == {"key": "value"}
        assert entry.etag == '"abc123"'
        assert entry.max_age == 300
        assert entry.stored_at == 100.0

    def test_no_etag(self) -> None:
        entry = CacheEntry(data=[], etag=None, max_age=60, stored_at=0.0)
        assert entry.etag is None


class TestHttpCache:
    """Integration tests for caching in _make_request."""

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(
        CVec,
        "_fetch_config",
        autospec=True,
        side_effect=mock_fetch_config_side_effect,
    )
    @patch("cvec.cvec.urlopen")
    def test_cache_stores_new_response(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """First GET stores the response in cache with correct max-age and etag."""
        client = _create_client()

        data = [{"id": 1, "name": "metric1"}]
        mock_response = _make_mock_response(
            json.dumps(data).encode("utf-8"),
            etag='"etag1"',
            cache_control="max-age=300",
        )
        mock_urlopen.return_value = mock_response

        client.get_metrics()

        assert len(client._cache) == 1
        url = next(iter(client._cache))
        entry = client._cache[url]
        assert entry.etag == '"etag1"'
        assert entry.max_age == 300

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(
        CVec,
        "_fetch_config",
        autospec=True,
        side_effect=mock_fetch_config_side_effect,
    )
    @patch("cvec.cvec.urlopen")
    def test_fresh_cache_hit_returns_cached_data(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """GET request with fresh cache entry returns data without HTTP call."""
        client = _create_client()

        data = [{"id": 1, "name": "metric1"}]
        mock_response = _make_mock_response(
            json.dumps(data).encode("utf-8"),
            etag='"etag1"',
            cache_control="max-age=300",
        )
        mock_urlopen.return_value = mock_response

        # First call: populates cache
        result1 = client.get_metrics()
        assert mock_urlopen.call_count == 1

        # Second call: should use cache, no HTTP call
        result2 = client.get_metrics()
        assert mock_urlopen.call_count == 1  # no additional call
        assert result1 == result2

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(
        CVec,
        "_fetch_config",
        autospec=True,
        side_effect=mock_fetch_config_side_effect,
    )
    @patch("cvec.cvec.urlopen")
    def test_stale_cache_with_etag_sends_if_none_match(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Stale entry triggers conditional request with If-None-Match."""
        client = _create_client()

        data = [{"id": 1, "name": "metric1"}]
        mock_response = _make_mock_response(
            json.dumps(data).encode("utf-8"),
            etag='"etag1"',
            cache_control="max-age=300",
        )
        mock_urlopen.return_value = mock_response

        # Populate cache
        client.get_metrics()

        # Make cache stale by backdating stored_at
        url = next(iter(client._cache))
        client._cache[url].stored_at = time.monotonic() - 400

        # New response for the stale request
        new_data = [{"id": 1, "name": "metric1_updated"}]
        mock_response2 = _make_mock_response(
            json.dumps(new_data).encode("utf-8"),
            etag='"etag2"',
            cache_control="max-age=300",
        )
        mock_urlopen.return_value = mock_response2

        client.get_metrics()

        # Check that If-None-Match was sent
        req = mock_urlopen.call_args[0][0]
        assert req.get_header("If-none-match") == '"etag1"'

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(
        CVec,
        "_fetch_config",
        autospec=True,
        side_effect=mock_fetch_config_side_effect,
    )
    @patch("cvec.cvec.urlopen")
    def test_304_response_returns_cached_data(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """304 response returns cached data and refreshes stored_at."""
        client = _create_client()

        data = [{"id": 1, "name": "metric1"}]
        mock_response = _make_mock_response(
            json.dumps(data).encode("utf-8"),
            etag='"etag1"',
            cache_control="max-age=300",
        )
        mock_urlopen.return_value = mock_response

        # Populate cache
        result1 = client.get_metrics()

        # Make cache stale
        url = next(iter(client._cache))
        client._cache[url].stored_at = time.monotonic() - 400
        old_stored_at = client._cache[url].stored_at

        # Return 304
        http_304 = HTTPError(
            url="https://test.example.com/api/metrics/",
            code=304,
            msg="Not Modified",
            hdrs={"Cache-Control": "max-age=300"},  # type: ignore[arg-type]
            fp=None,
        )
        mock_urlopen.side_effect = http_304

        result2 = client.get_metrics()

        # Should return same cached data
        assert result1 == result2
        # stored_at should be refreshed
        assert client._cache[url].stored_at > old_stored_at

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(
        CVec,
        "_fetch_config",
        autospec=True,
        side_effect=mock_fetch_config_side_effect,
    )
    @patch("cvec.cvec.urlopen")
    def test_post_request_not_cached(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """POST request bypasses cache entirely."""
        client = _create_client()

        mock_response = _make_mock_response(
            b"null",
            cache_control="max-age=300",
            etag='"etag1"',
        )
        mock_urlopen.return_value = mock_response

        client._make_request("POST", "/api/metrics/data", json_data={})

        assert len(client._cache) == 0

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(
        CVec,
        "_fetch_config",
        autospec=True,
        side_effect=mock_fetch_config_side_effect,
    )
    @patch("cvec.cvec.urlopen")
    def test_no_cache_control_header_not_cached(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Response without Cache-Control max-age is not cached."""
        client = _create_client()

        data = [{"id": 1, "name": "metric1"}]
        mock_response = _make_mock_response(
            json.dumps(data).encode("utf-8"),
        )
        mock_urlopen.return_value = mock_response

        client.get_metrics()

        assert len(client._cache) == 0

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(
        CVec,
        "_fetch_config",
        autospec=True,
        side_effect=mock_fetch_config_side_effect,
    )
    @patch("cvec.cvec.urlopen")
    def test_different_urls_cached_separately(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Two different URLs get separate cache entries."""
        client = _create_client()

        data1 = [{"id": 1, "name": "m1"}]
        data2 = [
            {
                "name": "m1",
                "time": "2024-01-01T00:00:00",
                "value_double": 1.0,
            }
        ]

        mock_response1 = _make_mock_response(
            json.dumps(data1).encode("utf-8"),
            etag='"etag1"',
            cache_control="max-age=300",
        )
        mock_response2 = _make_mock_response(
            json.dumps(data2).encode("utf-8"),
            etag='"etag2"',
            cache_control="max-age=300",
        )

        mock_urlopen.side_effect = [mock_response1, mock_response2]

        client.get_metrics()
        client.get_metric_data(names=["m1"])

        assert len(client._cache) == 2
