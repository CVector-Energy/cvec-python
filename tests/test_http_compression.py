"""Tests for HTTP compression support (gzip/deflate/brotli)."""

import gzip
import json
import zlib
from typing import Any
from unittest.mock import Mock, patch

import brotli  # type: ignore[import-untyped]

from cvec import CVec


def mock_fetch_config_side_effect(instance: CVec) -> str:
    """Side effect for _fetch_config mock that sets tenant_id."""
    instance._tenant_id = 1
    return "test_publishable_key"


def _make_mock_response(
    body: bytes,
    content_type: str = "application/json",
    content_encoding: str = "",
    extra_headers: dict[str, str] | None = None,
) -> Mock:
    """Create a mock HTTP response with the given body and headers."""
    mock_response = Mock()
    mock_response.read.return_value = body

    headers: dict[str, str] = {"content-type": content_type}
    if content_encoding:
        headers["Content-Encoding"] = content_encoding
    if extra_headers:
        headers.update(extra_headers)
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


class TestHttpCompression:
    """Test cases for HTTP compression support."""

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(
        CVec,
        "_fetch_config",
        autospec=True,
        side_effect=mock_fetch_config_side_effect,
    )
    @patch("cvec.cvec.urlopen")
    def test_accept_encoding_header_sent(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Verify Accept-Encoding includes br, gzip, deflate."""
        client = _create_client()

        mock_response = _make_mock_response(b"[]")
        mock_urlopen.return_value = mock_response

        client.get_metrics()

        req = mock_urlopen.call_args[0][0]
        assert req.get_header("Accept-encoding") == "br, gzip, deflate"

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(
        CVec,
        "_fetch_config",
        autospec=True,
        side_effect=mock_fetch_config_side_effect,
    )
    @patch("cvec.cvec.urlopen")
    def test_gzip_response_decompressed(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Mock a gzip-compressed JSON response, verify it is decompressed."""
        client = _create_client()

        data = [{"id": 1, "name": "metric1"}]
        compressed = gzip.compress(json.dumps(data).encode("utf-8"))
        mock_response = _make_mock_response(compressed, content_encoding="gzip")
        mock_urlopen.return_value = mock_response

        result = client.get_metrics()
        assert len(result) == 1
        assert result[0].name == "metric1"

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(
        CVec,
        "_fetch_config",
        autospec=True,
        side_effect=mock_fetch_config_side_effect,
    )
    @patch("cvec.cvec.urlopen")
    def test_deflate_response_decompressed(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Mock a deflate-compressed JSON response, verify decompression."""
        client = _create_client()

        data = [{"id": 2, "name": "metric2"}]
        raw = json.dumps(data).encode("utf-8")
        compressed = zlib.compress(raw)
        mock_response = _make_mock_response(compressed, content_encoding="deflate")
        mock_urlopen.return_value = mock_response

        result = client.get_metrics()
        assert len(result) == 1
        assert result[0].name == "metric2"

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(
        CVec,
        "_fetch_config",
        autospec=True,
        side_effect=mock_fetch_config_side_effect,
    )
    @patch("cvec.cvec.urlopen")
    def test_uncompressed_response_unchanged(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Verify responses without Content-Encoding still work."""
        client = _create_client()

        data = [{"id": 3, "name": "metric3"}]
        mock_response = _make_mock_response(json.dumps(data).encode("utf-8"))
        mock_urlopen.return_value = mock_response

        result = client.get_metrics()
        assert len(result) == 1
        assert result[0].name == "metric3"

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(
        CVec,
        "_fetch_config",
        autospec=True,
        side_effect=mock_fetch_config_side_effect,
    )
    @patch("cvec.cvec.urlopen")
    def test_gzip_arrow_response_decompressed(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Verify binary Arrow responses with gzip encoding are decompressed."""
        client = _create_client()

        import pyarrow as pa  # type: ignore[import-untyped]
        import pyarrow.ipc as ipc  # type: ignore[import-untyped]

        table = pa.table(
            {
                "name": ["m1"],
                "timestamp": [pa.scalar(1000000, type=pa.timestamp("us"))],
                "value": [1.0],
            }
        )
        sink = pa.BufferOutputStream()
        writer = ipc.new_file(sink, table.schema)
        writer.write_table(table)
        writer.close()
        arrow_bytes = sink.getvalue().to_pybytes()

        compressed = gzip.compress(arrow_bytes)
        mock_response = _make_mock_response(
            compressed,
            content_type="application/vnd.apache.arrow.stream",
            content_encoding="gzip",
        )
        mock_urlopen.return_value = mock_response

        result = client._make_request("GET", "/api/metrics/data/arrow")
        assert isinstance(result, bytes)
        assert result == arrow_bytes

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(
        CVec,
        "_fetch_config",
        autospec=True,
        side_effect=mock_fetch_config_side_effect,
    )
    @patch("cvec.cvec.urlopen")
    def test_brotli_response_decompressed(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Mock a brotli-compressed JSON response, verify it is decompressed."""
        client = _create_client()

        data = [{"id": 4, "name": "metric4"}]
        compressed = brotli.compress(json.dumps(data).encode("utf-8"))
        mock_response = _make_mock_response(compressed, content_encoding="br")
        mock_urlopen.return_value = mock_response

        result = client.get_metrics()
        assert len(result) == 1
        assert result[0].name == "metric4"
