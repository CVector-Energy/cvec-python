"""Tests for token refresh functionality."""

import pytest
from typing import Any
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from cvec import CVec


class TestTokenRefresh:
    """Test cases for automatic token refresh functionality."""

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    @patch("cvec.cvec.urlopen")
    def test_token_refresh_on_401(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Test that token refresh is triggered on 401 Unauthorized."""
        client = CVec(
            host="https://test.example.com",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )

        client._access_token = "expired_token"
        client._refresh_token = "valid_refresh_token"

        # Mock 401 error response
        http_error_401 = HTTPError(
            url="https://test.example.com/api/metrics/",
            code=401,
            msg="Unauthorized",
            hdrs={},  # type: ignore[arg-type]
            fp=None,  # type: ignore[arg-type]
        )

        # Mock successful response after refresh
        mock_success_response = Mock()
        mock_success_response.read.return_value = b"[]"
        mock_success_response.headers = {"content-type": "application/json"}
        mock_success_response.__enter__ = Mock(return_value=mock_success_response)
        mock_success_response.__exit__ = Mock(return_value=False)

        mock_urlopen.side_effect = [
            http_error_401,
            mock_success_response,
        ]

        # Mock refresh method
        refresh_called: list[bool] = []

        def mock_refresh() -> None:
            refresh_called.append(True)
            client._access_token = "new_token"

        with patch.object(client, "_refresh_supabase_token", side_effect=mock_refresh):
            # Execute request
            result = client.get_metrics()

        # Verify refresh was called
        assert len(refresh_called) == 1
        assert client._access_token == "new_token"
        assert result == []

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    @patch("cvec.cvec.urlopen")
    def test_token_refresh_handles_network_errors_gracefully(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Test that network errors during refresh don't crash, returns original error."""
        from urllib.error import URLError

        client = CVec(
            host="https://test.example.com",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )

        client._access_token = "expired_token"
        client._refresh_token = "valid_refresh_token"

        # Mock 401 error response
        http_error_401 = HTTPError(
            url="https://test.example.com/api/metrics/",
            code=401,
            msg="Unauthorized",
            hdrs={},  # type: ignore[arg-type]
            fp=None,  # type: ignore[arg-type]
        )

        mock_urlopen.side_effect = http_error_401

        # Mock refresh to raise network error
        def mock_refresh_with_error() -> None:
            raise URLError("Network unreachable")

        with patch.object(
            client, "_refresh_supabase_token", side_effect=mock_refresh_with_error
        ):
            # Should not crash, should raise the original 401 error
            with pytest.raises(HTTPError) as exc_info:
                client.get_metrics()
            assert exc_info.value.code == 401

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    @patch("cvec.cvec.urlopen")
    def test_token_refresh_handles_missing_refresh_token(
        self,
        mock_urlopen: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Test that missing refresh token is handled gracefully."""
        client = CVec(
            host="https://test.example.com",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )

        client._access_token = "expired_token"
        client._refresh_token = "valid_refresh_token"

        # Mock 401 error response
        http_error_401 = HTTPError(
            url="https://test.example.com/api/metrics/",
            code=401,
            msg="Unauthorized",
            hdrs={},  # type: ignore[arg-type]
            fp=None,  # type: ignore[arg-type]
        )

        mock_urlopen.side_effect = http_error_401

        # Mock refresh to raise ValueError (missing refresh token)
        def mock_refresh_with_error() -> None:
            raise ValueError("No refresh token available")

        with patch.object(
            client, "_refresh_supabase_token", side_effect=mock_refresh_with_error
        ):
            # Should not crash, should raise the original 401 error
            with pytest.raises(HTTPError) as exc_info:
                client.get_metrics()
            assert exc_info.value.code == 401
