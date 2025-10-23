"""Tests for token refresh functionality."""

import pytest
import requests
from typing import Any
from unittest.mock import Mock, patch

from cvec import CVec


class TestTokenRefresh:
    """Test cases for automatic token refresh functionality."""

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    @patch("cvec.cvec.requests.request")
    def test_token_refresh_on_401(
        self,
        mock_request: Any,
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

        # Mock response sequence
        mock_response_401 = Mock()
        mock_response_401.status_code = 401

        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.headers = {"content-type": "application/json"}
        mock_response_success.json.return_value = []

        mock_request.side_effect = [
            mock_response_401,
            mock_response_success,
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
    @patch("cvec.cvec.requests.request")
    def test_token_refresh_handles_network_errors_gracefully(
        self,
        mock_request: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Test that network errors during refresh don't crash, returns original error."""
        client = CVec(
            host="https://test.example.com",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )

        client._access_token = "expired_token"
        client._refresh_token = "valid_refresh_token"

        # Mock response: 401 triggers refresh
        mock_response_401 = Mock()
        mock_response_401.status_code = 401
        mock_response_401.raise_for_status.side_effect = requests.HTTPError(
            "401 Client Error: Unauthorized"
        )

        mock_request.return_value = mock_response_401

        # Mock refresh to raise network error
        def mock_refresh_with_error() -> None:
            raise requests.ConnectionError("Network unreachable")

        with patch.object(
            client, "_refresh_supabase_token", side_effect=mock_refresh_with_error
        ):
            # Should not crash, should raise the original 401 error
            with pytest.raises(requests.HTTPError, match="401"):
                client.get_metrics()

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    @patch("cvec.cvec.requests.request")
    def test_token_refresh_handles_missing_refresh_token(
        self,
        mock_request: Any,
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

        # Mock response: 401 triggers refresh
        mock_response_401 = Mock()
        mock_response_401.status_code = 401
        mock_response_401.raise_for_status.side_effect = requests.HTTPError(
            "401 Client Error: Unauthorized"
        )

        mock_request.return_value = mock_response_401

        # Mock refresh to raise ValueError (missing refresh token)
        def mock_refresh_with_error() -> None:
            raise ValueError("No refresh token available")

        with patch.object(
            client, "_refresh_supabase_token", side_effect=mock_refresh_with_error
        ):
            # Should not crash, should raise the original 401 error
            with pytest.raises(requests.HTTPError, match="401"):
                client.get_metrics()
