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
    def test_token_refresh_on_redirect_to_login(
        self,
        mock_request: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Test that token refresh is triggered on redirect to login page."""
        client = CVec(
            host="https://test.example.com",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )

        client._access_token = "expired_token"
        client._refresh_token = "valid_refresh_token"

        # Mock response sequence: 307 redirect to login
        mock_response_redirect = Mock()
        mock_response_redirect.status_code = 307
        mock_response_redirect.headers = {
            "Location": "/login?error=Token%20has%20expired"
        }

        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.headers = {"content-type": "application/json"}
        mock_response_success.json.return_value = []

        mock_request.side_effect = [
            mock_response_redirect,
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
    def test_no_refresh_on_normal_redirect(
        self,
        mock_request: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Test that token refresh is NOT triggered on normal redirects."""
        client = CVec(
            host="https://test.example.com",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )

        client._access_token = "valid_token"
        client._refresh_token = "valid_refresh_token"

        mock_response_redirect = Mock()
        mock_response_redirect.status_code = 302
        mock_response_redirect.headers = {"Location": "/api/v2/metrics"}

        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.headers = {"content-type": "application/json"}
        mock_response_success.json.return_value = []

        mock_request.side_effect = [
            mock_response_redirect,
            mock_response_success,
        ]

        refresh_called: list[bool] = []

        def mock_refresh() -> None:
            refresh_called.append(True)

        with patch.object(client, "_refresh_supabase_token", side_effect=mock_refresh):
            result = client.get_metrics()

        assert len(refresh_called) == 0
        assert client._access_token == "valid_token"
        assert result == []

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    @patch("cvec.cvec.requests.request")
    def test_refresh_on_302_redirect_with_token_keyword(
        self,
        mock_request: Any,
        mock_fetch_key: Any,
        mock_login: Any,
    ) -> None:
        """Test that token refresh works on 302 redirect with 'token' in URL."""
        client = CVec(
            host="https://test.example.com",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )

        client._access_token = "expired_token"
        client._refresh_token = "valid_refresh_token"

        mock_response_redirect = Mock()
        mock_response_redirect.status_code = 302
        mock_response_redirect.headers = {"Location": "/auth?error=invalid_token"}

        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.headers = {"content-type": "application/json"}
        mock_response_success.json.return_value = []

        mock_request.side_effect = [
            mock_response_redirect,
            mock_response_success,
        ]

        refresh_called: list[bool] = []

        def mock_refresh() -> None:
            refresh_called.append(True)
            client._access_token = "new_token"

        with patch.object(client, "_refresh_supabase_token", side_effect=mock_refresh):
            result = client.get_metrics()

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

        mock_response_401 = Mock()
        mock_response_401.status_code = 401
        mock_response_401.raise_for_status.side_effect = requests.HTTPError(
            "401 Client Error: Unauthorized"
        )

        mock_request.return_value = mock_response_401

        def mock_refresh_with_error() -> None:
            raise requests.ConnectionError("Network unreachable")

        with patch.object(
            client, "_refresh_supabase_token", side_effect=mock_refresh_with_error
        ):
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

        mock_response_401 = Mock()
        mock_response_401.status_code = 401
        mock_response_401.raise_for_status.side_effect = requests.HTTPError(
            "401 Client Error: Unauthorized"
        )

        mock_request.return_value = mock_response_401

        def mock_refresh_with_error() -> None:
            raise ValueError("No refresh token available")

        with patch.object(
            client, "_refresh_supabase_token", side_effect=mock_refresh_with_error
        ):
            with pytest.raises(requests.HTTPError, match="401"):
                client.get_metrics()
