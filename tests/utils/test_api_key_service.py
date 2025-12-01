import json
import os
import pytest
from unittest.mock import MagicMock, patch
from typing import Any

from cvec.utils import api_key_service


# Reset cache before each test
@pytest.fixture(autouse=True)
def reset_cache() -> Any:
    """Reset the global API keys cache before each test."""
    api_key_service._API_KEYS_CACHE = None
    yield
    api_key_service._API_KEYS_CACHE = None


class TestLoadFromAwsSecretsManager:
    def test_load_from_aws_success(self) -> None:
        """Test successful loading from AWS Secrets Manager."""
        # Mock boto3 module and client
        mock_boto3 = MagicMock()
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client

        # Mock secret response
        api_keys_data = {
            "https://tenant1.cvector.dev": "cva_1234567890123456789012345678901234567",
            "https://tenant2.cvector.dev": "cva_abcdefghijklmnopqrstuvwxyz1234567890",
        }
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps(api_keys_data)
        }

        with patch.dict("sys.modules", {"boto3": mock_boto3}):
            result = api_key_service._load_from_aws_secrets_manager("test-secret")

        assert result == api_keys_data
        mock_client.get_secret_value.assert_called_once_with(SecretId="test-secret")

    def test_load_from_aws_empty_secret(self) -> None:
        """Test loading from AWS with empty secret value."""
        mock_boto3 = MagicMock()
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.get_secret_value.return_value = {"SecretString": ""}

        with patch.dict("sys.modules", {"boto3": mock_boto3}):
            result = api_key_service._load_from_aws_secrets_manager("test-secret")

        assert result is None

    def test_load_from_aws_no_secret_string(self) -> None:
        """Test loading from AWS when SecretString is missing."""
        mock_boto3 = MagicMock()
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.get_secret_value.return_value = {}

        with patch.dict("sys.modules", {"boto3": mock_boto3}):
            result = api_key_service._load_from_aws_secrets_manager("test-secret")

        assert result is None

    def test_load_from_aws_invalid_json(self) -> None:
        """Test loading from AWS with invalid JSON."""
        mock_boto3 = MagicMock()
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.get_secret_value.return_value = {"SecretString": "not valid json"}

        with patch.dict("sys.modules", {"boto3": mock_boto3}):
            result = api_key_service._load_from_aws_secrets_manager("test-secret")

        assert result is None


class TestLoadFromLocalEnv:
    def test_load_from_local_env_success(self) -> None:
        """Test successful loading from local environment variable."""
        api_keys_data = {
            "https://tenant1.cvector.dev": "cva_1234567890123456789012345678901234567",
            "https://tenant2.cvector.dev": "cva_abcdefghijklmnopqrstuvwxyz1234567890",
        }
        json_string = json.dumps(api_keys_data)

        result = api_key_service._load_from_local_env(json_string)

        assert result == api_keys_data

    def test_load_from_local_env_invalid_json(self) -> None:
        """Test loading from local env with invalid JSON."""
        result = api_key_service._load_from_local_env("not valid json")

        assert result is None

    def test_load_from_local_env_not_dict(self) -> None:
        """Test loading from local env when JSON is not a dict."""
        result = api_key_service._load_from_local_env('["array", "not", "dict"]')

        assert result is None


class TestLoadApiKeys:
    @patch.dict(os.environ, {}, clear=True)
    def test_load_api_keys_no_source(self) -> None:
        """Test loading API keys when no source is available."""
        result = api_key_service.load_api_keys()

        assert result is None

    @patch("cvec.utils.api_key_service._load_from_aws_secrets_manager")
    @patch.dict(
        os.environ,
        {"AWS_API_KEYS_SECRET": "test-secret"},
        clear=True,
    )
    def test_load_api_keys_from_aws(self, mock_aws_load: Any) -> None:
        """Test loading API keys from AWS Secrets Manager."""
        api_keys_data = {
            "https://tenant1.cvector.dev": "cva_1234567890123456789012345678901234567",
        }
        mock_aws_load.return_value = api_keys_data

        result = api_key_service.load_api_keys()

        assert result == api_keys_data
        mock_aws_load.assert_called_once_with("test-secret")

    @patch.dict(
        os.environ,
        {
            "API_KEYS_MAPPING": '{"https://tenant1.cvector.dev": "cva_1234567890123456789012345678901234567"}'
        },
        clear=True,
    )
    def test_load_api_keys_from_local_env(self) -> None:
        """Test loading API keys from local environment variable."""
        result = api_key_service.load_api_keys()

        assert result == {
            "https://tenant1.cvector.dev": "cva_1234567890123456789012345678901234567"
        }

    @patch("cvec.utils.api_key_service._load_from_aws_secrets_manager")
    @patch.dict(
        os.environ,
        {
            "AWS_API_KEYS_SECRET": "test-secret",
            "API_KEYS_MAPPING": '{"https://local.cvector.dev": "cva_local123456789012345678901234567890"}',
        },
        clear=True,
    )
    def test_load_api_keys_aws_priority(self, mock_aws_load: Any) -> None:
        """Test that AWS Secrets Manager has priority over local env."""
        aws_keys = {"https://aws.cvector.dev": "cva_awskey1234567890123456789012345678"}
        mock_aws_load.return_value = aws_keys

        result = api_key_service.load_api_keys()

        assert result == aws_keys
        mock_aws_load.assert_called_once_with("test-secret")

    @patch("cvec.utils.api_key_service._load_from_aws_secrets_manager")
    @patch.dict(
        os.environ,
        {
            "AWS_API_KEYS_SECRET": "test-secret",
            "API_KEYS_MAPPING": '{"https://local.cvector.dev": "cva_local123456789012345678901234567890"}',
        },
        clear=True,
    )
    def test_load_api_keys_fallback_to_local(self, mock_aws_load: Any) -> None:
        """Test fallback to local env when AWS fails."""
        mock_aws_load.return_value = None

        result = api_key_service.load_api_keys()

        assert result == {
            "https://local.cvector.dev": "cva_local123456789012345678901234567890"
        }

    @patch("cvec.utils.api_key_service._load_from_aws_secrets_manager")
    @patch.dict(
        os.environ,
        {"AWS_API_KEYS_SECRET": "test-secret"},
        clear=True,
    )
    def test_load_api_keys_caching(self, mock_aws_load: Any) -> None:
        """Test that API keys are cached after first load."""
        api_keys_data = {
            "https://tenant1.cvector.dev": "cva_1234567890123456789012345678901234567",
        }
        mock_aws_load.return_value = api_keys_data

        # First call
        result1 = api_key_service.load_api_keys()
        assert result1 == api_keys_data
        assert mock_aws_load.call_count == 1

        # Second call should use cache
        result2 = api_key_service.load_api_keys()
        assert result2 == api_keys_data
        assert mock_aws_load.call_count == 1  # Still 1, not called again


class TestGetApiKeyForHost:
    @patch("cvec.utils.api_key_service.load_api_keys")
    def test_get_api_key_for_host_success(self, mock_load: Any) -> None:
        """Test successfully getting API key for a host."""
        api_keys_data = {
            "https://tenant1.cvector.dev": "cva_1234567890123456789012345678901234567",
            "https://tenant2.cvector.dev": "cva_abcdefghijklmnopqrstuvwxyz1234567890",
        }
        mock_load.return_value = api_keys_data

        result = api_key_service.get_api_key_for_host("https://tenant1.cvector.dev")

        assert result == "cva_1234567890123456789012345678901234567"

    @patch("cvec.utils.api_key_service.load_api_keys")
    def test_get_api_key_for_host_not_found(self, mock_load: Any) -> None:
        """Test getting API key for host that doesn't exist in mapping."""
        api_keys_data = {
            "https://tenant1.cvector.dev": "cva_1234567890123456789012345678901234567",
        }
        mock_load.return_value = api_keys_data

        result = api_key_service.get_api_key_for_host("https://unknown.cvector.dev")

        assert result is None

    @patch("cvec.utils.api_key_service.load_api_keys")
    def test_get_api_key_for_host_no_mapping(self, mock_load: Any) -> None:
        """Test getting API key when no mapping is available."""
        mock_load.return_value = None

        with pytest.raises(
            ValueError,
            match="No API keys mapping available",
        ):
            api_key_service.get_api_key_for_host("https://tenant1.cvector.dev")
