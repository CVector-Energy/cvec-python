import pytest
import os
from unittest.mock import patch
from cvec import CVec


class TestCVecConstructor:
    def test_constructor_with_arguments(self) -> None:
        """Test CVec constructor with all arguments provided."""
        client = CVec(
            host="test_host",
            tenant="test_tenant",
            api_key="test_api_key",
            default_start_at="test_start",
            default_end_at="test_end",
        )
        assert client.host == "test_host"
        assert client.tenant == "test_tenant"
        assert client.api_key == "test_api_key"
        assert client.default_start_at == "test_start"
        assert client.default_end_at == "test_end"

    @patch.dict(
        os.environ,
        {
            "CVEC_HOST": "env_host",
            "CVEC_TENANT": "env_tenant",
            "CVEC_API_KEY": "env_api_key",
        },
        clear=True,
    )
    def test_constructor_with_env_vars(self) -> None:
        """Test CVec constructor with environment variables."""
        client = CVec(default_start_at="env_start", default_end_at="env_end")
        assert client.host == "env_host"
        assert client.tenant == "env_tenant"
        assert client.api_key == "env_api_key"
        assert client.default_start_at == "env_start"
        assert client.default_end_at == "env_end"

    @patch.dict(os.environ, {}, clear=True)
    def test_constructor_missing_host_raises_value_error(self) -> None:
        """Test CVec constructor raises ValueError if host is missing."""
        with pytest.raises(
            ValueError,
            match="CVEC_HOST must be set either as an argument or environment variable",
        ):
            CVec(tenant="test_tenant", api_key="test_api_key")

    @patch.dict(os.environ, {}, clear=True)
    def test_constructor_missing_tenant_raises_value_error(self) -> None:
        """Test CVec constructor raises ValueError if tenant is missing."""
        with pytest.raises(
            ValueError,
            match="CVEC_TENANT must be set either as an argument or environment variable",
        ):
            CVec(host="test_host", api_key="test_api_key")

    @patch.dict(os.environ, {}, clear=True)
    def test_constructor_missing_api_key_raises_value_error(self) -> None:
        """Test CVec constructor raises ValueError if api_key is missing."""
        with pytest.raises(
            ValueError,
            match="CVEC_API_KEY must be set either as an argument or environment variable",
        ):
            CVec(host="test_host", tenant="test_tenant")

    @patch.dict(
        os.environ,
        {
            "CVEC_HOST": "env_host",
            # CVEC_TENANT is missing
            "CVEC_API_KEY": "env_api_key",
        },
        clear=True,
    )
    def test_constructor_missing_tenant_env_var_raises_value_error(self) -> None:
        """Test CVec constructor raises ValueError if CVEC_TENANT env var is missing."""
        with pytest.raises(
            ValueError,
            match="CVEC_TENANT must be set either as an argument or environment variable",
        ):
            CVec()

    def test_constructor_args_override_env_vars(self) -> None:
        """Test CVec constructor arguments override environment variables."""
        with patch.dict(
            os.environ,
            {
                "CVEC_HOST": "env_host",
                "CVEC_TENANT": "env_tenant",
                "CVEC_API_KEY": "env_api_key",
            },
            clear=True,
        ):
            client = CVec(
                host="arg_host",
                tenant="arg_tenant",
                api_key="arg_api_key",
                default_start_at="arg_start",
                default_end_at="arg_end",
            )
            assert client.host == "arg_host"
            assert client.tenant == "arg_tenant"
            assert client.api_key == "arg_api_key"
            assert client.default_start_at == "arg_start"
            assert client.default_end_at == "arg_end"
