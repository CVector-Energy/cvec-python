import pytest
import os
from unittest.mock import patch, MagicMock
from datetime import datetime
from cvec import CVec, Span


class TestCVecConstructor:
    def test_constructor_with_arguments(self) -> None:
        """Test CVec constructor with all arguments provided."""
        client = CVec(
            host="test_host",
            tenant="test_tenant",
            api_key="test_api_key",
            default_start_at=datetime(2023, 1, 1, 0, 0, 0),
            default_end_at=datetime(2023, 1, 2, 0, 0, 0),
        )
        assert client.host == "test_host"
        assert client.tenant == "test_tenant"
        assert client.api_key == "test_api_key"
        assert client.default_start_at == datetime(2023, 1, 1, 0, 0, 0)
        assert client.default_end_at == datetime(2023, 1, 2, 0, 0, 0)

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
        client = CVec(
            default_start_at=datetime(2023, 2, 1, 0, 0, 0),
            default_end_at=datetime(2023, 2, 2, 0, 0, 0),
        )
        assert client.host == "env_host"
        assert client.tenant == "env_tenant"
        assert client.api_key == "env_api_key"
        assert client.default_start_at == datetime(2023, 2, 1, 0, 0, 0)
        assert client.default_end_at == datetime(2023, 2, 2, 0, 0, 0)

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
                default_start_at=datetime(2023, 3, 1, 0, 0, 0),
                default_end_at=datetime(2023, 3, 2, 0, 0, 0),
            )
            assert client.host == "arg_host"
            assert client.tenant == "arg_tenant"
            assert client.api_key == "arg_api_key"
            assert client.default_start_at == datetime(2023, 3, 1, 0, 0, 0)
            assert client.default_end_at == datetime(2023, 3, 2, 0, 0, 0)


class TestCVecGetSpans:
    @patch("cvec.cvec.psycopg.connect")
    def test_get_spans_basic_case(self, mock_connect: MagicMock) -> None:
        """Test get_spans with a few data points."""
        # Setup mock connection and cursor
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        # Sample data (time, value_double, value_string) - newest first
        time1 = datetime(2023, 1, 1, 10, 0, 0)
        time2 = datetime(2023, 1, 1, 11, 0, 0)
        time3 = datetime(2023, 1, 1, 12, 0, 0)
        mock_db_rows = [
            (time3, 30.0, None),  # Newest
            (time2, None, "val2"),
            (time1, 10.0, None),  # Oldest
        ]
        mock_cur.fetchall.return_value = mock_db_rows

        client = CVec(host="test_host", tenant="test_tenant", api_key="test_api_key")
        tag_name = "test_tag"
        spans = client.get_spans(tag_name=tag_name)

        assert len(spans) == 3
        mock_cur.execute.assert_called_once()
        
        # Verify query parameters (optional, but good for sanity check)
        # args, kwargs = mock_cur.execute.call_args
        # assert kwargs['params']['metric'] == tag_name
        # assert kwargs['params']['limit'] is None # Default limit

        # Span 1 (from newest data point: time3)
        # Based on current implementation, raw_end_at for the first span is None
        assert spans[0].tag_name == tag_name
        assert spans[0].value == 30.0
        assert spans[0].raw_start_at == time3
        assert spans[0].raw_end_at is None 

        # Span 2 (from data point: time2)
        assert spans[1].tag_name == tag_name
        assert spans[1].value == "val2"
        assert spans[1].raw_start_at == time2
        assert spans[1].raw_end_at == time3

        # Span 3 (from oldest data point: time1)
        assert spans[2].tag_name == tag_name
        assert spans[2].value == 10.0
        assert spans[2].raw_start_at == time1
        assert spans[2].raw_end_at == time2

    # TODO: Add more tests for get_spans:
    # - No data points
    # - One data point
    # - With limit parameter
    # - With start_at/end_at parameters affecting results
    # - When _end_at is provided to get_spans (to see its effect on the first span's raw_end_at,
    #   once the suspected bug is addressed or confirmed as intended behavior)
