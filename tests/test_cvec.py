import pytest
import os
from unittest.mock import patch
from datetime import datetime
from cvec import CVec
from cvec.models.metric import Metric
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.ipc as ipc  # type: ignore[import-untyped]
import io
from typing import Any


class TestCVecConstructor:
    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_constructor_with_arguments(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test CVec constructor with all arguments provided."""
        client = CVec(
            host="test_host",
            default_start_at=datetime(2023, 1, 1, 0, 0, 0),
            default_end_at=datetime(2023, 1, 2, 0, 0, 0),
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        assert client.host == "test_host"
        assert client.default_start_at == datetime(2023, 1, 1, 0, 0, 0)
        assert client.default_end_at == datetime(2023, 1, 2, 0, 0, 0)
        assert client._publishable_key == "test_publishable_key"
        assert client._api_key == "cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O"

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="env_publishable_key")
    @patch.dict(
        os.environ,
        {
            "CVEC_HOST": "env_host",
            "CVEC_API_KEY": "cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        },
        clear=True,
    )
    def test_constructor_with_env_vars(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test CVec constructor with environment variables."""
        client = CVec(
            default_start_at=datetime(2023, 2, 1, 0, 0, 0),
            default_end_at=datetime(2023, 2, 2, 0, 0, 0),
        )
        assert client.host == "env_host"
        assert client._publishable_key == "env_publishable_key"
        assert client._api_key == "cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O"
        assert client.default_start_at == datetime(2023, 2, 1, 0, 0, 0)
        assert client.default_end_at == datetime(2023, 2, 2, 0, 0, 0)

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    @patch.dict(os.environ, {}, clear=True)
    def test_constructor_missing_host_raises_value_error(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test CVec constructor raises ValueError if host is missing."""
        with pytest.raises(
            ValueError,
            match="CVEC_HOST must be set either as an argument or environment variable",
        ):
            CVec(api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O")

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    @patch("cvec.cvec.get_api_key_for_host")
    @patch.dict(os.environ, {}, clear=True)
    def test_constructor_missing_api_key_raises_value_error(
        self, mock_get_api_key: Any, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test CVec constructor raises ValueError if api_key is missing."""
        # Mock the api_key_service to raise ValueError (no mapping available)
        mock_get_api_key.side_effect = ValueError("No API keys mapping available")

        with pytest.raises(
            ValueError,
            match="CVEC_API_KEY must be set either as an argument, environment variable",
        ):
            CVec(host="test_host")

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_constructor_args_override_env_vars(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test CVec constructor arguments override environment variables."""
        with patch.dict(
            os.environ,
            {
                "CVEC_HOST": "env_host",
                "CVEC_API_KEY": "cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
            },
            clear=True,
        ):
            client = CVec(
                host="arg_host",
                default_start_at=datetime(2023, 3, 1, 0, 0, 0),
                default_end_at=datetime(2023, 3, 2, 0, 0, 0),
                api_key="cva_differentKeyKALxMnxUdI9hanF0TBPvvvr1",
            )
            assert client.host == "arg_host"
            assert client._api_key == "cva_differentKeyKALxMnxUdI9hanF0TBPvvvr1"
            assert client.default_start_at == datetime(2023, 3, 1, 0, 0, 0)
            assert client.default_end_at == datetime(2023, 3, 2, 0, 0, 0)

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_construct_email_from_api_key(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test email construction from API key."""
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        email = client._construct_email_from_api_key()
        assert email == "cva+hHs0@cvector.app"

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_construct_email_from_api_key_invalid_format(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test email construction with invalid API key format."""
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._api_key = "invalid_key"
        with pytest.raises(ValueError, match="API key must start with 'cva_'"):
            client._construct_email_from_api_key()

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_construct_email_from_api_key_invalid_length(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test email construction with invalid API key length."""
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._api_key = "cva_short"
        with pytest.raises(
            ValueError, match="API key invalid length. Expected cva_ \\+ 36 symbols."
        ):
            client._construct_email_from_api_key()

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    @patch("cvec.cvec.get_api_key_for_host")
    @patch.dict(os.environ, {"CVEC_HOST": "https://tenant1.cvector.dev"}, clear=True)
    def test_constructor_loads_api_key_from_service(
        self, mock_get_api_key: Any, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test CVec constructor loads API key from api_key_service when not provided."""
        mock_get_api_key.return_value = "cva_service12345678901234567890123456789"

        client = CVec()

        assert client.host == "https://tenant1.cvector.dev"
        assert client._api_key == "cva_service12345678901234567890123456789"
        mock_get_api_key.assert_called_once_with("https://tenant1.cvector.dev")

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    @patch("cvec.cvec.get_api_key_for_host")
    @patch.dict(os.environ, {}, clear=True)
    def test_constructor_api_key_service_returns_none(
        self, mock_get_api_key: Any, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test CVec constructor raises when api_key_service returns None."""
        mock_get_api_key.return_value = None

        with pytest.raises(
            ValueError,
            match="CVEC_API_KEY must be set either as an argument, environment variable",
        ):
            CVec(host="https://tenant1.cvector.dev")

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    @patch("cvec.cvec.get_api_key_for_host")
    @patch.dict(
        os.environ,
        {
            "CVEC_HOST": "https://tenant1.cvector.dev",
            "CVEC_API_KEY": "cva_envkey123456789012345678901234567890",
        },
        clear=True,
    )
    def test_constructor_env_api_key_takes_precedence_over_service(
        self, mock_get_api_key: Any, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test that CVEC_API_KEY env var takes precedence over api_key_service."""
        client = CVec()

        assert client._api_key == "cva_envkey123456789012345678901234567890"
        # Should not call the service if env var is set
        mock_get_api_key.assert_not_called()

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    @patch("cvec.cvec.get_api_key_for_host")
    def test_constructor_arg_api_key_takes_precedence_over_service(
        self, mock_get_api_key: Any, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test that api_key argument takes precedence over api_key_service."""
        client = CVec(
            host="https://tenant1.cvector.dev",
            api_key="cva_argkey123456789012345678901234567890",
        )

        assert client._api_key == "cva_argkey123456789012345678901234567890"
        # Should not call the service if api_key argument is provided
        mock_get_api_key.assert_not_called()


class TestCVecGetSpans:
    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_get_spans_basic_case(self, mock_fetch_key: Any, mock_login: Any) -> None:
        # Simulate backend response
        response_data = [
            {
                "name": "test_tag",
                "value": 30.0,
                "raw_start_at": datetime(2023, 1, 1, 12, 0, 0),
                "raw_end_at": None,
            },
            {
                "name": "test_tag",
                "value": "val2",
                "raw_start_at": datetime(2023, 1, 1, 11, 0, 0),
                "raw_end_at": datetime(2023, 1, 1, 12, 0, 0),
            },
            {
                "name": "test_tag",
                "value": 10.0,
                "raw_start_at": datetime(2023, 1, 1, 10, 0, 0),
                "raw_end_at": datetime(2023, 1, 1, 11, 0, 0),
            },
        ]
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._make_request = lambda *args, **kwargs: response_data  # type: ignore[method-assign]
        spans = client.get_spans(name="test_tag")
        assert len(spans) == 3
        assert spans[0].name == "test_tag"
        assert spans[0].value == 30.0
        assert spans[0].raw_start_at == datetime(2023, 1, 1, 12, 0, 0)
        assert spans[0].raw_end_at is None
        assert spans[1].value == "val2"
        assert spans[2].value == 10.0


class TestCVecGetMetrics:
    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_get_metrics_no_interval(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        response_data = [
            {
                "id": 1,
                "name": "metric1",
                "birth_at": datetime(2023, 1, 1, 0, 0, 0),
                "death_at": datetime(2023, 1, 10, 0, 0, 0),
            },
            {
                "id": 2,
                "name": "metric2",
                "birth_at": datetime(2023, 2, 1, 0, 0, 0),
                "death_at": None,
            },
        ]
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._make_request = lambda *args, **kwargs: response_data  # type: ignore[method-assign]
        metrics = client.get_metrics()
        assert len(metrics) == 2
        assert isinstance(metrics[0], Metric)
        assert metrics[0].id == 1
        assert metrics[0].name == "metric1"
        assert metrics[1].id == 2
        assert metrics[1].name == "metric2"

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_get_metrics_with_interval(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        response_data = [
            {
                "id": 1,
                "name": "metric_in_interval",
                "birth_at": datetime(2023, 1, 1, 0, 0, 0),
                "death_at": None,
            },
        ]
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._make_request = lambda *args, **kwargs: response_data  # type: ignore[method-assign]
        metrics = client.get_metrics(
            start_at=datetime(2023, 1, 5, 0, 0, 0),
            end_at=datetime(2023, 1, 15, 0, 0, 0),
        )
        assert len(metrics) == 1
        assert metrics[0].name == "metric_in_interval"

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_get_metrics_no_data_found(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._make_request = lambda *args, **kwargs: []  # type: ignore[method-assign]
        metrics = client.get_metrics(
            start_at=datetime(2024, 1, 1), end_at=datetime(2024, 1, 2)
        )
        assert len(metrics) == 0


class TestCVecGetMetricData:
    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_get_metric_data_basic_case(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        # Simulate backend response
        time1 = datetime(2023, 1, 1, 10, 0, 0)
        time2 = datetime(2023, 1, 1, 11, 0, 0)
        time3 = datetime(2023, 1, 1, 12, 0, 0)
        response_data = [
            {"name": "tag1", "time": time1, "value_double": 10.0, "value_string": None},
            {"name": "tag1", "time": time2, "value_double": 20.0, "value_string": None},
            {
                "name": "tag2",
                "time": time3,
                "value_double": None,
                "value_string": "val_str",
            },
        ]
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._make_request = lambda *args, **kwargs: response_data  # type: ignore[method-assign]
        data_points = client.get_metric_data(names=["tag1", "tag2"])
        assert len(data_points) == 3
        assert data_points[0].name == "tag1"
        assert data_points[0].time == time1
        assert data_points[0].value_double == 10.0
        assert data_points[0].value_string is None
        assert data_points[2].name == "tag2"
        assert data_points[2].time == time3
        assert data_points[2].value_double is None
        assert data_points[2].value_string == "val_str"

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_get_metric_data_no_data_points(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._make_request = lambda *args, **kwargs: []  # type: ignore[method-assign]
        data_points = client.get_metric_data(names=["non_existent_tag"])
        assert data_points == []

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_get_metric_arrow_basic_case(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        # Prepare Arrow table
        names = ["tag1", "tag1", "tag2"]
        times = [
            datetime(2023, 1, 1, 10, 0, 0),
            datetime(2023, 1, 1, 11, 0, 0),
            datetime(2023, 1, 1, 12, 0, 0),
        ]
        value_doubles = [10.0, 20.0, None]
        value_strings = [None, None, "val_str"]
        table = pa.table(
            {
                "name": pa.array(names),
                "time": pa.array(times, type=pa.timestamp("us", tz=None)),
                "value_double": pa.array(value_doubles, type=pa.float64()),
                "value_string": pa.array(value_strings, type=pa.string()),
            }
        )
        sink = pa.BufferOutputStream()
        with ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        arrow_bytes = sink.getvalue().to_pybytes()
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._make_request = lambda *args, **kwargs: arrow_bytes  # type: ignore[method-assign]
        result = client.get_metric_arrow(names=["tag1", "tag2"])
        reader = ipc.open_file(io.BytesIO(result))
        result_table = reader.read_all()
        assert result_table.num_rows == 3
        assert result_table.column("name").to_pylist() == names
        assert result_table.column("value_double").to_pylist() == [10.0, 20.0, None]
        assert result_table.column("value_string").to_pylist() == [
            None,
            None,
            "val_str",
        ]

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_get_metric_arrow_empty(self, mock_fetch_key: Any, mock_login: Any) -> None:
        table = pa.table(
            {
                "name": pa.array([], type=pa.string()),
                "time": pa.array([], type=pa.timestamp("us", tz=None)),
                "value_double": pa.array([], type=pa.float64()),
                "value_string": pa.array([], type=pa.string()),
            }
        )
        sink = pa.BufferOutputStream()
        with ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        arrow_bytes = sink.getvalue().to_pybytes()
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._make_request = lambda *args, **kwargs: arrow_bytes  # type: ignore[method-assign]
        result = client.get_metric_arrow(names=["non_existent_tag"])
        reader = ipc.open_file(io.BytesIO(result))
        result_table = reader.read_all()
        assert result_table.num_rows == 0
        assert result_table.column("name").to_pylist() == []
        assert result_table.column("value_double").to_pylist() == []
        assert result_table.column("value_string").to_pylist() == []
