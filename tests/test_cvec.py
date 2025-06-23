import pytest
import os
from unittest.mock import patch
from datetime import datetime
from cvec import CVec
from cvec.models.metric import Metric
import pyarrow as pa
import pyarrow.ipc as ipc
import io


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
    def test_get_spans_basic_case(self):
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
        client = CVec(host="test_host", tenant="test_tenant", api_key="test_api_key")
        client._make_request = lambda *args, **kwargs: response_data
        spans = client.get_spans(name="test_tag")
        assert len(spans) == 3
        assert spans[0].name == "test_tag"
        assert spans[0].value == 30.0
        assert spans[0].raw_start_at == datetime(2023, 1, 1, 12, 0, 0)
        assert spans[0].raw_end_at is None
        assert spans[1].value == "val2"
        assert spans[2].value == 10.0


class TestCVecGetMetrics:
    def test_get_metrics_no_interval(self):
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
        client = CVec(host="test_host", tenant="test_tenant", api_key="test_api_key")
        client._make_request = lambda *args, **kwargs: response_data
        metrics = client.get_metrics()
        assert len(metrics) == 2
        assert isinstance(metrics[0], Metric)
        assert metrics[0].id == 1
        assert metrics[0].name == "metric1"
        assert metrics[1].id == 2
        assert metrics[1].name == "metric2"

    def test_get_metrics_with_interval(self):
        response_data = [
            {
                "id": 1,
                "name": "metric_in_interval",
                "birth_at": datetime(2023, 1, 1, 0, 0, 0),
                "death_at": None,
            },
        ]
        client = CVec(host="test_host", tenant="test_tenant", api_key="test_api_key")
        client._make_request = lambda *args, **kwargs: response_data
        metrics = client.get_metrics(
            start_at=datetime(2023, 1, 5, 0, 0, 0),
            end_at=datetime(2023, 1, 15, 0, 0, 0),
        )
        assert len(metrics) == 1
        assert metrics[0].name == "metric_in_interval"

    def test_get_metrics_no_data_found(self):
        client = CVec(host="test_host", tenant="test_tenant", api_key="test_api_key")
        client._make_request = lambda *args, **kwargs: []
        metrics = client.get_metrics(
            start_at=datetime(2024, 1, 1), end_at=datetime(2024, 1, 2)
        )
        assert len(metrics) == 0


class TestCVecGetMetricData:
    def test_get_metric_data_basic_case(self):
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
        client = CVec(host="test_host", tenant="test_tenant", api_key="test_api_key")
        client._make_request = lambda *args, **kwargs: response_data
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

    def test_get_metric_data_no_data_points(self):
        client = CVec(host="test_host", tenant="test_tenant", api_key="test_api_key")
        client._make_request = lambda *args, **kwargs: []
        data_points = client.get_metric_data(names=["non_existent_tag"])
        assert data_points == []

    def test_get_metric_arrow_basic_case(self):
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
        client = CVec(host="test_host", tenant="test_tenant", api_key="test_api_key")
        client._make_request = lambda *args, **kwargs: arrow_bytes
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

    def test_get_metric_arrow_empty(self):
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
        client = CVec(host="test_host", tenant="test_tenant", api_key="test_api_key")
        client._make_request = lambda *args, **kwargs: arrow_bytes
        result = client.get_metric_arrow(names=["non_existent_tag"])
        reader = ipc.open_file(io.BytesIO(result))
        result_table = reader.read_all()
        assert result_table.num_rows == 0
        assert result_table.column("name").to_pylist() == []
        assert result_table.column("value_double").to_pylist() == []
        assert result_table.column("value_string").to_pylist() == []
