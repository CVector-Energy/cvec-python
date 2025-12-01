import io
import os
from datetime import datetime
from typing import Any
from unittest.mock import patch

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.ipc as ipc  # type: ignore[import-untyped]
import pytest

from cvec import CVec, EAVFilter
from cvec.models.metric import Metric


class TestCVecConstructor:
    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_constructor_with_arguments(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test CVec constructor with all arguments provided."""
        client = CVec(
            host="https://test_host",
            default_start_at=datetime(2023, 1, 1, 0, 0, 0),
            default_end_at=datetime(2023, 1, 2, 0, 0, 0),
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        assert client.host == "https://test_host"
        assert client.default_start_at == datetime(2023, 1, 1, 0, 0, 0)
        assert client.default_end_at == datetime(2023, 1, 2, 0, 0, 0)
        assert client._publishable_key == "test_publishable_key"
        assert client._api_key == "cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O"

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_constructor_adds_https_scheme(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test CVec constructor adds https:// scheme if not provided."""
        client = CVec(
            host="example.cvector.dev",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        assert client.host == "https://example.cvector.dev"

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_constructor_preserves_https_scheme(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test CVec constructor preserves https:// scheme if already provided."""
        client = CVec(
            host="https://example.cvector.dev",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        assert client.host == "https://example.cvector.dev"

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_constructor_preserves_http_scheme(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test CVec constructor preserves http:// scheme if provided."""
        client = CVec(
            host="http://localhost:3000",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        assert client.host == "http://localhost:3000"

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
        assert client.host == "https://env_host"
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
    @patch.dict(os.environ, {}, clear=True)
    def test_constructor_missing_api_key_raises_value_error(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test CVec constructor raises ValueError if api_key is missing."""
        with pytest.raises(
            ValueError,
            match="CVEC_API_KEY must be set either as an argument or environment variable",
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
            assert client.host == "https://arg_host"
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


class TestEAVFilter:
    def test_eav_filter_with_column_name(self) -> None:
        """Test EAVFilter with column_name."""
        filter_obj = EAVFilter(column_name="Date")
        assert filter_obj.column_name == "Date"
        assert filter_obj.column_id is None

    def test_eav_filter_with_column_id(self) -> None:
        """Test EAVFilter with column_id."""
        filter_obj = EAVFilter(column_id="MTnaC")
        assert filter_obj.column_id == "MTnaC"
        assert filter_obj.column_name is None

    def test_eav_filter_numeric_range(self) -> None:
        """Test EAVFilter with numeric range."""
        filter_obj = EAVFilter(column_name="Date", numeric_min=100, numeric_max=200)
        assert filter_obj.column_name == "Date"
        assert filter_obj.numeric_min == 100
        assert filter_obj.numeric_max == 200

    def test_eav_filter_string_value(self) -> None:
        """Test EAVFilter with string value."""
        filter_obj = EAVFilter(column_name="Status", string_value="failure")
        assert filter_obj.column_name == "Status"
        assert filter_obj.string_value == "failure"

    def test_eav_filter_boolean_value(self) -> None:
        """Test EAVFilter with boolean value."""
        filter_obj = EAVFilter(column_name="Is Active", boolean_value=False)
        assert filter_obj.column_name == "Is Active"
        assert filter_obj.boolean_value is False

    def test_eav_filter_requires_column_identifier(self) -> None:
        """Test EAVFilter raises error when neither column_name nor column_id."""
        with pytest.raises(ValueError, match="Either column_name or column_id"):
            EAVFilter(numeric_min=100)

    def test_eav_filter_rejects_both_identifiers(self) -> None:
        """Test EAVFilter raises error when both column_name and column_id."""
        with pytest.raises(ValueError, match="Only one of column_name or column_id"):
            EAVFilter(column_name="Date", column_id="MTnaC")


class TestCVecSelectFromEAV:
    """Tests for select_from_eav using table_name and column_names."""

    def _mock_query_table(
        self, table_id: str = "7a80f3a2-6fa1-43ce-8483-76bd00dc93c6"
    ) -> Any:
        """Create a mock for _query_table that returns table and column data."""

        def mock_query(
            table_name: str, query_params: dict[str, str] | None = None
        ) -> Any:
            if table_name == "eav_tables":
                return [{"id": table_id, "tenant_id": 1, "name": "Test Table"}]
            elif table_name == "eav_columns":
                return [
                    {
                        "eav_table_id": table_id,
                        "eav_column_id": "col1_id",
                        "name": "Column 1",
                        "type": "number",
                    },
                    {
                        "eav_table_id": table_id,
                        "eav_column_id": "col2_id",
                        "name": "Column 2",
                        "type": "string",
                    },
                    {
                        "eav_table_id": table_id,
                        "eav_column_id": "is_active_id",
                        "name": "Is Active",
                        "type": "boolean",
                    },
                ]
            return []

        return mock_query

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_select_from_eav_basic(self, mock_fetch_key: Any, mock_login: Any) -> None:
        """Test select_from_eav with no filters."""
        # Response uses column IDs
        rpc_response = [
            {"id": "row1", "col1_id": 100.5, "col2_id": "value1"},
            {"id": "row2", "col1_id": 200.0, "col2_id": "value2"},
        ]
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._query_table = self._mock_query_table()  # type: ignore[method-assign]
        client._call_rpc = lambda *args, **kwargs: rpc_response  # type: ignore[method-assign]

        result = client.select_from_eav(
            tenant_id=1,
            table_name="Test Table",
        )

        # Result should have column names, not IDs
        assert len(result) == 2
        assert result[0]["id"] == "row1"
        assert result[0]["Column 1"] == 100.5
        assert result[1]["Column 2"] == "value2"

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_select_from_eav_with_column_names(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test select_from_eav with specific column_names."""
        rpc_response = [
            {"id": "row1", "col1_id": 100.5},
            {"id": "row2", "col1_id": 200.0},
        ]
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )

        captured_params: dict[str, Any] = {}

        def mock_call_rpc(name: str, params: Any) -> Any:
            captured_params.update(params)
            return rpc_response

        client._query_table = self._mock_query_table()  # type: ignore[method-assign]
        client._call_rpc = mock_call_rpc  # type: ignore[assignment, method-assign]

        result = client.select_from_eav(
            tenant_id=1,
            table_name="Test Table",
            column_names=["Column 1"],
        )

        assert len(result) == 2
        # Should translate column name to column ID for the RPC call
        assert captured_params["column_ids"] == ["col1_id"]

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_select_from_eav_with_filters(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test select_from_eav with filters."""
        rpc_response = [
            {"id": "row1", "col1_id": 150.0, "col2_id": "ACTIVE"},
        ]
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )

        captured_params: dict[str, Any] = {}

        def mock_call_rpc(name: str, params: Any) -> Any:
            captured_params.update(params)
            return rpc_response

        client._query_table = self._mock_query_table()  # type: ignore[method-assign]
        client._call_rpc = mock_call_rpc  # type: ignore[assignment, method-assign]

        filters = [
            EAVFilter(column_name="Column 1", numeric_min=100, numeric_max=200),
            EAVFilter(column_name="Column 2", string_value="ACTIVE"),
        ]

        result = client.select_from_eav(
            tenant_id=1,
            table_name="Test Table",
            filters=filters,
        )

        assert len(result) == 1
        assert result[0]["Column 1"] == 150.0
        # Filters should use column IDs in RPC call
        assert captured_params["filters"] == [
            {"column_id": "col1_id", "numeric_min": 100, "numeric_max": 200},
            {"column_id": "col2_id", "string_value": "ACTIVE"},
        ]

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_select_from_eav_with_boolean_filter(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test select_from_eav with boolean filter."""
        rpc_response = [
            {"id": "row1", "is_active_id": True},
        ]
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )

        captured_params: dict[str, Any] = {}

        def mock_call_rpc(name: str, params: Any) -> Any:
            captured_params.update(params)
            return rpc_response

        client._query_table = self._mock_query_table()  # type: ignore[method-assign]
        client._call_rpc = mock_call_rpc  # type: ignore[assignment, method-assign]

        filters = [EAVFilter(column_name="Is Active", boolean_value=True)]

        result = client.select_from_eav(
            tenant_id=1,
            table_name="Test Table",
            filters=filters,
        )

        assert len(result) == 1
        assert captured_params["filters"] == [
            {"column_id": "is_active_id", "boolean_value": True}
        ]

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_select_from_eav_empty_result(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test select_from_eav with empty result."""
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._query_table = self._mock_query_table()  # type: ignore[method-assign]
        client._call_rpc = lambda *args, **kwargs: []  # type: ignore[method-assign]

        result = client.select_from_eav(
            tenant_id=1,
            table_name="Test Table",
        )

        assert result == []

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_select_from_eav_table_not_found(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test select_from_eav raises error when table not found."""
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._query_table = lambda *args, **kwargs: []  # type: ignore[method-assign]

        with pytest.raises(ValueError, match="Table 'Unknown Table' not found"):
            client.select_from_eav(
                tenant_id=1,
                table_name="Unknown Table",
            )

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_select_from_eav_column_not_found(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test select_from_eav raises error when column not found."""
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._query_table = self._mock_query_table()  # type: ignore[method-assign]

        with pytest.raises(
            ValueError, match="Column 'Unknown Column' not found in table 'Test Table'"
        ):
            client.select_from_eav(
                tenant_id=1,
                table_name="Test Table",
                column_names=["Unknown Column"],
            )


class TestCVecSelectFromEAVId:
    """Tests for select_from_eav_id using table_id and column_ids directly."""

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_select_from_eav_id_basic(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test select_from_eav_id with no filters."""
        rpc_response = [
            {"id": "row1", "col1_id": 100.5, "col2_id": "value1"},
            {"id": "row2", "col1_id": 200.0, "col2_id": "value2"},
        ]
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._call_rpc = lambda *args, **kwargs: rpc_response  # type: ignore[method-assign]

        result = client.select_from_eav_id(
            tenant_id=1,
            table_id="7a80f3a2-6fa1-43ce-8483-76bd00dc93c6",
        )

        # Result keeps column IDs (no name translation)
        assert len(result) == 2
        assert result[0]["id"] == "row1"
        assert result[0]["col1_id"] == 100.5
        assert result[1]["col2_id"] == "value2"

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_select_from_eav_id_with_column_ids(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test select_from_eav_id with specific column_ids."""
        rpc_response = [
            {"id": "row1", "col1_id": 100.5},
        ]
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )

        captured_params: dict[str, Any] = {}

        def mock_call_rpc(name: str, params: Any) -> Any:
            captured_params.update(params)
            return rpc_response

        client._call_rpc = mock_call_rpc  # type: ignore[assignment, method-assign]

        result = client.select_from_eav_id(
            tenant_id=1,
            table_id="7a80f3a2-6fa1-43ce-8483-76bd00dc93c6",
            column_ids=["col1_id"],
        )

        assert len(result) == 1
        assert captured_params["column_ids"] == ["col1_id"]

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_select_from_eav_id_with_filters(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test select_from_eav_id with filters using column_id."""
        rpc_response = [
            {"id": "row1", "col1_id": 150.0, "col2_id": "ACTIVE"},
        ]
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )

        captured_params: dict[str, Any] = {}

        def mock_call_rpc(name: str, params: Any) -> Any:
            captured_params.update(params)
            return rpc_response

        client._call_rpc = mock_call_rpc  # type: ignore[assignment, method-assign]

        filters = [
            EAVFilter(column_id="col1_id", numeric_min=100, numeric_max=200),
            EAVFilter(column_id="col2_id", string_value="ACTIVE"),
        ]

        result = client.select_from_eav_id(
            tenant_id=1,
            table_id="7a80f3a2-6fa1-43ce-8483-76bd00dc93c6",
            filters=filters,
        )

        assert len(result) == 1
        assert captured_params["filters"] == [
            {"column_id": "col1_id", "numeric_min": 100, "numeric_max": 200},
            {"column_id": "col2_id", "string_value": "ACTIVE"},
        ]

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_select_from_eav_id_rejects_column_name_filter(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test select_from_eav_id raises error when filter uses column_name."""
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )

        filters = [EAVFilter(column_name="Column 1", numeric_min=100)]

        with pytest.raises(
            ValueError, match="Filters for select_from_eav_id must use column_id"
        ):
            client.select_from_eav_id(
                tenant_id=1,
                table_id="7a80f3a2-6fa1-43ce-8483-76bd00dc93c6",
                filters=filters,
            )

    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_fetch_publishable_key", return_value="test_publishable_key")
    def test_select_from_eav_id_empty_result(
        self, mock_fetch_key: Any, mock_login: Any
    ) -> None:
        """Test select_from_eav_id with empty result."""
        client = CVec(
            host="test_host",
            api_key="cva_hHs0CbkKALxMnxUdI9hanF0TBPvvvr1HjG6O",
        )
        client._call_rpc = lambda *args, **kwargs: []  # type: ignore[method-assign]

        result = client.select_from_eav_id(
            tenant_id=1,
            table_id="7a80f3a2-6fa1-43ce-8483-76bd00dc93c6",
        )

        assert result == []
