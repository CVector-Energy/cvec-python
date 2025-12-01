"""
Tests for the modeling functionality in the CVec client.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch


from cvec.cvec import CVec


def mock_fetch_config_side_effect(instance: CVec) -> str:
    """Side effect for _fetch_config mock that sets tenant_id."""
    instance._tenant_id = 1
    return "test_publishable_key"


class TestModelingMethods:
    """Test the modeling methods in the CVec class."""

    @patch.object(
        CVec, "_fetch_config", autospec=True, side_effect=mock_fetch_config_side_effect
    )
    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_make_request")
    def test_get_modeling_metrics(
        self, mock_make_request: Mock, mock_login: Mock, mock_fetch_key: Mock
    ) -> None:
        """Test get_modeling_metrics method."""

        # Mock the response
        mock_response = [
            {
                "id": 1,
                "name": "test_metric",
                "birth_at": "2024-01-01T12:00:00",
                "death_at": None,
            }
        ]
        mock_make_request.return_value = mock_response

        # Create CVec instance
        cvec = CVec(
            host="http://test.com", api_key="cva_test12345678901234567890123456789012"
        )

        # Call the method
        start_date = datetime(2024, 1, 1, 12, 0, 0)
        end_date = datetime(2024, 1, 1, 13, 0, 0)
        result = cvec.get_modeling_metrics(
            start_at=start_date,
            end_at=end_date,
        )

        # Verify the result
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].id == 1
        assert result[0].name == "test_metric"

        # Verify the request was made correctly
        mock_make_request.assert_called_once()
        call_args = mock_make_request.call_args
        assert call_args[0][0] == "GET"
        assert call_args[0][1] == "/api/modeling/metrics"
        assert call_args[1]["params"]["start_at"] == "2024-01-01T12:00:00"
        assert call_args[1]["params"]["end_at"] == "2024-01-01T13:00:00"

    @patch.object(
        CVec, "_fetch_config", autospec=True, side_effect=mock_fetch_config_side_effect
    )
    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_make_request")
    def test_get_modeling_metrics_data(
        self, mock_make_request: Mock, mock_login: Mock, mock_fetch_key: Mock
    ) -> None:
        """Test get_modeling_metrics_data method."""

        # Mock the response
        mock_response = [
            {
                "name": "test_metric",
                "time": "2024-01-01T12:00:00",
                "value_double": 42.5,
                "value_string": None,
            }
        ]
        mock_make_request.return_value = mock_response

        # Create CVec instance
        cvec = CVec(
            host="http://test.com", api_key="cva_test12345678901234567890123456789012"
        )

        # Call the method
        start_date = datetime(2024, 1, 1, 12, 0, 0)
        end_date = datetime(2024, 1, 1, 13, 0, 0)
        result = cvec.get_modeling_metrics_data(
            names=["test_metric"],
            start_at=start_date,
            end_at=end_date,
        )

        # Verify the result
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].name == "test_metric"
        assert result[0].value_double == 42.5

        # Verify the request was made correctly
        mock_make_request.assert_called_once()
        call_args = mock_make_request.call_args
        assert call_args[0][0] == "GET"
        assert call_args[0][1] == "/api/modeling/metrics/data"
        assert call_args[1]["params"]["names"] == "test_metric"
        assert call_args[1]["params"]["start_at"] == "2024-01-01T12:00:00"
        assert call_args[1]["params"]["end_at"] == "2024-01-01T13:00:00"

    @patch.object(
        CVec, "_fetch_config", autospec=True, side_effect=mock_fetch_config_side_effect
    )
    @patch.object(CVec, "_login_with_supabase", return_value=None)
    @patch.object(CVec, "_make_request")
    def test_get_modeling_metrics_data_arrow(
        self, mock_make_request: Mock, mock_login: Mock, mock_fetch_key: Mock
    ) -> None:
        """Test get_modeling_metrics_data_arrow method."""

        # Mock the response (Arrow data as bytes)
        mock_response = b"fake_arrow_data"
        mock_make_request.return_value = mock_response

        # Create CVec instance
        cvec = CVec(
            host="http://test.com", api_key="cva_test12345678901234567890123456789012"
        )

        # Call the method
        start_date = datetime(2024, 1, 1, 12, 0, 0)
        end_date = datetime(2024, 1, 1, 13, 0, 0)
        result = cvec.get_modeling_metrics_data_arrow(
            names=["test_metric"],
            start_at=start_date,
            end_at=end_date,
        )

        # Verify the result
        assert isinstance(result, bytes)
        assert result == b"fake_arrow_data"

        # Verify the request was made correctly
        mock_make_request.assert_called_once()
        call_args = mock_make_request.call_args
        assert call_args[0][0] == "GET"
        assert call_args[0][1] == "/api/modeling/metrics/data/arrow"
        assert call_args[1]["params"]["names"] == "test_metric"
        assert call_args[1]["params"]["start_at"] == "2024-01-01T12:00:00"
        assert call_args[1]["params"]["end_at"] == "2024-01-01T13:00:00"


if __name__ == "__main__":
    pytest.main([__file__])
