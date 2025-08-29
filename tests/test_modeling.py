"""
Tests for the modeling functionality in the CVec client.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch

from cvec.models.modeling import (
    FetchModelingReadingsRequest,
    ModelingReadingsDataResponse,
    ModelingReadingsGroup,
    ModelingReadingModel,
    LatestReadingsRequest,
    LatestReadingsResponse,
    LatestReadingsResponseItem,
    TagSourceType,
)
from cvec.cvec import CVec


class TestModelingModels:
    """Test the modeling data models."""

    def test_fetch_modeling_readings_request(self):
        """Test FetchModelingReadingsRequest model."""
        request = FetchModelingReadingsRequest(
            tag_ids=[1, 2, 3],
            start_date=datetime(2024, 1, 1, 12, 0, 0),
            end_date=datetime(2024, 1, 1, 13, 0, 0),
            desired_points=1000,
        )
        
        assert request.tag_ids == [1, 2, 3]
        assert request.desired_points == 1000
        assert request.model_dump()["tag_ids"] == [1, 2, 3]

    def test_modeling_reading_model(self):
        """Test ModelingReadingModel model."""
        reading = ModelingReadingModel(
            tag_id=1,
            tag_value=42.5,
            timestamp=1704110400.0,  # 2024-01-01 12:00:00 UTC
        )
        
        assert reading.tag_id == 1
        assert reading.tag_value == 42.5
        assert reading.timestamp == 1704110400.0

    def test_modeling_readings_group(self):
        """Test ModelingReadingsGroup model."""
        readings = [
            ModelingReadingModel(tag_id=1, tag_value=10.0, timestamp=1704110400.0),
            ModelingReadingModel(tag_id=1, tag_value=15.0, timestamp=1704114000.0),
        ]
        
        group = ModelingReadingsGroup(
            tag_id=1,
            data=readings,
            source=TagSourceType.Modeling,
        )
        
        assert group.tag_id == 1
        assert len(group.data) == 2
        assert group.source == TagSourceType.Modeling

    def test_modeling_readings_data_response(self):
        """Test ModelingReadingsDataResponse model."""
        readings = [
            ModelingReadingsGroup(
                tag_id=1,
                data=[
                    ModelingReadingModel(tag_id=1, tag_value=10.0, timestamp=1704110400.0),
                ],
                source=TagSourceType.Modeling,
            ),
        ]
        
        response = ModelingReadingsDataResponse(items=readings)
        assert len(response.items) == 1
        assert response.items[0].tag_id == 1

    def test_latest_readings_request(self):
        """Test LatestReadingsRequest model."""
        request = LatestReadingsRequest(tag_ids=[1, 2, 3])
        assert request.tag_ids == [1, 2, 3]

    def test_latest_readings_response(self):
        """Test LatestReadingsResponse model."""
        items = [
            LatestReadingsResponseItem(
                tag_id=1,
                tag_value=42.5,
                tag_value_changed_at=datetime(2024, 1, 1, 12, 0, 0),
            ),
        ]
        
        response = LatestReadingsResponse(items=items)
        assert len(response.items) == 1
        assert response.items[0].tag_id == 1
        assert response.items[0].tag_value == 42.5


class TestModelingMethods:
    """Test the modeling methods in the CVec class."""

    @patch('cvec.cvec.CVec._fetch_publishable_key')
    @patch('cvec.cvec.CVec._login_with_supabase')
    @patch('cvec.cvec.CVec._make_request')
    def test_get_modeling_readings(self, mock_make_request, mock_login, mock_fetch_key):
        """Test get_modeling_readings method."""
        # Mock the publishable key fetch
        mock_fetch_key.return_value = "test_publishable_key"
        
        # Mock the login method
        mock_login.return_value = None
        
        # Mock the response
        mock_response = {
            "items": [
                {
                    "tag_id": 1,
                    "data": [
                        {
                            "tag_id": 1,
                            "tag_value": 10.0,
                            "timestamp": 1704110400.0,
                        }
                    ],
                    "source": "modeling",
                }
            ]
        }
        mock_make_request.return_value = mock_response
        
        # Create CVec instance
        cvec = CVec(host="http://test.com", api_key="cva_test12345678901234567890123456789012")
        
        # Call the method
        start_date = datetime(2024, 1, 1, 12, 0, 0)
        end_date = datetime(2024, 1, 1, 13, 0, 0)
        result = cvec.get_modeling_readings(
            tag_ids=[1],
            start_at=start_date,
            end_at=end_date,
            desired_points=1000,
        )
        
        # Verify the result
        assert isinstance(result, ModelingReadingsDataResponse)
        assert len(result.items) == 1
        assert result.items[0].tag_id == 1
        assert result.items[0].source == TagSourceType.Modeling
        
        # Verify the request was made correctly
        mock_make_request.assert_called_once()
        call_args = mock_make_request.call_args
        assert call_args[0][0] == "POST"
        assert call_args[0][1] == "/api/modeling/fetch_modeling_data"
        assert call_args[1]["json"]["tag_ids"] == [1]
        assert call_args[1]["json"]["start_date"] == "2024-01-01T12:00:00"
        assert call_args[1]["json"]["end_date"] == "2024-01-01T13:00:00"
        assert call_args[1]["json"]["desired_points"] == 1000

    @patch('cvec.cvec.CVec._fetch_publishable_key')
    @patch('cvec.cvec.CVec._login_with_supabase')
    @patch('cvec.cvec.CVec._make_request')
    def test_get_modeling_latest_readings(self, mock_make_request, mock_login, mock_fetch_key):
        """Test get_modeling_latest_readings method."""
        # Mock the publishable key fetch
        mock_fetch_key.return_value = "test_publishable_key"
        
        # Mock the login method
        mock_login.return_value = None
        
        # Mock the response
        mock_response = {
            "items": [
                {
                    "tag_id": 1,
                    "tag_value": 42.5,
                    "tag_value_changed_at": "2024-01-01T12:00:00",
                }
            ]
        }
        mock_make_request.return_value = mock_response
        
        # Create CVec instance
        cvec = CVec(host="http://test.com", api_key="cva_test12345678901234567890123456789012")
        
        # Call the method
        result = cvec.get_modeling_latest_readings(tag_ids=[1])
        
        # Verify the result
        assert isinstance(result, LatestReadingsResponse)
        assert len(result.items) == 1
        assert result.items[0].tag_id == 1
        assert result.items[0].tag_value == 42.5
        
        # Verify the request was made correctly
        mock_make_request.assert_called_once()
        call_args = mock_make_request.call_args
        assert call_args[0][0] == "POST"
        assert call_args[0][1] == "/api/modeling/fetch_latest_readings"
        assert call_args[1]["json"]["tag_ids"] == [1]


if __name__ == "__main__":
    pytest.main([__file__])
