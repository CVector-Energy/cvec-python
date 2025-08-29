#!/usr/bin/env python3
"""
End-to-end test script for the modeling API functionality.

This script tests the real API calls to verify that:
1. The modeling endpoints are accessible
2. Authentication works properly
3. Data can be retrieved from the modeling database
4. The response format matches expectations

Make sure to set the following environment variables:
- CVEC_HOST: Your CVec backend host URL (e.g., https://your-tenant.cvector.dev)
- CVEC_API_KEY: Your CVec API key (must start with 'cva_' and be 40 characters)

Usage:
    python examples/test_modeling_api_end_to_end.py
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List

from cvec import CVec
from cvec.models.modeling import (
    ModelingReadingsDataResponse,
    ModelingReadingsGroup,
    LatestReadingsResponse,
)


def test_connection_and_auth(cvec: CVec) -> bool:
    """Test basic connection and authentication."""
    print("Testing connection and authentication...")
    
    try:
        # Try to get metrics to verify authentication works
        metrics = cvec.get_metrics()
        print(f"SUCCESS: Authentication successful! Retrieved {len(metrics)} metrics")
        return True
    except Exception as e:
        print(f"ERROR: Authentication failed: {e}")
        return False


def test_modeling_readings_endpoint(cvec: CVec) -> bool:
    """Test the modeling readings endpoint."""
    print("\nTesting modeling readings endpoint...")
    
    try:
        # Set time range (last 24 hours)
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=24)
        
        # Use some common tag IDs (you may need to adjust these)
        test_tag_ids = [1, 2, 3]
        
        print(f"   Requesting data for tag IDs: {test_tag_ids}")
        print(f"   Time range: {start_date} to {end_date}")
        
        # Make the API call
        response = cvec.get_modeling_readings(
            tag_ids=test_tag_ids,
            start_at=start_date,
            end_at=end_date,
            desired_points=100,  # Small number for testing
        )
        
        # Verify response structure
        assert isinstance(response, ModelingReadingsDataResponse), "Response should be ModelingReadingsDataResponse"
        print(f"SUCCESS: Modeling readings endpoint working! Retrieved {len(response.items)} tag groups")
        
        # Print details about the response
        for i, group in enumerate(response.items):
            print(f"   Tag {group.tag_id}: {len(group.data)} data points, source: {group.source}")
            if group.data:
                first_point = group.data[0]
                last_point = group.data[-1]
                print(f"     First: {datetime.fromtimestamp(first_point.timestamp)} = {first_point.tag_value}")
                print(f"     Last:  {datetime.fromtimestamp(last_point.timestamp)} = {last_point.tag_value}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Modeling readings endpoint failed: {e}")
        if "403" in str(e):
            print("   This might be a permission issue - check if your API key has VISUALIZATION_TRENDS_READ permission")
        elif "404" in str(e):
            print("   This might mean the modeling endpoint doesn't exist or isn't accessible")
        elif "500" in str(e):
            print("   This might be a server-side error - check the backend logs")
        return False


def test_latest_readings_endpoint(cvec: CVec) -> bool:
    """Test the latest modeling readings endpoint."""
    print("\nTesting latest modeling readings endpoint...")
    
    try:
        # Use some common tag IDs (you may need to adjust these)
        test_tag_ids = [1, 2, 3]
        
        print(f"   Requesting latest readings for tag IDs: {test_tag_ids}")
        
        # Make the API call
        response = cvec.get_modeling_latest_readings(tag_ids=test_tag_ids)
        
        # Verify response structure
        assert isinstance(response, LatestReadingsResponse), "Response should be LatestReadingsResponse"
        print(f"SUCCESS: Latest readings endpoint working! Retrieved {len(response.items)} latest readings")
        
        # Print details about the response
        for item in response.items:
            print(f"   Tag {item.tag_id}: Value {item.tag_value} at {item.tag_value_changed_at}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Latest readings endpoint failed: {e}")
        if "403" in str(e):
            print("   This might be a permission issue - check if your API key has TAG_DIRECTORIES_READ permission")
        return False


def test_error_handling(cvec: CVec) -> bool:
    """Test error handling with invalid requests."""
    print("\nTesting error handling...")
    
    try:
        # Test with invalid tag IDs (negative numbers)
        print("   Testing with invalid tag IDs...")
        response = cvec.get_modeling_readings(
            tag_ids=[-1, -2],  # Invalid negative tag IDs
            start_at=datetime.now() - timedelta(hours=1),
            end_at=datetime.now(),
            desired_points=10,
        )
        print("   ERROR: Should have failed with invalid tag IDs")
        return False
        
    except Exception as e:
        print(f"   SUCCESS: Properly handled invalid request: {e}")
        
    try:
        # Test with invalid date range (end before start)
        print("   Testing with invalid date range...")
        response = cvec.get_modeling_readings(
            tag_ids=[1, 2],
            start_at=datetime.now(),
            end_at=datetime.now() - timedelta(hours=1),  # End before start
            desired_points=10,
        )
        print("   ERROR: Should have failed with invalid date range")
        return False
        
    except Exception as e:
        print(f"   SUCCESS: Properly handled invalid date range: {e}")
    
    return True


def main():
    """Main test function."""
    print("Starting end-to-end test of modeling API functionality")
    print("=" * 60)
    
    # Check environment variables
    host = os.environ.get("CVEC_HOST")
    api_key = os.environ.get("CVEC_API_KEY")
    
    if not host:
        print("ERROR: CVEC_HOST environment variable not set")
        print("   Please set it to your CVec backend host (e.g., https://your-tenant.cvector.dev)")
        sys.exit(1)
    
    if not api_key:
        print("ERROR: CVEC_API_KEY environment variable not set")
        print("   Please set it to your CVec API key (must start with 'cva_' and be 40 characters)")
        sys.exit(1)
    
    if not api_key.startswith("cva_") or len(api_key) != 40:
        print("ERROR: Invalid API key format")
        print("   API key must start with 'cva_' and be exactly 40 characters")
        print(f"   Current key: {api_key[:10]}... (length: {len(api_key)})")
        sys.exit(1)
    
    print(f"Host: {host}")
    print(f"API Key: {api_key[:10]}...")
    print("=" * 60)
    
    try:
        # Initialize CVec client
        print("Initializing CVec client...")
        cvec = CVec(host=host, api_key=api_key)
        print("SUCCESS: CVec client initialized successfully")
        
        # Run tests
        tests = [
            ("Connection & Authentication", test_connection_and_auth),
            ("Modeling Readings Endpoint", test_modeling_readings_endpoint),
            ("Latest Readings Endpoint", test_latest_readings_endpoint),
            ("Error Handling", test_error_handling),
        ]
        
        results = []
        for test_name, test_func in tests:
            try:
                result = test_func(cvec)
                results.append((test_name, result))
            except Exception as e:
                print(f"ERROR: Test '{test_name}' crashed: {e}")
                results.append((test_name, False))
        
        # Print summary
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)
        
        passed = 0
        total = len(results)
        
        for test_name, result in results:
            status = "PASS" if result else "FAIL"
            print(f"{status} {test_name}")
            if result:
                passed += 1
        
        print(f"\nResults: {passed}/{total} tests passed")
        
        if passed == total:
            print("ALL TESTS PASSED! The modeling API is working correctly.")
            print("\nYou can now use the modeling functionality in your applications:")
            print("  - cvec.get_modeling_readings() for historical data")
            print("  - cvec.get_modeling_latest_readings() for current values")
        else:
            print("Some tests failed. Check the output above for details.")
            print("   Common issues:")
            print("   - Permission problems (check API key permissions)")
            print("   - Network connectivity issues")
            print("   - Backend service not running")
            print("   - Invalid tag IDs (use actual tag IDs from your system)")
        
        return passed == total
        
    except Exception as e:
        print(f"ERROR: Fatal error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
