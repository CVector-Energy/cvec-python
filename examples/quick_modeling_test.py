#!/usr/bin/env python3
"""
Quick test script for the modeling API functionality.

This is a simplified version for quick testing. It makes real API calls
to verify the modeling endpoints are working.

Set these environment variables:
- CVEC_HOST: Your CVec backend host (e.g., https://your-tenant.cvector.dev)
- CVEC_API_KEY: Your CVec API key

Usage:
    python examples/quick_modeling_test.py
"""

import os
from datetime import datetime, timedelta

from cvec import CVec


def main():
    """Quick test of modeling API."""
    print("Quick Modeling API Test")
    print("=" * 40)
    
    # Get environment variables
    host = os.environ.get("CVEC_HOST")
    api_key = os.environ.get("CVEC_API_KEY")
    host = "https://sandbox.cvector.dev"
    api_key = "cva_1cFKZhgZbRzbVQHQjpGLNMUSqyphh31AiVP8"
    
    if not host or not api_key:
        print("ERROR: Please set CVEC_HOST and CVEC_API_KEY environment variables")
        return
    
    try:
        # Initialize client
        print("Initializing CVec client...")
        cvec = CVec(host=host, api_key=api_key)
        print("SUCCESS: Client initialized")
        
        # Test 1: Basic authentication
        print("\nTesting authentication...")
        metrics = cvec.get_metrics()
        print(f"SUCCESS: Auth OK - got {len(metrics)} metrics")
        
        # Test 2: Modeling readings
        print("\nTesting modeling readings...")
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=3)  # Last hour
        
        response = cvec.get_modeling_readings(
            tag_ids=[63],  # Common tag IDs
            start_at=start_date,
            end_at=end_date,
            desired_points=50,
        )
        print(response)
        
        print(f"SUCCESS: Modeling readings OK - got {len(response.items)} tag groups")
        for group in response.items:
            print(f"   Tag {group.tag_id}: {len(group.data)} points")
        
        # Test 3: Latest readings
        # print("\nTesting latest readings...")
        # latest = cvec.get_modeling_latest_readings(tag_ids=[1, 2])
        # print(f"SUCCESS: Latest readings OK - got {len(latest.items)} readings")
        
        print("\nALL TESTS PASSED! Modeling API is working.")
        
    except Exception as e:
        print(f"\nERROR: Test failed: {e}")
        print("\nCommon issues:")
        print("- Check your API key permissions")
        print("- Verify the backend is running")
        print("- Check network connectivity")
        print("- Use valid tag IDs from your system")


if __name__ == "__main__":
    main()
