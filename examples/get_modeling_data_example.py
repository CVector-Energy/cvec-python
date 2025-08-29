#!/usr/bin/env python3
"""
Example script demonstrating how to retrieve modeling data from the CVec API.

This script shows how to:
1. Fetch modeling readings for specific tag IDs within a time range
2. Get the latest modeling readings for specific tag IDs
3. Handle the response data

Make sure to set the following environment variables:
- CVEC_HOST: Your CVec backend host URL
- CVEC_API_KEY: Your CVec API key
"""

import os
from datetime import datetime, timedelta
from typing import List

from cvec import CVec
from cvec.models.modeling import ModelingReadingsGroup, ModelingReadingModel


def print_modeling_readings(readings: List[ModelingReadingsGroup]) -> None:
    """Print modeling readings in a formatted way."""
    print(f"Retrieved {len(readings)} tag groups:")
    print("-" * 50)
    
    for group in readings:
        print(f"Tag ID: {group.tag_id}")
        print(f"Source: {group.source}")
        print(f"Number of data points: {len(group.data)}")
        
        if group.data:
            # Show first and last few data points
            print("  First few data points:")
            for i, point in enumerate(group.data[:3]):
                timestamp = datetime.fromtimestamp(point.timestamp)
                print(f"    {i+1}. Time: {timestamp}, Value: {point.tag_value}")
            
            if len(group.data) > 3:
                print(f"    ... and {len(group.data) - 3} more points")
            
            # Show last data point
            last_point = group.data[-1]
            last_timestamp = datetime.fromtimestamp(last_point.timestamp)
            print(f"  Last data point: Time: {last_timestamp}, Value: {last_point.tag_value}")
        
        print("-" * 30)


def main():
    """Main function demonstrating modeling data retrieval."""
    try:
        # Initialize CVec client
        cvec = CVec()
        print(f"Connected to CVec at: {cvec.host}")
        
        # Example tag IDs (replace with actual tag IDs from your system)
        tag_ids = [1, 2, 3]
        
        # Set time range (last 24 hours)
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=24)
        
        print(f"Fetching modeling data for tag IDs: {tag_ids}")
        print(f"Time range: {start_date} to {end_date}")
        print("=" * 60)
        
        # Fetch modeling readings
        print("1. Fetching modeling readings...")
        modeling_response = cvec.get_modeling_readings(
            tag_ids=tag_ids,
            start_at=start_date,
            end_at=end_date,
            desired_points=1000,  # Limit to 1000 points for performance
        )
        
        print(f"Successfully retrieved modeling data with {len(modeling_response.items)} tag groups")
        print_modeling_readings(modeling_response.items)
        
        print("\n" + "=" * 60)
        
        # Get latest modeling readings
        print("2. Fetching latest modeling readings...")
        latest_response = cvec.get_modeling_latest_readings(tag_ids=tag_ids)
        
        print(f"Successfully retrieved latest readings for {len(latest_response.items)} tags:")
        for item in latest_response.items:
            print(f"  Tag {item.tag_id}: Value {item.tag_value} at {item.tag_value_changed_at}")
        
        print("\n" + "=" * 60)
        print("Example completed successfully!")
        
    except Exception as e:
        print(f"Error occurred: {e}")
        print("Make sure your environment variables are set correctly:")
        print("- CVEC_HOST: Your CVec backend host URL")
        print("- CVEC_API_KEY: Your CVec API key")


if __name__ == "__main__":
    main()
