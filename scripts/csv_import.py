#!/usr/bin/env python3
"""
CSV import script for CVector Energy.

This script imports CSV data with a header row containing metric names.
The first column must be 'timestamp' and subsequent columns are treated as metrics.
"""

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from cvec import CVec
from cvec.models.metric import MetricDataPoint


def parse_timestamp(timestamp_str: str) -> datetime:
    """Parse timestamp from CSV, supporting common formats."""
    # Try different timestamp formats
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
        "%m/%d/%y %H:%M",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue

    # If all formats fail, try to parse as ISO format
    try:
        return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"Unable to parse timestamp: {timestamp_str}")


def parse_value(value_str: str) -> tuple[Optional[float], Optional[str]]:
    """Parse a value from CSV, returning (value_double, value_string)."""
    if not value_str.strip():
        return None, None

    # Try to parse as a number
    try:
        return float(value_str), None
    except ValueError:
        # If it's not a number, treat as string
        return None, value_str


def import_csv(
    csv_path: Path,
    cvec_client: CVec,
    metric_prefix: Optional[str] = None,
) -> None:
    """Import CSV data into CVec."""
    print(f"Reading CSV file: {csv_path}")

    with open(csv_path, "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)

        if not reader.fieldnames:
            raise ValueError("CSV file appears to be empty or malformed")

        # Find the timestamp column (case-insensitive)
        try:
            timestamp_col = next(
                col for col in reader.fieldnames if col.lower() == "timestamp"
            )
        except StopIteration:
            raise ValueError("CSV must have a 'timestamp' column")

        # Get metric names (all columns except timestamp)
        metric_names = [col for col in reader.fieldnames if col != timestamp_col]

        if not metric_names:
            raise ValueError("No metric columns found in CSV")

        print(f"Found metrics: {', '.join(metric_names)}")
        if metric_prefix:
            print(f"Using prefix: {metric_prefix}")

        # Process all rows and collect data points
        data_points: List[MetricDataPoint] = []
        row_count = 0

        for row in reader:
            row_count += 1

            try:
                timestamp = parse_timestamp(row[timestamp_col])
            except ValueError as e:
                print(f"Warning: Skipping row {row_count} due to timestamp error: {e}")
                continue

            # Process each metric in this row
            for metric_name in metric_names:
                value_str = row.get(metric_name, "")

                if not value_str.strip():
                    continue  # Skip empty values

                value_double, value_string = parse_value(value_str)

                # Apply prefix if specified
                full_metric_name = metric_name
                if metric_prefix:
                    full_metric_name = f"{metric_prefix}/{metric_name}"

                data_points.append(
                    MetricDataPoint(
                        name=full_metric_name,
                        time=timestamp,
                        value_double=value_double,
                        value_string=value_string,
                    )
                )

        if not data_points:
            print("No valid data points found in CSV")
            return

        print(f"Processed {row_count} rows, found {len(data_points)} data points")
        print("Uploading data to CVec...")

        # Upload data to CVec
        cvec_client.add_metric_data(data_points)
        print("Data successfully uploaded to CVec!")


def main() -> None:
    """Main entry point for the CSV import script."""
    parser = argparse.ArgumentParser(
        description="Import CSV data into CVector Energy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python csv_import.py data.csv
  python csv_import.py data.csv --prefix "sensors/building1"
  python csv_import.py data.csv --prefix "weather"

CSV format:
  The CSV must have a header row with 'timestamp' as the first column.
  Subsequent columns are treated as metric names.

  Example:
    timestamp,rain_rate,actual_inflow,predicted_inflow
    2025-01-01 00:00:00,0.5,100.2,95.8
    2025-01-01 01:00:00,1.2,150.5,145.3
        """,
    )

    parser.add_argument(
        "csv_file",
        type=Path,
        help="Path to the CSV file to import",
    )

    parser.add_argument(
        "--prefix",
        type=str,
        help="Prefix to add to metric names (separated by '/')",
    )

    parser.add_argument(
        "--host",
        type=str,
        help="CVec host URL (overrides CVEC_HOST environment variable)",
    )

    parser.add_argument(
        "--api-key",
        type=str,
        help="CVec API key (overrides CVEC_API_KEY environment variable)",
    )

    args = parser.parse_args()

    # Validate CSV file exists
    if not args.csv_file.exists():
        print(f"Error: CSV file '{args.csv_file}' does not exist")
        sys.exit(1)

    if not args.csv_file.is_file():
        print(f"Error: '{args.csv_file}' is not a file")
        sys.exit(1)

    try:
        # Initialize CVec client
        cvec_client = CVec(
            host=args.host,
            api_key=args.api_key,
        )

        # Import the CSV
        import_csv(
            csv_path=args.csv_file,
            cvec_client=cvec_client,
            metric_prefix=args.prefix,
        )

    except HTTPError as e:
        print(f"Error: {e}")
        # Display CloudFront ID if available
        if hasattr(e, "headers"):
            cf_id = e.headers.get("x-amz-cf-id")
            if cf_id:
                print(f"Cf-Id: {cf_id}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
