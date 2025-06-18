from datetime import datetime, timedelta, timezone
import io
import pyarrow.ipc as ipc
from cvec import CVec
from cvec.models.metric import MetricDataPoint
import random

def test_cvec():
    # Initialize CVec client
    cvec = CVec(
        host="https://cvec-backend-rzhang-cvec-sandbox.deployments.quix.io",  # Replace with your API host
        tenant="test",  # Replace with your tenant
        api_key="your-api-key",  # Replace with your API key
    )

    test_metric_name = "python-sdk/test"

    # Example 1: Get available metrics
    print("\nGetting available metrics...")
    metrics = cvec.get_metrics()
    print(f"Found {len(metrics)} metrics")
    for metric in metrics:  # Print first 5 metrics
        print(f"- {metric.name}")

    # Example 2: Get metric data as Arrow
    print("\nGetting metric data as Arrow...")
    arrow_data = cvec.get_metric_arrow(names=[test_metric_name])
    
    # Read the Arrow data
    reader = ipc.open_file(io.BytesIO(arrow_data))
    table = reader.read_all()
    
    print(f"Arrow table shape: {len(table)} rows")
    print("\nFirst few rows:")
    for i in range(min(5, len(table))):
        print(f"- {table['name'][i].as_py()}: {table['value_double'][i].as_py() or table['value_string'][i].as_py()} at {table['time'][i].as_py()}")

    # Example 3: Get metric data as objects
    print("\nGetting metric data as objects...")
    data_points = cvec.get_metric_data(names=[test_metric_name])
    print(f"Found {len(data_points)} data points")
    for point in data_points[:3]:  # Print first 3 data points
        print(f"- {point.name}: {point.value_double or point.value_string} at {point.time}")

    # Example 4: Get spans for a specific metric
    if metrics:
        metric_name = metrics[0].name
        print(f"\nGetting spans for metric '{metric_name}'...")
        spans = cvec.get_spans(metric_name, limit=5)
        print(f"Found {len(spans)} spans")
        for span in spans:
            print(f"- Value: {span.value} from {span.raw_start_at} to {span.raw_end_at}")

    # Example 5: Add new metric data
    print("\nAdding new metric data...")
    new_data = [
        MetricDataPoint(
            name=test_metric_name,
            time=datetime.now(timezone.utc),
            value_double=random.random() * 100.0,
            value_string=None,
        ),
        MetricDataPoint(
            name=test_metric_name,
            time=datetime.now(timezone.utc),
            value_double=random.random() * 100.0,
            value_string=None,
        ),
    ]
    cvec.add_metric_data(new_data, use_arrow=False)
    print("Data added successfully")

    # Example 6: Add new metric data using Arrow
    print("\nAdding new metric data using Arrow...")
    new_data = [
        MetricDataPoint(
            name=test_metric_name,
            time=datetime.now(timezone.utc),
            value_double=random.random() * 100.0,
            value_string=None,
        ),
        MetricDataPoint(
            name=test_metric_name,
            time=datetime.now(timezone.utc),
            value_double=random.random() * 100.0,
            value_string=None,
        ),
    ]
    cvec.add_metric_data(new_data, use_arrow=True)
    print("Data added successfully")
    


if __name__ == "__main__":
    test_cvec()