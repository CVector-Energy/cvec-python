from datetime import datetime, timedelta, timezone
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

    # Set default time range for queries
    end_at = datetime.utcnow()
    start_at = end_at - timedelta(hours=1)

    test_metric_name = "python-sdk/test"

    # # Example 1: Get available metrics
    # print("\nGetting available metrics...")
    # metrics = cvec.get_metrics()
    # print(f"Found {len(metrics)} metrics")
    # for metric in metrics:  # Print first 5 metrics
    #     print(f"- {metric.name}")

    # # Example 2: Get metric data as DataFrame (using JSON)
    # print("\nGetting metric data as DataFrame (JSON)...")
    # df_json = cvec.get_metric_dataframe(names=[test_metric_name], use_arrow=False)
    # print(f"DataFrame shape: {df_json.shape}")
    # print("\nFirst few rows:")
    # print(df_json.head())

    # # Example 3: Get metric data as DataFrame (using Arrow)
    # print("\nGetting metric data as DataFrame (Arrow)...")
    # df_arrow = cvec.get_metric_dataframe(names=[test_metric_name], use_arrow=True)
    # print(f"DataFrame shape: {df_arrow.shape}")
    # print("\nFirst few rows:")
    # print(df_arrow.head())

    # # Example 4: Get metric data as objects
    # print("\nGetting metric data as objects...")
    # data_points = cvec.get_metric_data(names=[test_metric_name], use_arrow=True)
    # print(f"Found {len(data_points)} data points")
    # for point in data_points[:3]:  # Print first 3 data points
    #     print(f"- {point.name}: {point.value_double or point.value_string} at {point.time}")

    # # Example 5: Get spans for a specific metric
    # if metrics:
    #     metric_name = metrics[0].name
    #     print(f"\nGetting spans for metric '{metric_name}'...")
    #     spans = cvec.get_spans(metric_name, limit=5)
    #     print(f"Found {len(spans)} spans")
    #     for span in spans:
    #         print(f"- Value: {span.value} from {span.raw_start_at} to {span.raw_end_at}")

    # Example 6: Add new metric data
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

    # Example 7: Add new metric data
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