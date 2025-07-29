import random
from cvec import CVec
from datetime import datetime, timedelta, timezone
import os
import io
import pyarrow.ipc as ipc  # type: ignore[import-untyped]

from cvec.models.metric import MetricDataPoint


def main() -> None:
    cvec = CVec(
        host=os.environ.get(
            "CVEC_HOST", "https://your-subdomain.cvector.dev"
        ),
        api_key=os.environ.get("CVEC_API_KEY", "your-api-key"),
    )
    test_metric_name = ""

    # fetch & pick metrics
    metrics = cvec.get_metrics(
        start_at=datetime(2025, 7, 14, 10, 0, 0),
        end_at=datetime(2025, 7, 14, 11, 0, 0),
    )
    print(f"Found {len(metrics)} metrics")
    for metric in metrics:
        print(f"- {metric.name} - {metric.id}")
    if metrics:
        found_metric_name = next(
            (m.name for m in metrics if "Sensor_" in m.name)
        )
        assert found_metric_name, "No suitable metric found"
        test_metric_name = found_metric_name
    print(f"\nUsing metric: {test_metric_name}")

    # Add metric non-Arrow data
    random_number_nonarrow = random.randint(10000, 20000)
    print(
        f"\nAdding new metric data point with non-Arrow format for metric "
        f"'{test_metric_name}' and values {random_number_nonarrow}..."
    )
    new_data = [
        MetricDataPoint(
            name=test_metric_name,
            time=datetime.now(timezone.utc),
            value_double=random_number_nonarrow,
            value_string=None,
        ),
        MetricDataPoint(
            name=test_metric_name,
            time=datetime.now(timezone.utc),
            value_double=None,
            value_string=str(random_number_nonarrow),
        ),
    ]
    cvec.add_metric_data(new_data, use_arrow=False)
    print("Non-Arrow Data added successfully")

    # Add metric Arrow data

    random_number_arrow = random.randint(10000, 20000)
    print(
        f"\nAdding new metric data point with Arrow format for metric "
        f"'{test_metric_name}' and value {random_number_arrow}..."
    )
    new_data = [
        MetricDataPoint(
            name=test_metric_name,
            time=datetime.now(timezone.utc),
            value_double=random_number_arrow,
            value_string=None,
        ),
    ]
    cvec.add_metric_data(new_data, use_arrow=True)
    print("Arrow Data added successfully")

    # Fetch and print metric data - non-Arrow
    data_points = cvec.get_metric_data(
        start_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        end_at=datetime.now(timezone.utc),
        names=[test_metric_name],
    )
    assert len(data_points) > 0, "No data points found for the metric"
    assert any(
        dp.value_double == random_number_nonarrow for dp in data_points
    ), "No data point found with the expected non-Arrow value"
    assert any(
        dp.value_string == str(random_number_nonarrow) for dp in data_points
    ), "No data point found with the expected non-Arrow string value"
    assert any(
        dp.value_double == random_number_arrow for dp in data_points
    ), "No data point found with the expected Arrow value"
    print(
        f"\nFound {len(data_points)} data points for metric '{test_metric_name}'"
    )
    for point in data_points:
        print(
            f"- {point.name}: {point.value_double or point.value_string} at {point.time}"
        )

    # Fetch and print metric data - Arrow
    arrow_data = cvec.get_metric_arrow(
        start_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        end_at=datetime.now(timezone.utc),
        names=[test_metric_name],
    )
    reader = ipc.open_file(io.BytesIO(arrow_data))
    table = reader.read_all()
    assert len(table) > 0, "No data found in Arrow format"
    print(f"Arrow table shape: {len(table)} rows")
    print("\nFirst few rows:")
    for i in range(min(5, len(table))):
        print(
            f"- {table['name'][i].as_py()}: {table['value_double'][i].as_py() or table['value_string'][i].as_py()} at {table['time'][i].as_py()}"
        )

    # spans
    spans = cvec.get_spans(
        start_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        end_at=datetime.now(timezone.utc),
        name=test_metric_name,
        limit=5,
    )
    assert len(spans) > 0, "No spans found for the metric"
    print(f"Found {len(spans)} spans")
    for span in spans:
        print(
            f"- Value: {span.value} from {span.raw_start_at} to {span.raw_end_at}"
        )

    print("\nAll operations completed successfully.")


if __name__ == "__main__":
    main()
