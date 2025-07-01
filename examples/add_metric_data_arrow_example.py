from cvec import CVec
from cvec.models.metric import MetricDataPoint
from datetime import datetime, timezone
import random
import os


def main() -> None:
    cvec = CVec(
        host=os.environ.get("CVEC_HOST", "https://sandbox.cvector.dev"),
        email=os.environ.get("CVEC_EMAIL", "your-email@example.com"),
        password=os.environ.get("CVEC_PASSWORD", "your-password"),
        publishable_key=os.environ.get("CVEC_PUBLISHABLE_KEY", "your-supabase-publishable-key"),
    )
    test_metric_name = "python-sdk/test"
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
    main()
