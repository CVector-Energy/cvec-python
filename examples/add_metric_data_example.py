from cvec import CVec
from cvec.models.metric import MetricDataPoint
from datetime import datetime, timezone
import random
import os


def main() -> None:
    cvec = CVec(
        host=os.environ.get("CVEC_HOST", "https://your-subdomain.cvector.dev"),
        api_key=os.environ.get("CVEC_API_KEY", "your-api-key"),
    )
    test_metric_name = "python-sdk/test"
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


if __name__ == "__main__":
    main()
