from cvec import CVec
from cvec.models.metric import MetricDataPoint
from datetime import datetime, timezone
import random

def main():
    cvec = CVec(
        host="https://cvec-backend-rzhang-cvec-sandbox.deployments.quix.io",
        tenant="test",
        api_key="your-api-key",
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