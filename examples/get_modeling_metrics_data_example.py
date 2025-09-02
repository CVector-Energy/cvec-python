from cvec import CVec
import os
from datetime import datetime, timedelta


def main() -> None:
    cvec = CVec(
        host=os.environ.get("CVEC_HOST", "https://your-subdomain.cvector.dev"),
        api_key=os.environ.get("CVEC_API_KEY", "your-api-key"),
    )
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=3)
    print("\nGetting modeling metrics data...")
    modeling_data = cvec.get_modeling_metrics_data(
        names=["Data_Marketplace/PROD/Miso_5min/ILLINOIS_HUB_lmp"],
        start_at=start_date,
        end_at=end_date,
    )
    print(f"Found {len(modeling_data)} data points")

    if modeling_data:
        print("\nFirst few data points:")
        for i, point in enumerate(modeling_data[:5]):
            print(
                f"- {point.name}: {point.value_double or point.value_string} at {point.time}"
            )

        if len(modeling_data) > 5:
            print(f"... and {len(modeling_data) - 5} more data points")


if __name__ == "__main__":
    main()
