from cvec import CVec
import os


def main() -> None:
    cvec = CVec(
        host=os.environ.get("CVEC_HOST", "https://sandbox.cvector.dev"),
        email=os.environ.get("CVEC_EMAIL", "your-email@example.com"),
        password=os.environ.get("CVEC_PASSWORD", "your-password"),
        publishable_key=os.environ.get("CVEC_PUBLISHABLE_KEY", "your-supabase-publishable-key"),
    )
    test_metric_name = "python-sdk/test"
    print("\nGetting metric data as objects...")
    data_points = cvec.get_metric_data(names=[test_metric_name])
    print(f"Found {len(data_points)} data points")
    for point in data_points[:3]:
        print(
            f"- {point.name}: {point.value_double or point.value_string} at {point.time}"
        )


if __name__ == "__main__":
    main()
