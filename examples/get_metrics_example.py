from cvec import CVec
import os


def main() -> None:
    cvec = CVec(
        host=os.environ.get("CVEC_HOST", "https://sandbox.cvector.dev"),  # Replace with your API host
        email=os.environ.get("CVEC_EMAIL", "your-email@example.com"),
        password=os.environ.get("CVEC_PASSWORD", "your-password"),
        publishable_key=os.environ.get("CVEC_PUBLISHABLE_KEY", "your-supabase-publishable-key"),
    )
    print("\nGetting available metrics...")
    metrics = cvec.get_metrics()
    print(f"Found {len(metrics)} metrics")
    for metric in metrics:
        print(f"- {metric.name}")


if __name__ == "__main__":
    main()
