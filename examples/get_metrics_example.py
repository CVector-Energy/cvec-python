from cvec import CVec
import os


def main() -> None:
    cvec = CVec(
        host=os.environ.get(
            "CVEC_HOST", "https://your-subdomain.cvector.dev"
        ),  # Replace with your API host
        api_key=os.environ.get("CVEC_API_KEY", "your-api-key"),
    )
    print("\nGetting available metrics...")
    metrics = cvec.get_metrics()
    print(f"Found {len(metrics)} metrics")
    for metric in metrics:
        print(f"- {metric.name}")


if __name__ == "__main__":
    main()
