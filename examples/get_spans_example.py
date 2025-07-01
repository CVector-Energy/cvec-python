from cvec import CVec
import os


def main() -> None:
    cvec = CVec(
        host=os.environ.get("CVEC_HOST", "https://your-subdomain.cvector.dev"),
        email=os.environ.get("CVEC_EMAIL", "your-email@cvector.app"),
        password=os.environ.get("CVEC_PASSWORD", "your-password"),
        publishable_key=os.environ.get(
            "CVEC_PUBLISHABLE_KEY", "your-cvec-publishable-key"
        ),
    )
    metrics = cvec.get_metrics()
    if metrics:
        metric_name = metrics[0].name
        print(f"\nGetting spans for metric '{metric_name}'...")
        spans = cvec.get_spans(metric_name, limit=5)
        print(f"Found {len(spans)} spans")
        for span in spans:
            print(
                f"- Value: {span.value} from {span.raw_start_at} to {span.raw_end_at}"
            )
    else:
        print("No metrics found to get spans.")


if __name__ == "__main__":
    main()
