from cvec import CVec
import os


def main() -> None:
    cvec = CVec(
        host=os.environ.get("CVEC_HOST", "https://your-subdomain.cvector.dev"),
        api_key=os.environ.get("CVEC_API_KEY", "your-api-key"),
    )
    print("\nGetting available modeling metrics...")
    modeling_metrics = cvec.get_modeling_metrics()
    print(f"Found {len(modeling_metrics)} modeling metrics")
    for metric in modeling_metrics:
        print(f"- {metric.name}")


if __name__ == "__main__":
    main()
