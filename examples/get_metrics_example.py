from cvec import CVec


def main():
    cvec = CVec(
        host="https://cvec-backend-rzhang-cvec-sandbox.deployments.quix.io",  # Replace with your API host
        tenant="test",  # Replace with your tenant
        api_key="your-api-key",  # Replace with your API key
    )
    print("\nGetting available metrics...")
    metrics = cvec.get_metrics()
    print(f"Found {len(metrics)} metrics")
    for metric in metrics:
        print(f"- {metric.name}")


if __name__ == "__main__":
    main()
