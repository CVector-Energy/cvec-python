from cvec import CVec

def main():
    cvec = CVec(
        host="https://cvec-backend-rzhang-cvec-sandbox.deployments.quix.io",
        tenant="test",
        api_key="your-api-key",
    )
    metrics = cvec.get_metrics()
    if metrics:
        metric_name = "python-sdk/test"
        print(f"\nGetting spans for metric '{metric_name}'...")
        spans = cvec.get_spans(metric_name, limit=5)
        print(f"Found {len(spans)} spans")
        for span in spans:
            print(f"- Value: {span.value} from {span.raw_start_at} to {span.raw_end_at}")
    else:
        print("No metrics found to get spans.")

if __name__ == "__main__":
    main() 