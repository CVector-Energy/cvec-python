from cvec import CVec

def main():
    cvec = CVec(
        host="https://cvec-backend-rzhang-cvec-sandbox.deployments.quix.io",
        tenant="test",
        api_key="your-api-key",
    )
    test_metric_name = "python-sdk/test"
    print("\nGetting metric data as objects...")
    data_points = cvec.get_metric_data(names=[test_metric_name])
    print(f"Found {len(data_points)} data points")
    for point in data_points[:3]:
        print(f"- {point.name}: {point.value_double or point.value_string} at {point.time}")

if __name__ == "__main__":
    main() 