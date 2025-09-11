from cvec import CVec
import io
import pyarrow.ipc as ipc  # type: ignore[import-untyped]
import os


def main() -> None:
    cvec = CVec(
        host=os.environ.get("CVEC_HOST", "https://your-subdomain.cvector.dev"),
        api_key=os.environ.get("CVEC_API_KEY", "your-api-key"),
    )
    test_metric_name = "Data_Marketplace/PROD/Miso_5min/ILLINOIS_HUB_lmp"
    print("\nGetting modeling metrics data as Arrow...")
    arrow_data = cvec.get_modeling_metrics_data_arrow(names=[test_metric_name])
    reader = ipc.open_file(io.BytesIO(arrow_data))
    table = reader.read_all()
    print(f"Arrow table shape: {len(table)} rows")
    print("\nFirst few rows:")
    for i in range(min(5, len(table))):
        print(
            f"- {table['name'][i].as_py()}: {table['value_double'][i].as_py() or table['value_string'][i].as_py()} at {table['time'][i].as_py()}"
        )


if __name__ == "__main__":
    main()
