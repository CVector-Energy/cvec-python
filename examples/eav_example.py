import os

from cvec import CVec, EAVFilter


def main() -> None:
    cvec = CVec(
        host=os.environ.get(
            "CVEC_HOST", "https://your-subdomain.cvector.dev"
        ),  # Replace with your API host
        api_key=os.environ.get("CVEC_API_KEY", "your-api-key"),
    )

    # Example: Query with numeric range filter
    print("\nQuerying with numeric range filter...")
    rows = cvec.select_from_eav_id(
        table_id="00000000-0000-0000-0000-000000000000",
        column_ids=["abcd", "defg", "hijk"],
        filters=[
            EAVFilter(column_id="abcd", numeric_min=45992, numeric_max=45993),
        ],
    )
    print(f"Found {len(rows)} rows with abcd in range [45992, 45993)")
    for row in rows:
        print(f"- {row}")


if __name__ == "__main__":
    main()
