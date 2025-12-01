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
    rows = cvec.select_from_eav(
        tenant_id=5,
        table_id="916310b2-2eab-4538-b179-98fe77c0c24d",  # Maintenance Entries
        column_ids=["date", "operator", "pipeline"],
        filters=[
            EAVFilter(column_id="date", numeric_min=45992, numeric_max=45993),
        ],
    )
    print(f"Found {len(rows)} rows with date in range [45992, 45993)")
    for row in rows:
        print(f"- {row}")


if __name__ == "__main__":
    main()
