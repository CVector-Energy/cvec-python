import os

from cvec import CVec, EAVFilter


def main() -> None:
    cvec = CVec(
        host=os.environ.get("CVEC_HOST", "your-subdomain.cvector.dev"),
        api_key=os.environ.get("CVEC_API_KEY", "your-api-key"),
    )

    tenant_id = 1  # Replace with your tenant ID

    # Example: List available EAV tables
    print("\nListing EAV tables...")
    tables = cvec.get_eav_tables(tenant_id)
    print(f"Found {len(tables)} tables")
    for table in tables:
        print(f"- {table.name} (id: {table.id})")

    if not tables:
        print("No tables found. Exiting.")
        return

    # Use the first table for demonstration
    table_name = tables[0].name

    # Example: List columns for the table
    print(f"\nListing columns for '{table_name}'...")
    columns = cvec.get_eav_columns(tables[0].id)
    print(f"Found {len(columns)} columns")
    for column in columns:
        print(f"- {column.name} ({column.type})")

    # Example: Query all rows from the table
    print(f"\nQuerying '{table_name}' (all columns, no filters)...")
    rows = cvec.select_from_eav(
        tenant_id=tenant_id,
        table_name=table_name,
    )
    print(f"Found {len(rows)} rows")
    for row in rows[:5]:  # Show first 5 rows
        print(f"- {row}")

    # Example: Query with numeric range filter (if there's a numeric column)
    numeric_columns = [c for c in columns if c.type == "number"]
    if numeric_columns:
        col_name = numeric_columns[0].name
        print(f"\nQuerying with numeric filter on '{col_name}'...")
        rows = cvec.select_from_eav(
            tenant_id=tenant_id,
            table_name=table_name,
            filters=[
                EAVFilter(column_name=col_name, numeric_min=0),
            ],
        )
        print(f"Found {len(rows)} rows with {col_name} >= 0")

    # Example: Query with string filter (if there's a string column)
    string_columns = [c for c in columns if c.type == "string"]
    if string_columns and rows:
        col_name = string_columns[0].name
        # Get a sample value from the first row
        sample_value = rows[0].get(col_name) if rows else None
        if sample_value:
            print(f"\nQuerying with string filter on '{col_name}'...")
            rows = cvec.select_from_eav(
                tenant_id=tenant_id,
                table_name=table_name,
                filters=[
                    EAVFilter(column_name=col_name, string_value=str(sample_value)),
                ],
            )
            print(f"Found {len(rows)} rows with {col_name}='{sample_value}'")


if __name__ == "__main__":
    main()
