#!/usr/bin/env python3
"""Show EAV tables and columns."""

import os

from cvec import CVec


def main() -> None:
    cvec = CVec(
        host=os.environ.get("CVEC_HOST", ""),
        api_key=os.environ.get("CVEC_API_KEY", ""),
    )

    tables = cvec.get_eav_tables()
    print(f"Found {len(tables)} EAV tables\n")

    for table in tables:
        print(f"{table.name} (id: {table.id})")
        columns = cvec.get_eav_columns(table.id)
        for column in columns:
            print(f"  - {column.name} ({column.type}, id: {column.eav_column_id})")
        print()


if __name__ == "__main__":
    main()
