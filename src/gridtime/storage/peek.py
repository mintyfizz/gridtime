"""Quick look at Bronze using DuckDB on top of an Iceberg scan."""

import duckdb

from gridtime.storage.tables import bronze_table

if __name__ == "__main__":
    bronze = bronze_table().scan().to_arrow()  # noqa: F841  (queried by name below)
    print(
        duckdb.sql(
            """
            SELECT dataset_id,
                   count(*)                AS rows,
                   count(DISTINCT valid_time) AS quarter_hours,
                   min(valid_time)         AS first_valid,
                   max(valid_time)         AS last_valid,
                   max(transaction_time)   AS last_received
            FROM bronze
            GROUP BY dataset_id
            ORDER BY dataset_id
            """
        )
    )
