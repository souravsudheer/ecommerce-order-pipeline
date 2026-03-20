import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import duckdb
import pandas as pd
from config import BRONZE_DIR, SILVER_DIR


def get_connection() -> duckdb.DuckDBPyConnection:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(SILVER_DIR / "silver.duckdb"))


def attach_bronze(conn: duckdb.DuckDBPyConnection) -> None:
    bronze_path = str(BRONZE_DIR / "bronze.duckdb")
    conn.execute(f"ATTACH '{bronze_path}' AS bronze (READ_ONLY)")


def transform_customers(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    - Cast customer_id to integer
    - Lowercase and strip whitespace from email
    - Cast signup_date to date
    - Drop rows with null customer_id or null email
    - Drop rows where email does not contain '@' (malformed)
    """
    df = conn.execute("""
        SELECT
            CAST(customer_id AS INTEGER)    AS customer_id,
            TRIM(name)                      AS name,
            LOWER(TRIM(email))              AS email,
            region,
            CAST(signup_date AS DATE)       AS signup_date
        FROM bronze.customers
        WHERE customer_id IS NOT NULL
          AND email IS NOT NULL
          AND email LIKE '%@%'
    """).fetchdf()
    return df


def transform_products(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    - Cast product_id to integer
    - Cast price to decimal, take absolute value to fix negatives
    - Drop rows with null product_id
    - Flag rows with originally negative price (kept but corrected)
    """
    df = conn.execute("""
        SELECT
            CAST(product_id AS INTEGER)         AS product_id,
            TRIM(product_name)                  AS product_name,
            category,
            ABS(CAST(price AS DECIMAL(10,2)))   AS price,
            CASE WHEN price < 0
                 THEN true ELSE false
            END                                 AS price_was_negative
        FROM bronze.products
        WHERE product_id IS NOT NULL
    """).fetchdf()
    return df


def transform_orders(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    - Cast order_id and customer_id to integer
    - Cast order_date to date, total_amount to decimal
    - Drop rows with null order_id
    - Drop rows whose customer_id was removed during customer cleaning
    - Cancelled orders stay in silver; excluded at gold layer
    """
    df = conn.execute("""
        SELECT
            CAST(o.order_id AS INTEGER)           AS order_id,
            CAST(o.customer_id AS INTEGER)        AS customer_id,
            CAST(o.order_date AS DATE)            AS order_date,
            o.status,
            CAST(o.total_amount AS DECIMAL(10,2)) AS total_amount
        FROM bronze.orders o
        INNER JOIN customers c
            ON CAST(o.customer_id AS INTEGER) = c.customer_id
        WHERE o.order_id IS NOT NULL
    """).fetchdf()
    return df


def transform_order_items(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    - Cast all IDs to integer, unit_price to decimal
    - Drop rows with null order_id or product_id
    - Drop rows with negative or zero quantity (bad data)
    - Drop rows whose order_id was removed during order cleaning
    - Compute line_total as a derived column
    """
    df = conn.execute("""
        SELECT
            CAST(oi.item_id AS INTEGER)                AS item_id,
            CAST(oi.order_id AS INTEGER)               AS order_id,
            CAST(oi.product_id AS INTEGER)             AS product_id,
            CAST(oi.quantity AS INTEGER)               AS quantity,
            CAST(oi.unit_price AS DECIMAL(10,2))       AS unit_price,
            CAST(oi.quantity AS INTEGER)
                * CAST(oi.unit_price AS DECIMAL(10,2)) AS line_total
        FROM bronze.order_items oi
        INNER JOIN orders o
            ON CAST(oi.order_id AS INTEGER) = o.order_id
        WHERE oi.order_id IS NOT NULL
          AND oi.product_id IS NOT NULL
          AND CAST(oi.quantity AS INTEGER) > 0
    """).fetchdf()
    return df


def transform_returns(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    - Cast all IDs to integer, return_date to date, refund_amount to decimal
    - Drop rows with null order_id or order_id removed during order cleaning
    - Drop rows where return_date is before the linked order_date (temporal integrity)
    """
    df = conn.execute("""
        SELECT
            CAST(r.return_id AS INTEGER)            AS return_id,
            CAST(r.order_id AS INTEGER)             AS order_id,
            CAST(r.return_date AS DATE)             AS return_date,
            r.reason,
            CAST(r.refund_amount AS DECIMAL(10,2))  AS refund_amount
        FROM bronze.returns r
        INNER JOIN orders o
            ON CAST(r.order_id AS INTEGER) = o.order_id
        WHERE r.order_id IS NOT NULL
          AND CAST(r.return_date AS DATE) > o.order_date
    """).fetchdf()
    return df


def write_silver(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name: str) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    df.to_csv(SILVER_DIR / f"{table_name}.csv", index=False)


def summarise_cleaning(table: str, before: int, after: int) -> None:
    dropped = before - after
    pct = (dropped / before * 100) if before > 0 else 0
    print(f"  {table}: {before:,} -> {after:,} rows ({dropped} dropped, {pct:.1f}%)")


def main():
    print("Transforming bronze -> silver...")
    conn = get_connection()
    attach_bronze(conn)

    bronze_conn = duckdb.connect(str(BRONZE_DIR / "bronze.duckdb"), read_only=True)

    transforms = {
        "customers":   transform_customers,
        "products":    transform_products,
        "orders":      transform_orders,
        "order_items": transform_order_items,
        "returns":     transform_returns,
    }

    print("\nCleaning summary:")
    for table, fn in transforms.items():
        before = bronze_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        df = fn(conn)
        write_silver(conn, df, table)
        summarise_cleaning(table, before, len(df))

    bronze_conn.close()
    conn.close()

    print(f"\nSilver layer written to: {SILVER_DIR}")


if __name__ == "__main__":
    main()
