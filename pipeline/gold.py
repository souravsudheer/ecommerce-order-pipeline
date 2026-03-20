import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import duckdb
import pandas as pd
from config import SILVER_DIR, GOLD_DIR


def get_connection() -> duckdb.DuckDBPyConnection:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(GOLD_DIR / "gold.duckdb"))


def attach_silver(conn: duckdb.DuckDBPyConnection) -> None:
    silver_path = str(SILVER_DIR / "silver.duckdb")
    conn.execute(f"ATTACH '{silver_path}' AS silver (READ_ONLY)")


def build_revenue_by_region(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    # Grain: one row per region per month. Excludes cancelled orders.
    df = conn.execute("""
        SELECT
            c.region,
            DATE_TRUNC('month', o.order_date)   AS order_month,
            COUNT(DISTINCT o.order_id)          AS total_orders,
            ROUND(SUM(oi.line_total), 2)        AS total_revenue
        FROM silver.orders o
        INNER JOIN silver.customers c
            ON o.customer_id = c.customer_id
        INNER JOIN silver.order_items oi
            ON o.order_id = oi.order_id
        WHERE o.status != 'cancelled'
        GROUP BY c.region, DATE_TRUNC('month', o.order_date)
        ORDER BY order_month, c.region
    """).fetchdf()
    return df


def build_return_rate_by_category(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    # Grain: one row per category.
    # Return rate = returned order_items / total order_items sold per category.
    df = conn.execute("""
        WITH category_sales AS (
            SELECT
                p.category,
                COUNT(DISTINCT oi.item_id)  AS total_items_sold,
                SUM(oi.line_total)          AS total_revenue
            FROM silver.order_items oi
            INNER JOIN silver.products p
                ON oi.product_id = p.product_id
            INNER JOIN silver.orders o
                ON oi.order_id = o.order_id
            WHERE o.status != 'cancelled'
            GROUP BY p.category
        ),
        category_returns AS (
            SELECT
                p.category,
                COUNT(DISTINCT r.return_id)     AS total_returns,
                SUM(r.refund_amount)            AS total_refunded
            FROM silver.returns r
            INNER JOIN silver.orders o
                ON r.order_id = o.order_id
            INNER JOIN silver.order_items oi
                ON o.order_id = oi.order_id
            INNER JOIN silver.products p
                ON oi.product_id = p.product_id
            GROUP BY p.category
        )
        SELECT
            s.category,
            s.total_items_sold,
            ROUND(s.total_revenue, 2)                               AS total_revenue,
            COALESCE(r.total_returns, 0)                            AS total_returns,
            ROUND(COALESCE(r.total_refunded, 0), 2)                 AS total_refunded,
            ROUND(
                COALESCE(r.total_returns, 0) * 100.0
                / NULLIF(s.total_items_sold, 0), 2
            )                                                       AS return_rate_pct
        FROM category_sales s
        LEFT JOIN category_returns r ON s.category = r.category
        ORDER BY return_rate_pct DESC
    """).fetchdf()
    return df


def build_top_products(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    # Grain: one row per product, top 10 by revenue.
    df = conn.execute("""
        WITH product_sales AS (
            SELECT
                p.product_id,
                p.product_name,
                p.category,
                COUNT(DISTINCT oi.item_id)  AS units_sold,
                ROUND(SUM(oi.line_total), 2) AS total_revenue
            FROM silver.order_items oi
            INNER JOIN silver.products p
                ON oi.product_id = p.product_id
            INNER JOIN silver.orders o
                ON oi.order_id = o.order_id
            WHERE o.status != 'cancelled'
            GROUP BY p.product_id, p.product_name, p.category
        ),
        product_returns AS (
            SELECT
                oi.product_id,
                COUNT(DISTINCT r.return_id) AS total_returns
            FROM silver.returns r
            INNER JOIN silver.orders o
                ON r.order_id = o.order_id
            INNER JOIN silver.order_items oi
                ON o.order_id = oi.order_id
            GROUP BY oi.product_id
        )
        SELECT
            s.product_id,
            s.product_name,
            s.category,
            s.units_sold,
            s.total_revenue,
            COALESCE(r.total_returns, 0) AS total_returns,
            ROUND(
                COALESCE(r.total_returns, 0) * 100.0
                / NULLIF(s.units_sold, 0), 2
            )                            AS return_rate_pct
        FROM product_sales s
        LEFT JOIN product_returns r ON s.product_id = r.product_id
        ORDER BY s.total_revenue DESC
        LIMIT 10
    """).fetchdf()
    return df


def build_customer_segments(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    # Grain: one row per customer with their LTV segment (High / Mid / Low).
    # Segmentation based on tertiles of lifetime revenue.
    df = conn.execute("""
        WITH customer_ltv AS (
            SELECT
                c.customer_id,
                c.name,
                c.region,
                COUNT(DISTINCT o.order_id)      AS total_orders,
                ROUND(SUM(oi.line_total), 2)    AS lifetime_value,
                ROUND(AVG(oi.line_total), 2)    AS avg_order_value
            FROM silver.customers c
            INNER JOIN silver.orders o
                ON c.customer_id = o.customer_id
            INNER JOIN silver.order_items oi
                ON o.order_id = oi.order_id
            WHERE o.status != 'cancelled'
            GROUP BY c.customer_id, c.name, c.region
        ),
        percentiles AS (
            SELECT
                PERCENTILE_CONT(0.33) WITHIN GROUP (ORDER BY lifetime_value) AS p33,
                PERCENTILE_CONT(0.66) WITHIN GROUP (ORDER BY lifetime_value) AS p66
            FROM customer_ltv
        )
        SELECT
            l.customer_id,
            l.name,
            l.region,
            l.total_orders,
            l.lifetime_value,
            l.avg_order_value,
            CASE
                WHEN l.lifetime_value >= p.p66 THEN 'High'
                WHEN l.lifetime_value >= p.p33 THEN 'Mid'
                ELSE 'Low'
            END AS ltv_segment
        FROM customer_ltv l
        CROSS JOIN percentiles p
        ORDER BY l.lifetime_value DESC
    """).fetchdf()
    return df


def write_gold(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame, table_name: str) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    df.to_csv(GOLD_DIR / f"{table_name}.csv", index=False)


def main():
    print("Building silver -> gold aggregates...")
    conn = get_connection()
    attach_silver(conn)

    builds = {
        "gold_revenue_by_region":       build_revenue_by_region,
        "gold_return_rate_by_category": build_return_rate_by_category,
        "gold_top_products":            build_top_products,
        "gold_customer_segments":       build_customer_segments,
    }

    print()
    for table_name, fn in builds.items():
        df = fn(conn)
        write_gold(conn, df, table_name)
        print(f"  {table_name}: {len(df):,} rows")

    conn.close()
    print(f"\nGold layer written to: {GOLD_DIR}")


if __name__ == "__main__":
    main()
