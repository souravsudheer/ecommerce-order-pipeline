import duckdb
import pandas as pd
from dataclasses import dataclass
from typing import List
from config import SILVER_DIR, BRONZE_DIR, QUALITY_REPORT_PATH


@dataclass
class QualityResult:
    check_name: str
    table: str
    passed: bool
    failing_rows: int
    description: str


def get_silver_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(SILVER_DIR / "silver.duckdb"), read_only=True)
    return conn


# Null checks

def check_no_null_order_ids(conn: duckdb.DuckDBPyConnection) -> QualityResult:
    failing = conn.execute(
        "SELECT COUNT(*) FROM orders WHERE order_id IS NULL"
    ).fetchone()[0]
    return QualityResult(
        check_name="no_null_order_ids",
        table="orders",
        passed=failing == 0,
        failing_rows=failing,
        description="Every order must have an order_id",
    )


def check_no_null_customer_ids(conn: duckdb.DuckDBPyConnection) -> QualityResult:
    failing = conn.execute(
        "SELECT COUNT(*) FROM customers WHERE customer_id IS NULL"
    ).fetchone()[0]
    return QualityResult(
        check_name="no_null_customer_ids",
        table="customers",
        passed=failing == 0,
        failing_rows=failing,
        description="Every customer must have a customer_id",
    )


def check_no_null_product_ids(conn: duckdb.DuckDBPyConnection) -> QualityResult:
    failing = conn.execute(
        "SELECT COUNT(*) FROM products WHERE product_id IS NULL"
    ).fetchone()[0]
    return QualityResult(
        check_name="no_null_product_ids",
        table="products",
        passed=failing == 0,
        failing_rows=failing,
        description="Every product must have a product_id",
    )


# Referential integrity

def check_order_items_have_valid_orders(conn: duckdb.DuckDBPyConnection) -> QualityResult:
    failing = conn.execute("""
        SELECT COUNT(*)
        FROM order_items oi
        LEFT JOIN orders o ON oi.order_id = o.order_id
        WHERE o.order_id IS NULL
    """).fetchone()[0]
    return QualityResult(
        check_name="order_items_have_valid_orders",
        table="order_items",
        passed=failing == 0,
        failing_rows=failing,
        description="Every order_item must link to a valid order",
    )


def check_returns_have_valid_orders(conn: duckdb.DuckDBPyConnection) -> QualityResult:
    failing = conn.execute("""
        SELECT COUNT(*)
        FROM returns r
        LEFT JOIN orders o ON r.order_id = o.order_id
        WHERE o.order_id IS NULL
    """).fetchone()[0]
    return QualityResult(
        check_name="returns_have_valid_orders",
        table="returns",
        passed=failing == 0,
        failing_rows=failing,
        description="Every return must link to a valid order",
    )


# Business logic

def check_no_negative_quantities(conn: duckdb.DuckDBPyConnection) -> QualityResult:
    failing = conn.execute(
        "SELECT COUNT(*) FROM order_items WHERE quantity <= 0"
    ).fetchone()[0]
    return QualityResult(
        check_name="no_negative_quantities",
        table="order_items",
        passed=failing == 0,
        failing_rows=failing,
        description="All order item quantities must be positive",
    )


def check_no_negative_prices(conn: duckdb.DuckDBPyConnection) -> QualityResult:
    failing = conn.execute(
        "SELECT COUNT(*) FROM products WHERE price < 0"
    ).fetchone()[0]
    return QualityResult(
        check_name="no_negative_prices",
        table="products",
        passed=failing == 0,
        failing_rows=failing,
        description="All product prices must be non-negative",
    )


def check_return_date_after_order_date(conn: duckdb.DuckDBPyConnection) -> QualityResult:
    failing = conn.execute("""
        SELECT COUNT(*)
        FROM returns r
        INNER JOIN orders o ON r.order_id = o.order_id
        WHERE r.return_date <= o.order_date
    """).fetchone()[0]
    return QualityResult(
        check_name="return_date_after_order_date",
        table="returns",
        passed=failing == 0,
        failing_rows=failing,
        description="Return date must be after the original order date",
    )


def check_valid_order_status(conn: duckdb.DuckDBPyConnection) -> QualityResult:
    failing = conn.execute("""
        SELECT COUNT(*)
        FROM orders
        WHERE status NOT IN ('completed', 'cancelled', 'returned')
    """).fetchone()[0]
    return QualityResult(
        check_name="valid_order_status",
        table="orders",
        passed=failing == 0,
        failing_rows=failing,
        description="Order status must be one of: completed, cancelled, returned",
    )


# Reconciliation

def check_order_total_matches_items(conn: duckdb.DuckDBPyConnection) -> QualityResult:
    """
    Checks that each order's total_amount is within 1% of the sum
    of its order_items line totals. Allows for rounding and discount variance.
    """
    failing = conn.execute("""
        WITH item_totals AS (
            SELECT
                order_id,
                SUM(line_total) AS calculated_total
            FROM order_items
            GROUP BY order_id
        )
        SELECT COUNT(*)
        FROM orders o
        INNER JOIN item_totals it ON o.order_id = it.order_id
        WHERE o.total_amount > 0
          AND ABS(o.total_amount - it.calculated_total)
              / NULLIF(o.total_amount, 0) > 0.01
    """).fetchone()[0]
    return QualityResult(
        check_name="order_total_matches_items",
        table="orders",
        passed=failing == 0,
        failing_rows=failing,
        description="Order total_amount must match sum of order_items within 1%",
    )


ALL_CHECKS = [
    check_no_null_order_ids,
    check_no_null_customer_ids,
    check_no_null_product_ids,
    check_order_items_have_valid_orders,
    check_returns_have_valid_orders,
    check_no_negative_quantities,
    check_no_negative_prices,
    check_return_date_after_order_date,
    check_valid_order_status,
    check_order_total_matches_items,
]


def run_all_checks(conn: duckdb.DuckDBPyConnection) -> List[QualityResult]:
    results = []
    for check_fn in ALL_CHECKS:
        result = check_fn(conn)
        status = "PASS" if result.passed else "FAIL"
        flag = "" if result.passed else " <--"
        print(f"  [{status}] {result.check_name}{flag}")
        results.append(result)
    return results


def write_quality_report(results: List[QualityResult]) -> None:
    rows = [
        {
            "check_name": r.check_name,
            "table": r.table,
            "passed": r.passed,
            "failing_rows": r.failing_rows,
            "description": r.description,
            "status": "PASS" if r.passed else "FAIL",
        }
        for r in results
    ]
    df = pd.DataFrame(rows)
    QUALITY_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(QUALITY_REPORT_PATH, index=False)
    print(f"\nQuality report written to: {QUALITY_REPORT_PATH}")


def main():
    print("Running data quality checks on silver layer...\n")
    conn = get_silver_connection()
    results = run_all_checks(conn)
    conn.close()

    passed = sum(1 for r in results if r.passed)
    total = len(results)
    print(f"\nResult: {passed}/{total} checks passed")

    write_quality_report(results)


if __name__ == "__main__":
    main()
