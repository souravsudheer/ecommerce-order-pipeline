"""
test_silver.py
Tests that silver transformations produce trusted, typed, clean data.
Checks that bad data from bronze has been removed, types are correct,
and derived columns are calculated properly.
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pytest
import duckdb
from pipeline.silver import get_connection


@pytest.fixture(scope="module")
def silver_conn():
    """
    Module-scoped fixture: connects to the real silver.duckdb.
    Assumes bronze.py and silver.py have already been run.
    """
    conn = get_connection()
    yield conn
    conn.close()


# ── Null removal ───────────────────────────────────────────────────────────────

def test_silver_customers_has_no_null_ids(silver_conn):
    nulls = silver_conn.execute(
        "SELECT COUNT(*) FROM customers WHERE customer_id IS NULL"
    ).fetchone()[0]
    assert nulls == 0, "Silver customers must have no null customer_ids"


def test_silver_orders_has_no_null_ids(silver_conn):
    nulls = silver_conn.execute(
        "SELECT COUNT(*) FROM orders WHERE order_id IS NULL"
    ).fetchone()[0]
    assert nulls == 0, "Silver orders must have no null order_ids"


def test_silver_customers_has_no_malformed_emails(silver_conn):
    bad = silver_conn.execute(
        "SELECT COUNT(*) FROM customers WHERE email NOT LIKE '%@%'"
    ).fetchone()[0]
    assert bad == 0, "Silver customers must have no malformed emails"


def test_silver_customers_emails_are_lowercase(silver_conn):
    mixed = silver_conn.execute(
        "SELECT COUNT(*) FROM customers WHERE email != LOWER(email)"
    ).fetchone()[0]
    assert mixed == 0, "Silver customer emails must all be lowercase"


# ── Bad data removal ───────────────────────────────────────────────────────────

def test_silver_order_items_has_no_negative_quantities(silver_conn):
    negs = silver_conn.execute(
        "SELECT COUNT(*) FROM order_items WHERE quantity <= 0"
    ).fetchone()[0]
    assert negs == 0, "Silver order_items must have no negative or zero quantities"


def test_silver_products_has_no_negative_prices(silver_conn):
    negs = silver_conn.execute(
        "SELECT COUNT(*) FROM products WHERE price < 0"
    ).fetchone()[0]
    assert negs == 0, "Silver products must have no negative prices"


def test_silver_returns_has_no_early_return_dates(silver_conn):
    bad = silver_conn.execute(
        "SELECT COUNT(*) FROM returns WHERE return_date < '2021-01-01'"
    ).fetchone()[0]
    assert bad == 0, "Silver returns must have no return dates before 2021"


def test_silver_returns_date_always_after_order_date(silver_conn):
    bad = silver_conn.execute("""
        SELECT COUNT(*)
        FROM returns r
        INNER JOIN orders o ON r.order_id = o.order_id
        WHERE r.return_date <= o.order_date
    """).fetchone()[0]
    assert bad == 0, "All return dates must be after their linked order date"


# ── Row count expectations ─────────────────────────────────────────────────────

def test_silver_has_fewer_customers_than_bronze(silver_conn):
    """Silver drops ~5% of customer rows due to null IDs and bad emails."""
    count = silver_conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    assert count < 1000, f"Silver customers should be less than 1000, got {count}"
    assert count > 900, f"Silver customers should not drop too many rows, got {count}"


def test_silver_has_fewer_orders_than_bronze(silver_conn):
    count = silver_conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    assert count < 10000, f"Silver orders should be less than 10000, got {count}"
    assert count > 9000, f"Silver orders should not drop too many rows, got {count}"


def test_silver_products_flags_corrected_prices(silver_conn):
    """Products with originally negative prices should be flagged."""
    flagged = silver_conn.execute(
        "SELECT COUNT(*) FROM products WHERE price_was_negative = true"
    ).fetchone()[0]
    assert flagged > 0, "Some products should be flagged as having had negative prices"


# ── Derived column checks ──────────────────────────────────────────────────────

def test_silver_order_items_line_total_is_correct(silver_conn):
    """line_total must equal quantity * unit_price for all rows."""
    mismatched = silver_conn.execute("""
        SELECT COUNT(*)
        FROM order_items
        WHERE ABS(line_total - (quantity * unit_price)) > 0.01
    """).fetchone()[0]
    assert mismatched == 0, "All line_total values must equal quantity * unit_price"


def test_silver_order_items_line_total_is_positive(silver_conn):
    bad = silver_conn.execute(
        "SELECT COUNT(*) FROM order_items WHERE line_total <= 0"
    ).fetchone()[0]
    assert bad == 0, "All line_total values must be positive"


# ── Referential integrity ──────────────────────────────────────────────────────

def test_silver_all_orders_have_valid_customers(silver_conn):
    orphans = silver_conn.execute("""
        SELECT COUNT(*)
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """).fetchone()[0]
    assert orphans == 0, "All orders must link to a valid customer in silver"


def test_silver_all_order_items_have_valid_orders(silver_conn):
    orphans = silver_conn.execute("""
        SELECT COUNT(*)
        FROM order_items oi
        LEFT JOIN orders o ON oi.order_id = o.order_id
        WHERE o.order_id IS NULL
    """).fetchone()[0]
    assert orphans == 0, "All order_items must link to a valid order in silver"


# ── Value range checks ─────────────────────────────────────────────────────────

def test_silver_products_categories_are_valid(silver_conn):
    valid = {"Electronics", "Clothing", "Home", "Beauty"}
    actual = set(
        silver_conn.execute(
            "SELECT DISTINCT category FROM products"
        ).fetchdf()["category"].tolist()
    )
    assert actual.issubset(valid), f"Unexpected categories in silver: {actual - valid}"


def test_silver_returns_reasons_are_valid(silver_conn):
    valid = {"damaged", "wrong_item", "changed_mind", "defective"}
    actual = set(
        silver_conn.execute(
            "SELECT DISTINCT reason FROM returns"
        ).fetchdf()["reason"].tolist()
    )
    assert actual.issubset(valid), f"Unexpected return reasons in silver: {actual - valid}"
