"""
test_quality.py
Tests for data_quality.py check functions.
Each test creates a minimal in-memory table with known bad data,
runs the quality check against it, and asserts the check catches
exactly what it should. This proves the checks work, not just that they ran.
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pytest
import duckdb
from pipeline.data_quality import (
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
)


# ── Null checks ────────────────────────────────────────────────────────────────

def test_null_order_ids_are_caught(mem_conn):
    mem_conn.execute("""
        CREATE TABLE orders AS
        SELECT * FROM (VALUES
            (1,   100, '2024-01-01'::DATE, 'completed', 150.00),
            (NULL, 101, '2024-01-02'::DATE, 'completed', 200.00)
        ) t(order_id, customer_id, order_date, status, total_amount)
    """)
    result = check_no_null_order_ids(mem_conn)
    assert result.passed is False
    assert result.failing_rows == 1


def test_no_null_order_ids_passes_clean_data(mem_conn):
    mem_conn.execute("""
        CREATE TABLE orders AS
        SELECT * FROM (VALUES
            (1, 100, '2024-01-01'::DATE, 'completed', 150.00),
            (2, 101, '2024-01-02'::DATE, 'completed', 200.00)
        ) t(order_id, customer_id, order_date, status, total_amount)
    """)
    result = check_no_null_order_ids(mem_conn)
    assert result.passed is True
    assert result.failing_rows == 0


def test_null_customer_ids_are_caught(mem_conn):
    mem_conn.execute("""
        CREATE TABLE customers AS
        SELECT * FROM (VALUES
            (1,    'Alice', 'alice@email.com', 'North', '2023-01-01'::DATE),
            (NULL, 'Bob',   'bob@email.com',   'South', '2023-02-01'::DATE)
        ) t(customer_id, name, email, region, signup_date)
    """)
    result = check_no_null_customer_ids(mem_conn)
    assert result.passed is False
    assert result.failing_rows == 1


def test_null_product_ids_are_caught(mem_conn):
    mem_conn.execute("""
        CREATE TABLE products AS
        SELECT * FROM (VALUES
            (1,    'Widget A', 'Electronics', 99.99),
            (NULL, 'Widget B', 'Clothing',    49.99)
        ) t(product_id, product_name, category, price)
    """)
    result = check_no_null_product_ids(mem_conn)
    assert result.passed is False
    assert result.failing_rows == 1


# ── Referential integrity checks ───────────────────────────────────────────────

def test_orphaned_order_items_are_caught(mem_conn):
    mem_conn.execute("""
        CREATE TABLE orders AS
        SELECT * FROM (VALUES
            (1, 100, '2024-01-01'::DATE, 'completed', 150.00)
        ) t(order_id, customer_id, order_date, status, total_amount)
    """)
    mem_conn.execute("""
        CREATE TABLE order_items AS
        SELECT * FROM (VALUES
            (1, 1,   1, 2, 50.00, 100.00),
            (2, 999, 2, 1, 75.00,  75.00)
        ) t(item_id, order_id, product_id, quantity, unit_price, line_total)
    """)
    result = check_order_items_have_valid_orders(mem_conn)
    assert result.passed is False
    assert result.failing_rows == 1


def test_orphaned_returns_are_caught(mem_conn):
    mem_conn.execute("""
        CREATE TABLE orders AS
        SELECT * FROM (VALUES
            (1, 100, '2024-01-01'::DATE, 'completed', 150.00)
        ) t(order_id, customer_id, order_date, status, total_amount)
    """)
    mem_conn.execute("""
        CREATE TABLE returns AS
        SELECT * FROM (VALUES
            (1, 1,   '2024-01-15'::DATE, 'damaged', 50.00),
            (2, 999, '2024-01-20'::DATE, 'damaged', 75.00)
        ) t(return_id, order_id, return_date, reason, refund_amount)
    """)
    result = check_returns_have_valid_orders(mem_conn)
    assert result.passed is False
    assert result.failing_rows == 1


# ── Business logic checks ──────────────────────────────────────────────────────

def test_negative_quantities_are_caught(mem_conn):
    mem_conn.execute("""
        CREATE TABLE order_items AS
        SELECT * FROM (VALUES
            (1, 1, 1,  3, 50.00, 150.00),
            (2, 1, 2, -1, 75.00, -75.00)
        ) t(item_id, order_id, product_id, quantity, unit_price, line_total)
    """)
    result = check_no_negative_quantities(mem_conn)
    assert result.passed is False
    assert result.failing_rows == 1


def test_negative_prices_are_caught(mem_conn):
    mem_conn.execute("""
        CREATE TABLE products AS
        SELECT * FROM (VALUES
            (1, 'Widget A', 'Electronics',  99.99),
            (2, 'Widget B', 'Clothing',    -49.99)
        ) t(product_id, product_name, category, price)
    """)
    result = check_no_negative_prices(mem_conn)
    assert result.passed is False
    assert result.failing_rows == 1


def test_return_date_before_order_date_is_caught(mem_conn):
    mem_conn.execute("""
        CREATE TABLE orders AS
        SELECT * FROM (VALUES
            (1, 100, '2024-03-01'::DATE, 'returned', 150.00)
        ) t(order_id, customer_id, order_date, status, total_amount)
    """)
    mem_conn.execute("""
        CREATE TABLE returns AS
        SELECT * FROM (VALUES
            (1, 1, '2024-01-01'::DATE, 'damaged', 50.00)
        ) t(return_id, order_id, return_date, reason, refund_amount)
    """)
    result = check_return_date_after_order_date(mem_conn)
    assert result.passed is False
    assert result.failing_rows == 1


def test_return_date_same_as_order_date_is_caught(mem_conn):
    """Return on the same day as the order is also invalid."""
    mem_conn.execute("""
        CREATE TABLE orders AS
        SELECT * FROM (VALUES
            (1, 100, '2024-03-01'::DATE, 'returned', 150.00)
        ) t(order_id, customer_id, order_date, status, total_amount)
    """)
    mem_conn.execute("""
        CREATE TABLE returns AS
        SELECT * FROM (VALUES
            (1, 1, '2024-03-01'::DATE, 'changed_mind', 50.00)
        ) t(return_id, order_id, return_date, reason, refund_amount)
    """)
    result = check_return_date_after_order_date(mem_conn)
    assert result.passed is False
    assert result.failing_rows == 1


def test_valid_return_date_passes(mem_conn):
    mem_conn.execute("""
        CREATE TABLE orders AS
        SELECT * FROM (VALUES
            (1, 100, '2024-03-01'::DATE, 'returned', 150.00)
        ) t(order_id, customer_id, order_date, status, total_amount)
    """)
    mem_conn.execute("""
        CREATE TABLE returns AS
        SELECT * FROM (VALUES
            (1, 1, '2024-03-10'::DATE, 'damaged', 50.00)
        ) t(return_id, order_id, return_date, reason, refund_amount)
    """)
    result = check_return_date_after_order_date(mem_conn)
    assert result.passed is True
    assert result.failing_rows == 0


def test_invalid_order_status_is_caught(mem_conn):
    mem_conn.execute("""
        CREATE TABLE orders AS
        SELECT * FROM (VALUES
            (1, 100, '2024-01-01'::DATE, 'completed', 150.00),
            (2, 101, '2024-01-02'::DATE, 'pending',   200.00)
        ) t(order_id, customer_id, order_date, status, total_amount)
    """)
    result = check_valid_order_status(mem_conn)
    assert result.passed is False
    assert result.failing_rows == 1


# ── Reconciliation check ───────────────────────────────────────────────────────

def test_order_total_mismatch_beyond_tolerance_is_caught(mem_conn):
    """Order total that differs by more than 1% from item sum is flagged."""
    mem_conn.execute("""
        CREATE TABLE orders AS
        SELECT * FROM (VALUES
            (1, 100, '2024-01-01'::DATE, 'completed', 999.00)
        ) t(order_id, customer_id, order_date, status, total_amount)
    """)
    mem_conn.execute("""
        CREATE TABLE order_items AS
        SELECT * FROM (VALUES
            (1, 1, 1, 2, 50.00, 100.00)
        ) t(item_id, order_id, product_id, quantity, unit_price, line_total)
    """)
    result = check_order_total_matches_items(mem_conn)
    assert result.passed is False
    assert result.failing_rows == 1


def test_order_total_within_tolerance_passes(mem_conn):
    """Order total within 1% of item sum is acceptable."""
    mem_conn.execute("""
        CREATE TABLE orders AS
        SELECT * FROM (VALUES
            (1, 100, '2024-01-01'::DATE, 'completed', 100.50)
        ) t(order_id, customer_id, order_date, status, total_amount)
    """)
    mem_conn.execute("""
        CREATE TABLE order_items AS
        SELECT * FROM (VALUES
            (1, 1, 1, 2, 50.00, 100.00)
        ) t(item_id, order_id, product_id, quantity, unit_price, line_total)
    """)
    result = check_order_total_matches_items(mem_conn)
    assert result.passed is True
    assert result.failing_rows == 0
