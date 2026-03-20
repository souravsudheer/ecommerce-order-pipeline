import pytest
import duckdb
from pipeline.bronze import get_connection, load_raw_to_bronze, TABLES


@pytest.fixture(scope="module")
def bronze_conn():
    # Connects to the real bronze.duckdb.
    # Assumes generate_data.py and bronze.py have already been run.
    conn = get_connection()
    yield conn
    conn.close()


def test_all_tables_exist(bronze_conn):
    existing = bronze_conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchdf()["table_name"].tolist()
    for table in TABLES:
        assert table in existing, f"Expected table '{table}' not found in bronze"


def test_all_tables_are_non_empty(bronze_conn):
    for table in TABLES:
        count = bronze_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        assert count > 0, f"Table '{table}' is empty in bronze"


def test_customers_has_expected_columns(bronze_conn):
    cols = bronze_conn.execute("DESCRIBE customers").fetchdf()["column_name"].tolist()
    expected = ["customer_id", "name", "email", "region", "signup_date"]
    for col in expected:
        assert col in cols, f"Missing column '{col}' in bronze.customers"


def test_orders_has_expected_columns(bronze_conn):
    cols = bronze_conn.execute("DESCRIBE orders").fetchdf()["column_name"].tolist()
    expected = ["order_id", "customer_id", "order_date", "status", "total_amount"]
    for col in expected:
        assert col in cols, f"Missing column '{col}' in bronze.orders"


def test_order_items_has_expected_columns(bronze_conn):
    cols = bronze_conn.execute("DESCRIBE order_items").fetchdf()["column_name"].tolist()
    expected = ["item_id", "order_id", "product_id", "quantity", "unit_price"]
    for col in expected:
        assert col in cols, f"Missing column '{col}' in bronze.order_items"


def test_returns_has_expected_columns(bronze_conn):
    cols = bronze_conn.execute("DESCRIBE returns").fetchdf()["column_name"].tolist()
    expected = ["return_id", "order_id", "return_date", "reason", "refund_amount"]
    for col in expected:
        assert col in cols, f"Missing column '{col}' in bronze.returns"


def test_customers_row_count(bronze_conn):
    count = bronze_conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    assert count == 1000, f"Expected 1000 customers, got {count}"


def test_products_row_count(bronze_conn):
    count = bronze_conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    assert count == 200, f"Expected 200 products, got {count}"


def test_orders_row_count(bronze_conn):
    count = bronze_conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    assert count == 10000, f"Expected 10000 orders, got {count}"


def test_order_items_has_more_rows_than_orders(bronze_conn):
    orders = bronze_conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    items = bronze_conn.execute("SELECT COUNT(*) FROM order_items").fetchone()[0]
    assert items > orders, "order_items should have more rows than orders"


def test_bronze_preserves_null_order_ids(bronze_conn):
    """Bronze must NOT clean data — nulls should still be present."""
    nulls = bronze_conn.execute(
        "SELECT COUNT(*) FROM orders WHERE order_id IS NULL"
    ).fetchone()[0]
    assert nulls > 0, "Bronze should preserve null order_ids from raw data"


def test_bronze_preserves_negative_prices(bronze_conn):
    """Bronze must NOT correct negative prices."""
    negs = bronze_conn.execute(
        "SELECT COUNT(*) FROM products WHERE CAST(price AS DOUBLE) < 0"
    ).fetchone()[0]
    assert negs > 0, "Bronze should preserve negative prices from raw data"


def test_bronze_preserves_early_return_dates(bronze_conn):
    """Bronze must NOT filter out temporally invalid return dates."""
    bad = bronze_conn.execute(
        "SELECT COUNT(*) FROM returns WHERE CAST(return_date AS DATE) < '2021-01-01'"
    ).fetchone()[0]
    assert bad > 0, "Bronze should preserve early return dates from raw data"


def test_customers_have_valid_regions(bronze_conn):
    valid_regions = {"North", "South", "East", "West"}
    actual = set(
        bronze_conn.execute("SELECT DISTINCT region FROM customers").fetchdf()["region"].tolist()
    )
    assert actual.issubset(valid_regions), f"Unexpected regions found: {actual - valid_regions}"


def test_orders_have_expected_statuses(bronze_conn):
    valid_statuses = {"completed", "cancelled", "returned"}
    actual = set(
        bronze_conn.execute("SELECT DISTINCT status FROM orders").fetchdf()["status"].tolist()
    )
    assert actual.issubset(valid_statuses), f"Unexpected statuses: {actual - valid_statuses}"
