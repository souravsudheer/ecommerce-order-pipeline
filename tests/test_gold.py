import pytest
import duckdb
from pipeline.gold import get_connection


@pytest.fixture(scope="module")
def gold_conn():
    # Connects to the real gold.duckdb.
    # Assumes the full pipeline has already been run.
    conn = get_connection()
    yield conn
    conn.close()


def test_gold_revenue_by_region_is_non_empty(gold_conn):
    count = gold_conn.execute(
        "SELECT COUNT(*) FROM gold_revenue_by_region"
    ).fetchone()[0]
    assert count > 0, "gold_revenue_by_region must not be empty"


def test_gold_return_rate_by_category_is_non_empty(gold_conn):
    count = gold_conn.execute(
        "SELECT COUNT(*) FROM gold_return_rate_by_category"
    ).fetchone()[0]
    assert count > 0, "gold_return_rate_by_category must not be empty"


def test_gold_top_products_is_non_empty(gold_conn):
    count = gold_conn.execute(
        "SELECT COUNT(*) FROM gold_top_products"
    ).fetchone()[0]
    assert count > 0, "gold_top_products must not be empty"


def test_gold_customer_segments_is_non_empty(gold_conn):
    count = gold_conn.execute(
        "SELECT COUNT(*) FROM gold_customer_segments"
    ).fetchone()[0]
    assert count > 0, "gold_customer_segments must not be empty"


def test_gold_return_rate_has_exactly_four_categories(gold_conn):
    count = gold_conn.execute(
        "SELECT COUNT(*) FROM gold_return_rate_by_category"
    ).fetchone()[0]
    assert count == 4, f"Expected 4 product categories, got {count}"


def test_gold_top_products_has_exactly_ten_rows(gold_conn):
    count = gold_conn.execute(
        "SELECT COUNT(*) FROM gold_top_products"
    ).fetchone()[0]
    assert count == 10, f"Expected 10 top products, got {count}"


def test_gold_revenue_has_all_four_regions(gold_conn):
    regions = set(
        gold_conn.execute(
            "SELECT DISTINCT region FROM gold_revenue_by_region"
        ).fetchdf()["region"].tolist()
    )
    assert regions == {"North", "South", "East", "West"}, \
        f"Expected all four regions, got {regions}"


def test_gold_customer_segments_has_three_tiers(gold_conn):
    segments = set(
        gold_conn.execute(
            "SELECT DISTINCT ltv_segment FROM gold_customer_segments"
        ).fetchdf()["ltv_segment"].tolist()
    )
    assert segments == {"High", "Mid", "Low"}, \
        f"Expected High/Mid/Low segments, got {segments}"


def test_gold_revenue_is_always_positive(gold_conn):
    bad = gold_conn.execute(
        "SELECT COUNT(*) FROM gold_revenue_by_region WHERE total_revenue <= 0"
    ).fetchone()[0]
    assert bad == 0, "All revenue values must be positive"


def test_gold_return_rate_is_between_zero_and_100(gold_conn):
    bad = gold_conn.execute("""
        SELECT COUNT(*) FROM gold_return_rate_by_category
        WHERE return_rate_pct < 0 OR return_rate_pct > 100
    """).fetchone()[0]
    assert bad == 0, "Return rate must be between 0 and 100"


def test_gold_top_products_return_rate_is_between_zero_and_100(gold_conn):
    bad = gold_conn.execute("""
        SELECT COUNT(*) FROM gold_top_products
        WHERE return_rate_pct < 0 OR return_rate_pct > 100
    """).fetchone()[0]
    assert bad == 0, "Product return rate must be between 0 and 100"


def test_gold_customer_lifetime_value_is_positive(gold_conn):
    bad = gold_conn.execute(
        "SELECT COUNT(*) FROM gold_customer_segments WHERE lifetime_value <= 0"
    ).fetchone()[0]
    assert bad == 0, "All customer lifetime values must be positive"


def test_gold_customer_total_orders_is_at_least_one(gold_conn):
    bad = gold_conn.execute(
        "SELECT COUNT(*) FROM gold_customer_segments WHERE total_orders < 1"
    ).fetchone()[0]
    assert bad == 0, "Every customer in gold must have at least one order"


def test_gold_top_products_ordered_by_revenue_descending(gold_conn):
    revenues = gold_conn.execute(
        "SELECT total_revenue FROM gold_top_products"
    ).fetchdf()["total_revenue"].tolist()
    assert revenues == sorted(revenues, reverse=True), \
        "Top products must be ordered by total_revenue descending"


def test_gold_high_segment_has_greater_avg_ltv_than_low(gold_conn):
    result = gold_conn.execute("""
        SELECT ltv_segment, AVG(lifetime_value) AS avg_ltv
        FROM gold_customer_segments
        GROUP BY ltv_segment
    """).fetchdf().set_index("ltv_segment")["avg_ltv"]
    assert result["High"] > result["Mid"], "High segment avg LTV must exceed Mid"
    assert result["Mid"] > result["Low"], "Mid segment avg LTV must exceed Low"


def test_gold_revenue_by_region_total_orders_is_positive(gold_conn):
    bad = gold_conn.execute(
        "SELECT COUNT(*) FROM gold_revenue_by_region WHERE total_orders <= 0"
    ).fetchone()[0]
    assert bad == 0, "All monthly order counts must be positive"


def test_gold_cancelled_orders_excluded_from_revenue(gold_conn):
    # Revenue in gold should exclude cancelled orders.
    # If any region-month has zero revenue, cancelled orders likely leaked in.
    zero_revenue = gold_conn.execute(
        "SELECT COUNT(*) FROM gold_revenue_by_region WHERE total_revenue = 0"
    ).fetchone()[0]
    assert zero_revenue == 0, \
        "No region-month should have zero revenue — likely cancelled orders leaked in"
