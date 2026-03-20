"""
generate_data.py
Generates synthetic e-commerce data across five tables and writes them
as CSVs to data/raw/. Deliberately injects ~5% bad records so the
data quality framework has real issues to catch.
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
from faker import Faker

from config import (
    RAW_DIR, RANDOM_SEED, N_CUSTOMERS, N_PRODUCTS, N_ORDERS,
    RETURN_RATE, BAD_DATA_RATE, REGIONS, CATEGORIES,
    ORDER_STATUSES, RETURN_REASONS
)

fake = Faker()
Faker.seed(RANDOM_SEED)
rng = np.random.default_rng(RANDOM_SEED)


def generate_customers() -> pd.DataFrame:
    records = []
    for i in range(1, N_CUSTOMERS + 1):
        records.append({
            "customer_id": i,
            "name": fake.name(),
            "email": fake.email(),
            "region": rng.choice(REGIONS),
            "signup_date": fake.date_between(start_date="-3y", end_date="-1m"),
        })

    df = pd.DataFrame(records)

    # Inject bad data: null customer_id and malformed emails
    bad_idx = rng.choice(df.index, size=int(N_CUSTOMERS * BAD_DATA_RATE), replace=False)
    df.loc[bad_idx[:len(bad_idx)//2], "customer_id"] = None
    df.loc[bad_idx[len(bad_idx)//2:], "email"] = "not-an-email"

    return df


def generate_products() -> pd.DataFrame:
    category_price_range = {
        "Electronics": (50, 1500),
        "Clothing":    (10, 200),
        "Home":        (15, 500),
        "Beauty":      (5, 150),
    }

    records = []
    for i in range(1, N_PRODUCTS + 1):
        category = rng.choice(CATEGORIES)
        low, high = category_price_range[category]
        records.append({
            "product_id": i,
            "product_name": fake.bs().title(),
            "category": category,
            "price": round(float(rng.uniform(low, high)), 2),
        })

    df = pd.DataFrame(records)

    # Inject bad data: negative prices
    bad_idx = rng.choice(df.index, size=int(N_PRODUCTS * BAD_DATA_RATE), replace=False)
    df.loc[bad_idx, "price"] = df.loc[bad_idx, "price"] * -1

    return df


def generate_orders(customer_ids: list) -> pd.DataFrame:
    # Weight statuses: mostly completed, some cancelled, fewer returned
    status_weights = [0.75, 0.15, 0.10]

    records = []
    for i in range(1, N_ORDERS + 1):
        records.append({
            "order_id": i,
            "customer_id": rng.choice(customer_ids),
            "order_date": fake.date_between(start_date="-2y", end_date="today"),
            "status": rng.choice(ORDER_STATUSES, p=status_weights),
            "total_amount": 0.0,  # filled after order_items are generated
        })

    df = pd.DataFrame(records)

    # Inject bad data: null order_ids
    bad_idx = rng.choice(df.index, size=int(N_ORDERS * BAD_DATA_RATE), replace=False)
    df.loc[bad_idx, "order_id"] = None

    return df


def generate_order_items(order_ids: list, product_ids: list, products_df: pd.DataFrame) -> pd.DataFrame:
    price_map = products_df.set_index("product_id")["price"].to_dict()

    records = []
    item_id = 1
    for order_id in order_ids:
        n_items = int(rng.integers(1, 5))
        chosen_products = rng.choice(product_ids, size=n_items, replace=False)
        for product_id in chosen_products:
            base_price = abs(price_map.get(int(product_id), 50.0))
            # Slight price variation to simulate discounts
            unit_price = round(base_price * float(rng.uniform(0.85, 1.0)), 2)
            records.append({
                "item_id": item_id,
                "order_id": order_id,
                "product_id": int(product_id),
                "quantity": int(rng.integers(1, 6)),
                "unit_price": unit_price,
            })
            item_id += 1

    df = pd.DataFrame(records)

    # Inject bad data: negative quantities
    bad_idx = rng.choice(df.index, size=int(len(df) * BAD_DATA_RATE), replace=False)
    df.loc[bad_idx, "quantity"] = df.loc[bad_idx, "quantity"] * -1

    return df


def generate_returns(orders_df: pd.DataFrame) -> pd.DataFrame:
    # Only completed or returned orders are eligible for returns
    eligible = orders_df[orders_df["status"].isin(["completed", "returned"])].copy()
    n_returns = int(len(eligible) * RETURN_RATE)
    sampled = eligible.sample(n=n_returns, random_state=RANDOM_SEED)

    records = []
    for i, (_, row) in enumerate(sampled.iterrows(), start=1):
        order_date = pd.to_datetime(row["order_date"])
        return_date = fake.date_between(
            start_date=order_date + pd.Timedelta(days=1),
            end_date=order_date + pd.Timedelta(days=30),
        )
        refund_amount = round(float(rng.uniform(10, float(row["total_amount"]) if row["total_amount"] > 10 else 50)), 2)
        records.append({
            "return_id": i,
            "order_id": row["order_id"],
            "return_date": return_date,
            "reason": rng.choice(RETURN_REASONS),
            "refund_amount": refund_amount,
        })

    df = pd.DataFrame(records)

    # Inject bad data: return_date before order_date
    bad_idx = rng.choice(df.index, size=int(len(df) * BAD_DATA_RATE), replace=False)
    df.loc[bad_idx, "return_date"] = pd.to_datetime("2020-01-01")

    return df


def update_order_totals(orders_df: pd.DataFrame, items_df: pd.DataFrame) -> pd.DataFrame:
    totals = (
        items_df[items_df["quantity"] > 0]
        .assign(line_total=lambda x: x["quantity"] * x["unit_price"])
        .groupby("order_id")["line_total"]
        .sum()
        .reset_index()
        .rename(columns={"line_total": "total_amount"})
    )
    orders_df = orders_df.drop(columns=["total_amount"]).merge(totals, on="order_id", how="left")
    orders_df["total_amount"] = orders_df["total_amount"].fillna(0).round(2)
    return orders_df


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print("Generating synthetic e-commerce data...")

    print("  customers...")
    customers = generate_customers()
    customers.to_csv(RAW_DIR / "customers.csv", index=False)

    print("  products...")
    products = generate_products()
    products.to_csv(RAW_DIR / "products.csv", index=False)

    # Use only valid customer/product IDs for foreign keys
    valid_customer_ids = customers["customer_id"].dropna().astype(int).tolist()
    valid_product_ids = products["product_id"].dropna().astype(int).tolist()

    print("  orders...")
    orders = generate_orders(valid_customer_ids)

    print("  order_items...")
    valid_order_ids = orders["order_id"].dropna().astype(int).tolist()
    order_items = generate_order_items(valid_order_ids, valid_product_ids, products)

    print("  updating order totals...")
    orders = update_order_totals(orders, order_items)
    orders.to_csv(RAW_DIR / "orders.csv", index=False)
    order_items.to_csv(RAW_DIR / "order_items.csv", index=False)

    print("  returns...")
    returns = generate_returns(orders)
    returns.to_csv(RAW_DIR / "returns.csv", index=False)

    print("\nGeneration complete. Row counts:")
    print(f"  customers:   {len(customers):,}")
    print(f"  products:    {len(products):,}")
    print(f"  orders:      {len(orders):,}")
    print(f"  order_items: {len(order_items):,}")
    print(f"  returns:     {len(returns):,}")
    print(f"\nFiles written to: {RAW_DIR}")


if __name__ == "__main__":
    main()
