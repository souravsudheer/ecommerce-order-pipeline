import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import duckdb
from config import RAW_DIR, BRONZE_DIR

TABLES = ["customers", "products", "orders", "order_items", "returns"]


def get_connection() -> duckdb.DuckDBPyConnection:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(BRONZE_DIR / "bronze.duckdb"))


def load_raw_to_bronze(conn: duckdb.DuckDBPyConnection) -> None:
    """Registers each raw CSV as a DuckDB table. No transformation."""
    for table in TABLES:
        csv_path = RAW_DIR / f"{table}.csv"

        if not csv_path.exists():
            raise FileNotFoundError(
                f"Raw file not found: {csv_path}\n"
                f"Run pipeline/generate_data.py first."
            )

        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(f"""
            CREATE TABLE {table} AS
            SELECT * FROM read_csv_auto('{csv_path}', header=True)
        """)

        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {row_count:,} rows loaded")


def validate_bronze(conn: duckdb.DuckDBPyConnection) -> None:
    # Minimal sanity checks — not quality checks, just confirming
    # all tables exist and are non-empty before downstream runs.
    print("\nValidating bronze layer...")
    all_ok = True
    for table in TABLES:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            cols = conn.execute(f"DESCRIBE {table}").fetchdf().shape[0]
            if count == 0:
                print(f"  WARNING: {table} is empty")
                all_ok = False
            else:
                print(f"  {table}: ok ({count:,} rows, {cols} columns)")
        except Exception as e:
            print(f"  ERROR: {table} failed — {e}")
            all_ok = False

    if all_ok:
        print("\nBronze layer ready.")
    else:
        print("\nBronze layer has issues. Check raw data generation.")


def main():
    print("Loading raw CSVs into bronze layer...")
    conn = get_connection()
    load_raw_to_bronze(conn)
    validate_bronze(conn)
    conn.close()
    print(f"\nBronze database written to: {BRONZE_DIR / 'bronze.duckdb'}")


if __name__ == "__main__":
    main()
