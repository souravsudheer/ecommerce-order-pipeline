import sys
import time
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from pipeline.generate_data import main as generate
from pipeline.bronze import main as bronze
from pipeline.silver import main as silver
from pipeline.gold import main as gold
from pipeline.data_quality import main as quality


def divider(title: str) -> None:
    print(f"\n{'=' * 55}")
    print(f"  {title}")
    print(f"{'=' * 55}")


def run_stage(name: str, fn) -> float:
    divider(name)
    start = time.time()
    fn()
    elapsed = time.time() - start
    print(f"\n  Completed in {elapsed:.1f}s")
    return elapsed


def main():
    print("\nE-commerce Order Pipeline")
    print("Starting full run...\n")
    overall_start = time.time()

    stages = [
        ("1/5  Generate synthetic data", generate),
        ("2/5  Load bronze layer",       bronze),
        ("3/5  Transform silver layer",  silver),
        ("4/5  Build gold aggregates",   gold),
        ("5/5  Run data quality checks", quality),
    ]

    for name, fn in stages:
        run_stage(name, fn)

    total = time.time() - overall_start
    divider("Pipeline complete")
    print(f"  All stages finished in {total:.1f}s")
    print(f"\n  Outputs:")
    print(f"    data/bronze/   bronze.duckdb")
    print(f"    data/silver/   silver.duckdb + CSVs")
    print(f"    data/gold/     gold.duckdb + CSVs")
    print(f"    data/          quality_report.csv")
    print(f"\n  Run tests with: pytest tests/ -v\n")


if __name__ == "__main__":
    main()
