from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

QUALITY_REPORT_PATH = DATA_DIR / "quality_report.csv"

# Generation constants
RANDOM_SEED = 42
N_CUSTOMERS = 1000
N_PRODUCTS = 200
N_ORDERS = 10000
RETURN_RATE = 0.08       # 8% of orders get a return
BAD_DATA_RATE = 0.05     # 5% of records injected with quality issues

REGIONS = ["North", "South", "East", "West"]
CATEGORIES = ["Electronics", "Clothing", "Home", "Beauty"]
ORDER_STATUSES = ["completed", "cancelled", "returned"]
RETURN_REASONS = ["damaged", "wrong_item", "changed_mind", "defective"]
