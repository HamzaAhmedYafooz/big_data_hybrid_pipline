import os

# Thresholds
SMALL_SAMPLE_GENERATION_SIZE_MB = 1_000_000
SMALL_FILE_THRESHOLD_MB = 400

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "hamza_db"

# Collections
COLLECTION_RAW = "orders_raw"
COLLECTION_VALIDATED = "orders_validated"
COLLECTION_QUARANTINE = "orders_quarantine"
COLLECTION_METRICS = "pipeline_metrics"
COLLECTION_CHECKPOINTS = "elt_checkpoints"
COLLECTION_MATERIALIZED = "monthly_city_products"

# Default Paths
_SAMPLE_NAME_1 = os.path.join("data", "orders_small_sample.csv")
_SAMPLE_NAME_2 = os.path.join("data", "01_student_test_small.csv")
SMALL_SAMPLE_PATH = _SAMPLE_NAME_1 if os.path.exists(_SAMPLE_NAME_1) else (_SAMPLE_NAME_2 if os.path.exists(_SAMPLE_NAME_2) else _SAMPLE_NAME_1)
HUGE_FILE_PATH = os.path.join("data", "orders_huge_mixed_quality.csv")
REPORTS_DIR = "reports"
RESULTS_FILE = os.path.join(REPORTS_DIR, "results.json")

# Pipeline Settings
BATCH_SIZE = 5000