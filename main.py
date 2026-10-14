"""
main.py
=======
Unified Non-Interactive & CLI Entry Point for Big Data Pipeline (Midterm & Final)
Al-Razi University - Big Data Course

Features:
  - run_ingest(file_path): Direct non-interactive execution for automated grading scripts.
  - CLI execution with arguments: python main.py <file_path>
  - Zero manual input prompts (no input() blocking).
"""

import os
import sys
import time
import uuid
import argparse

from config.settings import (
    SMALL_SAMPLE_PATH,
    HUGE_FILE_PATH,
)
from src.mongo_setup import setup_mongodb
from src.file_router import select_processing_engine
from src.batch_loader import load_batch
from src.spark_loader import load_spark
from src.elt_pipeline import run_elt
from src.metrics import generate_metrics


def run_ingest(file_path: str = None) -> dict:
    """
    Non-interactive execution of the ELT Data Ingestion pipeline.
    Suitable for automated grading scripts, API calls, and scheduled jobs.
    
    Args:
        file_path (str, optional): Path to CSV data file.
                                   If None, uses 01_student_test_small.csv or SMALL_SAMPLE_PATH.
                                   
    Returns:
        dict: Full metrics dictionary containing run_id, loaded, validated, quarantined, throughput, etc.
    """
    # 1. Resolve target file path
    if not file_path:
        candidates = [
            os.path.join("data", "01_student_test_small.csv"),
            SMALL_SAMPLE_PATH,
            os.path.join("data", "orders_small_sample.csv"),
            HUGE_FILE_PATH,
        ]
        for c in candidates:
            if c and os.path.exists(c):
                file_path = c
                break
        if not file_path:
            file_path = SMALL_SAMPLE_PATH

    # Normalize relative/data folder path if needed
    if not os.path.exists(file_path):
        alt_path = os.path.join("data", os.path.basename(file_path))
        if os.path.exists(alt_path):
            file_path = alt_path

    print("=" * 60)
    print("🚀 Starting Big Data Pipeline (Non-Interactive Ingest)")
    print(f"📁 Target File : {file_path}")
    print("=" * 60)

    # 2. Setup MongoDB (creates unique index for idempotency)
    setup_mongodb()

    # 3. Select processing engine (python_batch <= 400MB < pyspark)
    engine = select_processing_engine(file_path)
    if not engine:
        raise FileNotFoundError(f"File not found: {file_path}")

    # 4. Generate unique run ID
    run_id = f"run_{uuid.uuid4().hex[:8]}"
    print(f"🔑 Run ID: {run_id} | Engine: {engine}")

    # 5. Raw Ingestion stage
    start_time = time.time()
    raw_loaded = 0
    if engine == "python_batch":
        raw_loaded = load_batch(run_id, file_path)
    elif engine == "pyspark":
        raw_loaded = load_spark(run_id, file_path)

    print(f"📦 Raw Loaded: {raw_loaded:,} records")

    # 6. ELT Transformation stage (Cleaning, Quarantine, Audit Trail, Idempotency)
    elt_counters = {}
    if raw_loaded > 0:
        elt_counters, _ = run_elt(run_id)

    total_seconds = time.time() - start_time

    # 7. Metrics generation
    metrics = generate_metrics(
        run_id, file_path, engine, raw_loaded, elt_counters, total_seconds
    )

    print("=" * 60)
    print("✅ Ingestion & ELT Completed Successfully!")
    print(f"   - Validated Records   : {metrics.get('validated_count', 0):,}")
    print(f"   - Quarantined Records : {metrics.get('quarantine_count', 0):,}")
    print(f"   - Total Duration      : {metrics.get('total_seconds', total_seconds):.2f}s")
    print(f"   - Throughput          : {metrics.get('throughput', 0):.2f} rows/s")
    print(f"   - Idempotency Check   : {metrics.get('idempotency_check', 'PASS')}")
    print("=" * 60)

    return metrics


def main():
    parser = argparse.ArgumentParser(
        description="Unified Big Data Pipeline Ingestion CLI (Non-Interactive)"
    )
    parser.add_argument(
        "file",
        nargs="?",
        default=None,
        help="Path to CSV dataset file (positional argument)",
    )
    parser.add_argument(
        "--file",
        "-f",
        dest="file_flag",
        default=None,
        help="Path to CSV dataset file (flag argument)",
    )

    args = parser.parse_args()
    target_file = args.file_flag or args.file

    try:
        run_ingest(target_file)
        sys.exit(0)
    except Exception as e:
        print(f"❌ Pipeline Execution Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
