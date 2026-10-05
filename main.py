import sys
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='backslashreplace')
        sys.stderr.reconfigure(encoding='utf-8', errors='backslashreplace')
    except Exception:
        pass
import time
import uuid
import json

from config.settings import (
    HUGE_FILE_PATH,
    SMALL_SAMPLE_GENERATION_SIZE_MB,
    SMALL_SAMPLE_PATH,
    MONGO_URI,
    MONGO_DB_NAME
)

from src.create_small_sample import generate_sample
from src.mongo_setup import setup_mongodb
from src.file_router import select_processing_engine
from src.batch_loader import load_batch
from src.spark_loader import load_spark
from src.elt_pipeline import run_elt
from src.metrics import generate_metrics
from src import aggregations
from src import analytics_queries
from src import materialized_views
from src import scheduler_jobs


def run_ingest(file_path):
    setup_mongodb()
    run_id = f'run_{uuid.uuid4().hex[:8]}'
    print(f'\n-> Run ID: {run_id}')
    print('🔍 Checking file size to select the appropriate engine...')
    engine = select_processing_engine(file_path)

    if not engine:
        print('❌ File not found:', file_path)
        return

    print('⏱️  Starting the data processing pipeline...')
    start_time_total = time.time()
    print('📥 Stage 1: Load stage (Raw Ingestion)...')
    raw_loaded = 0
    if engine == 'python_batch':
        raw_loaded = load_batch(run_id, file_path)
    elif engine == 'pyspark':
        raw_loaded = load_spark(run_id, file_path)

    print(f'\n✅ Total records loaded raw: {raw_loaded:,}')
    if raw_loaded > 0:
        print('🛠️  Beginning cleanup, quarantine, and safe update processes (ELT)...')
        elt_counters, elt_elapsed = run_elt(run_id)
        print('📊 Generating and exporting reports...')
        total_seconds = time.time() - start_time_total
        metrics = generate_metrics(run_id, file_path, engine, raw_loaded, elt_counters, total_seconds)
        print('\n✅ Execution completed successfully! Extracted metrics:')
        print(json.dumps(metrics, indent=4, ensure_ascii=False))
    else:
        print('⚠️ No data available for processing.')


def main():
    while True:
        print("\n" + "=" * 65)
        print("🎓 Big Data Pipeline Project (Al-Razi University) - Main System")
        print("=" * 65)
        print(" [1] Generate Small Sample & Run Ingestion Pipeline")
        print(" [2] Run Ingestion Pipeline on Huge File")
        print(" [3] Run Practical Queries, Indexes & Explain Benchmark")
        print(" [4] Run Aggregation Reports (5 Reports)")
        print(" [5] Build & Incremental Refresh Materialized Views")
        print(" [6] Execute Scheduled Jobs & View Execution Audit Logs")
        print(" [7] Start Unified FastAPI Server")
        print(" [0] Exit")
        print("=" * 65)

        try:
            choice = int(input(" Enter your choice [0-7]: "))
        except ValueError:
            print("❌ Please enter a valid number.")
            continue

        if choice == 0:
            print("Goodbye!")
            break

        elif choice == 1:
            generate_sample(HUGE_FILE_PATH, SMALL_SAMPLE_PATH, num_rows=SMALL_SAMPLE_GENERATION_SIZE_MB)
            run_ingest(SMALL_SAMPLE_PATH)

        elif choice == 2:
            run_ingest(HUGE_FILE_PATH)

        elif choice == 3:
            from pymongo import MongoClient
            client = MongoClient(MONGO_URI)
            db = client[MONGO_DB_NAME]
            try:
                analytics_queries.run_explain_benchmarks(db)
            finally:
                client.close()

        elif choice == 4:
            from pymongo import MongoClient
            client = MongoClient(MONGO_URI)
            db = client[MONGO_DB_NAME]
            try:
                aggregations.run_all(db)
            finally:
                client.close()

        elif choice == 5:
            from pymongo import MongoClient
            client = MongoClient(MONGO_URI)
            db = client[MONGO_DB_NAME]
            try:
                res = materialized_views.refresh_all_views(db, mode="incremental")
                print("✅ Materialized Views refresh result:")
                print(json.dumps(res, indent=2, ensure_ascii=False))
            finally:
                client.close()

        elif choice == 6:
            from pymongo import MongoClient
            client = MongoClient(MONGO_URI)
            db = client[MONGO_DB_NAME]
            try:
                print("\n1. Executing Job 1 (refresh_materialized_views)...")
                r1 = scheduler_jobs.execute_job("refresh_materialized_views", db=db, triggered_by="cli_manual")
                print(f"Status: {r1['status']} | Duration: {r1['duration_seconds']}s")

                print("\n2. Executing Job 2 (generate_periodic_report)...")
                r2 = scheduler_jobs.execute_job("generate_periodic_report", db=db, triggered_by="cli_manual")
                print(f"Status: {r2['status']} | Duration: {r2['duration_seconds']}s")

                print("\n3. Latest execution audit logs in database:")
                logs = scheduler_jobs.get_job_history(db, limit=5)
                for l in logs:
                    print(f"   [{l['status']}] {l['job_name']} at {l['start_time']} (duration: {l['duration_seconds']}s)")
            finally:
                client.close()

        elif choice == 7:
            import uvicorn
            print("\n🚀 Starting FastAPI server...")
            print("🌐 Direct URL: http://127.0.0.1:8000")
            print("📚 Interactive Swagger Docs: http://127.0.0.1:8000/docs")
            uvicorn.run("src.api:app", host="127.0.0.1", port=8000, reload=False)
            break


if __name__ == "__main__":
    main()
