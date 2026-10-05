import time
import uuid
import argparse
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.mongo_setup import setup_mongodb
from src.file_router import select_processing_engine
from src.batch_loader import load_batch
from src.spark_loader import load_spark
from src.elt_pipeline import run_elt
from src.metrics import generate_metrics

def main():
    parser = argparse.ArgumentParser(description="Midterm Data Pipeline - ELT")
    parser.add_argument('--file', type=str, required=True, help="Path to the data file to process")
    args = parser.parse_args()
    
    file_path = args.file
    
    print("========================================")
    print("🚀 بدء خط أنابيب البيانات (Hybrid Pipeline)")
    print("========================================")
    
    # 1. إعداد قاعدة البيانات
    setup_mongodb()
    
    # 2. إنشاء Run ID
    run_id = f"run_{uuid.uuid4().hex[:8]}"
    print(f"-> Run ID: {run_id}")
    
    # 3. التوجيه (File Router)
    engine = select_processing_engine(file_path)
    if not engine:
        sys.exit(1)
        
    start_time_total = time.time()
    
    # 4. التحميل الخام (Raw Load)
    raw_loaded = 0
    if engine == "python_batch":
        raw_loaded = load_batch(run_id, file_path)
    else:
        raw_loaded = load_spark(run_id, file_path)
        
    if raw_loaded == 0:
        print("❌ لم يتم تحميل أي بيانات خام، إيقاف الخط.")
        sys.exit(1)
        
    # 5. عملية ELT (التنظيف، التصنيف، التحميل الآمن)
    elt_counters, elt_elapsed = run_elt(run_id)
    
    # 6. استخراج المقاييس وحفظها
    total_seconds = time.time() - start_time_total
    generate_metrics(run_id, file_path, engine, raw_loaded, elt_counters, total_seconds)
    
    print("\n✅ اكتمل التشغيل بنجاح.")

if __name__ == "__main__":
    main()