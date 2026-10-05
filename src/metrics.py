import os
import json
from config.settings import RESULTS_FILE, BATCH_SIZE

def generate_metrics(run_id, file_path, engine, raw_loaded, elt_counters, total_seconds):
    print(f"4. Generating reports and metrics...")
    
    file_size_mb = 0
    if os.path.exists(file_path):
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        

    total_elt_processed = elt_counters['valid'] + elt_counters['corrected'] + elt_counters['quarantined']
    idempotency_ok = (total_elt_processed == raw_loaded)
    
    metrics = {
        "run_id": run_id,
        "file_name": os.path.basename(file_path),
        "file_size_mb": round(file_size_mb, 2),
        "engine_used": engine,
        "rows_read": raw_loaded,
        "raw_loaded": raw_loaded,
        "valid_count": elt_counters['valid'],
        "corrected_count": elt_counters['corrected'],
        "validated_count": elt_counters.get('inserted', elt_counters['valid'] + elt_counters['corrected']),
        "quarantine_count": elt_counters['quarantined'],
        "elapsed_seconds": round(total_seconds, 2),
        "total_seconds": round(total_seconds, 2),
        "throughput": round(raw_loaded / total_seconds if total_seconds > 0 else 0, 2),
        "batch_size_or_partitions": BATCH_SIZE if engine == "python_batch" else 100,
        "error_case_counts": elt_counters.get("error_case_counts", {}),
        "inserted_count": elt_counters['inserted'],
        "updated_count": elt_counters['updated'],
        "unchanged_count": elt_counters['unchanged'],
        "idempotency_check": "PASS" if idempotency_ok else "FAIL"
    }
    
    os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)
    
    all_results = []
    if os.path.exists(RESULTS_FILE):
        try:
            with open(RESULTS_FILE, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    all_results = json.loads(content)
        except Exception as e:
            print(f"Could not read existing results: {e}")
            
    all_results.append(metrics)
    
    with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=4)
        
    print(f"✅  Report has been saved to {RESULTS_FILE} successfully.")
    return metrics
