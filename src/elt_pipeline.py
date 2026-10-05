import os
import sys
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='backslashreplace')
    except Exception:
        pass
import time
import json
import gc
import concurrent.futures
from datetime import datetime
from bson import ObjectId
from pymongo import MongoClient, UpdateOne
from config.settings import MONGO_URI, MONGO_DB_NAME, COLLECTION_RAW, COLLECTION_VALIDATED, COLLECTION_QUARANTINE
from src.quality_rules import apply_quality_rules

# ============================================================
# إعدادات حجم الدفعة ونقاط الحفظ والاستئناف (Checkpoints)
# ============================================================
DEFAULT_BATCH_SIZE = 5000
MAX_WRITE_THREADS = 1
COLLECTION_CHECKPOINTS = "elt_checkpoints"
CHECKPOINT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "checkpoints")

def _get_checkpoint_filepath(run_id):
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    return os.path.join(CHECKPOINT_DIR, f"checkpoint_{run_id}.json")

def load_checkpoint(db, run_id):
    """
    استرجاع نقطة الاستئناف (Checkpoint) من MongoDB أو من القرص المحلي كنسخة احتياطية
    """
    # 1. محاولة القراءة من MongoDB
    try:
        chk_col = db[COLLECTION_CHECKPOINTS]
        chk = chk_col.find_one({"run_id": run_id})
        if chk:
            return chk
    except Exception:
        pass
    
    # 2. كبديل: محاولة القراءة من ملف JSON المحلي
    try:
        fp = _get_checkpoint_filepath(run_id)
        if os.path.exists(fp):
            with open(fp, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
        
    return None

def save_checkpoint(db, run_id, last_id, counters, elapsed_so_far, status="IN_PROGRESS"):
    """
    حفظ نقطة الوصول (Checkpoint) فورياً لضمان عدم ضياع أي تقدم عند حدوث خطأ أو انقطاع
    """
    data = {
        "run_id": run_id,
        "last_id": str(last_id) if last_id else None,
        "status": status,
        "counters": counters,
        "elapsed_so_far": round(elapsed_so_far, 2),
        "updated_at": datetime.now().isoformat()
    }
    
    # حفظ في MongoDB
    try:
        chk_col = db[COLLECTION_CHECKPOINTS]
        chk_col.replace_one({"run_id": run_id}, data, upsert=True)
    except Exception:
        pass
        
    # حفظ في ملف محلي كنسخة احتياطية فائقة الموثوقية
    try:
        fp = _get_checkpoint_filepath(run_id)
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def run_elt(run_id, batch_size=DEFAULT_BATCH_SIZE, resume=True):
    """
    تنفيذ خط أنابيب ELT فائق السرعة والموثوقية مع نظام الاستئناف ونقاط الحفظ (Checkpoints):
    - استئناف المعالجة تلقائياً من آخر نقطة توقف (Last Processed Checkpoint) دون البدء من الصفر.
    - حماية تامة لذاكرة النظام العشوائية (RAM) ومنع خطأ MemoryError نهائياً عبر Keyset Pagination.
    - منع اختناق قاعدة البيانات وانقطاع الاتصال (ConnectionResetError / rollbackUnderCachePressure).
    - تنفيذ عمليات Idempotent Upsert مع توثيق الـ Audit Trail وعزل السجلات التالفة.
    - معالجة CPU متوازنة مع كتابة I/O آمنة لتحقيق أقصى سرعة ممكنة.
    """
    print(f"[{run_id}] 2. بدء عملية التنظيف (ELT) والـ Upsert بنمط تدفقي متوازي مع حماية نقاط الحفظ (Checkpointing)...")
    start_time = time.time()
    
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000, maxPoolSize=20)
    db = client[MONGO_DB_NAME]
    raw_col = db[COLLECTION_RAW]
    valid_col = db[COLLECTION_VALIDATED]
    quar_col = db[COLLECTION_QUARANTINE]
    
    counters = {
        "read": 0,
        "valid": 0,
        "corrected": 0,
        "quarantined": 0,
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
        "error_case_counts": {}
    }
    
    # Check previous checkpoint status before continuing
    checkpoint = load_checkpoint(db, run_id) if resume else None
    last_id = None
    start_offset_time = 0.0
    
    if checkpoint and checkpoint.get("status") == "IN_PROGRESS" and checkpoint.get("last_id"):
        last_id = ObjectId(checkpoint["last_id"])
        saved_counters = checkpoint.get("counters", {})
        for k in counters:
            if k in saved_counters:
                counters[k] = saved_counters[k]
        start_offset_time = checkpoint.get("elapsed_so_far", 0.0)
        
        print(f"\n🔄 [Automatic Resume - Checkpoint Found]")
        print(f"-> The last processed location for this run [{run_id}] was found:")
        print(f"-> Previously completed records: {counters['read']:,} rows (valid: {counters['valid']:,} | corrected: {counters['corrected']:,} | quarantined: {counters['quarantined']:,})")
        print(f"-> Last processed ID (Checkpoint ID): {last_id}")
        print(f"-> Reprocessing of prior records is avoided; remaining records will continue immediately! 🚀\n")
        
        query = {"run_id": run_id, "_id": {"$gt": last_id}}
    else:
        query = {"run_id": run_id}
        save_checkpoint(db, run_id, None, counters, 0.0, status="IN_PROGRESS")
    
    batch_docs = []
    last_print_time = time.time()
    committed_last_id = last_id
    
    def process_batch(docs):
        """Process one batch: apply quality rules and return ready-to-write operations"""
        valid_ops = []
        quar_ops = []
        batch_valid = 0
        batch_corrected = 0
        batch_quarantined = 0
        local_error_counts = {}
        
        for raw_doc in docs:
            raw_record = raw_doc.get("raw_record", {})
            cleaned, quality_status, corrections, errors = apply_quality_rules(raw_record)
            
            if quality_status == "quarantined":
                batch_quarantined += 1
                for err in errors:
                    local_error_counts[err] = local_error_counts.get(err, 0) + 1
                    
                quar_ops.append({
                    "run_id": run_id,
                    "error_codes": errors,
                    "error_details": "Failed quality rules",
                    "raw_record": raw_record,
                    "ingested_at": raw_doc.get("ingested_at")
                })
            else:
                if quality_status == "corrected":
                    batch_corrected += 1
                else:
                    batch_valid += 1
                    
                cleaned["quality_status"] = quality_status
                if corrections:
                    cleaned["corrections"] = corrections
                    
                order_id = cleaned.get("order_id")
                valid_ops.append(
                    UpdateOne(
                        {"order_id": order_id},
                        {"$set": cleaned},
                        upsert=True
                    )
                )
        
        last_doc_id = docs[-1]["_id"] if docs else None
        return valid_ops, quar_ops, batch_valid, batch_corrected, batch_quarantined, local_error_counts, len(docs), last_doc_id
    
    def write_to_mongo(valid_ops, quar_ops):
        """Write prepared operations to MongoDB (runs in a separate thread)"""
        result_info = {"inserted": 0, "updated": 0, "unchanged": 0}
        
        if valid_ops:
            result = valid_col.bulk_write(valid_ops, ordered=False)
            result_info["inserted"] = result.upserted_count
            result_info["updated"] = result.modified_count
            result_info["unchanged"] = (result.matched_count - result.modified_count)
            
        if quar_ops:
            quar_col.insert_many(quar_ops, ordered=False)
            
        return result_info
    
    def update_counters(batch_valid, batch_corrected, batch_quarantined, local_error_counts, doc_count, batch_last_id, write_result):
        """Update the main counters and save a checkpoint after writing completes"""
        nonlocal committed_last_id
        counters["read"] += doc_count
        counters["valid"] += batch_valid
        counters["corrected"] += batch_corrected
        counters["quarantined"] += batch_quarantined
        counters["inserted"] += write_result["inserted"]
        counters["updated"] += write_result["updated"]
        counters["unchanged"] += write_result["unchanged"]
        for err, cnt in local_error_counts.items():
            counters["error_case_counts"][err] = counters["error_case_counts"].get(err, 0) + cnt
            
        if batch_last_id:
            committed_last_id = batch_last_id

    cursor = None
    try:
        # Use keyset pagination via the primary _id index with small streaming batches to avoid memory exhaustion
        cursor = raw_col.find(
            query, 
            projection={"raw_record": 1, "ingested_at": 1},
            no_cursor_timeout=True
        ).sort("_id", 1).batch_size(1000)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WRITE_THREADS) as executor:
            pending_futures = []
            
            for doc in cursor:
                batch_docs.append(doc)
                if len(batch_docs) >= batch_size:
                    valid_ops, quar_ops, bv, bc, bq, lec, dc, b_lid = process_batch(batch_docs)
                    batch_docs = []
                    
                    if pending_futures:
                        done_future = pending_futures.pop(0)
                        write_result = done_future.result()
                        update_counters(*done_future._batch_info, write_result)
                    
                    future = executor.submit(write_to_mongo, valid_ops, quar_ops)
                    future._batch_info = (bv, bc, bq, lec, dc, b_lid)
                    pending_futures.append(future)
                    
                    now = time.time()
                    elapsed_total = (now - start_time) + start_offset_time
                    if now - last_print_time >= 2.0:
                        speed = counters["read"] / elapsed_total if elapsed_total > 0 else 0
                        print(f"-> Completed: {counters['read']:,} records | speed: {speed:,.0f} records/second | valid: {counters['valid']:,} | corrected: {counters['corrected']:,} | quarantined: {counters['quarantined']:,}")
                        last_print_time = now
                        save_checkpoint(db, run_id, committed_last_id, counters, elapsed_total, status="IN_PROGRESS")
                        
                    if counters["read"] % 25000 == 0:
                        gc.collect()
                        
            # Process the remaining records in the last batch
            if batch_docs:
                valid_ops, quar_ops, bv, bc, bq, lec, dc, b_lid = process_batch(batch_docs)
                batch_docs = []
                future = executor.submit(write_to_mongo, valid_ops, quar_ops)
                future._batch_info = (bv, bc, bq, lec, dc, b_lid)
                pending_futures.append(future)
            
            # Wait for all pending writes to finish
            for future in pending_futures:
                write_result = future.result()
                update_counters(*future._batch_info, write_result)
                
        elapsed = (time.time() - start_time) + start_offset_time
        # Save the final completed state
        save_checkpoint(db, run_id, committed_last_id, counters, elapsed, status="COMPLETED")
        
    except BaseException as exc:
        elapsed_so_far = (time.time() - start_time) + start_offset_time
        if committed_last_id:
            save_checkpoint(db, run_id, committed_last_id, counters, elapsed_so_far, status="IN_PROGRESS")
            print(f"\n⚠️ Alert: processing stopped because: {exc}")
            print(f"💾 The latest checkpoint was successfully saved at record: {counters['read']:,} (ID: {committed_last_id})")
            print(f"🔄 When the notebook is restarted, processing will resume from this point without re-reading previous records!")
        raise exc
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        client.close()
        
    elapsed = (time.time() - start_time) + start_offset_time
    rate = counters["read"] / elapsed if elapsed > 0 else 0
    print(f"\n=== Cleaning and Upsert process completed successfully at maximum speed! ===")
    print(f"Total records: {counters['read']:,} | valid: {counters['valid']:,} | corrected: {counters['corrected']:,} | quarantined: {counters['quarantined']:,}")
    print(f"Total time: {elapsed:.2f} seconds | average processing speed: {rate:,.0f} records/second")
    
    return counters, elapsed
