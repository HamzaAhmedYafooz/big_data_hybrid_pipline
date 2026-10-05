import csv
import time
from datetime import datetime
from pymongo import MongoClient
from tqdm import tqdm
from config.settings import MONGO_URI, MONGO_DB_NAME, COLLECTION_RAW, BATCH_SIZE

def load_batch(run_id, file_path):
    print(f"⚡ Starting to load Batch...")
    
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)#**********************************
    db = client[MONGO_DB_NAME]
    collection = db[COLLECTION_RAW]
    
    batch = []
    total_inserted = 0
    batch_num = 1
    row_num = 2
    
    start_time = time.time()
    try:
        with tqdm(desc="Loading Batches:", unit= " Batch") as pbar:
            with open(file_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
                reader = csv.DictReader(f)#************************************************
                for row in reader:
                    doc = {
                        "run_id": run_id,
                        "source_file": file_path,
                        "source_row_number": row_num,
                        "ingested_at": datetime.now().isoformat(),
                        "engine_used": "python_batch",
                        "raw_record": row
                    }
                    batch.append(doc)
                    
                    if len(batch) >= BATCH_SIZE:
                        try:
                            collection.insert_many(batch)
                            total_inserted += len(batch)
                            elapsed = time.time() - start_time
                            rate = total_inserted / elapsed if elapsed > 0 else 0
                            pbar.update(1)
                            pbar.set_postfix({"Records":f"{total_inserted:,}"})
                            # print(f"✅ Batch {batch_num}: Inserted {total_inserted} | Time: {elapsed:.2f}s | Rate: {rate:.2f} rows/s")
                        except Exception as batch_e:
                            print(f"❌ Error inserting batch {batch_num}: {batch_e}")
                            
                        batch = []
                        batch_num += 1
                    row_num += 1
                    
                # Insert remaining
                if batch:
                    try:
                        collection.insert_many(batch)
                        total_inserted += len(batch)
                        elapsed = time.time() - start_time
                        rate = total_inserted / elapsed if elapsed > 0 else 0
                        print(f"✅ Final Batch {batch_num}: Inserted {total_inserted} | Time: {elapsed:.2f}s | Rate: {rate:.2f} rows/s")
                    except Exception as batch_e:
                        print(f"❌ Error inserting final batch: {batch_e}")
                
                elapsed_time = time.time() - start_time
                print()
                print(f"✅ ","-"*40, flush=True)
                print(f"✅ Data loaded in {elapsed_time:.2f}s")
                print(f"✅ ","-"*40, flush=True)
            return total_inserted
            
    except Exception as e:
        print(f"❌ Error occurred while loading batch: {e}")
        return 0
    finally:
        client.close()