import os
from config.settings import SMALL_FILE_THRESHOLD_MB

def select_processing_engine(file_path):
    try:
        file_size_bytes = os.path.getsize(file_path)
        file_size_mb = file_size_bytes / (1024 * 1024)
        print(f"--> Size of the file: {file_size_mb:.2f} MB")
        
        if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
            print(f"✅ ","-"*40, flush=True)
            print(f"--> Decision: Using (python_batch) engine.")
            print(f"✅ ","-"*40, flush=True)
            return "python_batch"
        else:
            print(f"✅ ","-"*40, flush=True)
            print(f"--> Decision: Using (pyspark) engine.")
            print(f"✅ ","-"*40, flush=True)
            return "pyspark"
            
    except FileNotFoundError:
        print(f"❌ Error: File not found at path: {file_path}")
        print(f"❌ ","-"*40, flush=True)
        return None