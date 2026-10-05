import os
import time
import sys

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType
    from pyspark.sql.functions import lit, current_timestamp, struct, col
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkSession = None

from config.settings import MONGO_URI, MONGO_DB_NAME, COLLECTION_RAW

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

hadoop_home = os.environ.get("HADOOP_HOME", "C:\\hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["hadoop.home.dir"] = hadoop_home
hadoop_bin = os.path.join(hadoop_home, "bin")
if hadoop_bin not in sys.path:
    sys.path.append(hadoop_bin)
    os.environ["PATH"] += os.pathsep + hadoop_bin


def load_spark(run_id, file_path):
    if not PYSPARK_AVAILABLE:
        print("❌ Error: PySpark is not installed in the current Python environment.")
        print("💡 PySpark is only required for processing huge distributed files (>400 MB).")
        print("💡 To install PySpark, run: pip install pyspark")
        return 0

    print(f"⚡ Starting to load With PySpark...")
    start_time = time.time()
    
    try:
        spark = SparkSession.builder \
            .appName("EcommerceHugeData") \
            .master("local[*]") \
            .config("spark.driver.memory", "12g") \
            .getOrCreate()
            # .config("spark.executor.memory", "8g") \
            
        spark.sparkContext.setLogLevel("ERROR")
        
        # Fixed Schema (String for all to preserve dirty data for ELT)
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("status", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("customer_phone", StringType(), True),
            StructField("customer_email", StringType(), True),
            StructField("city", StringType(), True),
            StructField("district", StringType(), True),
            StructField("delivery_type", StringType(), True),
            StructField("delivery_cost", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("payment_status", StringType(), True),
            StructField("payment_amount", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("total_amount", StringType(), True),
            StructField("items_json", StringType(), True)
        ])
        
        # 1. Read csv with the defined schema, ensuring all data is treated as strings to preserve dirty data for ELT
        df = spark.read.option("header", "true") \
                    .option("encoding", "utf-8") \
                    .option("quote", "\"") \
                    .option("escape", "\"") \
                    .schema(schema) \
                    .csv(file_path)
        
        # 2. add Metadata Columns: run_id, source_file, source_row_number, ingested_at, engine_used
        df = df.withColumn("run_id", lit(run_id)) \
            .withColumn("source_file", lit(file_path)) \
            .withColumn("source_row_number", lit(None).cast(StringType())) \
            .withColumn("ingested_at", current_timestamp().cast("string")) \
            .withColumn("engine_used", lit("pyspark"))
        
        original_cols = schema.fieldNames()
        
        #  Struct the original columns into a single column named "raw_record" to preserve the original data structure
        df = df.withColumn("raw_record", struct([col(c) for c in original_cols]))
        
        final_df = df.select("run_id", "source_file", "source_row_number", "ingested_at", "engine_used", "raw_record")
        
        # 3. Repartition the DataFrame to optimize parallel writes to MongoDB
        final_df = final_df.repartition(100)
        
        partitions = final_df.rdd.getNumPartitions()
        print(f"⚡  Number of partitions for parallel processing: {partitions}")
        
        # use an accumulator to count the total inserted records across partitions
        acc = spark.sparkContext.accumulator(0)
        
        # 4. Write to MongoDB in parallel using foreachPartition 
        def write_partition(partition):
            from pymongo import MongoClient
            from config.settings import MONGO_URI, MONGO_DB_NAME, COLLECTION_RAW
            
            client = MongoClient(MONGO_URI)
            db = client[MONGO_DB_NAME]
            col_raw = db[COLLECTION_RAW]
            
            docs = []
            local_count = 0
            for row in partition:
                docs.append(row.asDict(recursive=True))
                local_count += 1
                if len(docs) >= 5000:
                    col_raw.insert_many(docs, ordered=False)
                    docs = []
            if docs:
                col_raw.insert_many(docs, ordered=False)
                
            acc.add(local_count)
            
        final_df.foreachPartition(write_partition)
        
        total_inserted = acc.value
        elapsed = time.time() - start_time
        rate = total_inserted / elapsed if elapsed > 0 else 0
        
        print(f"✅ ","-"*40, flush=True)
        print(f"✅ Data loaded successfully with PySpark.")
        print(f"✅ ","-"*40, flush=True)
        print(f"✅ Done in {elapsed:.2f}s, Total records inserted: {total_inserted}, Rate: {rate:.2f} records/s")
        print(f"✅ ","-"*40, flush=True)
        print(f"✅ Data loaded in {elapsed:.2f}s")
        print(f"✅ ","-"*40, flush=True)
        spark.stop()
        return total_inserted
        
    except Exception as e:
        print(f"❌ حدث خطأ أثناء تشغيل محرك PySpark: {e}")
        return 0