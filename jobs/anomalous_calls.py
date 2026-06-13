from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import json
import os
import sys
import math
from datetime import datetime

def custom_partitioner(key):
    # Isolate "caller_whale" to partition 0 to prevent data skew issues
    if key == "caller_whale":
        return 0
    # Map all other callers across partitions 1 to 9 (10 total partitions)
    return (hash(key) % 9) + 1

def detect_anomalies_in_partition(iterator):
    caller_records = {}
    for key, row in iterator:
        if key not in caller_records:
            caller_records[key] = []
        caller_records[key].append(row)
    
    for caller_id, rows in caller_records.items():
        durations = [r["duration_sec"] for r in rows]
        n = len(durations)
        if n < 2:
            continue
        
        mean_dur = sum(durations) / n
        variance = sum((x - mean_dur) ** 2 for x in durations) / (n - 1)
        stddev_dur = math.sqrt(variance)
        
        if stddev_dur == 0:
            continue
            
        for r in rows:
            dur = r["duration_sec"]
            if abs(dur - mean_dur) > (3 * stddev_dur):
                yield (
                    caller_id,
                    r["timestamp"],
                    int(dur),
                    float(mean_dur),
                    float(stddev_dur)
                )

def run_anomalous_detection():
    spark = SparkSession.builder.appName("AnomalousCalls").getOrCreate()
    
    run_id = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else datetime.now().strftime("%Y%m%d_%H%M%S")
    input_path = "/data/cdr_data.csv"
    output_path = f"/output/anomalous_call_detection/{run_id}"
    
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # RDD transformation and partitioning with custom partitioner
    kv_rdd = df.rdd.map(lambda r: (r["caller_id"], r))
    partitioned_rdd = kv_rdd.partitionBy(10, custom_partitioner)
    
    anomaly_rdd = partitioned_rdd.mapPartitions(detect_anomalies_in_partition)
    
    anomaly_schema = StructType([
        StructField("caller_id", StringType(), True),
        StructField("call_timestamp", StringType(), True),
        StructField("duration_sec", IntegerType(), True),
        StructField("user_mean_duration", DoubleType(), True),
        StructField("user_stddev", DoubleType(), True)
    ])
    
    anomalies_df = spark.createDataFrame(anomaly_rdd, anomaly_schema)
    anomalies_df.write.csv(output_path, header=True)
    
    # Generate Manifest
    manifest = {
        "job_name": "anomalous_call_detection",
        "run_id": run_id,
        "execution_timestamp_utc": datetime.utcnow().isoformat(),
        "input_path": input_path,
        "output_path": output_path,
        "input_record_count": df.count(),
        "output_record_count": anomalies_df.count(),
        "status": "SUCCESS"
    }
    
    os.makedirs(output_path, exist_ok=True)
    with open(f"{output_path}/_MANIFEST.json", "w") as f:
        json.dump(manifest, f)
        
    spark.stop()

if __name__ == "__main__":
    run_anomalous_detection()