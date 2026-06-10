from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, abs
from pyspark.sql.window import Window
import json
import os
import sys
from datetime import datetime

def run_anomalous_detection():
    spark = SparkSession.builder.appName("AnomalousCalls").getOrCreate()
    
    run_id = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else datetime.now().strftime("%Y%m%d_%H%M%S")
    input_path = "/data/cdr_data.csv"
    output_path = f"/output/anomalous_call_detection/{run_id}"
    
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Window function ensures data for a specific caller_id stays together
    # This is our "logical" equivalent to a custom partitioner
    window_spec = Window.partitionBy("caller_id")
    
    stats_df = df.withColumn("mean_dur", avg("duration_sec").over(window_spec)) \
                 .withColumn("stddev_dur", stddev("duration_sec").over(window_spec))
    
    # Filter anomalies: duration > 3 standard deviations
    anomalies = stats_df.filter(
        abs(col("duration_sec") - col("mean_dur")) > (3 * col("stddev_dur"))
    )
    
    anomalies.write.csv(output_path, header=True)
    
    # Generate Manifest
    manifest = {
        "job_name": "anomalous_call_detection",
        "run_id": run_id,
        "execution_timestamp_utc": datetime.utcnow().isoformat(),
        "input_path": input_path,
        "output_path": output_path,
        "input_record_count": df.count(),
        "output_record_count": anomalies.count(),
        "status": "SUCCESS"
    }
    
    os.makedirs(output_path, exist_ok=True)
    with open(f"{output_path}/_MANIFEST.json", "w") as f:
        json.dump(manifest, f)
        
    spark.stop()

if __name__ == "__main__":
    run_anomalous_detection()