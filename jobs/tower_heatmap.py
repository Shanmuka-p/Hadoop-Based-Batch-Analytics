from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp, count
import json
import os
import sys
from datetime import datetime

def run_job():
    spark = SparkSession.builder.appName("TowerHeatmap").getOrCreate()
    
    run_id = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else datetime.now().strftime("%Y%m%d_%H%M%S")
    input_path = "/data/cdr_data.csv"
    output_path = f"/output/tower_utilization_heatmap/{run_id}"
    
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Extract hour from ISO 8601 timestamp
    heatmap_df = df.withColumn("hour_of_day", hour(to_timestamp(col("timestamp")))) \
                   .groupBy("tower_id", "hour_of_day") \
                   .agg(count("*").alias("call_count"))
    
    heatmap_df.write.csv(output_path, header=True)
    
    # Generate Manifest
    manifest = {
        "job_name": "tower_utilization_heatmap",
        "run_id": run_id,
        "execution_timestamp_utc": datetime.utcnow().isoformat(),
        "input_path": input_path,
        "output_path": output_path,
        "input_record_count": df.count(),
        "output_record_count": heatmap_df.count(),
        "status": "SUCCESS"
    }
    
    os.makedirs(output_path, exist_ok=True)
    with open(f"{output_path}/_MANIFEST.json", "w") as f:
        json.dump(manifest, f)
        
    spark.stop()

if __name__ == "__main__":
    run_job()