from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import json
import os
import sys
from datetime import datetime

def run_job():
    spark = SparkSession.builder.appName("TopCallers").getOrCreate()
    
    run_id = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else datetime.now().strftime("%Y%m%d_%H%M%S")
    input_path = "/data/cdr_data.csv"
    output_path = f"/output/top_callers_by_spend/{run_id}"
    
    # Read the data
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Aggregation logic
    result = df.groupBy("caller_id") \
               .agg(_sum("charge_amount").alias("total_spend")) \
               .orderBy(col("total_spend").desc()) \
               .limit(100)
    
    # Write output
    result.write.csv(output_path, header=True)
    
    # Generate Manifest
    manifest = {
        "job_name": "top_callers_by_spend",
        "run_id": run_id,
        "execution_timestamp_utc": datetime.utcnow().isoformat(),
        "input_path": input_path,
        "output_path": output_path,
        "input_record_count": df.count(),
        "output_record_count": result.count(),
        "status": "SUCCESS"
    }
    
    os.makedirs(output_path, exist_ok=True)
    with open(f"{output_path}/_MANIFEST.json", "w") as f:
        json.dump(manifest, f)

    spark.stop()

if __name__ == "__main__":
    run_job()