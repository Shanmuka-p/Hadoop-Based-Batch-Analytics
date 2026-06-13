from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
import json
import os
import sys
from datetime import datetime

def run_job():
    spark = SparkSession.builder.appName("RevenueRecon").getOrCreate()
    
    run_id = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else datetime.now().strftime("%Y%m%d_%H%M%S")
    input_path = "/data/cdr_data.csv"
    output_path = f"/output/revenue_reconciliation/{run_id}"
    
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    total_rev = df.agg(_sum("charge_amount").alias("total_revenue")).collect()[0]["total_revenue"]
    if total_rev is None:
        total_rev = 0.0
        
    # Write output as CSV with column total_revenue
    result_df = spark.createDataFrame([(float(total_rev),)], ["total_revenue"])
    result_df.write.csv(output_path, header=True)
    
    # Also write total_revenue.txt within that directory for compatibility
    with open(f"{output_path}/total_revenue.txt", "w") as f:
        f.write(str(total_rev))
        
    # Generate Manifest
    manifest = {
        "job_name": "revenue_reconciliation",
        "run_id": run_id,
        "execution_timestamp_utc": datetime.utcnow().isoformat(),
        "input_path": input_path,
        "output_path": output_path,
        "input_record_count": df.count(),
        "output_record_count": 1,
        "status": "SUCCESS"
    }
    
    with open(f"{output_path}/_MANIFEST.json", "w") as f:
        json.dump(manifest, f)
        
    spark.stop()

if __name__ == "__main__":
    run_job()