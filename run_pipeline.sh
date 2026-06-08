#!/bin/bash

# Map logical query names to Airflow DAG IDs
case "$1" in
    top_callers) DAG_ID="top_callers_by_spend_dag" ;;
    tower_heatmap) DAG_ID="tower_utilization_heatmap_dag" ;;
    anomalous_calls) DAG_ID="anomalous_call_detection_dag" ;;
    revenue_recon) DAG_ID="revenue_reconciliation_dag" ;;
    *) echo "Unknown query: $1"; exit 1 ;;
esac

RUN_ID=$(date +%Y%m%d_%H%M%S)

# Trigger the DAG via Airflow CLI
docker exec airflow airflow dags trigger -r $RUN_ID --conf "{\"run_id\":\"$RUN_ID\"}" $DAG_ID

echo "Pipeline triggered for $1 (DAG: $DAG_ID) with Run ID: $RUN_ID"