from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2026, 6, 11),
}

with DAG('tower_utilization_heatmap_dag', default_args=default_args, schedule_interval=None) as dag:
    
    submit_job = SparkSubmitOperator(
        task_id='submit_tower_heatmap_job',
        application='/opt/airflow/jobs/tower_heatmap.py',
        conn_id='spark_default',
        application_args=["{{ dag_run.conf.get('run_id', '') }}"],
        conf={'spark.master': 'spark://spark-master:7077'}
    )
