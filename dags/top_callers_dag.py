from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2026, 6, 11),
}

with DAG('top_callers_by_spend_dag', default_args=default_args, schedule_interval=None) as dag:
    
    submit_job = SparkSubmitOperator(
        task_id='submit_top_callers_job',
        application='/opt/airflow/jobs/top_callers.py',
        conn_id='spark_default',
        application_args=["{{ dag_run.conf.get('run_id', '') }}"],
        conf={'spark.master': 'spark://spark-master:7077'}
    )