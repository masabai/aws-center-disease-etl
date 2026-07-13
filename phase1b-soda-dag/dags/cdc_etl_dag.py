"""
CDC ETL Airflow DAG — Phase 1b (Soda Validation).
Same pipeline as phase1-pandas-dag with Soda Core replacing Great Expectations.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from etl.extract_cdc import extract
from etl.transform_cdc import transform
from etl.load_cdc import load_to_postgres
from etl.validate_soda import validate_all_csvs

default_args = {
    "owner": "airflow",
    "retries": 1,
    "sla": timedelta(hours=1),
}

Path("/opt/airflow/data/raw").mkdir(parents=True, exist_ok=True)
Path("/opt/airflow/data/processed").mkdir(parents=True, exist_ok=True)

with DAG(
    dag_id="etl_cdc_soda",
    default_args=default_args,
    start_date=datetime(2025, 12, 22, 12, 30),
    schedule=None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(task_id="extract",   python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load",      python_callable=load_to_postgres)
    validate_task = PythonOperator(task_id="validate",  python_callable=validate_all_csvs)

    notify_slack_success = SlackWebhookOperator(
        task_id="notify_slack_success",
        slack_webhook_conn_id="cdc-pandas-etl",
        message=":white_check_mark: CDC Soda ETL DAG completed successfully!",
        trigger_rule="all_success",
    )

    notify_slack_fail = SlackWebhookOperator(
        task_id="notify_slack_fail",
        slack_webhook_conn_id="cdc-pandas-etl",
        message=":x: CDC Soda ETL DAG failed!",
        trigger_rule="one_failed",
    )

    extract_task >> transform_task >> load_task >> validate_task
    validate_task >> notify_slack_success
    validate_task >> notify_slack_fail
