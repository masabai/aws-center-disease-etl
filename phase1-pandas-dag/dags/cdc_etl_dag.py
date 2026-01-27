"""
CDC ETL Airflow DAG.

This DAG orchestrates a CDC data pipeline with extract, transform, load,
and validation steps, followed by Slack notifications on success or failure.
"""

import sys
from datetime import datetime
from pathlib import Path

# Allow Airflow container to resolve local ETL modules
sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from etl.extract_cdc import extract
from etl.transform_cdc import transform
from etl.load_cdc import load_to_postgres
from etl.validate_cdc import validate_all_csvs


# Default DAG arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
}

# Ensure required data directories exist before task execution
Path("/opt/airflow/data/raw").mkdir(parents=True, exist_ok=True)
Path("/opt/airflow/data/processed").mkdir(parents=True, exist_ok=True)


with DAG(
    dag_id="etl_cdc",
    default_args=default_args,
    start_date=datetime(2025, 12, 22, 12, 30),
    schedule_interval=None,  # can be switched to '@daily' when automated
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_to_postgres,
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=validate_all_csvs,
    )

    notify_slack_success = SlackWebhookOperator(
        task_id="notify_slack_success",
        http_conn_id="cdc-pandas-etl",
        message=":white_check_mark: CDC ETL DAG completed successfully!",
        trigger_rule="all_success",
    )

    notify_slack_fail = SlackWebhookOperator(
        task_id="notify_slack_fail",
        http_conn_id="cdc-pandas-etl",
        message=":x: CDC ETL DAG failed!",
        trigger_rule="one_failed",
    )

    # Core ETL flow
    extract_task >> transform_task >> load_task >> validate_task

    # Post-validation notifications
    validate_task >> notify_slack_success
    validate_task >> notify_slack_fail
