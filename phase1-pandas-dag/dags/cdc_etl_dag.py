"""
CDC ETL Airflow DAG.

This DAG orchestrates a CDC data pipeline with extract, transform, load,
and validation steps, followed by Slack notifications on success or failure.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

# Allow Airflow container to resolve local ETL modules
sys.path.append("/opt/airflow")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from etl.extract_cdc import extract
from etl.transform_cdc import transform
from etl.load_cdc import load_to_postgres
from etl.validate_cdc import validate_all_csvs, check_observability_drift


# Default DAG arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "sla": timedelta(hours=1)  # implement SLO => alert if DAG exceeds 2 hours
}

# Ensure required data directories exist before task execution
Path("/opt/airflow/data/raw").mkdir(parents=True, exist_ok=True)
Path("/opt/airflow/data/processed").mkdir(parents=True, exist_ok=True)


with DAG(
    dag_id="etl_cdc",
    default_args=default_args,
    start_date=datetime(2025, 12, 22, 12, 30),
    schedule=None,  # will switch to '@daily' when automated
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

    observe_task = PythonOperator(
        task_id="observability_drift_check",
        python_callable=check_observability_drift,
    )

    notify_slack_success = SlackWebhookOperator(
        task_id="notify_slack_success",
        slack_webhook_conn_id="cdc-pandas-etl",
        message=":white_check_mark: CDC ETL GX DAG completed successfully!",
        trigger_rule="all_success",
    )

    notify_slack_fail = SlackWebhookOperator(
        task_id="notify_slack_fail",
        slack_webhook_conn_id="cdc-pandas-etl",
        message=":x: CDC ETL GX DAG failed!",
        trigger_rule="one_failed",
    )

# Main Core Pipeline Stream
# Ingestion -> GX Validation -> Transformation -> DB Loading -> Drift Metrics
extract_task >> validate_task >> transform_task >> load_task >> observe_task

# Success Alert Path (Only fires if the final step finishes cleanly)
observe_task >> notify_slack_success

# Failure Path (Slack alerts fire if ANY core step crashes or fails validation)
extract_task >> notify_slack_fail
validate_task >> notify_slack_fail
transform_task >> notify_slack_fail
load_task >> notify_slack_fail
observe_task >> notify_slack_fail