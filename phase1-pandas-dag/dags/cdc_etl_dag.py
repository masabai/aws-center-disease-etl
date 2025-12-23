import sys
sys.path.append("/opt/airflow")  # Airflow container sees etl/ here
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime
from etl.extract_cdc import extract
from etl.transform_cdc import transform
from etl.load_cdc import load_to_postgres
from etl.validate_cdc import validate_all_csvs

default_args = {'owner': 'airflow', 'retries': 1}
from pathlib import Path

# Ensure folders exist before running any tasks
Path("/opt/airflow/data/raw").mkdir(parents=True, exist_ok=True)
Path("/opt/airflow/data/processed").mkdir(parents=True, exist_ok=True)


with DAG(
    dag_id="etl_cdc",
    default_args=default_args,
    start_date=datetime(2025, 12, 22),
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_to_postgres
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=validate_all_csvs
    )

    notify_slack_success = SlackWebhookOperator(
        task_id="notify_slack_success",
        http_conn_id="cdc-pandas-etl",
        message=":white_check_mark: CDC ETL DAG completed successfully!",
        trigger_rule="all_success"
    )

    notify_slack_fail = SlackWebhookOperator(
        task_id="notify_slack_fail",
        http_conn_id="cdc-pandas-etl",
        message=":x: CDC ETL DAG failed!",
        trigger_rule="one_failed"
    )

    # Task dependencies
    extract_task >> transform_task >> load_task >> validate_task

    # Notifications
    validate_task >> notify_slack_success
    validate_task >> notify_slack_fail
