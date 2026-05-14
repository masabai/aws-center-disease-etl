import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

pytest.importorskip("airflow", reason="airflow not installed — skipping DAG tests")

from airflow.models import DagBag

DAG_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")


@pytest.fixture(scope="module")
def dagbag():
    """Load Airflow DAGs from the dags folder."""
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_dag_loads_no_errors(dagbag):
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


def test_dag_exists(dagbag):
    assert "etl_cdc" in dagbag.dags, "etl_cdc DAG not found"


def test_dag_has_expected_tasks(dagbag):
    dag = dagbag.dags["etl_cdc"]
    task_ids = {t.task_id for t in dag.tasks}
    expected = {"extract", "transform", "load", "validate", "observability_drift_check", "notify_slack_success", "notify_slack_fail"}
    assert expected == task_ids, f"Unexpected tasks: {task_ids}"


def test_dag_task_order(dagbag):
    dag = dagbag.dags["etl_cdc"]
    assert dag.get_task("transform").upstream_task_ids == {"extract"}
    assert dag.get_task("load").upstream_task_ids == {"transform"}
    assert dag.get_task("validate").upstream_task_ids == {"load"}
    assert dag.get_task("observability_drift_check").upstream_task_ids == {"validate"}
    assert dag.get_task("notify_slack_success").upstream_task_ids == {"observability_drift_check"}
    assert dag.get_task("notify_slack_fail").upstream_task_ids == {"observability_drift_check"}
