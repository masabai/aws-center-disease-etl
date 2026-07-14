import sys
import os
import pytest

"""
Validate an Apache Airflow DAG:

1) The DAG loads successfully.
2) The DAG exists.
3) The DAG contains the expected tasks.
4) The tasks are connected in the correct order.

"""


# Add project root so DagBag can resolve etl.* imports locally
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, PROJECT_ROOT)
os.environ.setdefault("PYTHONPATH", PROJECT_ROOT)

pytest.importorskip("airflow", reason="airflow not installed — skipping DAG tests")

from airflow.models import DagBag

DAG_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")


@pytest.fixture(scope="module") # run once
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
 
    dag = dagbag.dags.get("etl_cdc_gx_version") or dagbag.dags.get("etl_cdc")
    assert dag is not None, "Target CDC DAG could not be loaded from DagBag"
    assert dag.get_task("validate_raw_gx").upstream_task_ids == {"extract"}
    assert dag.get_task("transform").upstream_task_ids == {"validate_raw_gx"}
    assert dag.get_task("load").upstream_task_ids == {"transform"}
    assert dag.get_task("observability_drift_check").upstream_task_ids == {"load"}
    assert dag.get_task("notify_slack_success").upstream_task_ids == {"observability_drift_check"}
    expected_fail_upstreams = {"extract", "validate_raw_gx", "transform", "load", "observability_drift_check"}
    assert dag.get_task("notify_slack_fail").upstream_task_ids == expected_fail_upstreams
