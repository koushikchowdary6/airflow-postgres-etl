"""DAG integrity tests.

Two layers:

1. **Source-level checks** that run everywhere, including in a CI job with no
   Airflow installed. They parse the DAG file and assert the operational
   settings a reviewer would ask about: retries, timeouts, no catchup, a single
   active run, and - importantly - that no database call happens at import
   time.
2. **A real DagBag import** that runs only where Airflow is installed, and
   verifies the task graph as Airflow actually builds it.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

DAG_FILE = Path(__file__).parent.parent / "dags" / "congress_demographics_etl.py"

EXPECTED_TASKS = [
    "ensure_schema",
    "extract",
    "load_and_upsert",
    "build_analytics",
    "data_quality",
    "advance_watermark",
]


@pytest.fixture(scope="module")
def dag_source() -> str:
    return DAG_FILE.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def dag_tree(dag_source: str) -> ast.Module:
    return ast.parse(dag_source)


class TestDagFileIsValid:
    def test_dag_file_exists(self):
        assert DAG_FILE.is_file()

    def test_dag_file_is_syntactically_valid_python(self, dag_source: str):
        ast.parse(dag_source)

    def test_defines_every_expected_task(self, dag_tree: ast.Module):
        functions = {node.name for node in ast.walk(dag_tree) if isinstance(node, ast.FunctionDef)}
        assert set(EXPECTED_TASKS) <= functions


class TestOperationalSettings:
    @pytest.mark.parametrize(
        "setting",
        [
            "retries",
            "retry_delay",
            "execution_timeout",
            "max_retry_delay",
            "retry_exponential_backoff",
        ],
    )
    def test_default_args_configure_retry_behaviour(self, dag_source: str, setting: str):
        assert setting in dag_source

    def test_catchup_is_disabled(self, dag_source: str):
        # The source is one cumulative file, so backfilling would re-load
        # identical data for every historical date.
        assert "catchup=False" in dag_source

    def test_only_one_run_at_a_time(self, dag_source: str):
        # Concurrent runs would contend on the staging table and watermark row.
        assert "max_active_runs=1" in dag_source

    def test_has_a_schedule(self, dag_source: str):
        assert "schedule=" in dag_source

    def test_has_a_dagrun_timeout(self, dag_source: str):
        assert "dagrun_timeout" in dag_source

    def test_is_tagged_for_discoverability(self, dag_source: str):
        assert "tags=" in dag_source


class TestNoWorkAtImportTime:
    """A DAG file is re-parsed constantly by the scheduler.

    Anything expensive or stateful at module level - a database connection, a
    config read that can raise, an HTTP call - degrades or breaks the whole
    scheduler, not just this DAG. These tests enforce that discipline.
    """

    def test_config_is_not_constructed_at_module_level(self, dag_tree: ast.Module):
        module_level_calls = [
            node
            for statement in dag_tree.body
            if not isinstance(statement, ast.FunctionDef | ast.ClassDef)
            for node in ast.walk(statement)
            if isinstance(node, ast.Call)
        ]
        rendered = [ast.unparse(call) for call in module_level_calls]
        offenders = [text for text in rendered if "Config.from_env" in text]
        assert offenders == [], f"Config.from_env() must be called inside tasks, found: {offenders}"

    def test_etl_package_is_imported_inside_tasks_not_at_module_level(self, dag_tree: ast.Module):
        top_level_imports = [node for node in dag_tree.body if isinstance(node, ast.Import | ast.ImportFrom)]
        etl_imports = [
            ast.unparse(node)
            for node in top_level_imports
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("etl")
        ]
        assert etl_imports == [], f"etl imports belong inside tasks, found: {etl_imports}"

    def test_no_pipeline_step_is_invoked_at_module_level(self, dag_tree: ast.Module):
        rendered = [
            ast.unparse(node)
            for statement in dag_tree.body
            if not isinstance(statement, ast.FunctionDef | ast.ClassDef)
            for node in ast.walk(statement)
            if isinstance(node, ast.Call)
        ]
        offenders = [text for text in rendered if text.startswith("step_") or "run_all(" in text]
        assert offenders == []


@pytest.fixture(scope="module")
def dagbag():
    """A real Airflow DagBag, or skip where Airflow is not installed."""
    pytest.importorskip("airflow", reason="Airflow not installed; skipping real DagBag checks")
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAG_FILE.parent), include_examples=False)


class TestWithRealAirflow:
    """Runs only where Airflow is installed."""

    def test_dag_imports_without_error(self, dagbag):
        assert dagbag.import_errors == {}

    def test_dag_is_registered(self, dagbag):
        assert "congress_demographics_etl" in dagbag.dags

    def test_task_ids_match(self, dagbag):
        dag = dagbag.dags["congress_demographics_etl"]
        assert sorted(t.task_id for t in dag.tasks) == sorted(EXPECTED_TASKS)

    def test_tasks_run_in_the_intended_order(self, dagbag):
        dag = dagbag.dags["congress_demographics_etl"]
        order = [t.task_id for t in dag.topological_sort()]
        for earlier, later in [
            ("ensure_schema", "extract"),
            ("extract", "load_and_upsert"),
            ("load_and_upsert", "build_analytics"),
            ("build_analytics", "data_quality"),
            ("data_quality", "advance_watermark"),
        ]:
            assert order.index(earlier) < order.index(later)

    def test_watermark_advances_only_after_quality_passes(self, dagbag):
        dag = dagbag.dags["congress_demographics_etl"]
        upstream = {t.task_id for t in dag.get_task("advance_watermark").upstream_list}
        assert "data_quality" in upstream

    def test_every_task_has_retries_and_a_timeout(self, dagbag):
        dag = dagbag.dags["congress_demographics_etl"]
        for task in dag.tasks:
            assert task.retries >= 1, task.task_id
            assert task.execution_timeout is not None, task.task_id

    def test_dag_has_no_cycles(self, dagbag):
        dag = dagbag.dags["congress_demographics_etl"]
        dag.topological_sort()  # raises if the graph is cyclic
        assert len(dag.tasks) == len(EXPECTED_TASKS)
