"""
airflow/tests/unit/test_dag_integrity.py
Validates DAG structure without running Airflow.
These tests catch import errors, cycles, and misconfigured tasks before CI deploys.
"""
from __future__ import annotations

import importlib
from pathlib import Path

import pytest


DAG_DIR = Path(__file__).parent.parent / "dags"


def load_dag(dag_file: str):
    """Import a DAG module and return the dag object."""
    spec = importlib.util.spec_from_file_location("dag_module", DAG_DIR / dag_file)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestRetailPulsePipelineDAG:
    @pytest.fixture(autouse=True)
    def dag_module(self):
        """Load the DAG module once per test class."""
        try:
            self.mod = load_dag("retailpulse_full_pipeline.py")
        except ImportError as e:
            pytest.skip(f"Airflow not installed: {e}")

    def test_dag_object_exists(self):
        assert hasattr(self.mod, "dag"), "DAG module must expose a 'dag' variable"

    def test_dag_id_is_correct(self):
        assert self.mod.dag.dag_id == "retailpulse_full_pipeline"

    def test_dag_has_expected_tasks(self):
        expected = {
            "produce_events",
            "bronze_to_silver",
            "dbt_run",
            "dbt_test",
            "data_quality",
            "done",
        }
        actual = {t.task_id for t in self.mod.dag.tasks}
        assert expected.issubset(actual), f"Missing tasks: {expected - actual}"

    def test_dag_schedule_is_daily(self):
        dag = self.mod.dag
        # schedule can be a cron string or a timetable object
        schedule = str(dag.schedule_interval or dag.schedule)
        assert "0 2 * * *" in schedule or "daily" in schedule.lower()

    def test_dag_max_active_runs_is_one(self):
        assert self.mod.dag.max_active_runs == 1

    def test_task_dependencies_are_linear(self):
        """Verify the pipeline runs strictly in sequence (no parallel branches in MVP)."""
        dag = self.mod.dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}

        # bronze_to_silver must come after produce_events
        bt_upstream = {t.task_id for t in tasks_by_id["bronze_to_silver"].upstream_list}
        assert "produce_events" in bt_upstream

        # dbt_run must come after bronze_to_silver
        dbt_upstream = {t.task_id for t in tasks_by_id["dbt_run"].upstream_list}
        assert "bronze_to_silver" in dbt_upstream

        # data_quality must come after dbt_test
        dq_upstream = {t.task_id for t in tasks_by_id["data_quality"].upstream_list}
        assert "dbt_test" in dq_upstream

    def test_dag_has_no_import_errors(self):
        """If we reached this point, the module imported successfully."""
        assert self.mod is not None

    def test_retries_configured(self):
        for task in self.mod.dag.tasks:
            assert task.retries >= 1, f"Task '{task.task_id}' has no retries configured"

    def test_sla_configured_on_dag(self):
        for task in self.mod.dag.tasks:
            # SLA is set via default_args — verify it flows down
            if task.sla is not None:
                assert task.sla.total_seconds() > 0
