"""End-to-end ELT pipeline for the R2 Capital challenge.

Stages: landing → raw (SCD Type 2 via dbt snapshots) → silver (incremental
upsert with validation) → gold (3 reports). dbt orchestration is delegated
to Astronomer Cosmos, which generates one Airflow task per dbt resource and
respects the manifest's dependency graph.

Idempotent: snapshots only insert new versions when source rows change,
silver models upsert on natural keys, and processed CSV files are moved to
the archive only after the entire dbt run succeeds.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode, TestBehavior

# Make scripts/ importable so the archive task can call into the shared logger.
INCLUDE_DIR = Path("/opt/airflow/include")
if str(INCLUDE_DIR) not in sys.path:
    sys.path.insert(0, str(INCLUDE_DIR))

DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/include/dbt"))
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/include/dbt"))
DBT_EXECUTABLE = Path(os.environ.get("DBT_EXECUTABLE_PATH", "/home/airflow/.local/bin/dbt"))

LANDING_PATH = os.environ.get("LANDING_PATH", "/opt/airflow/data/landing")
ARCHIVE_PATH = os.environ.get("ARCHIVE_PATH", "/opt/airflow/data/archive")

profile_config = ProfileConfig(
    profile_name="r2",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_DIR / "profiles.yml",
)

project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_DIR)
execution_config = ExecutionConfig(dbt_executable_path=str(DBT_EXECUTABLE))


@dag(
    dag_id="r2_pipeline",
    description="Daily landing → raw (SCD2) → silver → gold pipeline using dbt + DuckDB",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    # DuckDB allows only one read-write process at a time. Cosmos emits one
    # Airflow task per dbt resource, so without this cap Airflow would
    # parallelize them and they would race on the warehouse file lock.
    max_active_tasks=1,
    tags=["r2", "elt", "duckdb", "cosmos"],
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "depends_on_past": False,
    },
)
def r2_pipeline():
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def check_landing() -> bool:
        files = sorted(Path(LANDING_PATH).glob("*.csv"))
        log = logging.getLogger("r2.check_landing")
        log.info("found %d CSV files in %s: %s", len(files), LANDING_PATH, [f.name for f in files])
        return len(files) > 0

    dbt_pipeline = DbtTaskGroup(
        group_id="dbt",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_ALL,
            dbt_deps=False,  # mirror operator_args.install_deps; deps installed by airflow-init
        ),
        operator_args={
            "install_deps": False,
            "append_env": True,
        },
    )

    @task
    def export_parquet() -> int:
        from scripts.export_parquet import export  # local import; PYTHONPATH set above

        return export()

    @task
    def archive_processed() -> int:
        from scripts.archive_files import archive  # local import; PYTHONPATH set above

        moved = archive(LANDING_PATH, ARCHIVE_PATH)
        return moved

    check_landing() >> dbt_pipeline >> export_parquet() >> archive_processed()


r2_pipeline()
