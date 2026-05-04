"""End-to-end lakehouse ELT pipeline for the R2 Capital challenge.

Landing validation loads immutable CSV events into Iceberg through Trino.
dbt then manages raw views, native snapshots, silver merge incrementals, and
gold reporting tables through Astronomer Cosmos.
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

INCLUDE_DIR = Path("/opt/airflow/include")
if str(INCLUDE_DIR) not in sys.path:
    sys.path.insert(0, str(INCLUDE_DIR))

DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/include/dbt"))
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/include/dbt"))
DBT_EXECUTABLE = Path(os.environ.get("DBT_EXECUTABLE_PATH", "/home/airflow/.local/bin/dbt"))

LANDING_PATH = os.environ.get("LANDING_PATH", "/opt/airflow/data/landing")
ARCHIVE_PATH = os.environ.get("ARCHIVE_PATH", "/opt/airflow/data/archive")
QUARANTINE_PATH = os.environ.get("QUARANTINE_PATH", "/opt/airflow/data/quarantine")

profile_config = ProfileConfig(
    profile_name="r2",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_DIR / "profiles.yml",
)

project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_DIR)
execution_config = ExecutionConfig(dbt_executable_path=str(DBT_EXECUTABLE))


@dag(
    dag_id="r2_pipeline",
    description="Daily landing to Iceberg lakehouse pipeline using dbt, Trino, Nessie, and MinIO",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["r2", "elt", "trino", "iceberg", "nessie", "cosmos"],
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
            dbt_deps=False,
        ),
        operator_args={
            "install_deps": False,
            "append_env": True,
        },
    )

    @task
    def validate_landing() -> int:
        from scripts.landing_manifest import validate_landing as validate

        return validate(LANDING_PATH, ARCHIVE_PATH, QUARANTINE_PATH)

    @task
    def archive_processed() -> int:
        from scripts.archive_files import archive

        moved = archive(LANDING_PATH, ARCHIVE_PATH)
        return moved

    check_landing() >> validate_landing() >> dbt_pipeline >> archive_processed()


r2_pipeline()
