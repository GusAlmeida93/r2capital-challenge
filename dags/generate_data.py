"""Manual DAG for generating synthetic landing CSV files."""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

INCLUDE_DIR = Path("/opt/airflow/include")
if str(INCLUDE_DIR) not in sys.path:
    sys.path.insert(0, str(INCLUDE_DIR))

DEFAULT_CONFIG = {
    "landing_path": os.environ.get("LANDING_PATH", "data/landing"),
    "num_stores": 50,
    "num_sales_files": 2,
    "rows_per_sales_file": 500,
    "batch_date": None,
    "invalid_rate": 0.05,
    "duplicate_rate": 0.02,
    "days_window": 7,
    "include_headers": "auto",
    "seed": None,
}

CLI_ARGS = {
    "landing_path": "--landing-path",
    "num_stores": "--num-stores",
    "num_sales_files": "--num-sales-files",
    "rows_per_sales_file": "--rows-per-sales-file",
    "batch_date": "--batch-date",
    "invalid_rate": "--invalid-rate",
    "duplicate_rate": "--duplicate-rate",
    "days_window": "--days-window",
    "include_headers": "--include-headers",
    "seed": "--seed",
}

NULLABLE_KEYS = {"batch_date", "seed"}

PARAMS = {
    "landing_path": Param(DEFAULT_CONFIG["landing_path"], type="string", minLength=1),
    "num_stores": Param(DEFAULT_CONFIG["num_stores"], type="integer", minimum=0),
    "num_sales_files": Param(DEFAULT_CONFIG["num_sales_files"], type="integer", minimum=0),
    "rows_per_sales_file": Param(DEFAULT_CONFIG["rows_per_sales_file"], type="integer", minimum=0),
    "batch_date": Param(DEFAULT_CONFIG["batch_date"], type=["null", "string"], pattern="^[0-9]{8}$"),
    "invalid_rate": Param(DEFAULT_CONFIG["invalid_rate"], type="number", minimum=0, maximum=1),
    "duplicate_rate": Param(DEFAULT_CONFIG["duplicate_rate"], type="number", minimum=0, maximum=1),
    "days_window": Param(DEFAULT_CONFIG["days_window"], type="integer", minimum=0),
    "include_headers": Param(DEFAULT_CONFIG["include_headers"], type="string", enum=["auto", "always", "never"]),
    "seed": Param(DEFAULT_CONFIG["seed"], type=["null", "integer"]),
}


def _build_config(conf: dict[str, Any] | None) -> dict[str, Any]:
    """Return generator configuration from validated Airflow trigger config."""
    trigger_conf = conf or {}
    unknown_keys = sorted(set(trigger_conf) - set(DEFAULT_CONFIG))
    if unknown_keys:
        allowed_keys = ", ".join(sorted(DEFAULT_CONFIG))
        raise ValueError(f"Unknown generate_data config keys: {unknown_keys}. Allowed keys: {allowed_keys}")

    config = DEFAULT_CONFIG | trigger_conf
    null_keys = sorted(k for k, v in config.items() if v is None and k not in NULLABLE_KEYS)
    if null_keys:
        raise ValueError(f"generate_data config keys cannot be null: {null_keys}")

    return config


def _to_argv(config: dict[str, Any]) -> list[str]:
    """Convert DAG config into the generator's existing CLI argument format."""
    argv = []
    for key, flag in CLI_ARGS.items():
        value = config[key]
        if value is None:
            continue
        argv.extend([flag, str(value)])
    return argv


@dag(
    dag_id="generate_data",
    description="Generate synthetic stores and sales CSV files into the landing folder",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["r2", "data-generation", "faker"],
    params=PARAMS,
    default_args={
        "owner": "data-eng",
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "depends_on_past": False,
    },
)
def generate_data_dag():
    @task
    def generate_files() -> dict[str, Any]:
        """Generate CSV files using trigger-time config."""
        from scripts.generate_data import generate, parse_args

        context = get_current_context()
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run else None
        config = _build_config(conf)

        try:
            args = parse_args(_to_argv(config))
        except SystemExit as exc:
            raise ValueError("Invalid generate_data config") from exc

        generate(args)
        return config

    generate_files()


generate_data_dag()
