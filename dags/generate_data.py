"""Synthetic data generator DAG for the R2 Capital challenge.

Wraps `include/scripts/generate_data.py` so users can produce stores + sales
landing CSVs from the Airflow UI. Every CLI flag from `parse_args()` is
exposed as an Airflow Param — override per-run via "Trigger DAG w/ config"
or `airflow dags trigger --conf '{...}'`.

Manual-only (`schedule=None`); pair with the `r2_pipeline` DAG to consume
the produced files end-to-end.
"""
from __future__ import annotations

import logging
import os
import sys
from argparse import Namespace
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param

INCLUDE_DIR = Path("/opt/airflow/include")
if str(INCLUDE_DIR) not in sys.path:
    sys.path.insert(0, str(INCLUDE_DIR))

LANDING_PATH = os.environ.get("LANDING_PATH", "/opt/airflow/data/landing")


@dag(
    dag_id="generate_data",
    description="Generate synthetic stores + sales landing CSVs (Faker-backed).",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["r2", "data-generation"],
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "depends_on_past": False,
    },
    params={
        "landing_path": Param(
            default=LANDING_PATH,
            type="string",
            title="Landing path",
            description="Directory inside the container where CSVs are written.",
        ),
        "num_stores": Param(
            default=50,
            type="integer",
            minimum=0,
            title="Number of stores",
            description="Rows in stores_<batch>.csv (a fraction become malformed via invalid_rate).",
        ),
        "num_sales_files": Param(
            default=2,
            type="integer",
            minimum=0,
            title="Number of sales files",
            description="Files named sales_<batch>_<NNN>.csv.",
        ),
        "rows_per_sales_file": Param(
            default=500,
            type="integer",
            minimum=0,
            title="Rows per sales file",
            description="Excludes the duplicates added by duplicate_rate.",
        ),
        "batch_date": Param(
            default="",
            type="string",
            pattern=r"^(\d{8})?$",
            title="Batch date (YYYYMMDD)",
            description="Leave blank to use the run's logical date (ds_nodash).",
        ),
        "invalid_rate": Param(
            default=0.05,
            type="number",
            minimum=0,
            maximum=1,
            title="Invalid-row rate",
            description="Fraction of rows deliberately malformed (0.0-1.0).",
        ),
        "duplicate_rate": Param(
            default=0.02,
            type="number",
            minimum=0,
            maximum=1,
            title="Duplicate-row rate",
            description="Fraction of sales rows duplicated within a file (0.0-1.0).",
        ),
        "days_window": Param(
            default=7,
            type="integer",
            minimum=1,
            title="Days window",
            description="transaction_time spread over this many days back from batch_date.",
        ),
        "include_headers": Param(
            default="auto",
            type="string",
            enum=["auto", "always", "never"],
            title="Include CSV headers",
            description="auto = random per-file; mirrors the upstream feed which is inconsistent.",
        ),
        "seed": Param(
            default=None,
            type=["null", "integer"],
            title="Random seed",
            description="Set for deterministic output; leave null for randomized runs.",
        ),
    },
)
def generate_data():
    @task
    def generate(**context) -> str:
        from scripts.generate_data import generate as generate_csvs

        log = logging.getLogger("r2.generate_data")
        params = context["params"]
        batch_date = params["batch_date"] or context["ds_nodash"]

        args = Namespace(
            landing_path=params["landing_path"],
            num_stores=params["num_stores"],
            num_sales_files=params["num_sales_files"],
            rows_per_sales_file=params["rows_per_sales_file"],
            batch_date=batch_date,
            invalid_rate=params["invalid_rate"],
            duplicate_rate=params["duplicate_rate"],
            days_window=params["days_window"],
            include_headers=params["include_headers"],
            seed=params["seed"],
        )
        log.info(
            "generating | batch_date=%s landing_path=%s stores=%d sales_files=%d rows/file=%d seed=%s",
            args.batch_date,
            args.landing_path,
            args.num_stores,
            args.num_sales_files,
            args.rows_per_sales_file,
            args.seed,
        )
        generate_csvs(args)
        return batch_date

    generate()


generate_data()
