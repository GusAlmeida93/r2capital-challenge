"""Export silver and gold tables from DuckDB to Parquet files.

One file per table, overwritten on every run. The UI service reads these
files (not the warehouse) to avoid the exclusive-lock contention DuckDB
imposes between writer processes (the DAG) and any reader process.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scripts.logger import get_logger  # type: ignore[no-redef]
else:
    from .logger import get_logger

import duckdb

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/opt/airflow/data/warehouse/duckdb.db")
EXPORT_PATH = os.environ.get("EXPORT_PATH", "/opt/airflow/data/exports")

EXPORT_TABLES: dict[str, list[str]] = {
    "raw": [
        "raw_stores_snapshot",
        "raw_sales_snapshot",
    ],
    "silver": [
        "silver_stores",
        "silver_sales",
        "silver_sales_rejected",
        "silver_sales_audit_by_batch",
    ],
    "gold": [
        "rpt_transactions_by_batch",
        "rpt_sales_by_transaction_date",
        "rpt_top_5_stores_by_date",
    ],
}


def export() -> int:
    log = get_logger("export_parquet")
    out_root = Path(EXPORT_PATH)
    out_root.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    total = 0
    for schema, tables in EXPORT_TABLES.items():
        sdir = out_root / schema
        sdir.mkdir(parents=True, exist_ok=True)
        for t in tables:
            try:
                con.execute(f"SELECT 1 FROM {schema}.{t} LIMIT 1")
            except duckdb.CatalogException as e:
                log.warning("skip %s.%s — table not found (%s)", schema, t, e)
                continue
            except Exception as e:  # noqa: BLE001
                log.error("skip %s.%s — error: %s", schema, t, e)
                continue
            target = sdir / f"{t}.parquet"
            tmp = target.with_suffix(".parquet.tmp")
            # Atomic-ish: write to .tmp then rename so partial writes never expose an empty file.
            con.execute(
                f"COPY (SELECT * FROM {schema}.{t}) TO '{tmp}' "
                f"(FORMAT 'parquet', COMPRESSION 'zstd')"
            )
            os.replace(tmp, target)
            n = con.execute(f"SELECT count(*) FROM {schema}.{t}").fetchone()[0]
            log.info("wrote %s rows=%d -> %s", f"{schema}.{t}", n, target)
            total += 1
    con.close()
    log.info("done | exported_tables=%d export_path=%s", total, out_root)
    return total


if __name__ == "__main__":
    export()
