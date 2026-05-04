# Architecture Questions

## Would a lakehouse let us use native dbt features?

Yes for the features this project needs: dbt snapshots, Trino merge incrementals, sources, tests, docs, table models, and view models are all now first-class parts of the design.

No for dynamic landing ingestion. dbt should not be the component that watches a folder, hashes files, quarantines conflicting replays, uploads objects, or inserts raw CSV rows. That remains a small Python loader before dbt runs.

## Is MinIO required?

MinIO is required for the local developer stack because it gives Nessie and Trino a reproducible S3-compatible warehouse. It is not a conceptual requirement. AWS S3 or Cloudflare R2 can replace it later through environment variables after testing the Iceberg REST catalog path against those endpoints.

## Why Nessie and Trino?

Nessie provides an open-source Iceberg REST catalog. Trino provides the SQL engine and dbt adapter target. That combination lets dbt use standard source, snapshot, test, table, view, and merge incremental workflows while keeping storage in open Iceberg tables.

## What moved out of the critical path?

Manual SCD2 SQL hooks, filesystem stages, and table export scripts are no longer part of the runtime path. Iceberg already writes Parquet files and dbt snapshots own the SCD2 behavior.
