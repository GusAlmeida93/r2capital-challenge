# Open Questions for the Product Team

Working assumptions that should be confirmed (or reversed) for v2.

> [!NOTE]
> Each question lists the **default** we ship with so the v1 pipeline is
> deployable without waiting for an answer.

## Data contract

| # | Question | v1 default |
|---|---|---|
| 1 | **Header presence** — files "may or may not" have headers. Is there a known SLA on which files have headers, or is it truly random per file? | Tolerated either way: explicit columns + literal-row filter |
| 2 | **Currency symbol on `amount`** — sample shows `$63.98`. Always `$`? Always present? Locale variation (`€`, `R$`, comma decimal)? | Strip leading `$` and any `,`, reject otherwise |
| 3 | **Timestamp format on `transaction_time`** — sample `20211001T174600.000`. Always `YYYYMMDDTHHMMSS.fff`? Timezone? | Parsed as `%Y%m%dT%H%M%S.%f`, treated as UTC |
| 4 | **`batch_date` semantics** — derived from filename. Partner local date, UTC, or upload date? | Used verbatim; no timezone conversion |
| 5 | **Sales file partitioning** — filenames `sales_YYYYMMDD[_NNN].csv`. Distinct suffixes per batch, or could two files share a suffix with overlapping content? | Assumed distinct |
| 6 | **Late-arriving files** — can `batch_date = 20260428` arrive after we've processed `20260430`? | Handled cleanly; Output 1's "latest 40" still picks it up |

## Reporting

| # | Question | v1 default |
|---|---|---|
| 7 | **Output 1 — `total_raw_transactions`** — should it be every row received (incl. within-file duplicates) or the deduped count? | Every row received (matches `Σ row_count` across files) |
| 8 | **Output 2 — top store ties** | Tie-break alphabetically on `store_token` |
| 9 | **Output 3 — fewer-than-5 stores per date** | Emit only as many rows as exist; "utmost 5" interpreted as upper bound |
| 10 | **`snapshot_date` timezone** — `current_date` uses container TZ (UTC) | UTC; partner-local possible if requested |
| 11 | **Store enrichment in Output 3** — left-join leaves `store_name` NULL if store unknown. Exclude or surface "Unknown"? | Left as NULL |

## Operational

| # | Question | v1 default |
|---|---|---|
| 12 | **Archive retention** — how long to keep `data/archive/{batch_date}/`? | Indefinite; no prune job |
| 13 | **Rejected-row retention** — `silver_sales_rejected` is append-only | Indefinite |
| 14 | **Schedule + SLA** — partner SLA for file delivery? Catch up missed days? | `@daily`, `catchup=False` |
| 15 | **Manual reprocessing** — replay = move files from archive back to landing | OK as a manual op; CLI helper deferred |
| 16 | **Backfill** — historical archive ingest before go-live | Single bulk drop into landing; chunked DAG deferred |
| 17 | **PII / `store_name`** — free text up to 200 chars. Treated as PII? | No masking |

## Tech direction

| # | Question | v1 default |
|---|---|---|
| 18 | **S3 + Parquet** — replace landing with `s3://` and persist warehouse as Parquet | Local-only |
| 19 | **Managed warehouse** — port models to Postgres / Snowflake / BigQuery | DuckDB; portable except for landing read_csv |
