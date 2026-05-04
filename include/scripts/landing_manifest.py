"""Validate landing files and load accepted rows into Iceberg through Trino."""
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import re
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scripts.logger import get_logger
else:
    from .logger import get_logger

STORE_HEADERS = ["store_group", "store_token", "store_name"]
SALES_HEADERS = [
    "store_token",
    "transaction_id",
    "receipt_token",
    "transaction_time",
    "amount",
    "user_role",
]
BATCH_RE = re.compile(r"^(stores|sales)_(\d{8})(?:_\d+)?\.csv$", re.IGNORECASE)
RAW_SCHEMA = os.environ.get("TRINO_SCHEMA", "raw")
MANIFEST_TABLE = "landing_file_manifest"
REJECTIONS_TABLE = "landing_file_rejections"
STORES_TABLE = "landing_stores_received"
SALES_TABLE = "landing_sales_received"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate landing files and load accepted rows.")
    p.add_argument("--landing-path", default=os.environ.get("LANDING_PATH", "data/landing"))
    p.add_argument("--archive-path", default=os.environ.get("ARCHIVE_PATH", "data/archive"))
    p.add_argument("--quarantine-path", default=os.environ.get("QUARANTINE_PATH", "data/quarantine"))
    p.add_argument("--trino-host", default=os.environ.get("TRINO_HOST", "localhost"))
    p.add_argument("--trino-port", type=int, default=int(os.environ.get("TRINO_PORT", "8080")))
    p.add_argument("--trino-user", default=os.environ.get("TRINO_USER", "dbt"))
    p.add_argument("--trino-catalog", default=os.environ.get("TRINO_CATALOG", "lakehouse"))
    p.add_argument("--trino-schema", default=RAW_SCHEMA)
    p.add_argument("--trino-http-scheme", default=os.environ.get("TRINO_HTTP_SCHEME", "http"))
    p.add_argument("--minio-endpoint", default=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"))
    p.add_argument("--minio-bucket", default=os.environ.get("MINIO_BUCKET", "r2-lakehouse"))
    p.add_argument("--minio-access-key", default=os.environ.get("MINIO_ROOT_USER", "minioadmin"))
    p.add_argument("--minio-secret-key", default=os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"))
    p.add_argument("--aws-region", default=os.environ.get("AWS_REGION", "us-east-1"))
    return p.parse_args(argv)


def file_metadata(path: Path) -> dict[str, str]:
    match = BATCH_RE.match(path.name)
    if not match:
        return {"file_type": "unknown", "batch_date": "_unparsed"}
    return {"file_type": match.group(1).lower(), "batch_date": match.group(2)}


def source_file_name(path: Path) -> str:
    return f"landing/{path.name}"


def object_key(path: Path, batch_date: str) -> str:
    return f"landing/{batch_date}/{path.name}"


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_blank_row(row: list[str]) -> bool:
    return all(cell.strip() == "" for cell in row)


def is_header_row(row: list[str], file_type: str) -> bool:
    if file_type == "stores":
        return [cell.strip() for cell in row[: len(STORE_HEADERS)]] == STORE_HEADERS
    if file_type == "sales":
        return [cell.strip() for cell in row[: len(SALES_HEADERS)]] == SALES_HEADERS
    return False


def count_data_rows(path: Path, file_type: str) -> int:
    with path.open("r", newline="", encoding="utf-8") as fh:
        return sum(1 for row in csv.reader(fh) if not is_blank_row(row) and not is_header_row(row, file_type))


def archive_candidate(archive_root: Path, file_name: str, batch_date: str) -> Path:
    return archive_root / batch_date / file_name


def quarantine_file(path: Path, quarantine_root: Path, batch_date: str) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    target_dir = quarantine_root / batch_date
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{stamp}_{path.name}"
    shutil.move(str(path), str(target))
    return target


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def relation(schema: str, table: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


def quote_literal(value: str | None) -> str:
    if value is None:
        return "null"
    return "'" + value.replace("'", "''") + "'"


def timestamp_value(value: datetime | str) -> str:
    if isinstance(value, datetime):
        normalized = value.astimezone(UTC) if value.tzinfo else value
        return normalized.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")
    return value


def timestamp_literal(value: datetime | str) -> str:
    return f"timestamp {quote_literal(timestamp_value(value))}"


def sql_value(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, datetime):
        return timestamp_literal(value)
    if isinstance(value, int):
        return str(value)
    return quote_literal(str(value))


def scalar(con, sql: str):
    rows = query(con, sql)
    return rows[0][0] if rows else None


def query(con, sql: str) -> list[tuple]:
    cur = con.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()


def command(con, sql: str) -> None:
    cur = con.cursor()
    try:
        cur.execute(sql)
    finally:
        cur.close()


def connect(
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    http_scheme: str | None = None,
):
    import trino

    return trino.dbapi.connect(
        host=host or os.environ.get("TRINO_HOST", "localhost"),
        port=port or int(os.environ.get("TRINO_PORT", "8080")),
        user=user or os.environ.get("TRINO_USER", "dbt"),
        catalog=catalog or os.environ.get("TRINO_CATALOG", "lakehouse"),
        schema=schema or RAW_SCHEMA,
        http_scheme=http_scheme or os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )


def s3_client(endpoint: str, access_key: str, secret_key: str, region: str):
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )


def upload_file_to_minio(client, path: Path, bucket: str, key: str) -> None:
    client.upload_file(str(path), bucket, key)


def ensure_tables(con, schema: str = RAW_SCHEMA) -> None:
    for schema_name in [schema, "silver", "gold", "snapshots"]:
        command(con, f"create schema if not exists {quote_identifier(schema_name)}")
    command(
        con,
        f"""
        create table if not exists {relation(schema, MANIFEST_TABLE)} (
            source_file varchar,
            file_name varchar,
            file_type varchar,
            batch_date varchar,
            object_key varchar,
            content_sha256 varchar,
            row_count bigint,
            first_seen_at timestamp(6),
            last_seen_at timestamp(6)
        )
        with (format = 'PARQUET')
        """
    )
    command(
        con,
        f"""
        create table if not exists {relation(schema, REJECTIONS_TABLE)} (
            rejection_id varchar,
            source_file varchar,
            file_name varchar,
            file_type varchar,
            batch_date varchar,
            expected_sha256 varchar,
            actual_sha256 varchar,
            quarantine_path varchar,
            rejected_at timestamp(6),
            reason varchar
        )
        with (format = 'PARQUET')
        """
    )
    command(
        con,
        f"""
        create table if not exists {relation(schema, STORES_TABLE)} (
            row_uid varchar,
            source_file varchar,
            file_name varchar,
            source_row_number bigint,
            store_group varchar,
            store_token varchar,
            store_name varchar,
            batch_date varchar,
            content_sha256 varchar,
            received_at timestamp(6)
        )
        with (format = 'PARQUET')
        """
    )
    command(
        con,
        f"""
        create table if not exists {relation(schema, SALES_TABLE)} (
            row_uid varchar,
            source_file varchar,
            file_name varchar,
            source_row_number bigint,
            store_token varchar,
            transaction_id varchar,
            receipt_token varchar,
            transaction_time varchar,
            amount varchar,
            user_role varchar,
            batch_date varchar,
            content_sha256 varchar,
            received_at timestamp(6)
        )
        with (format = 'PARQUET')
        """
    )


def fetch_manifest_row(con, source_file: str, schema: str = RAW_SCHEMA) -> tuple[str, datetime] | None:
    rows = query(
        con,
        f"""
        select content_sha256, first_seen_at
        from {relation(schema, MANIFEST_TABLE)}
        where source_file = {quote_literal(source_file)}
        order by last_seen_at desc
        limit 1
        """
    )
    return rows[0] if rows else None


def insert_rows(con, table: str, columns: list[str], rows: list[tuple], schema: str = RAW_SCHEMA) -> None:
    if not rows:
        return
    width = len(columns)
    quoted_columns = ", ".join(quote_identifier(column) for column in columns)
    for offset in range(0, len(rows), 500):
        chunk = rows[offset: offset + 500]
        values = ",\n".join(
            "(" + ", ".join(sql_value(row[index]) for index in range(width)) + ")"
            for row in chunk
        )
        command(
            con,
            f"""
            insert into {relation(schema, table)} ({quoted_columns})
            values
            {values}
            """
        )


def insert_manifest_row(
    con,
    source_file: str,
    file_name: str,
    file_type: str,
    batch_date: str,
    stored_object_key: str,
    content_sha256: str,
    row_count: int,
    first_seen_at: datetime | str,
    schema: str = RAW_SCHEMA,
) -> None:
    now = datetime.now(UTC)
    command(con, f"delete from {relation(schema, MANIFEST_TABLE)} where source_file = {quote_literal(source_file)}")
    insert_rows(
        con,
        MANIFEST_TABLE,
        [
            "source_file",
            "file_name",
            "file_type",
            "batch_date",
            "object_key",
            "content_sha256",
            "row_count",
            "first_seen_at",
            "last_seen_at",
        ],
        [
            (
                source_file,
                file_name,
                file_type,
                batch_date,
                stored_object_key,
                content_sha256,
                row_count,
                first_seen_at,
                now,
            )
        ],
        schema,
    )


def seed_legacy_manifest(
    con,
    source_file: str,
    file_name: str,
    file_type: str,
    batch_date: str,
    archive_root: Path,
    stored_object_key: str,
    schema: str = RAW_SCHEMA,
) -> tuple[str, datetime] | None:
    archived = archive_candidate(archive_root, file_name, batch_date)
    if not archived.exists():
        return None
    archived_hash = sha256_file(archived)
    first_seen_at = datetime.now(UTC)
    insert_manifest_row(
        con,
        source_file,
        file_name,
        file_type,
        batch_date,
        stored_object_key,
        archived_hash,
        count_data_rows(archived, file_type),
        first_seen_at,
        schema,
    )
    return archived_hash, first_seen_at


def insert_rejection(
    con,
    source_file: str,
    file_name: str,
    file_type: str,
    batch_date: str,
    expected_sha256: str | None,
    actual_sha256: str,
    quarantine_path: Path,
    reason: str,
    schema: str = RAW_SCHEMA,
) -> None:
    rejected_at = datetime.now(UTC)
    rejection_id = hashlib.sha256(
        f"{source_file}|{actual_sha256}|{rejected_at.isoformat()}".encode("utf-8")
    ).hexdigest()
    insert_rows(
        con,
        REJECTIONS_TABLE,
        [
            "rejection_id",
            "source_file",
            "file_name",
            "file_type",
            "batch_date",
            "expected_sha256",
            "actual_sha256",
            "quarantine_path",
            "rejected_at",
            "reason",
        ],
        [
            (
                rejection_id,
                source_file,
                file_name,
                file_type,
                batch_date,
                expected_sha256,
                actual_sha256,
                str(quarantine_path),
                rejected_at,
                reason,
            )
        ],
        schema,
    )


def normalized_cells(row: list[str], width: int) -> list[str | None]:
    padded = [cell.strip() for cell in row] + [None] * width
    return padded[:width]


def csv_data_rows(path: Path, file_type: str, width: int) -> list[tuple[int, list[str | None]]]:
    rows = []
    with path.open("r", newline="", encoding="utf-8") as fh:
        for line_number, row in enumerate(csv.reader(fh), start=1):
            if is_blank_row(row) or is_header_row(row, file_type):
                continue
            rows.append((line_number, normalized_cells(row, width)))
    return rows


def row_uid(source_file: str, source_row_number: int, content_sha256: str) -> str:
    return hashlib.md5(f"{source_file}|{source_row_number}|{content_sha256}".encode("utf-8")).hexdigest()


def load_stores_rows(
    con,
    path: Path,
    source_file: str,
    batch_date: str,
    content_sha256: str,
    received_at: datetime | str,
    schema: str = RAW_SCHEMA,
) -> None:
    command(con, f"delete from {relation(schema, STORES_TABLE)} where source_file = {quote_literal(source_file)}")
    rows = [
        (
            row_uid(source_file, line_number, content_sha256),
            source_file,
            path.name,
            line_number,
            cells[0],
            cells[1],
            cells[2],
            batch_date,
            content_sha256,
            received_at,
        )
        for line_number, cells in csv_data_rows(path, "stores", len(STORE_HEADERS))
    ]
    insert_rows(
        con,
        STORES_TABLE,
        [
            "row_uid",
            "source_file",
            "file_name",
            "source_row_number",
            "store_group",
            "store_token",
            "store_name",
            "batch_date",
            "content_sha256",
            "received_at",
        ],
        rows,
        schema,
    )


def load_sales_rows(
    con,
    path: Path,
    source_file: str,
    batch_date: str,
    content_sha256: str,
    received_at: datetime | str,
    schema: str = RAW_SCHEMA,
) -> None:
    command(con, f"delete from {relation(schema, SALES_TABLE)} where source_file = {quote_literal(source_file)}")
    rows = [
        (
            row_uid(source_file, line_number, content_sha256),
            source_file,
            path.name,
            line_number,
            cells[0],
            cells[1],
            cells[2],
            cells[3],
            cells[4],
            cells[5],
            batch_date,
            content_sha256,
            received_at,
        )
        for line_number, cells in csv_data_rows(path, "sales", len(SALES_HEADERS))
    ]
    insert_rows(
        con,
        SALES_TABLE,
        [
            "row_uid",
            "source_file",
            "file_name",
            "source_row_number",
            "store_token",
            "transaction_id",
            "receipt_token",
            "transaction_time",
            "amount",
            "user_role",
            "batch_date",
            "content_sha256",
            "received_at",
        ],
        rows,
        schema,
    )


def load_received_rows(
    con,
    path: Path,
    file_type: str,
    source_file: str,
    batch_date: str,
    content_sha256: str,
    received_at: datetime | str,
    schema: str = RAW_SCHEMA,
) -> None:
    if file_type == "stores":
        load_stores_rows(con, path, source_file, batch_date, content_sha256, received_at, schema)
    if file_type == "sales":
        load_sales_rows(con, path, source_file, batch_date, content_sha256, received_at, schema)


def validate_file(
    con,
    s3,
    bucket: str,
    path: Path,
    archive_root: Path,
    quarantine_root: Path,
    log,
    schema: str = RAW_SCHEMA,
) -> str:
    metadata = file_metadata(path)
    file_type = metadata["file_type"]
    batch_date = metadata["batch_date"]
    source_file = source_file_name(path)
    stored_object_key = object_key(path, batch_date)
    actual_hash = sha256_file(path)
    row_count = count_data_rows(path, file_type)
    existing = fetch_manifest_row(con, source_file, schema)
    expected = existing or seed_legacy_manifest(
        con,
        source_file,
        path.name,
        file_type,
        batch_date,
        archive_root,
        stored_object_key,
        schema,
    )
    expected_hash = expected[0] if expected else None
    first_seen_at = expected[1] if expected else datetime.now(UTC)
    if expected_hash and expected_hash != actual_hash:
        target = quarantine_file(path, quarantine_root, batch_date)
        insert_rejection(
            con,
            source_file,
            path.name,
            file_type,
            batch_date,
            expected_hash,
            actual_hash,
            target,
            "source_filename_hash_mismatch",
            schema,
        )
        log.error("quarantined %s -> %s", path.name, target)
        return f"{path.name}: expected {expected_hash}, got {actual_hash}"
    upload_file_to_minio(s3, path, bucket, stored_object_key)
    insert_manifest_row(
        con,
        source_file,
        path.name,
        file_type,
        batch_date,
        stored_object_key,
        actual_hash,
        row_count,
        first_seen_at,
        schema,
    )
    load_received_rows(con, path, file_type, source_file, batch_date, actual_hash, first_seen_at, schema)
    log.info("accepted %s sha256=%s rows=%d object=%s", path.name, actual_hash, row_count, stored_object_key)
    return ""


def validate_landing(
    landing_path: str,
    archive_path: str,
    quarantine_path: str,
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    http_scheme: str | None = None,
    manifest_schema: str = RAW_SCHEMA,
    minio_endpoint: str | None = None,
    minio_bucket: str | None = None,
    minio_access_key: str | None = None,
    minio_secret_key: str | None = None,
    aws_region: str | None = None,
) -> int:
    log = get_logger("landing_manifest")
    landing = Path(landing_path)
    archive_root = Path(archive_path)
    quarantine_root = Path(quarantine_path)
    files = sorted(f for f in landing.glob("*.csv") if f.is_file())
    if not files:
        log.info("done | accepted=0 landing_path=%s", landing)
        return 0
    conflicts = []
    con = connect(host, port, user, catalog, schema or manifest_schema, http_scheme)
    s3 = s3_client(
        minio_endpoint or os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        minio_access_key or os.environ.get("MINIO_ROOT_USER", "minioadmin"),
        minio_secret_key or os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
        aws_region or os.environ.get("AWS_REGION", "us-east-1"),
    )
    try:
        ensure_tables(con, manifest_schema)
        for path in files:
            conflict = validate_file(
                con,
                s3,
                minio_bucket or os.environ.get("MINIO_BUCKET", "r2-lakehouse"),
                path,
                archive_root,
                quarantine_root,
                log,
                manifest_schema,
            )
            if conflict:
                conflicts.append(conflict)
    finally:
        con.close()
    if conflicts:
        raise RuntimeError("; ".join(conflicts))
    log.info("done | accepted=%d landing_path=%s", len(files), landing)
    return len(files)


if __name__ == "__main__":
    args = parse_args()
    validate_landing(
        args.landing_path,
        args.archive_path,
        args.quarantine_path,
        args.trino_host,
        args.trino_port,
        args.trino_user,
        args.trino_catalog,
        args.trino_schema,
        args.trino_http_scheme,
        args.trino_schema,
        args.minio_endpoint,
        args.minio_bucket,
        args.minio_access_key,
        args.minio_secret_key,
        args.aws_region,
    )
