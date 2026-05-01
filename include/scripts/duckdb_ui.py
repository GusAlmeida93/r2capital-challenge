"""Long-running DuckDB UI server.

Backs the UI with an in-memory database whose silver/gold schemas are views
over the Parquet exports written by the DAG. This sidesteps DuckDB's
single-writer file lock — the UI never opens the live warehouse, so the
DAG retains exclusive write access whenever it runs.

A small TCP relay forwards 0.0.0.0:{DUCKDB_UI_PORT} to 127.0.0.1:4213 so
the docker-compose port publish actually reaches the UI server (which
binds only to localhost by default).
"""
from __future__ import annotations

import glob
import os
import socket
import sys
import threading
import time
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scripts.logger import get_logger  # type: ignore[no-redef]
else:
    from .logger import get_logger

import duckdb

EXPORTS = os.environ.get("EXPORT_PATH", "/opt/airflow/data/exports")
PUBLIC_HOST = os.environ.get("DUCKDB_UI_HOST", "0.0.0.0")
# Relay listens on this port inside the container; docker-compose publishes
# it to the host. Must NOT collide with BACKEND_PORT — binding 0.0.0.0:X
# would prevent the UI extension from later binding 127.0.0.1:X on Linux.
PUBLIC_PORT = int(os.environ.get("DUCKDB_UI_PUBLIC_PORT", "14213"))
# DuckDB UI binds to ::1 (IPv6 localhost), so getaddrinfo via "localhost"
# resolves it correctly while still falling back to 127.0.0.1 if needed.
BACKEND_HOST = "localhost"
BACKEND_PORT = int(os.environ.get("DUCKDB_UI_BACKEND_PORT", "4213"))


def setup_views(con: duckdb.DuckDBPyConnection, log) -> int:
    """Create one view per Parquet file under EXPORTS/{silver,gold}/.
    Returns the number of views (idempotent — uses CREATE OR REPLACE).

    Note: DuckDB rejects bind parameters inside CREATE VIEW, so the path is
    inlined with single-quote escaping. Inputs come from glob() over a fixed
    directory, so the surface for injection is non-user paths only.
    """
    n = 0
    for schema in ("raw", "silver", "gold"):
        d = os.path.join(EXPORTS, schema)
        if not os.path.isdir(d):
            continue
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for p in sorted(glob.glob(f"{d}/*.parquet")):
            table = os.path.basename(p)[: -len(".parquet")]
            safe_path = p.replace("'", "''")
            con.execute(
                f"CREATE OR REPLACE VIEW {schema}.{table} AS "
                f"SELECT * FROM read_parquet('{safe_path}', union_by_name=true)"
            )
            n += 1
    return n


def _copy(src: socket.socket, dst: socket.socket) -> None:
    try:
        while True:
            data = src.recv(4096)
            if not data:
                break
            dst.sendall(data)
    except Exception:
        pass
    finally:
        for s in (src, dst):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                s.close()
            except Exception:
                pass


def _handle_relay_client(client: socket.socket, log) -> None:
    try:
        upstream = socket.create_connection((BACKEND_HOST, BACKEND_PORT))
    except Exception as e:  # noqa: BLE001
        log.warning("relay: backend not yet ready: %s", e)
        try:
            client.close()
        except Exception:
            pass
        return
    threading.Thread(target=_copy, args=(client, upstream), daemon=True).start()
    threading.Thread(target=_copy, args=(upstream, client), daemon=True).start()


def relay_loop(log) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((PUBLIC_HOST, PUBLIC_PORT))
    s.listen(64)
    log.info("relay listening %s:%d -> %s:%d", PUBLIC_HOST, PUBLIC_PORT, BACKEND_HOST, BACKEND_PORT)
    while True:
        client, _ = s.accept()
        threading.Thread(target=_handle_relay_client, args=(client, log), daemon=True).start()


def main() -> None:
    log = get_logger("duckdb_ui")
    con = duckdb.connect(":memory:")

    log.info("loading ui extension")
    try:
        con.execute("INSTALL ui")
        con.execute("LOAD ui")
    except Exception as e:  # noqa: BLE001
        log.error("failed to load duckdb ui extension: %s", e)
        sys.exit(1)

    log.info("waiting for first Parquet export at %s", EXPORTS)
    while setup_views(con, log) == 0:
        time.sleep(5)
    log.info("created %d views", setup_views(con, log))

    def refresh_loop():
        while True:
            time.sleep(30)
            try:
                setup_views(con, log)
            except Exception as e:  # noqa: BLE001
                log.warning("view refresh failed: %s", e)

    threading.Thread(target=refresh_loop, daemon=True).start()
    threading.Thread(target=relay_loop, args=(log,), daemon=True).start()

    log.info("starting DuckDB UI server")
    try:
        con.execute("CALL start_ui_server()")
    except Exception:
        # Fallback for versions exposing the function at the top level.
        con.execute("SELECT start_ui_server()")
    log.info("DuckDB UI ready on http://localhost:%d", PUBLIC_PORT)

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
