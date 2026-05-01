"""Generate synthetic stores + sales CSV files using Faker.

Outputs match the schema in assessment.pdf:
- stores_<batch>.csv: store_group (8 hex upper), store_token (UUID lower), store_name
- sales_<batch>_<n>.csv: store_token, transaction_id (UUID), receipt_token (alnum 5-30),
  transaction_time (YYYYMMDDTHHMMSS.SSS), amount ($NN.NN), user_role

By default ~5% of rows are deliberately malformed to exercise the silver-layer
validation pipeline and to make Output 1's invalid_count meaningful.
"""
from __future__ import annotations

import argparse
import csv
import os
import random
import string
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker

# Allow running as a script (python generate_data.py) or as a module.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scripts.logger import get_logger  # type: ignore[no-redef]
else:
    from .logger import get_logger

USER_ROLES = ["Cashier", "Manager", "Owner", "Clerk", "Supervisor", "Admin"]
STORE_HEADERS = ["store_group", "store_token", "store_name"]
SALES_HEADERS = [
    "store_token",
    "transaction_id",
    "receipt_token",
    "transaction_time",
    "amount",
    "user_role",
]


def _store_group() -> str:
    return "".join(random.choices("0123456789ABCDEF", k=8))


def _alnum(min_len: int = 5, max_len: int = 30) -> str:
    n = random.randint(min_len, max_len)
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def _format_ts(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%S.") + f"{dt.microsecond // 1000:03d}"


def _malformed_store_row(faker: Faker) -> list[str]:
    pick = random.choice(["bad_group", "bad_token", "long_name", "blank_token"])
    if pick == "bad_group":
        return ["zzzzzzzz", str(uuid.uuid4()), faker.company()]
    if pick == "bad_token":
        return [_store_group(), "not-a-uuid", faker.company()]
    if pick == "long_name":
        return [_store_group(), str(uuid.uuid4()), faker.text(max_nb_chars=400).replace("\n", " ")]
    return [_store_group(), "", faker.company()]


def _malformed_sale_row(faker: Faker, base_dt: datetime) -> list[str]:
    pick = random.choice(
        ["bad_store_token", "bad_txn_id", "short_receipt", "bad_time", "neg_amount", "blank_role"]
    )
    if pick == "bad_store_token":
        return [
            "not-a-uuid",
            str(uuid.uuid4()),
            _alnum(),
            _format_ts(base_dt),
            f"${random.uniform(0.01, 999.99):.2f}",
            random.choice(USER_ROLES),
        ]
    if pick == "bad_txn_id":
        return [
            str(uuid.uuid4()),
            "ZZZ",
            _alnum(),
            _format_ts(base_dt),
            f"${random.uniform(0.01, 999.99):.2f}",
            random.choice(USER_ROLES),
        ]
    if pick == "short_receipt":
        return [
            str(uuid.uuid4()),
            str(uuid.uuid4()),
            "ab",
            _format_ts(base_dt),
            f"${random.uniform(0.01, 999.99):.2f}",
            random.choice(USER_ROLES),
        ]
    if pick == "bad_time":
        return [
            str(uuid.uuid4()),
            str(uuid.uuid4()),
            _alnum(),
            "not-a-timestamp",
            f"${random.uniform(0.01, 999.99):.2f}",
            random.choice(USER_ROLES),
        ]
    if pick == "neg_amount":
        return [
            str(uuid.uuid4()),
            str(uuid.uuid4()),
            _alnum(),
            _format_ts(base_dt),
            f"$-{random.uniform(0.01, 999.99):.2f}",
            random.choice(USER_ROLES),
        ]
    return [
        str(uuid.uuid4()),
        str(uuid.uuid4()),
        _alnum(),
        _format_ts(base_dt),
        f"${random.uniform(0.01, 999.99):.2f}",
        "X" * 50,
    ]


def _emit(path: Path, headers: list[str], rows: list[list[str]], include_header: bool) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if include_header:
            writer.writerow(headers)
        writer.writerows(rows)


def _decide_header(mode: str) -> bool:
    if mode == "always":
        return True
    if mode == "never":
        return False
    return random.random() < 0.5


def generate(args: argparse.Namespace) -> None:
    log = get_logger("generate_data")
    fake = Faker()
    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

    landing = Path(args.landing_path)
    landing.mkdir(parents=True, exist_ok=True)

    batch_date = args.batch_date
    try:
        datetime.strptime(batch_date, "%Y%m%d")
    except ValueError as exc:
        raise SystemExit(f"--batch-date must be YYYYMMDD: {exc}")

    # Stores
    stores: list[dict[str, str]] = []
    rows: list[list[str]] = []
    invalid_stores = 0
    for _ in range(args.num_stores):
        if random.random() < args.invalid_rate:
            rows.append(_malformed_store_row(fake))
            invalid_stores += 1
        else:
            store = {
                "store_group": _store_group(),
                "store_token": str(uuid.uuid4()),
                "store_name": fake.company()[:200],
            }
            stores.append(store)
            rows.append([store["store_group"], store["store_token"], store["store_name"]])

    store_path = landing / f"stores_{batch_date}.csv"
    _emit(store_path, STORE_HEADERS, rows, _decide_header(args.include_headers))
    log.info(
        "wrote %s rows=%d invalid=%d header=%s",
        store_path.name, len(rows), invalid_stores, store_path.exists(),
    )

    if not stores:
        log.warning("no valid stores generated — sales rows will all be malformed")

    # Sales — distributed across the last `--days-window` days from batch_date
    base_day = datetime.strptime(batch_date, "%Y%m%d")
    for file_idx in range(args.num_sales_files):
        rows = []
        invalid_sales = 0
        for _ in range(args.rows_per_sales_file):
            txn_dt = base_day - timedelta(
                days=random.randint(0, max(0, args.days_window - 1)),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
                microseconds=random.randint(0, 999) * 1000,
            )
            if random.random() < args.invalid_rate or not stores:
                rows.append(_malformed_sale_row(fake, txn_dt))
                invalid_sales += 1
                continue
            store = random.choice(stores)
            rows.append(
                [
                    store["store_token"],
                    str(uuid.uuid4()),
                    _alnum(),
                    _format_ts(txn_dt),
                    f"${random.uniform(0.50, 999.99):.2f}",
                    random.choice(USER_ROLES),
                ]
            )

        # Inject a few duplicate transactions across files to test SCD2 + silver upsert.
        if args.duplicate_rate > 0 and len(rows) > 1:
            n_dups = max(1, int(len(rows) * args.duplicate_rate))
            for _ in range(n_dups):
                src = random.choice(rows[: len(rows)])
                rows.append(list(src))

        sales_path = landing / f"sales_{batch_date}_{file_idx:03d}.csv"
        _emit(sales_path, SALES_HEADERS, rows, _decide_header(args.include_headers))
        log.info(
            "wrote %s rows=%d invalid=%d", sales_path.name, len(rows), invalid_sales,
        )

    log.info("done | landing_path=%s batch_date=%s", landing, batch_date)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate synthetic stores + sales CSVs.")
    p.add_argument("--landing-path", default=os.environ.get("LANDING_PATH", "data/landing"))
    p.add_argument("--num-stores", type=int, default=50)
    p.add_argument("--num-sales-files", type=int, default=2)
    p.add_argument("--rows-per-sales-file", type=int, default=500)
    p.add_argument("--batch-date", default=datetime.now(timezone.utc).strftime("%Y%m%d"),
                   help="YYYYMMDD; defaults to today (UTC)")
    p.add_argument("--invalid-rate", type=float, default=0.05,
                   help="Fraction of rows that are deliberately malformed")
    p.add_argument("--duplicate-rate", type=float, default=0.02,
                   help="Fraction of sales rows duplicated within a file")
    p.add_argument("--days-window", type=int, default=7,
                   help="transaction_time spread over this many days back from batch_date")
    p.add_argument("--include-headers", choices=("auto", "always", "never"), default="auto")
    p.add_argument("--seed", type=int, default=None)
    return p.parse_args(argv)


if __name__ == "__main__":
    generate(parse_args())
