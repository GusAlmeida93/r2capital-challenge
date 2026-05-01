"""Move processed files from the landing folder to the archive.

Files are grouped by batch_date (extracted from the filename pattern
stores_YYYYMMDD.csv / sales_YYYYMMDD[_NNN].csv) and moved into
{ARCHIVE_PATH}/{batch_date}/. If a file with the same name already exists
in the archive (e.g. re-processed batch), it is overwritten.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scripts.logger import get_logger  # type: ignore[no-redef]
else:
    from .logger import get_logger

BATCH_RE = re.compile(r"(?:stores|sales)_(\d{8})(?:_\d+)?\.csv$", re.IGNORECASE)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Move processed CSVs from landing to archive.")
    p.add_argument("--landing-path", default=os.environ.get("LANDING_PATH", "data/landing"))
    p.add_argument("--archive-path", default=os.environ.get("ARCHIVE_PATH", "data/archive"))
    return p.parse_args(argv)


def archive(landing_path: str, archive_path: str) -> int:
    log = get_logger("archive_files")
    landing = Path(landing_path)
    archive_root = Path(archive_path)

    if not landing.exists():
        log.warning("landing path does not exist: %s", landing)
        return 0

    moved = 0
    for f in sorted(landing.glob("*.csv")):
        if not f.is_file():
            continue
        m = BATCH_RE.match(f.name)
        target_dir = archive_root / (m.group(1) if m else "_unparsed")
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / f.name
        shutil.move(str(f), str(target))
        moved += 1
        log.info("moved %s -> %s", f.name, target)

    log.info("done | moved=%d archive=%s", moved, archive_root)
    return moved


if __name__ == "__main__":
    args = parse_args()
    archive(args.landing_path, args.archive_path)
