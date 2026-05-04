"""Remove all CSV files from the landing folder."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scripts.logger import get_logger
else:
    from .logger import get_logger


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Delete all *.csv files from the landing folder.")
    p.add_argument("--landing-path", default=os.environ.get("LANDING_PATH", "data/landing"))
    p.add_argument("--pattern", default="*.csv")
    return p.parse_args(argv)


def clear(landing_path: str, pattern: str = "*.csv") -> int:
    log = get_logger("clear_landing")
    landing = Path(landing_path)
    if not landing.exists():
        log.warning("landing path does not exist: %s", landing)
        return 0

    deleted = 0
    for f in landing.glob(pattern):
        if f.is_file():
            f.unlink()
            deleted += 1
            log.info("deleted %s", f.name)
    log.info("done | deleted=%d landing_path=%s", deleted, landing)
    return deleted


if __name__ == "__main__":
    args = parse_args()
    clear(args.landing_path, args.pattern)
