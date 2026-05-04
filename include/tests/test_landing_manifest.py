from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

try:
    from include.scripts import landing_manifest
except ModuleNotFoundError:
    from scripts import landing_manifest

try:
    from include.scripts import generate_data
except ModuleNotFoundError as e:
    if e.name != "faker":
        raise
    generate_data = None


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


class LandingManifestTests(unittest.TestCase):
    def test_counts_rows_without_headers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sales_20260501_000.csv"
            write_text(
                path,
                "store_token,transaction_id,receipt_token,transaction_time,amount,user_role\n"
                "a,b,c,d,e,f\n"
                "\n"
                "g,h,i,j,k,l\n",
            )

            self.assertEqual(landing_manifest.count_data_rows(path, "sales"), 2)

    def test_csv_rows_keep_physical_file_row_numbers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sales_20260501_000.csv"
            write_text(
                path,
                "store_token,transaction_id,receipt_token,transaction_time,amount,user_role\n"
                "b,2,r2,20260501T090000.000,$2.00,staff\n"
                "a,1,r1,20260501T080000.000,$1.00,staff\n",
            )

            rows = landing_manifest.csv_data_rows(path, "sales", len(landing_manifest.SALES_HEADERS))

            self.assertEqual([line_number for line_number, _ in rows], [2, 3])
            self.assertEqual(rows[0][1][0], "b")
            self.assertEqual(rows[1][1][0], "a")

    def test_row_uid_is_stable_for_source_file_row_and_hash(self) -> None:
        first = landing_manifest.row_uid("landing/sales_20260501_000.csv", 2, "a" * 64)
        second = landing_manifest.row_uid("landing/sales_20260501_000.csv", 2, "a" * 64)
        changed = landing_manifest.row_uid("landing/sales_20260501_000.csv", 3, "a" * 64)

        self.assertEqual(first, second)
        self.assertNotEqual(first, changed)

    def test_file_metadata_parses_batch_date(self) -> None:
        metadata = landing_manifest.file_metadata(Path("sales_20260501_000.csv"))

        self.assertEqual(metadata["file_type"], "sales")
        self.assertEqual(metadata["batch_date"], "20260501")


class GeneratorTests(unittest.TestCase):
    @unittest.skipIf(generate_data is None, "faker is not installed in this Python environment")
    def test_header_modes_are_deterministic(self) -> None:
        self.assertTrue(generate_data._decide_header("always"))
        self.assertFalse(generate_data._decide_header("never"))


if __name__ == "__main__":
    unittest.main()
