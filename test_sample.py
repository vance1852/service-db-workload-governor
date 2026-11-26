"""检查 SQL 底表、设备记录和本地读写连接。"""

from contextlib import closing
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest

from load_sample import load_sample


class SampleTest(unittest.TestCase):
    def test_each_record_loads(self):
        cases = sorted((Path(__file__).parent / "fixtures").glob("*/catalog.json"))
        self.assertGreaterEqual(len(cases), 2)
        with TemporaryDirectory() as directory:
            for index, catalog_path in enumerate(cases):
                with self.subTest(case=catalog_path.parent.name):
                    self.check_sample(catalog_path, Path(directory), index)

    def check_sample(self, catalog_path, destination, index):
        catalog = json.loads(catalog_path.read_bytes())
        records = json.loads(catalog_path.with_name("timeline.json").read_bytes())
        self.assertEqual(len(records), catalog["event_count"])
        self.assertTrue(all(row["action"] and isinstance(row["args"], list) for row in records))
        self.assertEqual([entry["at"] for entry in records], sorted(entry["at"] for entry in records))
        for role, fields in catalog["files"].items():
            target = destination / f"data-{index}-{role}.db"
            self.assertEqual(load_sample(catalog_path.parent, target, role), fields["tables"])
            with self.assertRaises(FileExistsError):
                load_sample(catalog_path.parent, target, role)
            with closing(sqlite3.connect(target)) as writer, closing(sqlite3.connect(target)) as reader:
                self.assertEqual(writer.execute("PRAGMA journal_mode=WAL").fetchone(), ("wal",))
                self.assertEqual({name for name, in reader.execute(
                    "SELECT name FROM sqlite_schema WHERE type='table'")}, set(fields["tables"]))
                writer.execute("CREATE TABLE local_connection_check(marker INTEGER)")
                reader.execute("BEGIN")
                self.assertEqual(reader.execute("SELECT COUNT(*) FROM local_connection_check").fetchone(), (0,))
                writer.execute("INSERT INTO local_connection_check VALUES (1)")
                writer.commit()
                self.assertEqual(reader.execute("SELECT COUNT(*) FROM local_connection_check").fetchone(), (0,))
                reader.commit()
                self.assertEqual(reader.execute("SELECT COUNT(*) FROM local_connection_check").fetchone(), (1,))


if __name__ == "__main__":
    unittest.main()
