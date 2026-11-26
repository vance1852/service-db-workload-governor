"""冒烟脚本：验证两类查询在小资料上的端到端结果与 SQLite 语义一致。"""

import json
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from load_sample import load_sample
from qservice.scheduler import ResourceLimits
from qservice.service import QueryService

ROOT = Path(__file__).resolve().parent.parent


def reference_sql(db_path, sql, store=None):
    with sqlite3.connect(db_path) as conn:
        if store is None:
            return [list(row) for row in conn.execute(sql)]
        return [list(row) for row in conn.execute(sql, (store,))]


def main():
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        db_path = tmp / "main.db"
        load_sample(ROOT / "fixtures" / "null-and-tie-order", db_path, "main")
        work = tmp / "work"
        limits = ResourceLimits(800, 1600, 100)
        service = QueryService(db_path, work, limits)

        with sqlite3.connect(db_path) as conn:
            sql = conn.execute(
                "SELECT sql FROM target_sql WHERE name='close'"
            ).fetchone()[0]
        expected = reference_sql(db_path, sql)
        print("SQLite 参考结果:", json.dumps(expected, ensure_ascii=False))

        service.start_query("one", "month", "close", None, 10, 20, estimate=2)
        pages = []
        while True:
            resp = service.fetch_page("one")
            if resp["rows"]:
                pages.extend(resp["rows"])
            if resp["terminal_reason"]:
                break
        print("服务结果:      ", json.dumps(pages, ensure_ascii=False))
        assert pages == expected, "close 结果与 SQLite 不一致"
        print("usage:", json.dumps(service.usage(), ensure_ascii=False, indent=2))
        service.close()


if __name__ == "__main__":
    main()
