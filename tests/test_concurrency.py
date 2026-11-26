"""连接倍增与公平性：多窗口并发下大任务不拖垮正常查询。"""

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from load_sample import load_sample
from qservice.scheduler import ResourceLimits
from qservice.service import QueryService, QueryState, TerminalReason

ROOT = Path(__file__).resolve().parent.parent


def reference(db_path: Path, name: str, store=None):
    with sqlite3.connect(db_path) as conn:
        sql = conn.execute(
            "SELECT sql FROM target_sql WHERE name=?", (name,)
        ).fetchone()[0]
        params = (store,) if store is not None else ()
        return [list(row) for row in conn.execute(sql, params)]


class ConcurrencyCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        fixture = ROOT / "fixtures" / "spill-pressure-and-snapshot-resume"
        self.db_path = self.dir / "main.db"
        load_sample(fixture, self.db_path, "main")
        catalog_path = fixture / "catalog.json"
        import json

        self.limits = ResourceLimits(**json.loads(catalog_path.read_bytes())["limits"])

    def tearDown(self):
        self.tmp.cleanup()

    def test_many_windows_big_task_does_not_starve_others(self):
        service = QueryService(self.db_path, self.dir / "work", self.limits)
        # 一个跨店大查询 + 三个不同门店的桌面窗口，模拟连接倍增。
        service.start_query("big", "month", "close", None, 30, 3)
        for tag, store in (("d1", "west"), ("d2", "north"), ("d3", "hill")):
            service.start_query(tag, "desk", "desk", store, 12, 4)

        delivered: dict[str, list] = {q: [] for q in ("big", "d1", "d2", "d3")}
        terminals: dict[str, str] = {}
        # 交错推进：每轮每个窗口各拉一次，持续到全部终止。
        for _ in range(200):
            active = [
                q for q in delivered
                if service.status(q)["state"] not in
                (QueryState.DELIVERED, QueryState.TERMINATED, QueryState.CANCELLED)
            ]
            if not active:
                break
            for query_id in active:
                response = service.fetch_page(query_id)
                delivered[query_id].extend(response["rows"])
                if response["terminal_reason"]:
                    terminals[query_id] = response["terminal_reason"]
                usage = service.usage()
                # 任何时刻都不突破两类资源上限。
                self.assertLessEqual(
                    usage["work_memory"]["used"],
                    usage["limits"]["work_memory_bytes"],
                )
                self.assertLessEqual(
                    usage["temporary"]["used"],
                    usage["limits"]["temporary_bytes"],
                )

        # 所有窗口都正常完成，没有任何一个被大任务拖到等待上限而终止。
        self.assertEqual(
            terminals,
            {q: TerminalReason.COMPLETED for q in delivered},
        )
        # 桌面小窗口在大查询仍在运行时就已经拿到首批页（不被饿死）：
        # 它们的总轮次内全部交付，且结果与 SQLite 一致。
        self.assertEqual(delivered["big"], reference(self.db_path, "close"))
        for tag, store in (("d1", "west"), ("d2", "north"), ("d3", "hill")):
            self.assertEqual(
                delivered[tag], reference(self.db_path, "desk", store=store)
            )
        service.close()

    def test_desk_reserve_keeps_small_query_moving_under_pressure(self):
        # 用更紧的工作内存凸显桌面保底：小查询仍能在限额内完成。
        tight = ResourceLimits(
            work_memory_bytes=1200,
            temporary_bytes=self.limits.temporary_bytes,
            desk_reserve_bytes=400,
        )
        service = QueryService(self.db_path, self.dir / "work2", tight)
        service.start_query("big", "month", "close", None, 30, 3)
        service.start_query("desk", "desk", "desk", "west", 12, 4)
        desk_rows = []
        for _ in range(120):
            if service.status("desk")["state"] == QueryState.DELIVERED:
                break
            response = service.fetch_page("desk")
            desk_rows.extend(response["rows"])
            service.fetch_page("big")  # 大查询持续争抢
        self.assertEqual(
            service.status("desk")["terminal_reason"], TerminalReason.COMPLETED
        )
        self.assertEqual(desk_rows, reference(self.db_path, "desk", store="west"))
        service.close()


if __name__ == "__main__":
    unittest.main()
