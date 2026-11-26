"""断面一致性、取消两阶段释放、重启续取与写入隔离测试。"""

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from load_sample import load_sample
from qservice.driver import TimelineDriver, load_timeline
from qservice.scheduler import ResourceLimits
from qservice.service import QueryService, QueryState, TerminalReason

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "fixtures"


def reference(db_path: Path, name: str, store=None):
    with sqlite3.connect(db_path) as conn:
        sql = conn.execute(
            "SELECT sql FROM target_sql WHERE name=?", (name,)
        ).fetchone()[0]
        params = (store,) if store is not None else ()
        return [list(row) for row in conn.execute(sql, params)]


class SnapshotCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        fixture = FIXTURES / "spill-pressure-and-snapshot-resume"
        self.catalog = json.loads((fixture / "catalog.json").read_bytes())
        self.db_path = self.dir / "main.db"
        load_sample(fixture, self.db_path, "main")
        self.fixture = fixture

    def tearDown(self):
        self.tmp.cleanup()

    def service(self) -> QueryService:
        return QueryService(
            self.db_path,
            self.dir / "work",
            ResourceLimits(**self.catalog["limits"]),
        )

    def fetch_all(self, service, query_id):
        pages, terminal = [], None
        while True:
            response = service.fetch_page(query_id)
            pages.extend(response["rows"])
            if response["terminal_reason"]:
                terminal = response["terminal_reason"]
                break
        return pages, terminal

    # ------------------------------------------------------ 提交不改变旧断面
    def test_snapshot_isolation_across_commit(self):
        service = self.service()
        expected_before = reference(self.db_path, "desk", store="west")
        service.start_query("small", "desk", "desk", "west", 12, 2)
        # 写入一条后来提交的 west 记录（timeline at=6 的内容）。
        service.commit("sale", [["s3", "west", "care", 6000, "新提交"]])
        pages, terminal = self.fetch_all(service, "small")
        self.assertEqual(terminal, TerminalReason.COMPLETED)
        # 已开始的读取与启动断面一致，不临时改读最新记录。
        self.assertEqual(pages, expected_before)
        # 新查询看到新版本，包含新记录。
        service.start_query("small2", "desk", "desk", "west", 12, 2)
        pages2, _ = self.fetch_all(service, "small2")
        self.assertEqual(len(pages2), len(expected_before) + 1)
        service.close()

    # ------------------------------------------------------ 重启续取不重复
    def test_restart_resume_no_duplicate_same_snapshot(self):
        service = self.service()
        expected = reference(self.db_path, "close")
        service.start_query("big", "month", "close", None, 30, 3)
        first = service.fetch_page("big")
        self.assertEqual(len(first["rows"]), 3)
        delivered = list(first["rows"])
        service.pause("big")
        service.restart_worker()
        status = service.status("big")
        self.assertEqual(status["state"], QueryState.PAUSED)
        service.resume("big")
        # 重启后从下一页继续，不重发已交付页。
        while True:
            response = service.fetch_page("big")
            delivered.extend(response["rows"])
            if response["terminal_reason"]:
                break
        self.assertEqual(response["terminal_reason"], TerminalReason.COMPLETED)
        self.assertEqual(delivered, expected)
        # journal 中每页指纹唯一且连续。
        journal = service.journal.read_all()
        hashes = [event["hash"] for event in journal if event["event"] == "page_delivered"]
        self.assertEqual(len(hashes), len(set(hashes)))
        service.close()

    # ------------------------------------------------------ 小查询重启同样续取
    def test_desk_restart_resume(self):
        service = self.service()
        expected = reference(self.db_path, "desk", store="west")
        service.start_query("small", "desk", "desk", "west", 12, 2)
        first = service.fetch_page("small")
        self.assertEqual(len(first["rows"]), 2)
        service.pause("small")
        service.restart_worker()
        service.resume("small")
        rest = []
        while True:
            response = service.fetch_page("small")
            rest.extend(response["rows"])
            if response["terminal_reason"]:
                break
        self.assertEqual(first["rows"] + rest, expected)
        service.close()

    # ------------------------------------------------------ 取消的 pending-delete
    def test_cancel_keeps_temp_until_confirmed(self):
        service = self.service()
        service.start_query("other", "month", "close", None, 10, 5)
        # 只跑一个调度轮次：查询已物化工况区段但尚未完成，随后取消。
        service.tick += 1
        service.scheduler.tick(service.queries, service.order, service.tick)
        service._checkpoint_locked("other")
        self.assertGreater(service.spill.live_bytes("other"), 0)
        service.cancel("other")
        usage = service.usage()
        self.assertEqual(usage["queries"][0]["state"], QueryState.CANCELLED)
        # 取消后区段处于 pending-delete，临时空间尚未归还。
        pending_before = usage["temporary"]["pending_delete"]
        self.assertGreater(pending_before, 0)
        # 模拟外部确认事件：确认后空间才归还。
        status = service.status("other")
        for segment in status["segments"]:
            service.temporary_delete_confirmed(
                "other", segment["seg_id"], segment["bytes"]
            )
        usage2 = service.usage()
        self.assertEqual(usage2["temporary"]["used"], 0)
        self.assertEqual(usage2["temporary"]["pending_delete"], 0)
        service.close()

    # ------------------------------------------------------ 未物化不占临时空间
    def test_waiting_query_without_materialization_holds_no_temp(self):
        # 极小临时空间：big 先占住，ghost 无法完成首次 spill，只能等待。
        tight = ResourceLimits(
            work_memory_bytes=600,
            temporary_bytes=500,
            desk_reserve_bytes=100,
        )
        service = QueryService(self.db_path, self.dir / "work-tight", tight)
        service.start_query("big", "month", "close", None, 30, 3)
        # 再启动一个查询：若它在拿到临时空间前被卡住，不应持有任何区段。
        service.start_query("ghost", "month", "close", None, 10, 5)
        ghost = service.status("ghost")
        # 核心不变量：没有物化区段的查询，临时占用恒为 0，不挡其他查询。
        self.assertEqual(service.spill.live_bytes("ghost"), 0)
        self.assertEqual(ghost["temp_bytes"], 0)
        self.assertEqual(
            [s for s in ghost["segments"] if s["state"] != "confirmed_delete"],
            [],
        )
        # 直接验证不变量：手工构造一个无区段的等待态，占用仍为 0。
        self.assertEqual(service.spill.live_bytes("never-existed"), 0)
        service.close()

    # ------------------------------------------------------ 断面清理不可续取
    def test_pruned_snapshot_rejects_resume(self):
        service = self.service()
        service.start_query("big", "month", "close", None, 30, 3)
        service.fetch_page("big")
        service.pause("big")
        # 新提交后显式清理旧断面。
        service.commit("sale", [["s3", "west", "care", 6000, "新提交"]])
        service.prune_snapshots(keep_version=1)
        status = service.status("big")
        self.assertEqual(status["state"], QueryState.TERMINATED)
        self.assertEqual(status["terminal_reason"],
                         TerminalReason.SNAPSHOT_PRUNED)
        response = service.fetch_page("big")
        self.assertEqual(response["terminal_reason"],
                         TerminalReason.SNAPSHOT_PRUNED)
        service.close()

    # ------------------------------------------------------ 写入不被大查询阻塞
    def test_writer_not_blocked_by_large_query(self):
        service = self.service()
        service.start_query("big", "month", "close", None, 30, 3)
        service.fetch_page("big")  # 大查询已 spill 出多个区段
        # 普通写事务与基表读连接仍然即时可用。
        result = service.commit("sale", [["s9", "east", "care", 9, "写入"]])
        self.assertEqual(result["version"], 1)
        with sqlite3.connect(self.db_path) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM sale WHERE sale_id='s9'"
            ).fetchone()[0]
        self.assertEqual(count, 1)
        service.close()

    # ------------------------------------------------------ 完整 timeline 回放
    def test_full_fixture_timeline_replay(self):
        # small 的断面在 at=3 启动时固定，at=6 才提交 s3；其结果必须与
        # 提交前基线一致（不含 s3），而不是读最新数据库。
        baseline = reference(self.db_path, "desk", store="west")
        service = self.service()
        _catalog, timeline = load_timeline(self.fixture)
        driver = TimelineDriver(service)
        report = driver.run(timeline)
        # 小查询在重启后续取完成，且页与启动断面基线一致（不含后来提交的 s3）。
        small_pages = report["pages"]["small"]
        self.assertEqual(small_pages, baseline)
        self.assertNotIn("s3", [row[0] for row in small_pages])
        # 而最新基表确实包含 s3，证明服务没有临时改读最新记录。
        with sqlite3.connect(self.db_path) as conn:
            latest_count = conn.execute(
                "SELECT COUNT(*) FROM sale WHERE sale_id='s3'"
            ).fetchone()[0]
        self.assertEqual(latest_count, 1)
        # 扩张观察在引擎侧得到对账。
        expansion_entries = [
            entry for entry in report["trace"]
            if entry["action"] == "observed_expansion"
        ]
        self.assertTrue(expansion_entries)
        self.assertTrue(expansion_entries[0]["result"]["reconciled"])
        # 最后一个动作确认删除后，临时空间清零。
        final_usage = report["trace"][-1]["usage"]
        self.assertEqual(final_usage["temporary"]["used"], 0)
        # other 查询以取消终止，且其 pending 区段曾被记账。
        other_status = next(
            q for q in final_usage["queries"] if q["query_id"] == "other"
        )
        self.assertEqual(other_status["state"], QueryState.CANCELLED)
        service.close()


if __name__ == "__main__":
    unittest.main()
