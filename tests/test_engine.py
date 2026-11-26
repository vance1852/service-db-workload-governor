"""端到端测试：SQL 语义、NULL/并列、分批 spill 与资源记账。"""

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from load_sample import load_sample
from qservice.encoding import encode_lines, row_bytes
from qservice.scheduler import ResourceLimits
from qservice.service import QueryService, QueryState, TerminalReason
from qservice.sql_contract import load_contracts, parse_contract

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "fixtures"


def reference(db_path: Path, name: str, store=None):
    with sqlite3.connect(db_path) as conn:
        sql = conn.execute(
            "SELECT sql FROM target_sql WHERE name=?", (name,)
        ).fetchone()[0]
        params = (store,) if store is not None else ()
        return [list(row) for row in conn.execute(sql, params)]


class ServiceCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def service_for(self, fixture: str, db_name: str = "main.db"):
        fixture_dir = FIXTURES / fixture
        catalog = json.loads((fixture_dir / "catalog.json").read_bytes())
        db_path = self.dir / db_name
        load_sample(fixture_dir, db_path, "main")
        limits = ResourceLimits(**catalog["limits"])
        return QueryService(db_path, self.dir / f"work-{db_name}", limits), catalog

    def fetch_all(self, service: QueryService, query_id: str):
        pages, terminal = [], None
        while True:
            response = service.fetch_page(query_id)
            pages.extend(response["rows"])
            if response["terminal_reason"]:
                terminal = response["terminal_reason"]
                break
        return pages, terminal

    # ------------------------------------------------------------ 契约解析
    def test_contract_parse_matches_target_sql(self):
        service, _ = self.service_for("null-and-tie-order")
        contracts = load_contracts(service.db_path)
        close = contracts["close"]
        self.assertEqual(close.kind, "close")
        self.assertEqual(close.group_columns, ["s.family"])
        self.assertEqual(
            [term.upper() for term in close.order_terms],
            ["SUM(A.AMOUNT)", "S.FAMILY"],
        )
        service.close()

    # ------------------------------------------------------------ NULL 与并列
    def test_null_sum_first_and_tie_break(self):
        service, _ = self.service_for("null-and-tie-order")
        expected = reference(service.db_path, "close")
        service.start_query("one", "month", "close", None, 10, 20, estimate=2)
        pages, terminal = self.fetch_all(service, "one")
        self.assertEqual(terminal, TerminalReason.COMPLETED)
        self.assertEqual(pages, expected)
        # NULL 之和排最前；同额 -20 按附加列 family 定并列（one 在 two 前）。
        self.assertIsNone(pages[0][2])
        self.assertEqual([row[0] for row in pages], ["three", "one", "two"])
        service.close()

    def test_desk_keeps_null_refund_rows_and_duplicates(self):
        service, _ = self.service_for("spill-pressure-and-snapshot-resume")
        expected = reference(service.db_path, "desk", store="west")
        service.start_query("small", "desk", "desk", "west", 12, 2)
        pages, terminal = self.fetch_all(service, "small")
        self.assertEqual(terminal, TerminalReason.COMPLETED)
        self.assertEqual(pages, expected)
        # s0 有两条更正 -> 重复事实两行；无更正的 west 账单保留 NULL 明细。
        s0_rows = [row for row in pages if row[0] == "s0"]
        self.assertEqual(len(s0_rows), 2)
        null_rows = [row for row in pages if row[2] is None]
        self.assertTrue(null_rows)
        service.close()

    # ------------------------------------------------------------ 分页稳定性
    def test_pages_are_stable_slices(self):
        service, _ = self.service_for("null-and-tie-order")
        service.start_query("one", "month", "close", None, 10, 2, estimate=2)
        first = service.fetch_page("one")
        second = service.fetch_page("one")
        self.assertEqual(len(first["rows"]), 2)
        self.assertEqual(len(second["rows"]), 1)
        full = first["rows"] + second["rows"]
        self.assertEqual(full, reference(service.db_path, "close"))
        # 再拉取只得到终止态，不重发旧页。
        third = service.fetch_page("one")
        self.assertEqual(third["rows"], [])
        self.assertEqual(third["terminal_reason"], TerminalReason.COMPLETED)
        service.close()

    # ------------------------------------------------------------ spill 正确性
    def test_close_spill_pipeline_matches_sqlite(self):
        service, catalog = self.service_for("spill-pressure-and-snapshot-resume")
        # 工作内存仅 2400 字节，964 条 JOIN 事实必然多次 spill。
        self.assertEqual(catalog["limits"]["work_memory_bytes"], 2400)
        service.start_query("big", "month", "close", None, 30, 3)
        pages, terminal = self.fetch_all(service, "big")
        self.assertEqual(terminal, TerminalReason.COMPLETED)
        self.assertEqual(pages, reference(service.db_path, "close"))
        usage = service.usage()
        # 完成后区段已走两阶段删除，临时空间全部归还。
        self.assertEqual(usage["temporary"]["used"], 0)
        service.close()

    def test_never_exceeds_resource_limits(self):
        service, _ = self.service_for("spill-pressure-and-snapshot-resume")
        service.start_query("big", "month", "close", None, 30, 3)
        service.start_query("small", "desk", "desk", "west", 12, 2)
        for _ in range(40):
            for query_id in ("big", "small"):
                state = service.status(query_id)["state"]
                if state in (QueryState.DELIVERED, QueryState.TERMINATED,
                             QueryState.CANCELLED):
                    continue
                service.fetch_page(query_id)
            usage = service.usage()
            self.assertLessEqual(
                usage["work_memory"]["used"], usage["limits"]["work_memory_bytes"]
            )
            self.assertLessEqual(
                usage["temporary"]["used"], usage["limits"]["temporary_bytes"]
            )
        service.close()

    # ------------------------------------------------------------ 字节计量
    def test_byte_accounting_is_recheckable(self):
        service, _ = self.service_for("null-and-tie-order")
        service.start_query("one", "month", "close", None, 10, 20, estimate=2)
        service.fetch_page("one")
        status = service.status("one")
        # 工作内存占用等于结果行的 UTF-8 编码字节之和，可独立复算。
        expected_bytes = sum(row_bytes(row) for row in
                             reference(service.db_path, "close"))
        self.assertEqual(status["work_bytes"], expected_bytes)
        # 区段文件字节等于 JSON Lines 编码长度。
        for segment in status["segments"]:
            path = Path(service.work_dir / "segments" / "one" /
                        f"{segment['seg_id']}.jsonl")
            if path.exists():
                self.assertEqual(segment["bytes"], path.stat().st_size)
        service.close()

    # ------------------------------------------------------------ 等待上限
    def test_wait_limit_ends_with_traceable_reason(self):
        # 极小临时空间 + 极小等待上限：跨调度轮仍无法推进时必须带理由结束。
        service, _ = self.service_for("spill-pressure-and-snapshot-resume")
        service.limits.temporary_bytes = 200
        service.scheduler.limits.temporary_bytes = 200
        service.start_query("big", "month", "close", None, wait_limit=1, page_size=3)
        response = None
        for _ in range(10):
            response = service.fetch_page("big")
            if response["terminal_reason"]:
                break
        self.assertEqual(response["terminal_reason"],
                         TerminalReason.WAIT_LIMIT)
        self.assertIn("temporary_space", response["status"]["terminal_detail"])
        service.close()


if __name__ == "__main__":
    unittest.main()
