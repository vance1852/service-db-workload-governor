"""Unix 域套接字多窗口入口测试。"""

import json
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from load_sample import load_sample
from qservice.client import call
from qservice.scheduler import ResourceLimits
from qservice.server import QueryServer

ROOT = Path(__file__).resolve().parent.parent


class SocketCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.db_path = self.dir / "main.db"
        load_sample(ROOT / "fixtures" / "null-and-tie-order", self.db_path, "main")
        self.sock_path = self.dir / "q.sock"
        self.server = QueryServer(
            self.sock_path,
            self.db_path,
            self.dir / "work",
            ResourceLimits(800, 1600, 100),
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        # 等待监听就绪。
        for _ in range(50):
            if self.sock_path.exists():
                break
            time.sleep(0.02)

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.tmp.cleanup()

    def test_two_windows_share_service_and_pages(self):
        # 窗口 1 启动查询并取第一页。
        resp = call(self.sock_path, {
            "command": "start_query", "query_id": "w1",
            "client_class": "month", "query_name": "close",
            "store": None, "wait_limit": 10, "page_size": 2, "estimate": 2,
        })
        self.assertTrue(resp["ok"])
        page1 = call(self.sock_path, {"command": "fetch_page", "query_id": "w1"})
        self.assertEqual(len(page1["result"]["rows"]), 2)

        # 窗口 2 并发查看占用，无需等待窗口 1 完成。
        usage = call(self.sock_path, {"command": "usage"})
        self.assertTrue(usage["ok"])
        self.assertEqual(usage["result"]["queries"][0]["query_id"], "w1")

        # 窗口 1 取第二页并收到终止原因，且不重发旧页。
        page2 = call(self.sock_path, {"command": "fetch_page", "query_id": "w1"})
        self.assertEqual(len(page2["result"]["rows"]), 1)
        self.assertEqual(page2["result"]["terminal_reason"], "completed")
        page3 = call(self.sock_path, {"command": "fetch_page", "query_id": "w1"})
        self.assertEqual(page3["result"]["rows"], [])

    def test_protocol_returns_error_without_disconnect(self):
        # 错误请求得到结构化错误响应，连接不中断。
        resp = call(self.sock_path, {
            "command": "start_query", "query_id": "x",
            "client_class": "month", "query_name": "no_such_query",
            "store": None, "wait_limit": 1, "page_size": 1,
        })
        self.assertFalse(resp["ok"])
        self.assertIn("detail", resp)


if __name__ == "__main__":
    unittest.main()
