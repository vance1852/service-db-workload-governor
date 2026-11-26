"""追加式持久事件日志。

journal 是服务重启后“不重复发页、不串断面”的依据，记录三类事实：

- 查询生命周期：start / page_delivered / state_change / terminal；
- 资源动作：segment_write / segment_pending_delete / segment_confirmed_delete；
- 环境动作：snapshot_prune / restart。

每条记录带单调 ``seq`` 与到达逻辑时刻 ``tick``，以 JSON Lines 追加并 fsync。
重启时顺序回放即可重建每个查询的状态、已发页边界与临时区段记账。
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable


class Journal:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a+", encoding="utf-8")
        existing = self.path.read_text(encoding="utf-8").splitlines() if self.path.exists() else []
        self._seq = len(existing)

    def append(self, event_type: str, tick: int, **fields: Any) -> dict[str, Any]:
        record = {"seq": self._seq, "at": tick, "event": event_type, **fields}
        self._handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._handle.flush()
        # fsync 保证 restart 动作后这些事实仍然成立。
        import os

        os.fsync(self._handle.fileno())
        self._seq += 1
        return record

    @staticmethod
    def page_hash(rows: Iterable[Iterable[Any]]) -> str:
        """已交付页内容的指纹：重启后用它证明页没有被换断面重算。"""
        digest = hashlib.sha256()
        for row in rows:
            digest.update(json.dumps(list(row), ensure_ascii=False).encode("utf-8"))
            digest.update(b"\n")
        return digest.hexdigest()

    def read_all(self) -> list[dict[str, Any]]:
        self._handle.flush()
        return [
            json.loads(line)
            for line in self.path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    def close(self) -> None:
        self._handle.close()


def replay(path: str | Path) -> list[dict[str, Any]]:
    """离线回放工具：重启流程与测试都用它读取完整日志。"""
    p = Path(path)
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
