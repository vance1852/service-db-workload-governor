"""按 timeline.json 回放资料、产出可核对的执行轨迹。

驱动把仓库给出的到达逻辑时刻逐条翻译成服务调用，并在每个动作后采集
:meth:`QueryService.usage` 快照。start 的 args 同时兼容六参形式
``[句柄, 客户端类, 查询名, 门店, 等待上限, 页大小]`` 与七参形式
（末尾再带估计行数）。

外部观察类动作（observed_expansion、temporary_delete_*）既核对服务内部
记账，也作为环境事件登记，轨迹中保留“期望 vs 实际”的核对结果。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .service import QueryService


class TimelineDriver:
    def __init__(self, service: QueryService):
        self.service = service
        self.trace: list[dict[str, Any]] = []
        self.pages: dict[str, list[list[Any]]] = {}

    def run(self, timeline: list[dict[str, Any]]) -> dict[str, Any]:
        for record in timeline:
            at = record["at"]
            action = record["action"]
            args = record["args"]
            entry: dict[str, Any] = {"at": at, "action": action, "args": args}
            handler = getattr(self, f"_on_{action}", None)
            if handler is None:
                entry["note"] = f"未识别动作: {action}"
            else:
                entry["result"] = handler(args)
            entry["usage"] = self.service.usage()
            self.trace.append(entry)
        return {"trace": self.trace, "pages": self.pages}

    # ------------------------------------------------------------------ 动作
    def _on_start(self, args: list[Any]) -> dict[str, Any]:
        query_id, client_class, query_name, store, wait_limit, page_size = args[:6]
        estimate = args[6] if len(args) > 6 else None
        status = self.service.start_query(
            query_id=query_id,
            client_class=client_class,
            query_name=query_name,
            store=store,
            wait_limit=wait_limit,
            page_size=page_size,
            estimate=estimate,
        )
        return {"status": status}

    def _on_page(self, args: list[Any]) -> dict[str, Any]:
        query_id = args[0]
        response = self.service.fetch_page(query_id)
        if response["rows"]:
            self.pages.setdefault(query_id, []).extend(response["rows"])
        return {
            "page_no": response["page_no"],
            "rows": response["rows"],
            "state": response["state"],
            "terminal_reason": response["terminal_reason"],
            "blocked": response["blocked"],
            "wait_reason": response["wait_reason"],
        }

    def _on_pause(self, args: list[Any]) -> dict[str, Any]:
        return self.service.pause(args[0])

    def _on_resume(self, args: list[Any]) -> dict[str, Any]:
        status = self.service.resume(args[0])
        # 恢复意味着该窗口继续取结果：续取剩余页，直到再次暂停/终止，
        # 验证“从证实位置继续、不重复、不串断面”。
        drained: list[list[Any]] = []
        query_id = args[0]
        while True:
            current = self.service.status(query_id)
            if current["state"] in (
                "paused", "terminated", "cancelled", "delivered"
            ):
                break
            response = self.service.fetch_page(query_id)
            if response["rows"]:
                drained.extend(response["rows"])
                self.pages.setdefault(query_id, []).extend(response["rows"])
            if response["terminal_reason"] or not response["rows"]:
                break
        return {"status": status, "resumed_pages": drained}

    def _on_cancel(self, args: list[Any]) -> dict[str, Any]:
        return self.service.cancel(args[0])

    def _on_commit(self, args: list[Any]) -> dict[str, Any]:
        _version, table, row = args
        return self.service.commit(table, [row])

    def _on_restart(self, args: list[Any]) -> dict[str, Any]:
        return self.service.restart_worker()

    def _on_temporary_delete_pending(self, args: list[Any]) -> dict[str, Any]:
        query_id, seg_id, bytes_ = args
        return self.service.temporary_delete_pending(query_id, seg_id, bytes_)

    def _on_temporary_delete_confirmed(self, args: list[Any]) -> dict[str, Any]:
        query_id, seg_id, bytes_ = args
        return self.service.temporary_delete_confirmed(query_id, seg_id, bytes_)

    def _on_observed_expansion(self, args: list[Any]) -> dict[str, Any]:
        query_id, store, family = args
        status = self.service.status(query_id)
        label = f"{store}/{family}"
        observed = next(
            (item for item in status["expansion"] if item["group"] == label),
            None,
        )
        return {
            "expected_group": label,
            "engine_expansion": observed,
            "reconciled": observed is not None,
        }


def load_timeline(fixture_dir: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    catalog = json.loads((fixture_dir / "catalog.json").read_bytes())
    timeline = json.loads((fixture_dir / "timeline.json").read_bytes())
    return catalog, timeline
