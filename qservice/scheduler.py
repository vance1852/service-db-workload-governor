"""资源份额与公平轮转调度。

限额（来自 catalog.json）
    work_memory_bytes: 所有查询当前留在工作内存中的编码字节总和上限；
    temporary_bytes:   临时空间上限，包含已写出但正在删除（pending）的块；
    desk_reserve_bytes: 桌面类（desk/小范围复核）可用的资源份额，保证大查询
                       占满资源时小范围复核仍能推进。

每一轮 tick 做一次公平轮转：按句柄注册顺序轮流给每个可运行查询一个时间片，
时间片的字节配额 = 当前空闲资源在可运行查询间均分，但 desk 查询保底拿到
桌面预留。查询在片内若预检发现资源不足即进入明确等待（带原因），等待年龄
逐轮累加，超过本次等待上限则以可追查理由结束；等待中从未物化的查询不持有
任何临时区段，因此不会占着别的查询能用的空间。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from .engine import (
    STATE_RUNNING,
    STATE_TERMINATED,
    STATE_WAITING,
    STATE_CANCELLED,
    STATE_DELIVERED,
    TERM_WAIT_LIMIT,
    WAIT_NONE,
    QueryExecution,
    QueryEngine,
)
from .spill import SpillManager


@dataclass
class ResourceLimits:
    work_memory_bytes: int
    temporary_bytes: int
    desk_reserve_bytes: int


class Scheduler:
    def __init__(
        self,
        engine: QueryEngine,
        spill_manager: SpillManager,
        limits: ResourceLimits,
    ):
        self.engine = engine
        self.spill = spill_manager
        self.limits = limits

    # ------------------------------------------------------------------ 状态视图
    def used_work_bytes(self, queries: dict[str, QueryExecution]) -> int:
        return sum(
            execution.work_bytes
            for execution in queries.values()
            if execution.state not in (STATE_TERMINATED, STATE_DELIVERED, STATE_CANCELLED)
        )

    def free_work_bytes(self, queries: dict[str, QueryExecution]) -> int:
        return max(0, self.limits.work_memory_bytes - self.used_work_bytes(queries))

    def used_temp_bytes(self) -> int:
        # 临时占用包含 pending-delete 块——SpillManager 已按此规则记账。
        return self.spill.live_bytes()

    def free_temp_bytes(self) -> int:
        return max(0, self.limits.temporary_bytes - self.used_temp_bytes())

    # ------------------------------------------------------------------ 一轮调度
    def tick(
        self,
        queries: dict[str, QueryExecution],
        order: list[str],
        tick_no: int,
    ) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        runnable = [
            query_id
            for query_id in order
            if (execution := queries.get(query_id)) is not None
            and execution.state in (STATE_RUNNING, STATE_WAITING)
        ]
        if not runnable:
            return events

        free_work = self.free_work_bytes(queries)
        share = max(1, len(runnable))

        for query_id in runnable:
            execution = queries[query_id]
            # 每轮基于最新占用重新计算配额：上一个查询的 spill 会立即反映。
            free_work = self.free_work_bytes(queries)
            # 临时空间水位门控：当全局空闲已低于桌面预留，只让已经持有
            # 工作区（缓冲或区段）的查询继续推进——它们能通过分批/回收打破
            # 占用；尚未物化的查询明确等待，避免多个查询同时陷入“想 spill
            # 却无空间、缓冲又占着工作内存”的循环依赖。
            free_temp = self.free_temp_bytes()
            holds_workspace = (
                execution.work_bytes > 0
                or self.spill.live_bytes(query_id) > 0
            )
            if free_temp < self.limits.desk_reserve_bytes and not holds_workspace:
                execution.state = STATE_WAITING
                execution.wait_reason = "temporary_space"
                continue
            work_quota = self._work_quota(execution, free_work, share)
            if work_quota <= 0 and execution.work_bytes == 0:
                # 连保底份额都被占住：明确等待而非超用。
                execution.state = STATE_WAITING
                execution.wait_reason = "work_memory"
                continue
            # 硬上限不得超过“自身已占 + 当前全局空闲”，保证所有查询
            # 工作内存之和永远不超过 work_memory_bytes；桌面保底只提高
            # 该查询在空闲资源中的竞争份额，不允许突破全局限额。
            work_byte_limit = min(
                max(1, work_quota + execution.work_bytes),
                execution.work_bytes + free_work,
            )
            # 临时空间天花板：先按类别切分（month 不得越过“总量-桌面预留”，
            # desk 共享桌面预留及之外的空闲），再在同类可运行查询间均分，
            # 避免先到的查询把临时区占满、饿死后到的同窗口。
            temporary_byte_limit = self._temp_ceiling(
                execution, queries, runnable
            )
            slice_events = self.engine.run_slice(
                execution,
                work_byte_limit=work_byte_limit,
                # 传天花板而非片首余量：引擎每次落盘前按实时占用动态计算，
                # 同一片内前一次 spill 立即反映，绝不超临时空间。
                temporary_byte_limit=temporary_byte_limit,
                tick=tick_no,
            )
            for event in slice_events:
                event.setdefault("query_id", query_id)
            events.extend(slice_events)

        # 等待上限不在每个内部 tick 结算（否则一次取页内的空转会耗尽额度）；
        # 由 settle_wait 在跨取页的调度边界统一结算。
        for query_id in runnable:
            execution = queries[query_id]
            if execution.state == STATE_RUNNING:
                execution.wait_reason = WAIT_NONE
                execution.wait_age = 0
        return events

    def settle_wait(
        self, queries: dict[str, QueryExecution], query_id: str
    ) -> Optional[dict[str, Any]]:
        """在一次取页边界结算等待年龄：仍在等待则 +1，超限则带理由终止。

        这样等待上限衡量的是“跨了多少个真实调度轮仍拿不到资源”，而不是
        单次取页内部的轮转次数；其他窗口在其间有机会取页并回收空间。
        """
        execution = queries.get(query_id)
        if execution is None:
            return None
        if execution.state == STATE_RUNNING:
            execution.wait_reason = WAIT_NONE
            execution.wait_age = 0
            return None
        if execution.state != STATE_WAITING:
            return None
        execution.wait_age += 1
        if execution.wait_age > execution.spec.wait_limit:
            execution.state = STATE_TERMINATED
            execution.terminal_reason = TERM_WAIT_LIMIT
            execution.terminal_detail = (
                f"等待 {execution.wait_reason} 已达 "
                f"{execution.wait_age} 轮，超过上限 {execution.spec.wait_limit}"
            )
            return {
                "event": "terminal",
                "query_id": query_id,
                "reason": TERM_WAIT_LIMIT,
                "detail": execution.terminal_detail,
            }
        return None

    def _work_quota(
        self, execution: QueryExecution, free_work: int, share: int
    ) -> int:
        fair = free_work // share
        if execution.spec.client_class == "desk":
            # 桌面保底：小范围复核始终有可用的资源份额。
            reserve = min(self.limits.desk_reserve_bytes, self.limits.work_memory_bytes)
            return max(fair, reserve - execution.work_bytes)
        return fair

    def _temp_ceiling(
        self,
        execution: QueryExecution,
        queries: dict[str, QueryExecution],
        runnable: list[str],
    ) -> int:
        """计算该查询本时间片允许达到的全局临时占用天花板。

        类别池模型（不做查询间均分，避免小天花板饿死分批）：
        - month 类的硬天花板 = 临时总量 - 桌面预留。所有 month 合起来不得
          越过这条线，因此桌面预留池始终不会被大任务占走；
        - desk 类的天花板 = 临时总量。desk 之间通过“分块落盘 + 明确等待 +
          分页即回收 run”共享空间，先到者交付后会持续腾出新空间。
        已占用部分允许保留（天花板不低于自身当前占用）。
        """
        total = self.limits.temporary_bytes
        reserve = min(self.limits.desk_reserve_bytes, total)
        if execution.spec.client_class == "desk":
            ceiling = total
        else:
            ceiling = max(0, total - reserve)
        return max(ceiling, self.spill.live_bytes(execution.spec.query_id))

    # ------------------------------------------------------------------ 推进至就绪
    def advance_until(
        self,
        queries: dict[str, QueryExecution],
        order: list[str],
        target_query: str,
        tick_start: int,
        max_rounds: Optional[int] = None,
    ) -> tuple[int, list[dict[str, Any]], Optional[str]]:
        """持续轮转直到目标查询就绪/终止，或其等待上限被耗尽。

        大查询因此会“随着分批工作持续推进”：每轮都参与公平轮转，
        而不是一次性占满临时区后让其他窗口饿死。
        返回 (新 tick 序号, 事件列表, 终止理由或 None)。
        """
        all_events: list[dict[str, Any]] = []
        tick_no = tick_start
        rounds = 0
        while True:
            execution = queries.get(target_query)
            if execution is None:
                return tick_no, all_events, "unknown_query"
            if execution.state in (STATE_TERMINATED,):
                return tick_no, all_events, execution.terminal_reason
            if execution.state == STATE_CANCELLED:
                return tick_no, all_events, "cancelled"
            if execution.result_ready:
                return tick_no, all_events, None
            tick_no += 1
            rounds += 1
            before_proof = (execution.last_sale_id, len(execution.segments),
                            execution.merge_index)
            all_events.extend(self.tick(queries, order, tick_no))
            if max_rounds is not None and rounds >= max_rounds:
                return tick_no, all_events, None
            execution = queries[target_query]
            if execution.state == STATE_TERMINATED:
                return tick_no, all_events, execution.terminal_reason
            # 目标查询在明确等待且本轮证实位置没有推进时，立即把控制权
            # 交回调用方：资源往往要靠其他窗口取页/回收才能释放，继续
            # 原地空转只会白白耗尽它的等待上限。
            if (
                execution.state == STATE_WAITING
                and (execution.last_sale_id, len(execution.segments),
                     execution.merge_index) == before_proof
            ):
                return tick_no, all_events, "waiting_for_resources"
