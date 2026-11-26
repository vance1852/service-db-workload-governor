"""两类目标查询的分批执行机（按 target_sql 契约驱动）。

desk（单店明细）
    断面内按 ``sale_id`` 顺序分批扫 sale，对每行用 LEFT JOIN 语义展开
    adjustment：无更正仍产出 NULL 行，有 n 条更正产出 n 行（重复事实保留）。
    排序与投影按契约 ORDER BY / SELECT 列执行。

close（分组聚合）
    与目标 SQL 同语义的 GROUP BY + COUNT(*) + SUM(amount)。在严格工作内存
    限额下采用外部聚合：扫描期把 JOIN 事实行按批 spill 成区段；合并期逐
    区段做局部聚合并同样落盘，最后归并所有局部聚合，内存中同时只保留一个
    分组键的合并状态。SUM 遵循 SQLite 语义（忽略非 NULL；全 NULL 得 NULL，
    ASC 排序时 NULL 最前），并列顺序由契约写明的附加列决定。

所有阶段的断点都是可证实位置：desk/close 扫描用最后一个 ``sale_id``，
合并用已完成区段列表，分页用已交付行数。状态写入 journal 后才对外发页，
因此重启后页既不重复也不会接到别的断面。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from .encoding import encode_lines, row_bytes
from .spill import SpillManager
from .sql_contract import SqlContract
from .storage import SnapshotPruned, Storage

# --------------------------------------------------------------------- 常量枚举
PHASE_DESK_RUN = "desk_scan"
PHASE_CLOSE_SCAN = "close_scan"
PHASE_CLOSE_MERGE = "close_merge"
PHASE_CLOSE_SORT = "close_sort"
PHASE_READY = "ready"

STATE_RUNNING = "running"
STATE_WAITING = "waiting"
STATE_PAUSED = "paused"
STATE_CANCELLED = "cancelled"
STATE_TERMINATED = "terminated"
STATE_DELIVERED = "delivered"

TERM_COMPLETED = "completed"
TERM_CANCELLED = "cancelled"
TERM_WAIT_LIMIT = "wait_limit_exceeded"
TERM_SNAPSHOT_PRUNED = "snapshot_pruned"
TERM_TEMP_LOST = "temporary_data_invalid"
TERM_GROUP_EXPANSION = "group_expansion"

WAIT_WORK_MEMORY = "work_memory"
WAIT_TEMP_SPACE = "temporary_space"
WAIT_NONE = ""

EXPANSION_FACTOR = 1.5

# 单个调度时间片读取的 sale 行数上限。工作内存约束不由此值决定——循环内
# 按精确编码字节收口，超限即 spill 后继续；较大的行级批量保证一个时间片
# 能积累足够事实，及时触发“某组远大于估计”的扩张观察。
SCAN_BATCH_ROWS = 256

# 事实行中 amount 列的固定位置（末尾）；分组键在行首，宽度等于分组列数。
FACT_AMOUNT = -1

# 事实行中 amount 列的固定位置（末尾）；分组键在行首，宽度等于分组列数。
FACT_AMOUNT = -1


@dataclass
class QuerySpec:
    query_id: str
    client_class: str
    query_name: str
    store: Optional[str]
    wait_limit: int
    page_size: int
    estimate: Optional[int]
    snapshot_version: int
    contract: SqlContract


@dataclass
class QueryExecution:
    spec: QuerySpec
    phase: str = PHASE_DESK_RUN
    state: str = STATE_RUNNING
    wait_reason: str = WAIT_NONE
    wait_age: int = 0
    last_sale_id: Optional[str] = None
    buffered_rows: list[list[Any]] = field(default_factory=list)
    work_bytes: int = 0
    segments: list[str] = field(default_factory=list)
    merge_partials: list[str] = field(default_factory=list)
    merge_index: int = 0
    result_rows: list[list[Any]] = field(default_factory=list)
    result_ready: bool = False
    # desk 流式归并：内存缓冲 + 若干有序溢出院，分页时 k 路归并按需产出。
    desk_runs: list[str] = field(default_factory=list)
    desk_buffer_sorted: bool = False
    desk_cursor: int = 0
    desk_run_heap: Optional[list[Any]] = field(default=None, repr=False)
    desk_route_iterators: dict[int, Any] = field(default_factory=dict, repr=False)
    desk_total_rows: int = 0
    # desk 分页期回收 run 产生的待记账事件，由 service 落 journal。
    pending_reclaim_events: list[dict[str, Any]] = field(
        default_factory=list, repr=False
    )
    pages_delivered: int = 0
    rows_delivered: int = 0
    delivered_hashes: list[str] = field(default_factory=list)
    terminal_reason: Optional[str] = None
    terminal_detail: Optional[str] = None
    group_counts: dict[tuple[Any, ...], int] = field(default_factory=dict)
    non_null_groups: set[tuple[Any, ...]] = field(default_factory=set)
    expansion_seen: list[dict[str, Any]] = field(default_factory=list)
    seg_counter: int = 0

    # ------------------------------------------------------------- 持久化
    def to_journal(self) -> dict[str, Any]:
        return {
            "query_id": self.spec.query_id,
            "client_class": self.spec.client_class,
            "query_name": self.spec.query_name,
            "store": self.spec.store,
            "wait_limit": self.spec.wait_limit,
            "page_size": self.spec.page_size,
            "estimate": self.spec.estimate,
            "snapshot_version": self.spec.snapshot_version,
            "contract_sql": self.spec.contract.raw,
            "phase": self.phase,
            "state": self.state,
            "wait_reason": self.wait_reason,
            "wait_age": self.wait_age,
            "last_sale_id": self.last_sale_id,
            "work_bytes": self.work_bytes,
            "buffered_rows": list(self.buffered_rows),
            "result_rows": list(self.result_rows),
            "segments": list(self.segments),
            "merge_partials": list(self.merge_partials),
            "merge_index": self.merge_index,
            "result_ready": self.result_ready,
            "desk_runs": list(self.desk_runs),
            "desk_buffer_sorted": self.desk_buffer_sorted,
            "desk_cursor": self.desk_cursor,
            "desk_total_rows": self.desk_total_rows,
            "pages_delivered": self.pages_delivered,
            "rows_delivered": self.rows_delivered,
            "delivered_hashes": list(self.delivered_hashes),
            "terminal_reason": self.terminal_reason,
            "terminal_detail": self.terminal_detail,
            "group_counts": {self._group_key_to_str(key): value for key, value in self.group_counts.items()},
            "non_null_groups": [self._group_key_to_str(key) for key in self.non_null_groups],
            "expansion_seen": list(self.expansion_seen),
            "seg_counter": self.seg_counter,
        }

    @staticmethod
    def _group_key_to_str(key: tuple[Any, ...]) -> str:
        return "\x1f".join("" if part is None else str(part) for part in key)

    @staticmethod
    def _group_key_from_str(text: str) -> tuple[Any, ...]:
        return tuple(None if part == "" else part for part in text.split("\x1f"))

    @classmethod
    def from_journal(cls, data: dict[str, Any], contract: SqlContract) -> "QueryExecution":
        spec = QuerySpec(
            query_id=data["query_id"],
            client_class=data["client_class"],
            query_name=data["query_name"],
            store=data["store"],
            wait_limit=data["wait_limit"],
            page_size=data["page_size"],
            estimate=data["estimate"],
            snapshot_version=data["snapshot_version"],
            contract=contract,
        )
        execution = cls(spec=spec)
        execution.phase = data["phase"]
        execution.state = data["state"]
        execution.wait_reason = data.get("wait_reason", WAIT_NONE)
        execution.wait_age = data.get("wait_age", 0)
        execution.last_sale_id = data.get("last_sale_id")
        execution.work_bytes = data.get("work_bytes", 0)
        execution.buffered_rows = [list(row) for row in data.get("buffered_rows", [])]
        execution.result_rows = [list(row) for row in data.get("result_rows", [])]
        execution.segments = list(data.get("segments", []))
        execution.merge_partials = list(data.get("merge_partials", []))
        execution.merge_index = data.get("merge_index", 0)
        execution.result_ready = data.get("result_ready", False)
        execution.desk_runs = list(data.get("desk_runs", []))
        execution.desk_buffer_sorted = data.get("desk_buffer_sorted", False)
        execution.desk_cursor = data.get("desk_cursor", 0)
        execution.desk_total_rows = data.get("desk_total_rows", 0)
        # 堆与迭代器不持久化：重启后惰性重建，已交付行数保证不重复发页。
        execution.desk_run_heap = None
        execution.desk_route_iterators = {}
        execution.pages_delivered = data.get("pages_delivered", 0)
        execution.rows_delivered = data.get("rows_delivered", 0)
        execution.delivered_hashes = list(data.get("delivered_hashes", []))
        execution.terminal_reason = data.get("terminal_reason")
        execution.terminal_detail = data.get("terminal_detail")
        execution.group_counts = {
            cls._group_key_from_str(key): value
            for key, value in data.get("group_counts", {}).items()
        }
        execution.non_null_groups = {
            cls._group_key_from_str(key) for key in data.get("non_null_groups", [])
        }
        execution.expansion_seen = list(data.get("expansion_seen", []))
        execution.seg_counter = data.get("seg_counter", 0)
        return execution


class QueryEngine:
    def __init__(self, storage: Storage, spill_manager: SpillManager):
        self.storage = storage
        self.spill = spill_manager

    # ============================================================== 执行入口
    def run_slice(
        self,
        execution: QueryExecution,
        work_byte_limit: int,
        temporary_byte_limit: int,
        tick: int,
    ) -> list[dict[str, Any]]:
        if execution.state in (STATE_TERMINATED, STATE_DELIVERED, STATE_CANCELLED):
            return []
        events: list[dict[str, Any]] = []
        try:
            if execution.spec.contract.kind == "desk":
                self._run_desk(execution, work_byte_limit, temporary_byte_limit, tick, events)
            else:
                self._run_close(execution, work_byte_limit, temporary_byte_limit, tick, events)
        except SnapshotPruned as exc:
            self._terminate(execution, TERM_SNAPSHOT_PRUNED, str(exc))
            events.append({"event": "terminal", "reason": TERM_SNAPSHOT_PRUNED, "detail": str(exc)})
        except FileNotFoundError as exc:
            self._terminate(execution, TERM_TEMP_LOST, str(exc))
            events.append({"event": "terminal", "reason": TERM_TEMP_LOST, "detail": str(exc)})
        return events

    def _free_temp(
        self, execution: QueryExecution, temporary_ceiling: int
    ) -> int:
        """该查询在其类别临时天花板下还能落盘多少字节。

        ``temporary_ceiling`` 是该查询被允许看到的全局临时占用天花板：
        month 查询的天花板已扣除桌面预留，desk 查询为全量。余量按全局
        实时占用计算，因此同一片内每次 spill 都立即反映，绝不超总量；
        只要 month 不越过自己的天花板，desk 始终至少有预留量可用。
        """
        return max(0, temporary_ceiling - self.spill.live_bytes())

    # ============================================================== 共用 JOIN
    def _left_join_facts(
        self,
        sale,
        adj_map: dict[str, list[tuple[str, Optional[int]]]],
        contract: SqlContract,
    ) -> list[dict[str, Any]]:
        """LEFT JOIN 展开成事实字典列表。

        无更正保留一条全 NULL 更正事实（退款为空的明细不丢）；有 n 条
        更正产出 n 条，连接产生的重复事实全部保留。
        """
        corrections = adj_map.get(sale["sale_id"])
        sale_values = {
            "s.sale_id": sale["sale_id"],
            "s.store": sale["store"],
            "s.family": sale["family"],
            "s.amount": sale["amount"],
            "s.incident": sale["incident"],
        }
        if not corrections:
            facts = [dict(sale_values, **{"a.ref": None, "a.sale_id": None, "a.amount": None})]
        else:
            facts = [
                dict(sale_values, **{"a.ref": ref, "a.sale_id": sale["sale_id"], "a.amount": amount})
                for ref, amount in corrections
            ]
        return facts

    # ============================================================== desk 明细
    def _run_desk(
        self,
        execution: QueryExecution,
        work_byte_limit: int,
        temporary_byte_limit: int,
        tick: int,
        events: list[dict[str, Any]],
    ) -> None:
        spec = execution.spec
        if execution.phase == PHASE_READY:
            execution.state = STATE_RUNNING
            execution.wait_reason = WAIT_NONE
            return

        adj_map = self.storage.adjustment_map(spec.snapshot_version)
        row_limit = SCAN_BATCH_ROWS
        sales = self.storage.scan_sale(
            spec.snapshot_version,
            limit=row_limit,
            after_sale_id=execution.last_sale_id,
            store=spec.store,
        )
        if not sales:
            self._finalize_desk(execution)
            return

        made_progress = False
        for sale in sales:
            facts = self._left_join_facts(sale, adj_map, spec.contract)
            projected = [self._project(fact, spec.contract) for fact in facts]
            added_bytes = sum(row_bytes(row) for row in projected)
            if execution.work_bytes + added_bytes > work_byte_limit and made_progress:
                break
            if execution.work_bytes + added_bytes > work_byte_limit:
                if execution.buffered_rows:
                    spilled = self._spill_buffer(
                        execution, tick, events, temporary_byte_limit
                    )
                    if not spilled:
                        # 临时空间满、无法 spill：明确等待（保留缓冲与证实
                        # 位置）。其他窗口取页回收 run 后这里即可继续；若等待
                        # 超过本查询上限，则由调度器带可追查理由结束。
                        self._block(execution, WAIT_TEMP_SPACE)
                        return
                    # 部分落盘后缓冲可能仍有残留（剩余临时空间不足），
                    # 继续尝试把当前 sale 放入腾出的工作内存；放不下则
                    # 本分片到此为止，下轮继续，保证持续小步推进。
                    if execution.work_bytes + added_bytes > work_byte_limit:
                        break
                else:
                    self._block(execution, WAIT_WORK_MEMORY)
                    return
            execution.buffered_rows.extend(projected)
            execution.work_bytes += added_bytes
            execution.last_sale_id = sale["sale_id"]
            made_progress = True
        if made_progress:
            self._unblock(execution)

    def _finalize_desk(self, execution: QueryExecution) -> None:
        # 缓冲与各溢出院都是按契约 ORDER BY 排好的有序 run。最终结果不
        # 一次性读回（那会突破工作内存），而是登记 run 列表，由分页通过
        # 流式 k 路归并按需产出，内存中同时只保留每路一行。
        if execution.buffered_rows:
            execution.buffered_rows = self._apply_order(
                execution.buffered_rows, execution.spec.contract, desk_mode=True
            )
        execution.desk_runs = list(execution.segments)
        execution.desk_buffer_sorted = True
        execution.result_ready = True
        execution.phase = PHASE_READY
        self._unblock(execution)

    # ============================================================== close 聚合
    def _run_close(
        self,
        execution: QueryExecution,
        work_byte_limit: int,
        temporary_byte_limit: int,
        tick: int,
        events: list[dict[str, Any]],
    ) -> None:
        if execution.phase == PHASE_DESK_RUN:
            execution.phase = PHASE_CLOSE_SCAN
        if execution.phase == PHASE_CLOSE_SCAN:
            self._close_scan(execution, work_byte_limit, temporary_byte_limit, tick, events)
        elif execution.phase == PHASE_CLOSE_MERGE:
            self._close_merge(execution, work_byte_limit, temporary_byte_limit, tick, events)
        elif execution.phase == PHASE_CLOSE_SORT:
            self._close_finalize(execution, work_byte_limit, events)
        elif execution.phase == PHASE_READY:
            execution.state = STATE_RUNNING
            execution.wait_reason = WAIT_NONE

    def _compact_scan_segments(
        self,
        execution: QueryExecution,
        tick: int,
        events: list[dict[str, Any]],
        temporary_byte_limit: int,
    ) -> bool:
        """把当前全部 JOIN 事实区段（含缓冲）压缩成一个局部聚合并回收。

        分组数远小于事实行数，压缩后临时占用大幅下降，使大查询在严格
        临时空间内也能持续推进。源区段统一走 pending -> confirmed 回收。
        返回 False 表示连压缩后的局部聚合都放不下（应由等待上限结束）。
        """
        contract = execution.spec.contract
        group_width = len(contract.group_columns)
        aggregated: dict[tuple[Any, ...], list[Any]] = {}
        rows = list(execution.buffered_rows)
        for seg_id in execution.segments:
            rows.extend(self.spill.read(execution.spec.query_id, seg_id))
        for row in rows:
            key = tuple(row[:group_width])
            amount = row[FACT_AMOUNT]
            acc = aggregated.get(key)
            if acc is None:
                acc = [*key, 0, 0]
                aggregated[key] = acc
            acc[-2] += 1
            if amount is not None:
                acc[-1] += amount

        # 先回收源区段释放空间，再写局部聚合，顺序保证不会超临时限额。
        reclaimed: list[tuple[str, int]] = []
        for seg_id in list(execution.segments):
            segment = self.spill.get(execution.spec.query_id, seg_id)
            if segment is None or segment.confirmed_delete:
                continue
            reclaimed.append((seg_id, segment.bytes_))
            self.spill.consume(execution.spec.query_id, seg_id)
        for seg_id, original_bytes in reclaimed:
            events.append({"event": "segment_pending_delete", "seg_id": seg_id,
                           "bytes": original_bytes})
            events.append({"event": "segment_confirmed_delete", "seg_id": seg_id,
                           "bytes": original_bytes})
        execution.segments = []
        execution.merge_index = 0
        execution.buffered_rows = []
        execution.work_bytes = 0

        partial_id = self._write_partial(
            execution, list(aggregated.values()), tick, events, temporary_byte_limit
        )
        return partial_id is not None

    def _close_scan(
        self,
        execution: QueryExecution,
        work_byte_limit: int,
        temporary_byte_limit: int,
        tick: int,
        events: list[dict[str, Any]],
    ) -> None:
        spec = execution.spec
        contract = spec.contract
        adj_map = self.storage.adjustment_map(spec.snapshot_version)
        row_limit = SCAN_BATCH_ROWS
        sales = self.storage.scan_sale(
            spec.snapshot_version, limit=row_limit,
            after_sale_id=execution.last_sale_id,
        )
        if not sales and not execution.buffered_rows and not execution.segments:
            execution.phase = PHASE_CLOSE_MERGE
            execution.merge_index = 0
            return

        made_progress = False
        for sale in sales:
            facts = self._left_join_facts(sale, adj_map, contract)
            # close 事实行 = 分组键值前缀 + [sale_id, ref, amount]，落盘自洽。
            fact_rows = [
                [fact[column] for column in contract.group_columns]
                + [fact["s.sale_id"], fact["a.ref"], fact["a.amount"]]
                for fact in facts
            ]
            added_bytes = sum(row_bytes(row) for row in fact_rows)
            key = tuple(fact_rows[0][i] for i in range(len(contract.group_columns)))
            execution.group_counts[key] = execution.group_counts.get(key, 0) + len(fact_rows)
            if any(fact["a.amount"] is not None for fact in facts):
                execution.non_null_groups.add(key)
            self._note_expansion(execution, key, tick, events)

            if execution.work_bytes + added_bytes > work_byte_limit:
                if not self._spill_buffer(execution, tick, events, temporary_byte_limit):
                    # 临时空间不足：压缩已扫描事实，腾出空间后再试一次。
                    if not self._compact_scan_segments(
                        execution, tick, events, temporary_byte_limit
                    ):
                        self._block(execution, WAIT_TEMP_SPACE)
                        return
                    if not self._spill_buffer(execution, tick, events, temporary_byte_limit):
                        self._block(execution, WAIT_TEMP_SPACE)
                        return
            execution.buffered_rows.extend(fact_rows)
            execution.work_bytes += added_bytes
            execution.last_sale_id = sale["sale_id"]
            made_progress = True

        if not sales and execution.buffered_rows:
            if not self._spill_buffer(execution, tick, events, temporary_byte_limit):
                if not self._compact_scan_segments(
                    execution, tick, events, temporary_byte_limit
                ):
                    self._block(execution, WAIT_TEMP_SPACE)
                    return
            execution.phase = PHASE_CLOSE_MERGE
            execution.merge_index = 0
            return
        if made_progress:
            self._unblock(execution)

    def _note_expansion(
        self,
        execution: QueryExecution,
        key: tuple[Any, ...],
        tick: int,
        events: list[dict[str, Any]],
    ) -> None:
        spec = execution.spec
        # 缺省估计时以断面上的基表行数为估计基线，保证“某组远大于估计”
        # 的判断在任何 start 参数下都生效；显式 estimate 优先。
        if spec.estimate is not None:
            estimate_base = spec.estimate
        else:
            estimate_base = self.storage.counts(spec.snapshot_version)["sale"]
        group_share = max(
            1, estimate_base // max(1, len(execution.group_counts) or 1)
        )
        observed = execution.group_counts[key]
        label = "/".join("" if part is None else str(part) for part in key)
        already = {item["group"] for item in execution.expansion_seen}
        if observed > group_share * EXPANSION_FACTOR and label not in already:
            record = {
                "event": "group_expansion",
                "group": label,
                "observed_join_rows": observed,
                "estimated_share": group_share,
                "at": tick,
            }
            execution.expansion_seen.append(record)
            events.append(record)

    def _close_merge(
        self,
        execution: QueryExecution,
        work_byte_limit: int,
        temporary_byte_limit: int,
        tick: int,
        events: list[dict[str, Any]],
    ) -> None:
        if execution.merge_index >= len(execution.segments):
            execution.phase = PHASE_CLOSE_SORT
            return

        contract = execution.spec.contract
        group_width = len(contract.group_columns)
        seg_id = execution.segments[execution.merge_index]
        rows = self.spill.read(execution.spec.query_id, seg_id)
        partial: dict[tuple[Any, ...], list[Any]] = {}
        partial_bytes = 0
        for row in rows:
            key = tuple(row[:group_width])
            amount = row[FACT_AMOUNT]
            acc = partial.get(key)
            if acc is None:
                acc = [*key, 0, 0]   # 分组键 + COUNT(*) + 非NULL金额累加
                partial[key] = acc
            acc[-2] += 1
            if amount is not None:
                acc[-1] += amount
            partial_bytes += row_bytes(acc)
            if partial_bytes > work_byte_limit:
                if self._write_partial(execution, list(partial.values()), tick,
                                       events, temporary_byte_limit) is None:
                    self._block(execution, WAIT_TEMP_SPACE)
                    return
                partial = {}
                partial_bytes = 0

        if partial:
            if self._write_partial(execution, list(partial.values()), tick,
                                   events, temporary_byte_limit) is None:
                self._block(execution, WAIT_TEMP_SPACE)
                return
        # 该区段已被完整消费：走 pending -> confirmed 两阶段回收，
        # 让大查询在合并过程中持续腾出临时空间给其他查询。
        original_bytes = self.spill.get(execution.spec.query_id, seg_id).bytes_
        self.spill.consume(execution.spec.query_id, seg_id)
        events.append({
            "event": "segment_pending_delete",
            "seg_id": seg_id,
            "bytes": original_bytes,
        })
        events.append({
            "event": "segment_confirmed_delete",
            "seg_id": seg_id,
            "bytes": original_bytes,
        })
        execution.merge_index += 1
        self._unblock(execution)
        if execution.merge_index >= len(execution.segments):
            execution.phase = PHASE_CLOSE_SORT

    def _close_finalize(
        self,
        execution: QueryExecution,
        work_byte_limit: int,
        events: list[dict[str, Any]],
    ) -> None:
        contract = execution.spec.contract
        group_width = len(contract.group_columns)
        merged: dict[tuple[Any, ...], list[Any]] = {}
        for seg_id in execution.merge_partials:
            for row in self.spill.read(execution.spec.query_id, seg_id):
                key = tuple(row[:group_width])
                acc = merged.get(key)
                if acc is None:
                    acc = [*key, 0, 0]
                    merged[key] = acc
                acc[-2] += row[-2]
                acc[-1] += row[-1]

        result: list[list[Any]] = []
        for key, acc in merged.items():
            count, total = acc[-2], acc[-1]
            total_value = total if key in execution.non_null_groups else None
            group_values = list(key)
            result.append(self._project_aggregate(contract, group_values, count, total_value))

        result = self._apply_order(result, contract, desk_mode=False)
        if sum(row_bytes(row) for row in result) > work_byte_limit:
            self._block(execution, WAIT_WORK_MEMORY)
            return
        # 所有局部聚合都已归并：回收 partial 区段（两阶段并留痕）。
        for seg_id in list(execution.merge_partials):
            segment = self.spill.get(execution.spec.query_id, seg_id)
            if segment is None or segment.confirmed_delete:
                continue
            original_bytes = segment.bytes_
            self.spill.consume(execution.spec.query_id, seg_id)
            events.append({
                "event": "segment_pending_delete",
                "seg_id": seg_id,
                "bytes": original_bytes,
            })
            events.append({
                "event": "segment_confirmed_delete",
                "seg_id": seg_id,
                "bytes": original_bytes,
            })
        execution.result_rows = result
        execution.buffered_rows = []
        execution.work_bytes = sum(row_bytes(row) for row in result)
        execution.result_ready = True
        execution.phase = PHASE_READY
        self._unblock(execution)

    # ============================================================== 投影与排序
    @staticmethod
    def _project(fact: dict[str, Any], contract: SqlContract) -> list[Any]:
        output: list[Any] = []
        for projection in contract.projections:
            if projection.kind == "column":
                output.append(fact[projection.alias_column])
            else:
                raise ValueError("desk 契约不应出现聚合投影")
        return output

    @staticmethod
    def _project_aggregate(
        contract: SqlContract,
        group_values: list[Any],
        count: int,
        sum_value: Optional[int],
    ) -> list[Any]:
        group_lookup = dict(zip(contract.group_columns, group_values))
        output: list[Any] = []
        for projection in contract.projections:
            if projection.kind == "column":
                output.append(group_lookup[projection.alias_column])
            elif projection.kind == "count_star":
                output.append(count)
            elif projection.kind == "sum":
                output.append(sum_value)
        return output

    def _apply_order(
        self, rows: list[list[Any]], contract: SqlContract, desk_mode: bool
    ) -> list[list[Any]]:
        if not contract.order_terms:
            return rows
        # 建立排序表达式 -> 行内取值函数。聚合表达式映射到对应投影位置；
        # 列表达式直接按限定名取值（desk 用事实字典重建，close 用分组值）。
        proj_index = {
            projection.expr_text.upper(): index
            for index, projection in enumerate(contract.projections)
        }

        def sort_key(row: list[Any]):
            key_parts: list[Any] = []
            for term in contract.order_terms:
                upper = term.upper()
                if upper in proj_index:
                    value = row[proj_index[upper]]
                else:
                    # ORDER BY 引用了未投影的列（如 desk 的 s.sale_id, a.ref）：
                    # desk 行投影里一定含 sale_id 契约列；此处用列名兜底匹配。
                    column = term.split(".")[-1]
                    value = self._find_column(row, contract, column)
                # ASC：NULL 排最前（(is_not_none, value)）。
                key_parts.append((value is not None, value))
            return tuple(key_parts)

        return sorted(rows, key=sort_key)

    @staticmethod
    def _find_column(row: list[Any], contract: SqlContract, column: str) -> Any:
        for index, projection in enumerate(contract.projections):
            if projection.kind == "column" and projection.alias_column.endswith(f".{column}"):
                return row[index]
        raise KeyError(f"排序项在投影中找不到列: {column}")

    # ============================================================== 分页
    def next_page(self, execution: QueryExecution) -> Optional[list[list[Any]]]:
        if not execution.result_ready:
            return None
        if execution.spec.contract.kind == "desk":
            return self._next_desk_page(execution)
        rows = execution.result_rows
        start = execution.rows_delivered
        if start >= len(rows):
            return []
        return rows[start:start + execution.spec.page_size]

    def _desk_sort_key(self, contract: SqlContract):
        proj_index = {
            projection.expr_text.upper(): index
            for index, projection in enumerate(contract.projections)
        }

        def key_function(row: list[Any]):
            parts: list[Any] = []
            for term in contract.order_terms:
                upper = term.upper()
                if upper in proj_index:
                    value = row[proj_index[upper]]
                else:
                    column = term.split(".")[-1]
                    value = self._find_column(row, contract, column)
                parts.append((value is not None, value))
            return tuple(parts)

        return key_function

    def _next_desk_page(self, execution: QueryExecution) -> list[list[Any]]:
        """desk 流式分页：缓冲 run 与溢出院统一 k 路归并。

        溢出院逐行读取（内存中每路只持一行）；某路全部交付后立即两阶段
        回收该区段，使 desk 查询在分页过程中持续释放临时空间，不挤占其他
        窗口。已交付页由 service 层 journal 指纹背书，回收不影响续取。
        """
        if execution.desk_total_rows and execution.rows_delivered >= execution.desk_total_rows:
            return []
        contract = execution.spec.contract
        key_function = self._desk_sort_key(contract)
        if execution.desk_run_heap is None:
            self._init_desk_heap(execution, key_function)
            # 重启后续取：堆是惰性重建的，需要先跳过重启前已交付的行，
            # 已交付页由 journal 指纹背书，这里只移动证实位置、绝不重发。
            for _ in range(execution.rows_delivered):
                if not execution.desk_run_heap:
                    break
                import heapq

                _key, route, _row = heapq.heappop(execution.desk_run_heap)
                self._advance_desk_route(execution, route, key_function)
        page: list[list[Any]] = []
        import heapq

        while len(page) < execution.spec.page_size:
            if not execution.desk_run_heap:
                break
            _key, route, row = heapq.heappop(execution.desk_run_heap)
            page.append(row)
            self._advance_desk_route(execution, route, key_function)
        return page

    def _init_desk_heap(self, execution: QueryExecution, key_function) -> None:
        import heapq

        heap: list[tuple[Any, int, list[Any]]] = []
        # route 0 是内存缓冲 run（已排序）；其后是各溢出院，逐行流式读取。
        if execution.buffered_rows:
            execution.desk_cursor = 0
            first = execution.buffered_rows[0]
            heapq.heappush(heap, (key_function(first), 0, first))
        for route, seg_id in enumerate(execution.desk_runs, start=1):
            iterator = self.spill.iter_rows(execution.spec.query_id, seg_id)
            try:
                first = next(iterator)
            except StopIteration:
                continue
            heapq.heappush(heap, (key_function(first), route, first))
            execution.desk_route_iterators[route] = iterator
        execution.desk_run_heap = heap
        # 总行数用于终止判定：缓冲 + 各溢出院。
        total = len(execution.buffered_rows)
        for seg_id in execution.desk_runs:
            segment = self.spill.get(execution.spec.query_id, seg_id)
            total += 0 if segment is None else segment.rows
        execution.desk_total_rows = total

    def _advance_desk_route(self, execution: QueryExecution, route: int, key_function) -> None:
        import heapq

        if route == 0:
            execution.desk_cursor += 1
            buffer_rows = execution.buffered_rows
            if execution.desk_cursor < len(buffer_rows):
                row = buffer_rows[execution.desk_cursor]
                heapq.heappush(execution.desk_run_heap, (key_function(row), 0, row))
            return
        iterator = execution.desk_route_iterators.get(route)
        if iterator is None:
            return
        try:
            row = next(iterator)
        except StopIteration:
            # 该溢出院已全部交付：立即回收区段，腾出临时空间给其他窗口。
            self._reclaim_desk_run(execution, route)
            return
        heapq.heappush(execution.desk_run_heap, (key_function(row), route, row))

    def _reclaim_desk_run(self, execution: QueryExecution, route: int) -> None:
        if route < 1 or route > len(execution.desk_runs):
            return
        seg_id = execution.desk_runs[route - 1]
        segment = self.spill.get(execution.spec.query_id, seg_id)
        if segment is None or segment.confirmed_delete:
            return
        original_bytes = segment.bytes_
        self.spill.consume(execution.spec.query_id, seg_id)
        # 回收事件挂到执行状态上，由 service 在取页后统一写入 journal。
        execution.pending_reclaim_events.extend([
            {"event": "segment_pending_delete", "seg_id": seg_id, "bytes": original_bytes},
            {"event": "segment_confirmed_delete", "seg_id": seg_id, "bytes": original_bytes},
        ])

    def result_total_rows(self, execution: QueryExecution) -> int:
        if execution.spec.contract.kind == "desk":
            if execution.desk_total_rows == 0 and execution.result_ready:
                # 尚未初始化堆时的兜底计数。
                total = len(execution.buffered_rows)
                for seg_id in execution.desk_runs:
                    segment = self.spill.get(execution.spec.query_id, seg_id)
                    total += 0 if segment is None else segment.rows
                execution.desk_total_rows = total
            return execution.desk_total_rows
        return len(execution.result_rows)

    # ============================================================== 工具
    def _spill_buffer(
        self,
        execution: QueryExecution,
        tick: int,
        events: list[dict[str, Any]],
        temporary_byte_limit: int,
    ) -> bool:
        if not execution.buffered_rows:
            return True
        # desk 溢出行先按契约排序键排序，使每个区段成为有序 run，
        # 供最终 k 路归并；close 事实行保持扫描序（合并阶段按分组聚合）。
        if execution.spec.contract.kind == "desk":
            execution.buffered_rows = self._apply_order(
                execution.buffered_rows, execution.spec.contract, desk_mode=True
            )
        # 按当前可用临时空间分块物化：每块都不超过实时余量，因此在严格
        # 限额（含 desk 仅有的保底份额）下也能逐块落盘，而不是整批失败。
        remaining = list(execution.buffered_rows)
        wrote_any = False
        while remaining:
            free_temp = self._free_temp(execution, temporary_byte_limit)
            chunk, leftover = self._fit_chunk(remaining, free_temp)
            if not chunk:
                # 连一行都放不下：若已写过部分块则保留缓冲等待下轮，否则
                # 明确返回资源不足，由上层进入等待。
                if wrote_any:
                    execution.buffered_rows = leftover
                    execution.work_bytes = sum(row_bytes(r) for r in leftover)
                    return True
                return False
            execution.seg_counter += 1
            seg_id = f"segment-{tick}-{execution.seg_counter}"
            segment = self.spill.write(
                execution.spec.query_id, seg_id, chunk, tick
            )
            execution.segments.append(seg_id)
            events.append({
                "event": "segment_write",
                "seg_id": seg_id,
                "bytes": segment.bytes_,
                "rows": segment.rows,
            })
            wrote_any = True
            remaining = leftover
        execution.buffered_rows = []
        execution.work_bytes = 0
        return True

    @staticmethod
    def _fit_chunk(
        rows: list[list[Any]], byte_budget: int
    ) -> tuple[list[list[Any]], list[list[Any]]]:
        """在字节预算内贪心切出尽量大的前缀块，返回 (块, 剩余)。"""
        if byte_budget <= 0:
            return [], list(rows)
        used = 0
        for index, row in enumerate(rows):
            size = len(encode_lines([row]))
            if used + size > byte_budget:
                if index == 0:
                    return [], list(rows)
                return rows[:index], rows[index:]
            used += size
        return list(rows), []

    def _write_partial(
        self,
        execution: QueryExecution,
        rows: list[list[Any]],
        tick: int,
        events: list[dict[str, Any]],
        temporary_byte_limit: int,
    ) -> Optional[str]:
        if not rows:
            return None
        free_temp = self._free_temp(execution, temporary_byte_limit)
        chunk, _leftover = self._fit_chunk(rows, free_temp)
        if not chunk:
            return None
        execution.seg_counter += 1
        seg_id = f"partial-{tick}-{execution.seg_counter}"
        segment = self.spill.write(execution.spec.query_id, seg_id, chunk, tick)
        execution.merge_partials.append(seg_id)
        events.append({
            "event": "partial_write",
            "seg_id": seg_id,
            "bytes": segment.bytes_,
            "rows": segment.rows,
        })
        return seg_id

    @staticmethod
    def _block(execution: QueryExecution, reason: str) -> None:
        execution.state = STATE_WAITING
        execution.wait_reason = reason

    @staticmethod
    def _unblock(execution: QueryExecution) -> None:
        if execution.state == STATE_WAITING:
            execution.state = STATE_RUNNING
        execution.wait_reason = WAIT_NONE
        execution.wait_age = 0

    @staticmethod
    def _terminate(execution: QueryExecution, reason: str, detail: str) -> None:
        execution.state = STATE_TERMINATED
        execution.terminal_reason = reason
        execution.terminal_detail = detail
