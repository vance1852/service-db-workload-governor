"""多窗口本地查询服务门面。

:class:`QueryService` 把存储、执行机、调度器、临时区段与持久日志串成一个
线程安全的入口，供同一进程内的多个窗口（或 server 转发来的多个客户端）
共用。所有对外可见的状态变化先写 journal 再返回，restart 后据此重建，
保证已经发出的页不重复、不串断面。
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Optional, Sequence

from .engine import (
    STATE_CANCELLED,
    STATE_DELIVERED,
    STATE_PAUSED,
    STATE_RUNNING,
    STATE_TERMINATED,
    STATE_WAITING,
    TERM_CANCELLED,
    TERM_COMPLETED,
    TERM_SNAPSHOT_PRUNED,
    TERM_TEMP_LOST,
    QueryEngine,
    QueryExecution,
    QuerySpec,
)
from .journal import Journal
from .scheduler import ResourceLimits, Scheduler
from .spill import SpillManager
from .sql_contract import SqlContract, load_contracts, parse_contract
from .storage import SnapshotPruned, Storage


class QueryState:
    """管理者可见状态（与 engine 内部状态同名常量集中暴露）。"""

    RUNNING = STATE_RUNNING
    WAITING = STATE_WAITING
    PAUSED = STATE_PAUSED
    CANCELLED = STATE_CANCELLED
    TERMINATED = STATE_TERMINATED
    DELIVERED = STATE_DELIVERED


class TerminalReason:
    COMPLETED = TERM_COMPLETED
    CANCELLED = TERM_CANCELLED
    WAIT_LIMIT = "wait_limit_exceeded"
    SNAPSHOT_PRUNED = TERM_SNAPSHOT_PRUNED
    TEMP_LOST = TERM_TEMP_LOST
    GROUP_EXPANSION = "group_expansion"


class WaitReason:
    WORK_MEMORY = "work_memory"
    TEMP_SPACE = "temporary_space"


class QueryService:
    def __init__(
        self,
        db_path: str | Path,
        work_dir: str | Path,
        limits: ResourceLimits | dict[str, int],
    ):
        self.db_path = Path(db_path)
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        if isinstance(limits, dict):
            limits = ResourceLimits(
                work_memory_bytes=limits["work_memory_bytes"],
                temporary_bytes=limits["temporary_bytes"],
                desk_reserve_bytes=limits["desk_reserve_bytes"],
            )
        self.limits = limits
        self._lock = threading.RLock()
        self.storage = Storage(self.db_path)
        # 仓库契约来自 target_sql：服务只接受其中登记的查询。
        self.contracts: dict[str, SqlContract] = load_contracts(self.db_path)
        self.spill = SpillManager(self.work_dir / "segments")
        self.engine = QueryEngine(self.storage, self.spill)
        self.scheduler = Scheduler(self.engine, self.spill, limits)
        self.journal = Journal(self.work_dir / "journal.jsonl")
        self.queries: dict[str, QueryExecution] = {}
        self.order: list[str] = []
        self.tick = 0
        self._recover()

    # ============================================================== 内部持久化
    def _record(self, event_type: str, **fields: Any) -> dict[str, Any]:
        return self.journal.append(event_type, self.tick, **fields)

    def _checkpoint_locked(self, query_id: str) -> None:
        """把查询当前状态写入 journal，作为暂停/重启前的证实位置。"""
        execution = self.queries[query_id]
        self._record("checkpoint", query_id=query_id, state=execution.to_journal())

    def _recover(self) -> None:
        """重启恢复：回放 journal，重建查询状态与临时区段记账。"""
        records = self.journal.read_all()
        latest: dict[str, dict[str, Any]] = {}
        segment_latest: dict[tuple[str, str], dict[str, Any]] = {}
        pruned = 0
        for record in records:
            if record["event"] == "checkpoint":
                # 检查点随状态变化不断追加，恢复时以最后一个为准。
                latest[record["query_id"]] = record["state"]
            elif record["event"] in ("segment_write", "partial_write"):
                segment_latest[(record["query_id"], record["seg_id"])] = {
                    "seg_id": record["seg_id"],
                    "query_id": record["query_id"],
                    "path": str(
                        self.work_dir / "segments" / record["query_id"]
                        / f"{record['seg_id']}.jsonl"
                    ),
                    "bytes": record["bytes"],
                    "rows": record["rows"],
                    "state": "live",
                }
            elif record["event"] == "segment_pending_delete":
                key = (record["query_id"], record["seg_id"])
                if key in segment_latest:
                    segment_latest[key]["state"] = "pending_delete"
            elif record["event"] == "segment_confirmed_delete":
                key = (record["query_id"], record["seg_id"])
                if key in segment_latest:
                    segment_latest[key]["state"] = "confirmed_delete"
            elif record["event"] == "snapshot_prune":
                pruned = max(pruned, record["pruned_version"])
        segment_records = list(segment_latest.values())
        if segment_records:
            self.spill.restore_state(segment_records)
        for query_id, state in latest.items():
            contract = parse_contract(state["contract_sql"])
            self.queries[query_id] = QueryExecution.from_journal(state, contract)
        self.order = [
            query_id
            for query_id in self.order_from_records(records)
            if query_id in self.queries
        ]
        self.tick = max((record["at"] for record in records), default=0)
        self._pruned_version = pruned

    @staticmethod
    def order_from_records(records: list[dict[str, Any]]) -> list[str]:
        order: list[str] = []
        for record in records:
            if record["event"] == "start":
                order.append(record["query_id"])
        return order

    # ============================================================== 查询生命周期
    def start_query(
        self,
        query_id: str,
        client_class: str,
        query_name: str,
        store: Optional[str],
        wait_limit: int,
        page_size: int,
        estimate: Optional[int] = None,
    ) -> dict[str, Any]:
        """开启一个查询。断面在启动一刻固定（当前已提交版本）。"""
        with self._lock:
            if query_id in self.queries:
                raise ValueError(f"查询句柄已存在: {query_id}")
            if query_name not in self.contracts:
                raise ValueError(f"未知查询契约: {query_name}")
            contract = self.contracts[query_name]
            if contract.kind == "desk" and store is None:
                raise ValueError(f"desk 查询 {query_name} 必须带门店范围参数")
            version = self.storage.open_snapshot()
            spec = QuerySpec(
                query_id=query_id,
                client_class=client_class,
                query_name=query_name,
                store=store,
                wait_limit=wait_limit,
                page_size=page_size,
                estimate=estimate,
                snapshot_version=version,
                contract=contract,
            )
            execution = QueryExecution(spec=spec)
            if contract.kind == "close":
                from .engine import PHASE_CLOSE_SCAN

                execution.phase = PHASE_CLOSE_SCAN
            self.queries[query_id] = execution
            self.order.append(query_id)
            self.tick += 1
            self._record(
                "start",
                query_id=query_id,
                client_class=client_class,
                query_name=query_name,
                store=store,
                wait_limit=wait_limit,
                page_size=page_size,
                estimate=estimate,
                snapshot_version=version,
            )
            # 到达即开工：立刻给新查询一个调度时间片，使扩张观察与初始
            # 占用在 start 之后、下一个动作之前就成立，而不是等到首次取页。
            self.tick += 1
            initial_events = self.scheduler.tick(self.queries, self.order, self.tick)
            for event in initial_events:
                self._journal_engine_event(query_id, event)
            self._checkpoint_locked(query_id)
            return self.status(query_id)

    def pause(self, query_id: str) -> dict[str, Any]:
        """暂停：工作区保留，证实位置持久化，断面保持不变。"""
        with self._lock:
            execution = self._require(query_id)
            if execution.state in (STATE_TERMINATED, STATE_DELIVERED, STATE_CANCELLED):
                return self.status(query_id)
            execution.state = STATE_PAUSED
            self.tick += 1
            self._record("pause", query_id=query_id)
            self._checkpoint_locked(query_id)
            return self.status(query_id)

    def resume(self, query_id: str) -> dict[str, Any]:
        """恢复：从证实位置继续；断面失效则明确终止，不临时改读最新记录。"""
        with self._lock:
            execution = self._require(query_id)
            self.tick += 1
            if execution.state == STATE_TERMINATED:
                self._record("resume_rejected", query_id=query_id,
                             reason=execution.terminal_reason)
                return self.status(query_id)
            try:
                self.storage.check_snapshot(execution.spec.snapshot_version)
                # 仅当前执行状态仍引用的存活区段是续取依赖；已回收的不算。
                for seg_id in execution.segments + execution.merge_partials:
                    segment = self.spill.get(query_id, seg_id)
                    if segment is None or segment.confirmed_delete:
                        continue
                    if not segment.path.exists():
                        raise FileNotFoundError(f"续取所需临时区段失效: {seg_id}")
            except SnapshotPruned as exc:
                execution.state = STATE_TERMINATED
                execution.terminal_reason = TERM_SNAPSHOT_PRUNED
                execution.terminal_detail = str(exc)
                self._record("resume_rejected", query_id=query_id,
                             reason=TERM_SNAPSHOT_PRUNED, detail=str(exc))
                self._checkpoint_locked(query_id)
                return self.status(query_id)
            except FileNotFoundError as exc:
                execution.state = STATE_TERMINATED
                execution.terminal_reason = TERM_TEMP_LOST
                execution.terminal_detail = str(exc)
                self._record("resume_rejected", query_id=query_id,
                             reason=TERM_TEMP_LOST, detail=str(exc))
                self._checkpoint_locked(query_id)
                return self.status(query_id)
            execution.state = STATE_RUNNING
            execution.wait_age = 0
            self._record("resume", query_id=query_id)
            self._checkpoint_locked(query_id)
            return self.status(query_id)

    def cancel(self, query_id: str) -> dict[str, Any]:
        """取消：查询退出，但已写出的工作区只进入 pending-delete，
        空间暂不归还；从未物化的等待查询不占任何临时空间。"""
        with self._lock:
            execution = self._require(query_id)
            if execution.state in (STATE_TERMINATED, STATE_DELIVERED):
                return self.status(query_id)
            execution.state = STATE_CANCELLED
            execution.terminal_reason = TERM_CANCELLED
            self.tick += 1
            self._record("cancel", query_id=query_id)
            pending = self.spill.delete_query_segments(query_id)
            for segment in pending:
                self._record(
                    "segment_pending_delete",
                    query_id=query_id,
                    seg_id=segment.seg_id,
                    bytes=segment.bytes_,
                )
            self._checkpoint_locked(query_id)
            return self.status(query_id)

    def _release_segments(self, query_id: str) -> None:
        """把查询已物化但不再被承诺结果占用的区段走两阶段删除。

        已发出的页本身保存在客户端侧并有 journal 指纹背书，不需要再依赖
        临时区段；但删除仍先 pending（字节继续计入临时占用），由确认动作
        或内部确认点释放，保证“正在写出的工作区不能立即说空间已归还”。
        """
        pending = self.spill.delete_query_segments(query_id)
        for segment in pending:
            self._record(
                "segment_pending_delete",
                query_id=query_id,
                seg_id=segment.seg_id,
                bytes=segment.bytes_,
            )
        for segment in pending:
            self.spill.confirm_delete(query_id, segment.seg_id)
            self._record(
                "segment_confirmed_delete",
                query_id=query_id,
                seg_id=segment.seg_id,
                bytes=segment.bytes_,
            )

    # ============================================================== 分页
    def fetch_page(self, query_id: str) -> dict[str, Any]:
        """拉取下一页。必要时先让调度器持续分批推进直到结果就绪。

        已交付页与启动断面绑定：结果行在 ready 时固定，分页只做切片；
        重复调用在全部交付后返回空页与 completed，不会重发旧页。
        """
        with self._lock:
            execution = self._require(query_id)
            self.tick += 1
            self._record("page_request", query_id=query_id)

            if execution.state in (STATE_TERMINATED,):
                return self._page_response(execution, None)
            if execution.state == STATE_CANCELLED:
                return self._page_response(execution, None)

            if not execution.result_ready:
                if execution.state == STATE_PAUSED:
                    return self._page_response(execution, None, blocked="paused")
                self.tick, events, reason = self.scheduler.advance_until(
                    self.queries,
                    self.order,
                    target_query=query_id,
                    tick_start=self.tick,
                    max_rounds=execution.spec.wait_limit + 1,
                )
                for event in events:
                    self._journal_engine_event(query_id, event)
                # 取页边界结算等待年龄：跨调度轮仍等不到资源才计数，
                # 超限则带可追查理由终止，否则返回可重试的等待响应。
                wait_event = self.scheduler.settle_wait(self.queries, query_id)
                if wait_event is not None:
                    self._journal_engine_event(query_id, dict(wait_event))
                execution = self.queries[query_id]
                if not execution.result_ready:
                    self._checkpoint_locked(query_id)
                    blocked = (
                        execution.terminal_reason
                        if execution.terminal_reason
                        else reason or "not_ready"
                    )
                    return self._page_response(execution, None, blocked=blocked)

            page = self.engine.next_page(execution)
            if page is None:
                self._checkpoint_locked(query_id)
                return self._page_response(execution, None, blocked="not_ready")

            # desk 流式归并在本页期间回收的 run：统一补记两阶段删除事件。
            if execution.pending_reclaim_events:
                reclaim_events = list(execution.pending_reclaim_events)
                execution.pending_reclaim_events.clear()
                for event in reclaim_events:
                    self._journal_engine_event(query_id, dict(event))

            if page:
                fingerprint = Journal.page_hash(page)
                execution.pages_delivered += 1
                execution.rows_delivered += len(page)
                execution.delivered_hashes.append(fingerprint)
                self._record(
                    "page_delivered",
                    query_id=query_id,
                    page_no=execution.pages_delivered,
                    rows=len(page),
                    first_key=page[0],
                    last_key=page[-1],
                    hash=fingerprint,
                    snapshot_version=execution.spec.snapshot_version,
                )
            finished = execution.rows_delivered >= self.engine.result_total_rows(
                execution
            )
            if finished:
                execution.state = STATE_DELIVERED
                execution.terminal_reason = TERM_COMPLETED
                self._record(
                    "terminal",
                    query_id=query_id,
                    reason=TERM_COMPLETED,
                    pages=execution.pages_delivered,
                )
                # 结果已全部交付：未被承诺结果占用的工作区可以让出。
                # 区段删除同样走 pending -> confirmed 两阶段并留痕。
                self._release_segments(query_id)
            self._checkpoint_locked(query_id)
            return self._page_response(execution, page)

    def _journal_engine_event(self, query_id: str, event: dict[str, Any]) -> None:
        event_type = event.pop("event")
        event.setdefault("query_id", query_id)
        self._record(event_type, **event)

    def _page_response(
        self,
        execution: QueryExecution,
        page: Optional[list[list[Any]]],
        blocked: Optional[str] = None,
    ) -> dict[str, Any]:
        return {
            "query_id": execution.spec.query_id,
            "page_no": execution.pages_delivered + (1 if page else 0),
            "rows": page or [],
            "snapshot_version": execution.spec.snapshot_version,
            "state": execution.state,
            "wait_reason": execution.wait_reason,
            "terminal_reason": execution.terminal_reason,
            "blocked": blocked,
            "status": self.status(execution.spec.query_id),
        }

    # ============================================================== 写入与断面清理
    def commit(self, table: str, rows: Sequence[Sequence[Any]]) -> dict[str, Any]:
        """接受同一数据源上的持续写入；短事务，不被大查询阻塞。"""
        with self._lock:
            self.tick += 1
            version = self.storage.commit_rows(table, rows)
            self._record("commit", table=table, rows=list(rows), version=version)
            return {"version": version, "tick": self.tick}

    def prune_snapshots(self, keep_version: int) -> dict[str, Any]:
        """显式允许清理旧断面。仍持有旧断面的查询下次动作即被终止并说明。"""
        with self._lock:
            self.tick += 1
            pruned = self.storage.prune_snapshot(keep_version)
            self._record("snapshot_prune", keep_version=keep_version,
                         pruned_version=pruned)
            for query_id, execution in self.queries.items():
                if (
                    execution.spec.snapshot_version <= pruned
                    and execution.state not in (STATE_TERMINATED, STATE_DELIVERED, STATE_CANCELLED)
                ):
                    execution.state = STATE_TERMINATED
                    execution.terminal_reason = TERM_SNAPSHOT_PRUNED
                    execution.terminal_detail = (
                        f"断面版本 {execution.spec.snapshot_version} 已清理，结果不能续取"
                    )
                    self._record(
                        "terminal",
                        query_id=query_id,
                        reason=TERM_SNAPSHOT_PRUNED,
                        detail=execution.terminal_detail,
                    )
                    self._checkpoint_locked(query_id)
            return {"pruned_version": pruned}

    # ============================================================== 临时删除确认
    def temporary_delete_pending(self, query_id: str, seg_id: str, bytes_: int) -> dict[str, Any]:
        """外部观察接口：登记某区段进入 pending-delete（字节仍占临时空间）。

        外部事件的段名可能与内部命名不同；若段名命中内部区段则标记它，
        否则仅作为外部信号留痕，不虚构占用（占用一律按实际编码字节记账）。
        """
        with self._lock:
            self.tick += 1
            segment = self.spill.get(query_id, seg_id)
            if segment is not None and not segment.pending_delete:
                self.spill.mark_pending_delete(query_id, seg_id)
            self._record(
                "segment_pending_delete",
                query_id=query_id,
                seg_id=seg_id,
                bytes=bytes_,
                external=True,
            )
            return self.usage()

    def temporary_delete_confirmed(self, query_id: str, seg_id: str, bytes_: int) -> dict[str, Any]:
        """删除确认：此刻临时空间才真正归还。

        若外部段名命中内部区段则确认它；否则把它视为对该查询整个待删
        工作区的确认（取消后的工作区删除是工作区级事件），确认其全部
        pending-delete 区段。实际归还字节按内部编码记账，外部字节仅留痕。
        """
        with self._lock:
            self.tick += 1
            segment = self.spill.get(query_id, seg_id)
            if segment is not None:
                if not segment.confirmed_delete:
                    self.spill.confirm_delete(query_id, seg_id)
            else:
                for pending in self.spill.query_live_segments(query_id):
                    if pending.pending_delete:
                        original = pending.bytes_
                        self.spill.confirm_delete(query_id, pending.seg_id)
                        self._record(
                            "segment_confirmed_delete",
                            query_id=query_id,
                            seg_id=pending.seg_id,
                            bytes=original,
                            external_workspace=seg_id,
                        )
            self._record(
                "segment_confirmed_delete",
                query_id=query_id,
                seg_id=seg_id,
                bytes=bytes_,
                external=True,
            )
            return self.usage()

    # ============================================================== 重启
    def restart_worker(self) -> dict[str, Any]:
        """模拟 worker 重启：持久状态保留，内存态从 journal 与区段重建。

        已发页的指纹与行数在 journal 中，续取从下一页开始，不重复；
        断面或临时数据失效的查询以明确理由终止。
        """
        with self._lock:
            self.tick += 1
            self._record("restart", scope="worker")
            self.journal.close()
            # 重新装配存储与执行态（工作目录中的区段文件原样保留）。
            self.storage.close()
            self.storage = Storage(self.db_path)
            self.contracts = load_contracts(self.db_path)
            self.spill = SpillManager(self.work_dir / "segments")
            self.engine = QueryEngine(self.storage, self.spill)
            self.scheduler = Scheduler(self.engine, self.spill, self.limits)
            self.journal = Journal(self.work_dir / "journal.jsonl")
            self.queries = {}
            self.order = []
            self._recover()
            # 重启后让每个仍活跃的查询做一次可续取性校验。只检查其当前
            # 执行状态仍引用、且未完成两阶段删除的区段；已回收区段不是
            # 续取依赖，不能据此误判 temporary_data_invalid。
            for query_id, execution in list(self.queries.items()):
                if execution.state in (STATE_TERMINATED, STATE_DELIVERED, STATE_CANCELLED):
                    continue
                try:
                    self.storage.check_snapshot(execution.spec.snapshot_version)
                    for seg_id in execution.segments + execution.merge_partials:
                        segment = self.spill.get(query_id, seg_id)
                        if segment is None or segment.confirmed_delete:
                            continue
                        if not segment.path.exists():
                            raise FileNotFoundError(f"续取所需临时区段失效: {seg_id}")
                except SnapshotPruned as exc:
                    execution.state = STATE_TERMINATED
                    execution.terminal_reason = TERM_SNAPSHOT_PRUNED
                    execution.terminal_detail = str(exc)
                    self._record("resume_rejected", query_id=query_id,
                                 reason=TERM_SNAPSHOT_PRUNED, detail=str(exc))
                except FileNotFoundError as exc:
                    execution.state = STATE_TERMINATED
                    execution.terminal_reason = TERM_TEMP_LOST
                    execution.terminal_detail = str(exc)
                    self._record("resume_rejected", query_id=query_id,
                                 reason=TERM_TEMP_LOST, detail=str(exc))
            return self.usage()

    # ============================================================== 状态与占用
    def _require(self, query_id: str) -> QueryExecution:
        if query_id not in self.queries:
            raise KeyError(f"未知查询句柄: {query_id}")
        return self.queries[query_id]

    def status(self, query_id: str) -> dict[str, Any]:
        with self._lock:
            execution = self._require(query_id)
            return {
                "query_id": query_id,
                "state": execution.state,
                "phase": execution.phase,
                "client_class": execution.spec.client_class,
                "query_name": execution.spec.query_name,
                "store": execution.spec.store,
                "snapshot_version": execution.spec.snapshot_version,
                "wait_reason": execution.wait_reason,
                "wait_age": execution.wait_age,
                "wait_limit": execution.spec.wait_limit,
                "pages_delivered": execution.pages_delivered,
                "rows_delivered": execution.rows_delivered,
                "terminal_reason": execution.terminal_reason,
                "terminal_detail": execution.terminal_detail,
                "work_bytes": execution.work_bytes,
                "temp_bytes": self.spill.live_bytes(query_id),
                "pending_delete_bytes": self.spill.pending_delete_bytes(query_id),
                "segments": [segment.as_dict() for segment in self.spill.query_live_segments(query_id)],
                "proof": {
                    "last_sale_id": execution.last_sale_id,
                    "merge_index": execution.merge_index,
                    "rows_delivered": execution.rows_delivered,
                },
                "expansion": list(execution.expansion_seen),
            }

    def usage(self) -> dict[str, Any]:
        """管理者视角：业务查询在等资源、在用还是已退出清理，及全局占用。"""
        with self._lock:
            return {
                "tick": self.tick,
                "limits": {
                    "work_memory_bytes": self.limits.work_memory_bytes,
                    "temporary_bytes": self.limits.temporary_bytes,
                    "desk_reserve_bytes": self.limits.desk_reserve_bytes,
                },
                "work_memory": {
                    "used": self.scheduler.used_work_bytes(self.queries),
                    "free": self.scheduler.free_work_bytes(self.queries),
                },
                "temporary": {
                    "used": self.spill.live_bytes(),
                    "free": self.scheduler.free_temp_bytes(),
                    "pending_delete": self.spill.pending_delete_bytes(),
                },
                "queries": [self.status(query_id) for query_id in self.order],
            }

    def close(self) -> None:
        with self._lock:
            self.journal.close()
            self.storage.close()
