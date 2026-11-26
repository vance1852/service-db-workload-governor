"""临时工作区段（spill segment）管理。

资源契约里的临时空间有两条容易做错的规则，本模块把它们固化下来：

1. 已写出的工作区即使查询已取消，也不能立刻算作归还——删除要经过
   ``pending -> confirmed`` 两阶段。pending 期间文件仍在磁盘上，字节仍计入
   临时占用，直到外部确认删除（或本地 delete_confirmed 被调用）才释放。
2. 等待中的查询若根本没有物化任何内容，就不持有任何区段，自然不占用
   临时空间。区段只在真正 ``write`` 时创建。

每个区段是一个 JSON Lines 文件（见 :mod:`qservice.encoding`），文件大小即
其临时占用字节，重启后可原样读回并继续处理。
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional, Sequence

from .encoding import decode_lines, encode_lines


@dataclass
class Segment:
    """一个临时区段的元数据与状态。"""

    seg_id: str
    query_id: str
    path: Path
    bytes_: int = 0
    rows: int = 0
    pending_delete: bool = False
    confirmed_delete: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "seg_id": self.seg_id,
            "query_id": self.query_id,
            "bytes": self.bytes_,
            "rows": self.rows,
            "state": (
                "confirmed_delete"
                if self.confirmed_delete
                else "pending_delete"
                if self.pending_delete
                else "live"
            ),
        }


class SpillManager:
    """在指定工作目录下管理全部查询的临时区段。"""

    def __init__(self, work_dir: str | Path):
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self._segments: dict[str, Segment] = {}

    # ------------------------------------------------------------------ 写出
    def write(
        self,
        query_id: str,
        seg_id: str,
        rows: Iterable[Sequence[Any]],
        tick: int,
    ) -> Segment:
        """把一批行物化成新区段。

        返回写入后的区段，调用方应先按 :func:`encoding.lines_bytes` 预检
        临时空间余量，再调用本方法；本方法只负责真实落盘与记账。
        """
        rows = list(rows)
        blob = encode_lines(rows)
        path = self._path(query_id, seg_id)
        # 'xb' 保证不会静默覆盖重启后仍需续取的旧区段。
        with path.open("xb") as handle:
            handle.write(blob)
            handle.flush()
        segment = Segment(
            seg_id=seg_id,
            query_id=query_id,
            path=path,
            bytes_=len(blob),
            rows=len(rows),
        )
        self._segments[self._key(query_id, seg_id)] = segment
        return segment

    def read(self, query_id: str, seg_id: str) -> list[list[Any]]:
        segment = self._require(query_id, seg_id)
        return decode_lines(segment.path.read_bytes())

    def iter_rows(self, query_id: str, seg_id: str) -> Iterator[list[Any]]:
        """逐行流式读取区段，调用方内存中同时只持有一行。"""
        segment = self._require(query_id, seg_id)
        import json

        with segment.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)

    # ------------------------------------------------------------ 删除两阶段
    def mark_pending_delete(self, query_id: str, seg_id: str) -> Segment:
        """进入“正在删除”状态：文件仍在、字节仍计入临时占用。"""
        segment = self._require(query_id, seg_id)
        segment.pending_delete = True
        return segment

    def confirm_delete(self, query_id: str, seg_id: str) -> Segment:
        """确认删除：文件真正移除，临时空间此刻才归还。"""
        segment = self._require(query_id, seg_id)
        if not segment.pending_delete:
            # 允许外部事件直接带确认，但本地路径统一先 pending 再 confirm。
            segment.pending_delete = True
        if segment.path.exists():
            segment.path.unlink()
        segment.confirmed_delete = True
        segment.bytes_ = 0
        segment.rows = 0
        return segment

    def delete_query_segments(self, query_id: str) -> list[Segment]:
        """取消/退出时把某查询仍存活的区段全部转为 pending-delete。

        注意这里不释放空间——契约明确要求已写出的工作区不能立即说已归还。
        """
        affected: list[Segment] = []
        for segment in self._segments.values():
            if segment.query_id == query_id and not segment.confirmed_delete:
                if not segment.pending_delete:
                    segment.pending_delete = True
                    affected.append(segment)
        return affected

    def consume(self, query_id: str, seg_id: str) -> Segment:
        """区段被下游完整消费后回收。

        仍遵循两阶段语义：先进入 pending-delete（在同一调度动作内可被
        观察），随后确认删除并真正归还字节。调用方负责把两步都写入
        journal，使“正在删除仍占用”的过程可追查。
        """
        self.mark_pending_delete(query_id, seg_id)
        return self.confirm_delete(query_id, seg_id)

    # ------------------------------------------------------------------ 记账
    @property
    def segments(self) -> list[Segment]:
        return list(self._segments.values())

    def live_bytes(self, query_id: Optional[str] = None) -> int:
        """仍占着磁盘的字节：live 与 pending-delete 都计入，已确认的不计。"""
        total = 0
        for segment in self._segments.values():
            if segment.confirmed_delete:
                continue
            if query_id is not None and segment.query_id != query_id:
                continue
            total += segment.bytes_
        return total

    def pending_delete_bytes(self, query_id: Optional[str] = None) -> int:
        total = 0
        for segment in self._segments.values():
            if not segment.pending_delete or segment.confirmed_delete:
                continue
            if query_id is not None and segment.query_id != query_id:
                continue
            total += segment.bytes_
        return total

    def query_live_segments(self, query_id: str) -> list[Segment]:
        return [
            segment
            for segment in self._segments.values()
            if segment.query_id == query_id and not segment.confirmed_delete
        ]

    def get(self, query_id: str, seg_id: str) -> Optional[Segment]:
        return self._segments.get(self._key(query_id, seg_id))

    def merged_read(
        self,
        query_id: str,
        seg_ids: Sequence[str],
        key_function: Callable[[Sequence[Any]], Any],
    ) -> Iterator[list[Any]]:
        """对若干有序区段做流式 k 路归并，内存中同时只保留每路一行。

        每个区段在写出前已按同一 key 排序；堆顶始终是全局最小键，
        因此最终结果与把全部行读回再排序完全一致，却不突破工作内存。
        并列行的相对顺序按 (key, 路序号) 稳定确定。
        """
        iterators: list[Iterator[list[Any]]] = []
        heap: list[tuple[Any, int, list[Any]]] = []
        for route, seg_id in enumerate(seg_ids):
            iterator = iter(self.read(query_id, seg_id))
            iterators.append(iterator)
            try:
                first = next(iterator)
            except StopIteration:
                continue
            heapq.heappush(heap, (key_function(first), route, first))
        while heap:
            _key, route, row = heapq.heappop(heap)
            yield row
            try:
                nxt = next(iterators[route])
            except StopIteration:
                continue
            heapq.heappush(heap, (key_function(nxt), route, nxt))

    # ------------------------------------------------------------------ 持久化
    def snapshot_state(self) -> list[dict[str, Any]]:
        """导出区段状态，供 journal 持久化，重启后恢复记账。"""
        return [segment.as_dict() | {"path": str(segment.path)} for segment in self._segments.values()]

    def restore_state(self, records: Iterable[dict[str, Any]]) -> None:
        for record in records:
            path = Path(record["path"])
            state = record["state"]
            segment = Segment(
                seg_id=record["seg_id"],
                query_id=record["query_id"],
                path=path,
                bytes_=record["bytes"] if state != "confirmed_delete" else 0,
                rows=record["rows"] if state != "confirmed_delete" else 0,
                pending_delete=state == "pending_delete",
                confirmed_delete=state == "confirmed_delete",
            )
            if state != "confirmed_delete" and not path.exists():
                raise FileNotFoundError(
                    f"重启后缺少续取所需临时区段: {path}"
                )
            self._segments[self._key(segment.query_id, segment.seg_id)] = segment

    # ------------------------------------------------------------------ 内部
    @staticmethod
    def _key(query_id: str, seg_id: str) -> str:
        return f"{query_id}/{seg_id}"

    def _path(self, query_id: str, seg_id: str) -> Path:
        query_dir = self.work_dir / query_id
        query_dir.mkdir(parents=True, exist_ok=True)
        return query_dir / f"{seg_id}.jsonl"

    def _require(self, query_id: str, seg_id: str) -> Segment:
        segment = self._segments.get(self._key(query_id, seg_id))
        if segment is None:
            raise KeyError(f"临时区段不存在: {query_id}/{seg_id}")
        return segment
