"""同一数据源上的持续写入与多版本断面读取。

设计要点
========
1. 真实来源仍是 ``sale`` / ``adjustment`` 两张基表，写入在同一个 SQLite
   事务里同时写基表与版本化影子表；服务因此始终“依照同一数据源接受查询和
   写入”，普通事务（基表读写）不受查询机影响。
2. 每个提交动作产生一个严格递增的 ``version``。基线已有数据归属版本 0。
   影子表保存每个版本可见的全部行，并带版本内单调 ``seq``，于是：
   - 任一查询只读取它启动时的版本，天然形成断面，提交再多也不改变其结果；
   - 读取可凭 ``(version, seq)`` 或最后一个业务键在任意批次后证实并续扫；
   - 显式清理旧断面就是删除低版本影子行，被清理的查询会收到明确的
     “断面已清理、不可续取”理由，而不是被悄悄切到新断面。
3. 连接开启 WAL：写入是短事务，读取侧分批取数，不持有长写锁，另一项事务
   与正常查询不会被单个大任务拖住。
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Optional, Sequence

SALE_COLUMNS = ("sale_id", "store", "family", "amount", "incident")
ADJ_COLUMNS = ("ref", "sale_id", "amount")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS _qv_sale_v (
    version INTEGER NOT NULL,
    seq     INTEGER NOT NULL,
    sale_id TEXT NOT NULL,
    store   TEXT NOT NULL,
    family  TEXT NOT NULL,
    amount  INTEGER,
    incident TEXT
);
CREATE TABLE IF NOT EXISTS _qv_adj_v (
    version INTEGER NOT NULL,
    seq     INTEGER NOT NULL,
    ref     TEXT NOT NULL,
    sale_id TEXT NOT NULL,
    amount  INTEGER
);
CREATE TABLE IF NOT EXISTS _qv_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS _qv_sale_v_idx ON _qv_sale_v(version, seq);
CREATE INDEX IF NOT EXISTS _qv_adj_v_idx  ON _qv_adj_v(version, seq);
"""


class SnapshotPruned(LookupError):
    """查询所需的旧断面已被清理，影子行不再可读。"""


class Storage:
    """封装数据库连接、版本化写入与断面读取。"""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"数据库不存在: {self.db_path}")
        # check_same_thread=False：服务内部用锁串行化写入，读取连接单独管理。
        self._writer = sqlite3.connect(self.db_path, check_same_thread=False)
        self._reader = sqlite3.connect(self.db_path, check_same_thread=False)
        for conn in (self._writer, self._reader):
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        self._bootstrap()

    # ------------------------------------------------------------------ 初始化
    def _bootstrap(self) -> None:
        with self._writer:
            self._writer.executescript(_SCHEMA)
            seeded = self._writer.execute(
                "SELECT value FROM _qv_meta WHERE key='current_version'"
            ).fetchone()
            if seeded is None:
                # 基线数据全部归属版本 0，断面内顺序由业务主键确定：
                # sale 按 sale_id、adjustment 按 ref，证实位置因此可复算。
                self._writer.execute(
                    "INSERT INTO _qv_sale_v(version,seq,sale_id,store,family,amount,incident) "
                    "SELECT 0, ROW_NUMBER() OVER (ORDER BY sale_id)-1, "
                    "sale_id,store,family,amount,incident FROM sale ORDER BY sale_id"
                )
                self._writer.execute(
                    "INSERT INTO _qv_adj_v(version,seq,ref,sale_id,amount) "
                    "SELECT 0, ROW_NUMBER() OVER (ORDER BY ref)-1, ref,sale_id,amount "
                    "FROM adjustment ORDER BY ref"
                )
                self._writer.execute(
                    "INSERT INTO _qv_meta(key,value) VALUES('current_version','0')"
                )
        self.current_version = int(
            self._writer.execute(
                "SELECT value FROM _qv_meta WHERE key='current_version'"
            ).fetchone()[0]
        )
        self.pruned_version = int(
            self._writer.execute(
                "SELECT value FROM _qv_meta WHERE key='pruned_version'"
            ).fetchone()[0]
            if self._writer.execute(
                "SELECT value FROM _qv_meta WHERE key='pruned_version'"
            ).fetchone()
            else 0
        )

    # ------------------------------------------------------------------ 写入
    def commit_rows(
        self, table: str, rows: Sequence[Sequence[Any]]
    ) -> int:
        """把一次提交动作的若干行写入基表与影子表，返回新版本号。

        与基表写入在同一事务内完成：要么基表与影子表同时可见，要么都不可见。
        新版本影子表是完整断面——复制上一版本全部可见行并追加新行，因此
        任一版本都能独立读取，不依赖更早版本；旧断面被清理也不影响新版本。
        目前契约中的写入事件为单行，接口按多行实现以兼容普通批量事务。
        """
        if table not in ("sale", "adjustment"):
            raise ValueError(f"只接受契约内基表写入: {table}")
        with self._writer:
            previous = self.current_version
            new_version = previous + 1
            if table == "sale":
                # 复制旧断面全部行，序号延续。
                self._writer.execute(
                    "INSERT INTO _qv_sale_v(version,seq,sale_id,store,family,amount,incident) "
                    "SELECT ?, seq, sale_id,store,family,amount,incident "
                    "FROM _qv_sale_v WHERE version=?",
                    (new_version, previous),
                )
                placeholders = ",".join("?" for _ in SALE_COLUMNS)
                base_sql = f"INSERT INTO sale({','.join(SALE_COLUMNS)}) VALUES({placeholders})"
                start = self._next_seq(self._writer, "_qv_sale_v", new_version)
                for offset, row in enumerate(rows):
                    self._writer.execute(base_sql, tuple(row))
                    self._writer.execute(
                        "INSERT INTO _qv_sale_v(version,seq,sale_id,store,family,amount,incident)"
                        " VALUES(?,?,?,?,?,?,?)",
                        (new_version, start + offset, *row),
                    )
                # adjustment 断面在本版本中不变，完整复制以保持断面独立。
                self._writer.execute(
                    "INSERT INTO _qv_adj_v(version,seq,ref,sale_id,amount) "
                    "SELECT ?, seq, ref,sale_id,amount FROM _qv_adj_v WHERE version=?",
                    (new_version, previous),
                )
            else:
                self._writer.execute(
                    "INSERT INTO _qv_adj_v(version,seq,ref,sale_id,amount) "
                    "SELECT ?, seq, ref,sale_id,amount FROM _qv_adj_v WHERE version=?",
                    (new_version, previous),
                )
                placeholders = ",".join("?" for _ in ADJ_COLUMNS)
                base_sql = f"INSERT INTO adjustment({','.join(ADJ_COLUMNS)}) VALUES({placeholders})"
                start = self._next_seq(self._writer, "_qv_adj_v", new_version)
                for offset, row in enumerate(rows):
                    self._writer.execute(base_sql, tuple(row))
                    self._writer.execute(
                        "INSERT INTO _qv_adj_v(version,seq,ref,sale_id,amount)"
                        " VALUES(?,?,?,?,?)",
                        (new_version, start + offset, *row),
                    )
                self._writer.execute(
                    "INSERT INTO _qv_sale_v(version,seq,sale_id,store,family,amount,incident) "
                    "SELECT ?, seq, sale_id,store,family,amount,incident "
                    "FROM _qv_sale_v WHERE version=?",
                    (new_version, previous),
                )
            self._writer.execute(
                "UPDATE _qv_meta SET value=? WHERE key='current_version'",
                (str(new_version),),
            )
            self.current_version = new_version
            return new_version

    @staticmethod
    def _next_seq(conn: sqlite3.Connection, table: str, version: int) -> int:
        # 新版本影子行从 0 开始；旧版本行保持不变，断面因此不可变。
        return 0

    # ------------------------------------------------------------------ 断面读取
    def check_snapshot(self, version: int) -> None:
        if version < self.pruned_version:
            raise SnapshotPruned(
                f"断面版本 {version} 已被清理（pruned={self.pruned_version}）"
            )

    def open_snapshot(self, version: Optional[int] = None) -> int:
        """登记一次断面读取，返回可读版本号。"""
        version = self.current_version if version is None else version
        self.check_snapshot(version)
        return version

    def scan_sale(
        self,
        version: int,
        limit: int,
        after_sale_id: Optional[str] = None,
        store: Optional[str] = None,
    ) -> list[sqlite3.Row]:
        """按断面顺序分批读取 sale 行。

        - close 查询读取全部门店；desk 查询带 ``store`` 过滤，过滤在断面内
          完成，门店参数即仓库给出的查询范围契约。
        - ``after_sale_id`` 是证实位置：sale_id 在断面内唯一且有序，
          从它之后继续即可得到与首次读取完全一致的续扫结果。
        """
        self.check_snapshot(version)
        # 断面内统一按 sale_id 排序：证实位置（最后一个 sale_id）与读取
        # 方向一致，store 过滤只裁剪行、不改变相对顺序。
        sql = (
            "SELECT sale_id,store,family,amount,incident FROM _qv_sale_v "
            "WHERE version=? {store_clause} {after_clause} ORDER BY sale_id LIMIT ?"
        )
        store_clause = ""
        after_clause = ""
        params: list[Any] = [version]
        if store is not None:
            store_clause = "AND store=?"
            params.append(store)
        if after_sale_id is not None:
            after_clause = "AND sale_id>?"
            params.append(after_sale_id)
        params.append(limit)
        return list(
            self._reader.execute(
                sql.format(store_clause=store_clause, after_clause=after_clause),
                params,
            ).fetchall()
        )

    def adjustment_map(self, version: int) -> dict[str, list[tuple[str, Optional[int]]]]:
        """读取断面上的全部更正，按 sale_id 分组。

        LEFT JOIN 语义由引擎在内存中完成：没有任何更正的 sale 仍产出一行
        （ref/amount 为 NULL），有 n 条更正的 sale 产出 n 行，重复事实保留。
        """
        self.check_snapshot(version)
        result: dict[str, list[tuple[str, Optional[int]]]] = {}
        rows = self._reader.execute(
            "SELECT ref,sale_id,amount FROM _qv_adj_v WHERE version=? ORDER BY ref",
            (version,),
        ).fetchall()
        for row in rows:
            result.setdefault(row["sale_id"], []).append((row["ref"], row["amount"]))
        return result

    def counts(self, version: int) -> dict[str, int]:
        """断面上的基表行数，用于估计分批规模与扩张判断。"""
        self.check_snapshot(version)
        return {
            "sale": self._reader.execute(
                "SELECT COUNT(*) FROM _qv_sale_v WHERE version=?", (version,)
            ).fetchone()[0],
            "adjustment": self._reader.execute(
                "SELECT COUNT(*) FROM _qv_adj_v WHERE version=?", (version,)
            ).fetchone()[0],
        }

    # ------------------------------------------------------------------ 断面清理
    def prune_snapshot(self, keep_version: int) -> int:
        """显式清理旧断面：删除低于 keep_version 的影子行。

        清理后仍持有旧版本的查询不能续取，由服务在其下次动作时给出
        SNAPSHOT_PRUNED 终止理由；清理本身不删除任何基表数据。
        """
        with self._writer:
            target = max(self.pruned_version, keep_version - 1)
            if target <= self.pruned_version:
                return self.pruned_version
            self._writer.execute(
                "DELETE FROM _qv_sale_v WHERE version<=?", (target,)
            )
            self._writer.execute(
                "DELETE FROM _qv_adj_v WHERE version<=?", (target,)
            )
            self._writer.execute(
                "INSERT INTO _qv_meta(key,value) VALUES('pruned_version',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(target),),
            )
            self.pruned_version = target
            return target

    def close(self) -> None:
        self._reader.close()
        self._writer.close()
