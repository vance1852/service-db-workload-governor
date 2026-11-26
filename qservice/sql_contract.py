"""目标 SQL 契约解析。

仓库把支持的查询范围、分组及排序契约写在 ``target_sql`` 表中，语义采用
SQLite 的 LEFT JOIN / GROUP BY / SUM / ORDER BY。本模块不实现通用 SQL，
只解析契约内出现过的固定语法形态，提取：

- 查询类别（有无 GROUP BY 区分 close 与 desk）；
- 投影列（表列或 COUNT(*) / SUM(...) 聚合）；
- 分组列与排序项（含 ASC 下 NULL 优先的 SQLite 语义，由 engine 执行）；
- 可选的 ``WHERE 表.列 = ?`` 参数过滤（desk 的门店范围）。

解析结果是一份普通字典，engine 据此产出与直接执行 SQLite 完全一致的行。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

_SELECT_RE = re.compile(r"^\s*SELECT\s+(.+?)\s+FROM\s+", re.IGNORECASE | re.DOTALL)
_GROUP_RE = re.compile(r"GROUP\s+BY\s+(.+?)(?:\s+ORDER\s+BY|$)", re.IGNORECASE | re.DOTALL)
_ORDER_RE = re.compile(r"ORDER\s+BY\s+(.+?)$", re.IGNORECASE | re.DOTALL)
_WHERE_RE = re.compile(r"WHERE\s+(.+?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|$)",
                       re.IGNORECASE | re.DOTALL)
_JOIN_RE = re.compile(r"LEFT\s+JOIN\s+(\w+)\s+(\w+)\s+ON\s+(.+?)(?:\s+WHERE|\s+GROUP|\s+ORDER|$)",
                      re.IGNORECASE | re.DOTALL)


@dataclass
class Projection:
    # kind: 'column' | 'count_star' | 'sum'
    kind: str
    alias_column: Optional[str] = None   # 源表限定列，如 s.store
    expr_text: str = ""                  # 原始表达式文本，用于排序项匹配


@dataclass
class SqlContract:
    raw: str
    kind: str                            # 'close' | 'desk'
    projections: list[Projection]
    group_columns: list[str]            # 如 ['s.store', 's.family']
    order_terms: list[str]              # 原始表达式，如 'SUM(a.amount)'
    where_param: Optional[str]          # 如 's.store'，无参数为 None
    join_table: Optional[str] = None
    join_alias: Optional[str] = None
    join_on: Optional[str] = None


def _split_top_level(text: str) -> list[str]:
    """按顶层逗号切分（契约表达式没有嵌套逗号，聚合内也无逗号）。"""
    return [part.strip() for part in text.split(",") if part.strip()]


def parse_contract(sql: str) -> SqlContract:
    compact = " ".join(sql.split())
    select_match = _SELECT_RE.search(compact)
    if not select_match:
        raise ValueError(f"无法解析目标 SQL: {sql}")
    select_body = select_match.group(1)

    projections: list[Projection] = []
    for item in _split_top_level(select_body):
        upper = item.upper()
        if upper == "COUNT(*)":
            projections.append(Projection(kind="count_star", expr_text=item))
        elif upper.startswith("SUM("):
            projections.append(Projection(kind="sum", expr_text=item))
        else:
            projections.append(
                Projection(kind="column", alias_column=item, expr_text=item)
            )

    group_match = _GROUP_RE.search(compact)
    group_columns = _split_top_level(group_match.group(1)) if group_match else []

    order_match = _ORDER_RE.search(compact)
    order_terms = (
        [term.strip().rstrip(" ").split()[0] for term in _split_top_level(order_match.group(1))]
        if order_match
        else []
    )

    where_match = _WHERE_RE.search(compact)
    where_param = None
    if where_match:
        predicate = where_match.group(1).strip()
        param_match = re.search(r"([\w]+\.[\w]+)\s*=\s*\?", predicate, re.IGNORECASE)
        if param_match:
            where_param = param_match.group(1)

    join_match = _JOIN_RE.search(compact)
    join_table = join_alias = join_on = None
    if join_match:
        join_table, join_alias, join_on = (
            join_match.group(1),
            join_match.group(2),
            join_match.group(3).strip(),
        )

    return SqlContract(
        raw=compact,
        kind="close" if group_columns else "desk",
        projections=projections,
        group_columns=group_columns,
        order_terms=order_terms,
        where_param=where_param,
        join_table=join_table,
        join_alias=join_alias,
        join_on=join_on,
    )


def load_contracts(db_path: str) -> dict[str, SqlContract]:
    """从数据库 target_sql 表读取全部受支持契约。"""
    import sqlite3

    contracts: dict[str, SqlContract] = {}
    with sqlite3.connect(db_path) as conn:
        for name, sql in conn.execute("SELECT name, sql FROM target_sql"):
            contracts[name] = parse_contract(sql)
    return contracts
