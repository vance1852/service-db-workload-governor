"""行编码与字节计量。

契约要求“工作资源以实际编码行和临时内容的字节数计，UTF-8 与 UTF-8 长度
可复查”。因此所有内存与临时空间占用都使用同一种规范编码：

- 一行 = 一个 JSON 数组（列序固定），紧凑分隔、不转义非 ASCII；
- 工作内存中的一行按其 UTF-8 字节数计量；
- 临时区段把若干行以 JSON Lines（每行末尾一个 ``\\n``）落盘，
  区段占用即文件实际字节数，任何外部观察都能用同一公式复算。

NULL 用 ``None`` 表示，排序与聚合遵循 SQLite 的 NULL 语义，由 engine 处理。
"""

import json
from typing import Any, Iterable, Sequence

# 紧凑、确定、不把中文转成 \\uXXXX 的行编码，保证 UTF-8 字节可人工复查。
_DUMPS_KW = dict(separators=(",", ":"), ensure_ascii=False)


def encode_row(row: Sequence[Any]) -> bytes:
    """把一行（列值序列）编码成不带行尾换行的 UTF-8 JSON 字节。"""
    return json.dumps(list(row), **_DUMPS_KW).encode("utf-8")


def row_bytes(row: Sequence[Any]) -> int:
    """一行留在工作内存中的字节成本。"""
    return len(encode_row(row))


def encode_lines(rows: Iterable[Sequence[Any]]) -> bytes:
    """把多行编码成 JSON Lines 字节块（每行末尾恰有一个换行）。"""
    return b"".join(encode_row(row) + b"\n" for row in rows)


def decode_lines(blob: bytes) -> list[list[Any]]:
    """encode_lines 的逆操作，用于重启后读回临时区段。"""
    return [json.loads(line) for line in blob.splitlines()]


def lines_bytes(rows: Iterable[Sequence[Any]]) -> int:
    """多行落盘内容的字节数（含每行换行）。"""
    return len(encode_lines(rows))
