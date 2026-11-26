"""本地窗口使用的命令行客户端。

用法示例::

    python3 -m qservice.client --socket ./qservice.sock usage
    python3 -m qservice.client --socket ./qservice.sock start_query \\
        --query-id w1 --client-class desk --query-name desk --store west \\
        --wait-limit 12 --page-size 2
    python3 -m qservice.client --socket ./qservice.sock fetch_page --query-id w1

每个窗口可各自打开一个客户端进程，共享同一个服务入口与数据源。
"""

from __future__ import annotations

import argparse
import json
import socket
from pathlib import Path
from typing import Any


def call(socket_path: str | Path, request: dict[str, Any]) -> dict[str, Any]:
    payload = json.dumps(request, ensure_ascii=False).encode("utf-8") + b"\n"
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.connect(str(socket_path))
        conn.sendall(payload)
        conn.shutdown(socket.SHUT_WR)
        chunks = []
        while True:
            chunk = conn.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
    return json.loads(b"".join(chunks))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", type=Path, default=Path("./qservice.sock"))
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("usage")

    start = sub.add_parser("start_query")
    start.add_argument("--query-id", required=True)
    start.add_argument("--client-class", required=True, choices=["month", "desk"])
    start.add_argument("--query-name", required=True)
    start.add_argument("--store", default=None)
    start.add_argument("--wait-limit", type=int, required=True)
    start.add_argument("--page-size", type=int, required=True)
    start.add_argument("--estimate", type=int, default=None)

    for name in ("fetch_page", "pause", "resume", "cancel", "status"):
        command = sub.add_parser(name)
        command.add_argument("--query-id", required=True)

    sub.add_parser("close")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    request: dict[str, Any] = {"command": args.command}
    if args.command == "start_query":
        request.update(
            query_id=args.query_id,
            client_class=args.client_class,
            query_name=args.query_name,
            store=args.store,
            wait_limit=args.wait_limit,
            page_size=args.page_size,
            estimate=args.estimate,
        )
    elif args.command in ("fetch_page", "pause", "resume", "cancel", "status"):
        request["query_id"] = args.query_id
    response = call(args.socket, request)
    print(json.dumps(response, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
