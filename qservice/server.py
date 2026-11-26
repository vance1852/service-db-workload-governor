"""Unix 域套接字行协议服务端。

多个本地窗口通过同一个 socket 入口访问同一套数据与同一个调度器：
每条消息是一行 JSON 请求，服务端回一行 JSON 响应。线程安全由
:class:`QueryService` 的内部锁保证，连接可以并发打开，单个大查询
不会阻塞其他窗口的请求与普通写入。
"""

from __future__ import annotations

import json
import os
import socket
import threading
from pathlib import Path
from typing import Any

from .scheduler import ResourceLimits
from .service import QueryService


class QueryServer:
    def __init__(
        self,
        socket_path: str | Path,
        db_path: str | Path,
        work_dir: str | Path,
        limits: ResourceLimits | dict[str, int],
    ):
        self.socket_path = Path(socket_path)
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        if self.socket_path.exists():
            self.socket_path.unlink()
        self.service = QueryService(db_path, work_dir, limits)
        self._server: socket.socket | None = None
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def serve_forever(self) -> None:
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.bind(str(self.socket_path))
        self._server.listen(16)
        self._server.settimeout(0.5)
        try:
            while not self._stop.is_set():
                try:
                    conn, _ = self._server.accept()
                except socket.timeout:
                    continue
                except OSError:
                    # shutdown 关闭监听套接字后 accept 抛错：正常退出路径。
                    if self._stop.is_set():
                        break
                    raise
                thread = threading.Thread(
                    target=self._handle_connection, args=(conn,), daemon=True
                )
                thread.start()
                self._threads.append(thread)
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        self._stop.set()
        if self._server is not None:
            self._server.close()
            self._server = None
        if self.socket_path.exists():
            self.socket_path.unlink()
        self.service.close()

    def _handle_connection(self, conn: socket.socket) -> None:
        with conn:
            buffer = b""
            while not self._stop.is_set():
                chunk = conn.recv(65536)
                if not chunk:
                    return
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    if not line.strip():
                        continue
                    response = self.dispatch(line)
                    conn.sendall(json.dumps(response, ensure_ascii=False).encode("utf-8") + b"\n")

    def dispatch(self, raw: bytes | str) -> dict[str, Any]:
        try:
            request = json.loads(raw)
            command = request.pop("command")
            handler = getattr(self.service, command)
            result = handler(**request)
            return {"ok": True, "result": result}
        except Exception as exc:  # 协议层把异常变成可追查错误响应而非断连
            return {"ok": False, "error": type(exc).__name__, "detail": str(exc)}


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="本地查询执行服务（Unix socket）")
    parser.add_argument("db_path", type=Path)
    parser.add_argument("work_dir", type=Path)
    parser.add_argument("--socket", type=Path, default=Path("./qservice.sock"))
    parser.add_argument("--work-memory", type=int, required=True)
    parser.add_argument("--temporary", type=int, required=True)
    parser.add_argument("--desk-reserve", type=int, required=True)
    args = parser.parse_args()

    limits = ResourceLimits(
        work_memory_bytes=args.work_memory,
        temporary_bytes=args.temporary,
        desk_reserve_bytes=args.desk_reserve,
    )
    server = QueryServer(args.socket, args.db_path, args.work_dir, limits)
    print(f"qservice listening on {args.socket}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
