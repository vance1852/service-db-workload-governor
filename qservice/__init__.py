"""本地查询执行服务。

在持续写入的服务记录上，为多个本地窗口提供受资源限额约束的分批查询执行、
稳定分页、断面一致的断点续取，以及可观察的占用与释放状态。

模块划分：
- encoding: 行编码与字节计量（UTF-8，长度可复查）
- storage:  同一数据源上的写入与多版本断面读取（SQLite + WAL + 影子表 MVCC）
- spill:    临时工作区段（spill segment）的写出、读取与 pending-delete 记账
- journal:  追加式持久事件日志，重启后重建状态、防止重复发页
- engine:   两类目标查询的分批执行机（desk 明细 / close 分组聚合）
- scheduler: 资源份额、等待上限与公平轮转
- service:  多窗口入口（Python API），串起存储、调度、日志与状态
- server/client: Unix 域套接字行协议入口与命令行客户端
- driver:   按 timeline.json 回放资料、产出可核对的执行轨迹
"""

from .service import QueryService, QueryState, TerminalReason, WaitReason

__all__ = [
    "QueryService",
    "QueryState",
    "TerminalReason",
    "WaitReason",
]
