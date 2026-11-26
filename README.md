# 服务质量库的查询资源调度与断点交付

本项目的本地资料保存各方使用的原始关系、接入操作及外部观察。记录里的人员、机构和凭据均为合成。各资料目录独立使用。

## 原始关系及接入约定

sale 与 adjustment 按 sale_id 关联。一个 sale 可有多条已入库更正，题目使用的聚合必须保持连接产生的重复事实。target_sql 保存客户端支持的两类查询，语义采用 SQLite 的 LEFT JOIN、GROUP BY、SUM 和 ORDER BY，NULL 留给 SQL 按其规则处理。并列以查询写明的附加列决定。store 是本地门店代码。
`start` 参数依次为句柄、客户端类、查询名称、门店参数、等待上限及页大小。写入事件中的数据为后来提交的输入，基线中尚未应用它。工作资源以实际编码行和临时内容的字节数计，UTF-8 与 UTF-8 长度可复查。最大临时空间包括已经写出但正在删除的块；时轴为到达逻辑时刻，提交位置另由 commit 动作携带。

catalog.json 列出各数据库文件的角色、源表及运行限额。timeline.json 中的 at 是操作或观察到达的顺序，同一时刻按数组内的位置排列。args 是相应动作携带的字段，各记录里的其他位点使用该记录的业务含义。SQL 文件中只有开始联调前的行和表定义。

## 装载资料

Python 3.11.2 自带 SQLite 3.40.1。以下命令从合成资料生成独立数据库，目标文件须尚未存在。可用 sqlite3 打开生成的文件并读取源表。

~~~sh
python3 load_sample.py fixtures/spill-pressure-and-snapshot-resume main-local.db --role main
~~~

执行 python3 -m unittest -v 可核对各份资料的 SQL 是否可装载、基础表行数，以及普通 SQLite 读写连接能否并行打开。

## 本地查询执行服务（qservice）

`qservice/` 在同一套数据库上提供受资源限额约束的本地查询执行服务，多个本地
窗口通过进程内 API 或 Unix 域套接字共用同一调度器与数据源。

### 设计如何对应需求

- **仓库契约**：`target_sql` 表中的查询由 `sql_contract.py` 解析出范围、
  分组、投影与排序；引擎按 SQLite 语义执行 LEFT JOIN（保留退款为 NULL 的
  明细与连接产生的重复事实）、GROUP BY、COUNT/SUM（全 NULL 得 NULL，
  ASC 下 NULL 最前）与 ORDER BY（并列按契约附加列稳定排序）。
- **持续写入与断面**：`storage.py` 在基表之外维护版本化影子表，每次提交
  生成新版本的完整断面。查询在启动时固定版本，提交不改变其结果；暂停后从
  证实位置（最后 `sale_id`、合并进度、已交付行数）续取，不临时改读最新记录。
  显式清理旧断面或临时数据失效时，查询以 `snapshot_pruned` /
  `temporary_data_invalid` 明确终止并交代“结果不能续取”。
- **资源按实际内容计量**：`encoding.py` 用规范 JSON Lines 的 UTF-8 字节
  计量工作内存与临时区段，文件大小与占用可独立复算。
- **大查询分批推进**：工作内存超限即把批次按可用临时空间**分块** spill 为
  临时区段；close 采用外部聚合（扫描 spill → 逐段局部聚合 → 归并），临时
  空间不足时压缩已扫描事实继续推进；desk 的溢出院在分页时流式 k 路归并
  （逐行读取、每路只持一行），不一次性读回，且某 run 交付完即两阶段回收。
- **公平与保底**：`scheduler.py` 每轮公平轮转。临时空间按类别分池——
  month 类的硬天花板为“总临时空间 − 桌面预留”，桌面预留池不会被大任务
  占走；desk 类靠分块落盘、明确等待与分页即回收共享空间。等待年龄在
  跨取页的调度边界结算，超过本次等待上限即以 `wait_limit_exceeded`
  带理由结束，单次取页内的资源等待不会空转耗尽额度。某组远大于估计时
  记录 `group_expansion` 观察并继续在已有资源内分批。
- **两阶段删除**：已写出的工作区在取消/消费后先进入 pending-delete（字节
  仍占临时空间），确认后才归还；从未物化的等待查询不持有任何区段、零占用。
- **重启不重复不串断面**：所有状态变化先写追加式 `journal`（含每页内容
  指纹）再发页；重启后回放重建，已发页不重发，失效数据给出可追查理由。

### 入口

进程内：

```python
from qservice import QueryService
from qservice.scheduler import ResourceLimits

service = QueryService("main-local.db", "work/",
                       ResourceLimits(2400, 6800, 400))
service.start_query("big", "month", "close", None, 30, 3)
page = service.fetch_page("big")      # 稳定分组/排序页
service.usage()                        # 占用、等待与待确认释放
```

Unix 套接字（多窗口）：

```sh
python3 -m qservice main-local.db work/ --socket ./q.sock \
    --work-memory 2400 --temporary 6800 --desk-reserve 400
python3 -m qservice.client --socket ./q.sock usage
```

回放资料并打印管理者轨迹：

```sh
python3 scripts/replay_fixture.py fixtures/spill-pressure-and-snapshot-resume
```

### 测试

```sh
python3 -m unittest discover -s tests -v   # 语义、资源、断面、重启、套接字
```

