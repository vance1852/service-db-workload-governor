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
