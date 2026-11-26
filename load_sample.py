"""将原始合成关系资料载入独立 SQLite 文件。"""

import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3


def load_sample(source, target, role):
    catalog = json.loads((source / "catalog.json").read_bytes())
    script = source / catalog["files"][role]["sql"]
    with target.open("xb"):
        pass
    with closing(sqlite3.connect(target)) as db:
        db.executescript(script.read_text(encoding="utf-8"))
        if db.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            raise ValueError("SQLite 原始资料不完整")
        return {name: db.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
                for name in catalog["files"][role]["tables"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="资料目录")
    parser.add_argument("target", type=Path, help="新建数据库")
    parser.add_argument("--role", default="main", help="输入数据库角色")
    args = parser.parse_args()
    print(json.dumps(load_sample(args.source, args.target, args.role), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
