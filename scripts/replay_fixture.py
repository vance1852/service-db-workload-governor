"""回放一份资料的 timeline，打印分页结果与资源/状态轨迹。

用法::

    python3 scripts/replay_fixture.py fixtures/spill-pressure-and-snapshot-resume
"""

import argparse
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from load_sample import load_sample
from qservice.driver import TimelineDriver, load_timeline
from qservice.scheduler import ResourceLimits
from qservice.service import QueryService


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--keep-db", action="store_true",
                        help="保留生成的数据库与工作目录（默认临时目录）")
    args = parser.parse_args()

    catalog, timeline = load_timeline(args.fixture)
    if args.keep_db:
        work_root = args.fixture / "_run"
        work_root.mkdir(exist_ok=True)
        db_path = work_root / "main.db"
        if not db_path.exists():
            load_sample(args.fixture, db_path, "main")
        cleanup = None
    else:
        tmp = tempfile.TemporaryDirectory()
        work_root = Path(tmp.name)
        db_path = work_root / "main.db"
        load_sample(args.fixture, db_path, "main")
        cleanup = tmp.cleanup

    service = QueryService(
        db_path, work_root / "work", ResourceLimits(**catalog["limits"])
    )
    try:
        report = TimelineDriver(service).run(timeline)
        for entry in report["trace"]:
            usage = entry["usage"]
            line = (
                f"at={entry['at']:>2} {entry['action']:<28} "
                f"work={usage['work_memory']['used']:>5}/"
                f"{usage['limits']['work_memory_bytes']:<5} "
                f"temp={usage['temporary']['used']:>5}/"
                f"{usage['limits']['temporary_bytes']:<5} "
                f"pending={usage['temporary']['pending_delete']:>5}"
            )
            print(line)
        print("\n各查询最终状态：")
        for query in service.usage()["queries"]:
            print(
                f"  {query['query_id']:<6} {query['state']:<10} "
                f"终止原因={query['terminal_reason']} "
                f"已交付页={query['pages_delivered']} 断面=v{query['snapshot_version']}"
            )
        print("\n交付页（按查询）：")
        print(json.dumps(report["pages"], ensure_ascii=False, indent=2))
    finally:
        service.close()
        if cleanup is not None:
            cleanup()


if __name__ == "__main__":
    main()
