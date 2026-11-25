"""命令行整理入口：python -m app.maintenance compact [选项]。

对已有旧库执行压缩整理（服务启动时也会自动打开旧库，写入时自动把本键
转为紧凑格式；本命令适合在停机窗口一次性全量整理）。

    python -m app.maintenance status
    python -m app.maintenance compact --month 2026-11 --resource ER-0001
    python -m app.maintenance compact --batch-size 500
"""

from __future__ import annotations

import argparse
import os
import sys

from .db import Database


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="HLL 存档维护")
    parser.add_argument(
        "action",
        choices=["status", "compact"],
        help="status 查看计数格式；compact 把旧格式整理为紧凑格式",
    )
    parser.add_argument("--db", default=os.environ.get("HLL_DB_PATH", "/data/hll.sqlite3"))
    parser.add_argument("--month", default=None, help="只整理该月份")
    parser.add_argument("--resource", default=None, help="只整理该资源号")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="每个事务最多整理的 (月份,资源号) 组数（默认 200）",
    )
    args = parser.parse_args(argv)

    db = Database(args.db)
    if args.action == "status":
        print(f"counts_format: {db.counts_format()}")
        return 0

    try:
        result = db.compact_counts(args.month, args.resource, args.batch_size)
    except (ValueError, RuntimeError) as exc:
        print(f"整理失败，已保留届时已提交的计数与生效格式：{exc}", file=sys.stderr)
        print("可修正问题后重新执行本命令继续整理。", file=sys.stderr)
        return 1
    print(
        "整理完成" if result.legacy_remaining == 0 else "整理暂未完成",
        f"计数 {result.counts_converted} 组，合并摘要 {result.merged_converted} 组，"
        f"剩余旧计数 {result.legacy_remaining} 组，"
        f"当前格式 {db.counts_format()}",
    )
    return 0 if result.legacy_remaining == 0 else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
