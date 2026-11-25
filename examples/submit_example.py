#!/usr/bin/env python3
"""仅用标准库的报送示例：同一哈希口径生成 HLL-128 摘要并调用报送接口。

用法：
  python examples/submit_example.py http://localhost:8000 2026-11 ER-0001 LIB-A
"""

from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.hll import REGISTER_COUNT, build_summary  # noqa: E402


def main(base_url: str, month: str, resource: str, branch: str) -> None:
    # 本馆本月该资源的去重读者标识；各馆务必使用 app.hll 同一哈希口径
    reader_ids = [f"{branch}-reader-{i}" for i in range(500)]
    payload = {
        "month": month,
        "resource": resource,
        "branch": branch,
        "registers": build_summary(reader_ids),
    }
    assert len(payload["registers"]) == REGISTER_COUNT

    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/reports",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read().decode("utf-8"))
    print(f"HTTP {resp.status} 报送成功，合并后非零寄存器数：",
          sum(1 for x in result["merged"] if x))

    # 下载合并摘要
    with urllib.request.urlopen(
        f"{base_url}/merged/{month}/{resource}/download", timeout=30
    ) as resp:
        merged = json.loads(resp.read().decode("utf-8"))
    print("下载合并摘要：", json.dumps(merged, ensure_ascii=False)[:120], "...")


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) != 4:
        print(__doc__)
        raise SystemExit(1)
    main(*args)
