import io
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from app import create_app
from app.db import Database
from app.errors import NotFoundError, ValidationError
from app.hll import REGISTER_COUNT, build_summary, hll_register

ZERO = [0] * REGISTER_COUNT


@pytest.fixture()
def db(tmp_path):
    return Database(str(tmp_path / "hll.sqlite3"))


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "web.sqlite3"))
    app.testing = True
    return app.test_client()


def regs(overrides=None):
    out = ZERO.copy()
    for position, value in (overrides or {}).items():
        out[position] = value
    return out


# ---------- 哈希口径 ----------

def test_hash_is_stable_and_in_range():
    position, rank = hll_register("reader-0001")
    assert 0 <= position < REGISTER_COUNT
    assert 1 <= rank <= 58
    # 同输入同输出，各馆可复现同一口径
    assert hll_register("reader-0001") == (position, rank)


def test_build_summary_is_idempotent():
    ids = [f"reader-{i}" for i in range(200)]
    s1 = build_summary(ids)
    s2 = build_summary(ids + ids)  # 重复读者不改变摘要
    assert s1 == s2
    assert len(s1) == REGISTER_COUNT
    assert max(s1) <= 58


# ---------- 校验 ----------

@pytest.mark.parametrize("bad_month", ["2026-13", "202611", "2026/11", "", 202611])
def test_bad_month_rejected(db, bad_month):
    with pytest.raises(ValidationError):
        db.upsert_report(bad_month, "ER-1", "LIB-A", ZERO)


def test_bad_identifier_rejected(db):
    with pytest.raises(ValidationError):
        db.upsert_report("2026-11", "ER/1", "LIB-A", ZERO)


def test_registers_must_be_128_values_0_to_58(db):
    with pytest.raises(ValidationError):
        db.upsert_report("2026-11", "ER-1", "LIB-A", [0] * 127)
    bad = ZERO.copy()
    bad[10] = 59
    with pytest.raises(ValidationError):
        db.upsert_report("2026-11", "ER-1", "LIB-A", bad)
    bad[10] = True
    with pytest.raises(ValidationError):
        db.upsert_report("2026-11", "ER-1", "LIB-A", bad)


# ---------- 报送、合并、换报、撤回 ----------

def test_merge_is_positionwise_max(db):
    a = regs({0: 5, 1: 3, 2: 10})
    b = regs({0: 8, 1: 1, 3: 20})
    c = regs({2: 15, 3: 12})

    r = db.upsert_report("2026-11", "ER-1", "LIB-A", a)
    assert r["merged"] == a
    assert r["replaced"] is False

    r = db.upsert_report("2026-11", "ER-1", "LIB-B", b)
    assert r["replaced"] is False
    assert r["merged"] == regs({0: 8, 1: 3, 2: 10, 3: 20})

    r = db.upsert_report("2026-11", "ER-1", "LIB-C", c)
    assert r["merged"] == regs({0: 8, 1: 3, 2: 15, 3: 20})


def test_replace_report_increments_without_touching_others(db):
    a = regs({0: 5, 1: 9})
    b = regs({0: 8, 2: 4})
    db.upsert_report("2026-11", "ER-1", "LIB-A", a)
    db.upsert_report("2026-11", "ER-1", "LIB-B", b)

    # A 换报：位置 0 从 5 降到 2（最大值仍为 B 的 8），位置 1 撤空，新增位置 4=30
    new_a = regs({0: 2, 4: 30})
    r = db.upsert_report("2026-11", "ER-1", "LIB-A", new_a)
    assert r["replaced"] is True
    assert r["merged"] == regs({0: 8, 2: 4, 4: 30})

    # B 的摘要原样保留
    assert db.get_report("2026-11", "ER-1", "LIB-B")["registers"] == b


def test_withdraw_recomputes_from_remaining_branches(db):
    a = regs({0: 5, 1: 9})
    b = regs({0: 8, 2: 4})
    db.upsert_report("2026-11", "ER-1", "LIB-A", a)
    db.upsert_report("2026-11", "ER-1", "LIB-B", b)

    r = db.withdraw_report("2026-11", "ER-1", "LIB-A")
    assert r["merged"] == b

    with pytest.raises(NotFoundError):
        db.get_report("2026-11", "ER-1", "LIB-A")

    # 撤回最后一个馆：合并摘要全 0，但记录仍已保存可下载
    db.withdraw_report("2026-11", "ER-1", "LIB-B")
    merged = db.get_merged("2026-11", "ER-1")
    assert merged["registers"] == ZERO


def test_withdraw_missing_report_404(db):
    with pytest.raises(NotFoundError):
        db.withdraw_report("2026-11", "ER-1", "LIB-X")


def test_merged_404_when_never_reported(db):
    with pytest.raises(NotFoundError):
        db.get_merged("2026-11", "ER-9")


def test_different_months_and_resources_are_isolated(db):
    db.upsert_report("2026-10", "ER-1", "LIB-A", regs({0: 3}))
    db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 7}))
    db.upsert_report("2026-11", "ER-2", "LIB-A", regs({1: 11}))

    assert db.get_merged("2026-10", "ER-1")["registers"] == regs({0: 3})
    assert db.get_merged("2026-11", "ER-1")["registers"] == regs({0: 7})
    assert db.get_merged("2026-11", "ER-2")["registers"] == regs({1: 11})


def test_tie_counts_allow_withdraw_to_reveal_next_max(db):
    # 两个馆在位置 0 同为 8，撤回一个后仍应保留 8；再撤回另一个才降为 0
    db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 8}))
    db.upsert_report("2026-11", "ER-1", "LIB-B", regs({0: 8, 4: 6}))
    db.withdraw_report("2026-11", "ER-1", "LIB-A")
    assert db.get_merged("2026-11", "ER-1")["registers"] == regs({0: 8, 4: 6})
    db.withdraw_report("2026-11", "ER-1", "LIB-B")
    assert db.get_merged("2026-11", "ER-1")["registers"] == ZERO


def test_withdraw_max_reveals_second_largest(db):
    # 位置 0：A=10、B=5、C=3。撤回 A 后必须降为 B 的 5，而不是 0
    db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 10, 1: 7}))
    db.upsert_report("2026-11", "ER-1", "LIB-B", regs({0: 5, 1: 2}))
    db.upsert_report("2026-11", "ER-1", "LIB-C", regs({0: 3}))
    db.withdraw_report("2026-11", "ER-1", "LIB-A")
    assert db.get_merged("2026-11", "ER-1")["registers"] == regs({0: 5, 1: 2})

    # A 换报为更低值后，最大值同样应来自其余馆
    db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 1, 1: 9}))
    assert db.get_merged("2026-11", "ER-1")["registers"] == regs({0: 5, 1: 9})

    # 再撤回 B，位置 0 只剩 C 的 3
    db.withdraw_report("2026-11", "ER-1", "LIB-B")
    assert db.get_merged("2026-11", "ER-1")["registers"] == regs({0: 3, 1: 9})


def test_histogram_matches_branch_reports(db):
    # 增量直方图与独立重算的逐位置最大值始终一致（多轮随机换报/撤回）
    import random
    import sqlite3 as sqlite

    rng = random.Random(7)
    branches = [f"LIB-{i}" for i in range(6)]
    current = {b: ZERO.copy() for b in branches}
    for _ in range(60):
        branch = rng.choice(branches)
        if rng.random() < 0.7:
            new_regs = [rng.randint(0, 58) if rng.random() < 0.3 else 0
                        for _ in range(REGISTER_COUNT)]
            db.upsert_report("2026-11", "ER-1", branch, new_regs)
            current[branch] = new_regs
        else:
            try:
                db.withdraw_report("2026-11", "ER-1", branch)
                current[branch] = ZERO.copy()
            except NotFoundError:
                pass

    expected = [0] * REGISTER_COUNT
    for branch_regs in current.values():
        for i, v in enumerate(branch_regs):
            expected[i] = max(expected[i], v)
    assert db.get_merged("2026-11", "ER-1")["registers"] == expected

    # 直方图行数不随馆数膨胀：每位置最多 59 个非零取值
    conn = sqlite.connect(db.path)
    n_rows = conn.execute("SELECT COUNT(*) FROM register_counts").fetchone()[0]
    conn.close()
    assert n_rows <= REGISTER_COUNT * 58


# ---------- 原子性 ----------

def test_commit_failure_keeps_original_data(db, monkeypatch):
    from app.db import Database

    db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 5}))

    def failing_commit(conn):
        raise sqlite3.OperationalError("simulated commit failure")

    monkeypatch.setattr(Database, "_commit", staticmethod(failing_commit))
    with pytest.raises(sqlite3.OperationalError):
        db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 40}))
    monkeypatch.undo()

    # 馆报与合并摘要都沿用原数据
    assert db.get_report("2026-11", "ER-1", "LIB-A")["registers"] == regs({0: 5})
    assert db.get_merged("2026-11", "ER-1")["registers"] == regs({0: 5})


# ---------- 并发：每笔成功操作都计入汇总 ----------

def test_concurrent_different_branches_all_commit(db):
    branches = [f"LIB-{i:02d}" for i in range(8)]
    barrier = threading.Barrier(len(branches))

    def worker(branch_index):
        branch = branches[branch_index]
        # 每个馆在独立位置贡献不同值，最终合并结果应包含全部 8 笔
        registers = regs({branch_index: branch_index + 1})
        barrier.wait()
        db.upsert_report("2026-11", "ER-1", branch, registers)

    with ThreadPoolExecutor(max_workers=len(branches)) as pool:
        list(pool.map(worker, range(len(branches))))

    merged = db.get_merged("2026-11", "ER-1")["registers"]
    for i in range(8):
        assert merged[i] == i + 1

    # 每笔馆报均已落盘
    for i, branch in enumerate(branches):
        assert db.get_report("2026-11", "ER-1", branch)["registers"][i] == i + 1


def test_concurrent_replace_and_withdraw_converges(db):
    branches = [f"LIB-{i}" for i in range(6)]
    for i, branch in enumerate(branches):
        db.upsert_report("2026-11", "ER-1", branch, regs({0: 10 + i, 5: 3}))

    errors = []

    def replace_worker():
        try:
            for _ in range(5):
                db.upsert_report("2026-11", "ER-1", branches[0], regs({0: 50}))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def withdraw_worker(branch):
        try:
            db.withdraw_report("2026-11", "ER-1", branch)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(replace_worker)]
        futures += [pool.submit(withdraw_worker, b) for b in branches[1:]]
        for future in futures:
            future.result()

    assert not errors
    merged = db.get_merged("2026-11", "ER-1")["registers"]
    # 其余馆撤回后，只剩 LIB-0（换报为 50）；位置 5 全部撤回后为 0
    assert merged[0] == 50
    assert merged[5] == 0
    assert sum(merged) == 50


# ---------- HTTP 接口 ----------

def _payload(branch="LIB-A", reg_overrides=None):
    return {
        "month": "2026-11",
        "resource": "ER-1",
        "branch": branch,
        "registers": regs(reg_overrides),
    }


def test_http_submit_replace_and_download(client):
    r = client.post("/reports", json=_payload(reg_overrides={0: 5}))
    assert r.status_code == 201
    r = client.post("/reports", json=_payload(branch="LIB-B", reg_overrides={0: 9, 2: 4}))
    assert r.status_code == 201
    # 同馆再报 => 换报 200
    r = client.post("/reports", json=_payload(reg_overrides={0: 7}))
    assert r.status_code == 200

    r = client.get("/merged/2026-11/ER-1/download")
    assert r.status_code == 200
    assert r.headers["Content-Disposition"].startswith("attachment;")
    body = r.get_json()
    assert body["registers"] == regs({0: 9, 2: 4})
    assert len(body["registers"]) == REGISTER_COUNT


def test_http_upload_json_file_multipart(client):
    payload = _payload(reg_overrides={3: 12})
    data = {
        "month": "2026-11",
        "resource": "ER-1",
        "branch": "LIB-A",
        "file": (io.BytesIO(json.dumps(payload).encode()), "report.json"),
    }
    r = client.post("/reports", data=data, content_type="multipart/form-data")
    # 浏览器表单走 302 跳转
    assert r.status_code == 302
    r = client.get("/merged/2026-11/ER-1/download")
    assert r.get_json()["registers"] == regs({3: 12})


def test_http_validation_error_is_400_json(client):
    bad = _payload()
    bad["registers"] = [0] * 127
    r = client.post("/reports", json=bad)
    assert r.status_code == 400
    assert "error" in r.get_json()


def test_http_withdraw_and_404(client):
    client.post("/reports", json=_payload(reg_overrides={1: 6}))
    r = client.post("/reports/withdraw", json={
        "month": "2026-11", "resource": "ER-1", "branch": "LIB-A"
    })
    assert r.status_code == 200
    assert r.get_json()["merged"][1] == 0

    r = client.delete("/reports/withdraw", query_string={
        "month": "2026-11", "resource": "ER-1", "branch": "LIB-A"
    })
    assert r.status_code == 404

    assert client.get("/merged/2026-11/ER-9/download").status_code == 404


def test_http_get_branch_report_and_health(client):
    client.post("/reports", json=_payload(reg_overrides={2: 8}))
    r = client.get("/reports/2026-11/ER-1/LIB-A")
    assert r.status_code == 200
    assert r.get_json()["registers"][2] == 8
    assert client.get("/health").status_code == 200
    assert client.get("/").status_code == 200


# ---------- 紧凑格式 ----------

from app.compact import (
    decode_histogram,
    decode_merged,
    encode_histogram,
    encode_merged,
)


def test_compact_codec_roundtrip_and_zero_size():
    import random

    rng = random.Random(11)
    histogram = [
        {value: rng.randint(1, 300) for value in rng.sample(range(1, 59), rng.randint(0, 9))}
        for _ in range(REGISTER_COUNT)
    ]
    assert decode_histogram(encode_histogram(histogram)) == histogram
    merged = [rng.randint(0, 58) for _ in range(REGISTER_COUNT)]
    assert decode_merged(encode_merged(merged)) == merged
    assert len(encode_merged([0] * REGISTER_COUNT)) == 97
    # 全零直方图只占版本号 + 位图
    assert len(encode_histogram([{} for _ in range(REGISTER_COUNT)])) == 17


def test_compact_codec_rejects_corruption():
    blob = bytearray(encode_merged([0] * REGISTER_COUNT))
    blob[0] = 9
    with pytest.raises(ValueError):
        decode_merged(bytes(blob))
    hist = bytearray(encode_histogram([{3: 1}] + [{}] * 127))
    with pytest.raises(ValueError):
        decode_histogram(bytes(hist[:-1]))


def _require_dbstat():
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("SELECT COUNT(*) FROM dbstat")
    except sqlite3.OperationalError:  # pragma: no cover - 精简构建可能没有
        conn.close()
        pytest.skip("当前 SQLite 构建不支持 dbstat，无法统计页数")
    conn.close()


def _legacy_page_usage(tmp_path, groups=100):
    """按旧结构直接建库，返回 (计数表页数, 合并表页数)。"""
    _require_dbstat()
    path = tmp_path / "legacy_pages.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE branch_reports (
            month TEXT NOT NULL, resource TEXT NOT NULL, branch TEXT NOT NULL,
            registers TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (month, resource, branch)) WITHOUT ROWID;
        CREATE TABLE register_counts (
            month TEXT NOT NULL, resource TEXT NOT NULL, position INTEGER NOT NULL,
            value INTEGER NOT NULL, cnt INTEGER NOT NULL,
            PRIMARY KEY (month, resource, position, value)) WITHOUT ROWID;
        CREATE TABLE merged_summaries (
            month TEXT NOT NULL, resource TEXT NOT NULL, registers TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (month, resource)) WITHOUT ROWID;
        """
    )
    counters = {}
    for group in range(groups):
        month, resource = _group_key(group)
        merged = [0] * REGISTER_COUNT
        for branch in range(8):
            # 8 个馆在 128 个位置各报出不同的非零值（最大 57，取值域内）
            registers = [branch + 1 + (position * 7 + group) % 50
                         for position in range(REGISTER_COUNT)]
            merged = [max(a, b) for a, b in zip(merged, registers)]
            conn.execute(
                "INSERT INTO branch_reports VALUES (?, ?, ?, ?, '')",
                (month, resource, f"LIB-{branch}", json.dumps(registers)),
            )
            for position, value in enumerate(registers):
                if value:
                    key = (month, resource, position, value)
                    counters[key] = counters.get(key, 0) + 1
        conn.execute(
            "INSERT INTO merged_summaries VALUES (?, ?, ?, '')",
            (month, resource, json.dumps(merged, separators=(",", ":"))),
        )
    conn.executemany(
        "INSERT INTO register_counts VALUES (?, ?, ?, ?, ?)",
        [key + (cnt,) for key, cnt in counters.items()],
    )
    conn.commit()
    pages = dict(
        conn.execute(
            "SELECT name, SUM(pgsize) FROM dbstat "
            "WHERE name IN ('register_counts', 'merged_summaries') GROUP BY name"
        )
    )
    conn.close()
    return pages


def _group_key(index):
    return f"20{20 + index // 12:02d}-{index % 12 + 1:02d}", f"ER-{index:04d}"


def test_compact_counts_and_indexes_fit_in_half_the_pages(tmp_path):
    """同一 100 组月份/资源、每组 8 馆各位置报不同非零值：页数 ≤ 旧格式一半。"""
    old_pages = _legacy_page_usage(tmp_path)

    db = Database(str(tmp_path / "compact_pages.sqlite3"))
    for group in range(100):
        month, resource = _group_key(group)
        for branch in range(8):
            db.upsert_report(
                month,
                resource,
                f"LIB-{branch}",
                [branch + 1 + (position * 7 + group) % 50
                 for position in range(REGISTER_COUNT)],
            )

    conn = sqlite3.connect(db.path)
    new_pages = dict(
        conn.execute(
            "SELECT name, SUM(pgsize) FROM dbstat "
            "WHERE name IN ('register_counts', 'merged_summaries') GROUP BY name"
        )
    )
    conn.close()
    # 计数表（WITHOUT ROWID，主键即索引）最多为旧格式一半
    assert new_pages["register_counts"] <= old_pages["register_counts"] / 2
    # 计数 + 索引（合并表）合计同样最多为旧格式一半
    assert (
        new_pages["register_counts"] + new_pages["merged_summaries"]
        <= (old_pages["register_counts"] + old_pages["merged_summaries"]) / 2
    )
    # 新库直接使用紧凑格式，无需整理
    assert db.counts_format() == "compact"


def test_legacy_counts_compacted_also_fit_in_half_the_pages(tmp_path):
    """旧库整理后计数与索引占页同样减半，且合并值不变。"""
    path = str(tmp_path / "legacy_pages.sqlite3")
    old_pages = _legacy_page_usage(tmp_path)
    db = Database(path)
    assert db.counts_format() == "legacy"
    before = {
        _group_key(group): db.get_merged(*_group_key(group))["registers"]
        for group in range(100)
    }
    assert db.compact_counts().completed is True
    assert db.counts_format() == "compact"
    assert "register_counts_legacy" not in {
        row[0]
        for row in sqlite3.connect(path).execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    conn = sqlite3.connect(path)
    new_pages = dict(
        conn.execute(
            "SELECT name, SUM(pgsize) FROM dbstat "
            "WHERE name IN ('register_counts', 'merged_summaries') GROUP BY name"
        )
    )
    conn.close()
    assert new_pages["register_counts"] <= old_pages["register_counts"] / 2
    for key, merged in before.items():
        assert db.get_merged(*key)["registers"] == merged


def test_new_summary_is_stored_compact(tmp_path):
    db = Database(str(tmp_path / "compact.sqlite3"))
    db.upsert_report("2026-11", "ER-1", "LIB-A", regs({0: 5}))
    conn = sqlite3.connect(db.path)
    kind = conn.execute(
        "SELECT typeof(registers) FROM register_counts"
    ).fetchone()[0]
    merged_kind = conn.execute(
        "SELECT typeof(registers) FROM merged_summaries"
    ).fetchone()[0]
    conn.close()
    assert kind == "blob"
    assert merged_kind == "blob"


# ---------- 旧计数整理 ----------

def _build_legacy_database(path, groups=3):
    """用旧结构写入数据，模拟升级前的库。"""
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE branch_reports (
            month TEXT NOT NULL, resource TEXT NOT NULL, branch TEXT NOT NULL,
            registers TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (month, resource, branch)) WITHOUT ROWID;
        CREATE TABLE register_counts (
            month TEXT NOT NULL, resource TEXT NOT NULL, position INTEGER NOT NULL,
            value INTEGER NOT NULL, cnt INTEGER NOT NULL,
            PRIMARY KEY (month, resource, position, value)) WITHOUT ROWID;
        CREATE TABLE merged_summaries (
            month TEXT NOT NULL, resource TEXT NOT NULL, registers TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (month, resource)) WITHOUT ROWID;
        """
    )
    counters = {}
    for group in range(groups):
        month, resource = _group_key(group)
        merged = [0] * REGISTER_COUNT
        for branch in range(8):
            # 8 个馆在 128 个位置各报出不同的非零值（最大 57，取值域内）
            registers = [branch + 1 + (position * 7 + group) % 50
                         for position in range(REGISTER_COUNT)]
            merged = [max(a, b) for a, b in zip(merged, registers)]
            conn.execute(
                "INSERT INTO branch_reports VALUES (?, ?, ?, ?, '')",
                (month, resource, f"LIB-{branch}", json.dumps(registers)),
            )
            for position, value in enumerate(registers):
                if value:
                    key = (month, resource, position, value)
                    counters[key] = counters.get(key, 0) + 1
        conn.execute(
            "INSERT INTO merged_summaries VALUES (?, ?, ?, '')",
            (month, resource, json.dumps(merged, separators=(",", ":"))),
        )
    conn.executemany(
        "INSERT INTO register_counts VALUES (?, ?, ?, ?, ?)",
        [key + (cnt,) for key, cnt in counters.items()],
    )
    conn.commit()
    conn.close()


def _expected_max(db, month, resource):
    conn = sqlite3.connect(db.path)
    rows = [
        json.loads(row[0])
        for row in conn.execute(
            "SELECT registers FROM branch_reports WHERE month = ? AND resource = ?",
            (month, resource),
        )
    ]
    conn.close()
    if not rows:
        return ZERO
    return [max(values) for values in zip(*rows)]


def test_legacy_database_is_compacted_and_only_new_format_remains(tmp_path):
    path = str(tmp_path / "legacy.sqlite3")
    _build_legacy_database(path)

    db = Database(path)
    assert db.counts_format() == "legacy"

    result = db.compact_counts()
    assert result.completed is True
    assert result.counts_converted == 3
    assert result.legacy_remaining == 0
    assert db.counts_format() == "compact"

    conn = sqlite3.connect(path)
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    )}
    kind = conn.execute(
        "SELECT DISTINCT typeof(registers) FROM register_counts"
    ).fetchall()
    merged_kind = conn.execute(
        "SELECT DISTINCT typeof(registers) FROM merged_summaries"
    ).fetchall()
    conn.close()
    assert "register_counts_legacy" not in tables
    assert kind == [("blob",)]
    assert merged_kind == [("blob",)]

    for group in range(3):
        month, resource = _group_key(group)
        assert db.get_merged(month, resource)["registers"] == _expected_max(
            db, month, resource
        )

    # 整理后可继续换报/撤回，合并仍正确
    db.upsert_report("2020-01", "ER-0000", "LIB-0", regs({0: 58}))
    assert db.get_merged("2020-01", "ER-0000")["registers"] == _expected_max(
        db, "2020-01", "ER-0000"
    )
    db.withdraw_report("2020-01", "ER-0000", "LIB-0")
    assert db.get_merged("2020-01", "ER-0000")["registers"] == _expected_max(
        db, "2020-01", "ER-0000"
    )


def test_writes_during_compaction_are_kept(tmp_path):
    path = str(tmp_path / "legacy.sqlite3")
    _build_legacy_database(path, groups=3)
    db = Database(path)

    # 整理前对同一月份、资源换报/撤回：写入路径已把该键转为紧凑格式
    db.upsert_report("2020-01", "ER-0000", "LIB-9", regs({0: 58, 1: 40}))
    db.withdraw_report("2020-01", "ER-0000", "LIB-0")

    # 分范围整理：只整理 2020-01/ER-0001，其余组仍在旧表
    result = db.compact_counts("2020-01", "ER-0001")
    assert result.completed is True
    assert result.legacy_remaining == 2
    assert db.counts_format() == "legacy"

    # 整理后其余旧键仍可正常换报/撤回
    db.upsert_report("2020-01", "ER-0002", "LIB-7", regs({5: 33}))
    db.withdraw_report("2020-01", "ER-0002", "LIB-7")

    result = db.compact_counts()
    assert result.completed is True
    assert db.counts_format() == "compact"
    for group in range(3):
        month, resource = _group_key(group)
        assert db.get_merged(month, resource)["registers"] == _expected_max(
            db, month, resource
        )


def test_compaction_failure_keeps_committed_state_and_can_retry(
    tmp_path, monkeypatch
):
    path = str(tmp_path / "legacy.sqlite3")
    _build_legacy_database(path, groups=5)
    db = Database(path)

    # 让第一批成功提交、第二批失败：仅按批次提交，失败批整体回滚
    real_commit = Database._commit
    calls = {"n": 0}

    def flaky_commit(conn):
        calls["n"] += 1
        if calls["n"] == 2:
            raise sqlite3.OperationalError("simulated commit failure")
        return real_commit(conn)

    monkeypatch.setattr(Database, "_commit", staticmethod(flaky_commit))
    with pytest.raises(sqlite3.OperationalError):
        db.compact_counts(batch_size=2)
    monkeypatch.undo()

    # 已提交的一批保留为紧凑格式；格式判定仍为 legacy，可重新整理
    assert db.counts_format() == "legacy"
    conn = sqlite3.connect(path)
    compact_rows = conn.execute(
        "SELECT COUNT(*) FROM register_counts"
    ).fetchone()[0]
    legacy_rows = conn.execute(
        "SELECT COUNT(*) FROM register_counts_legacy"
    ).fetchone()[0]
    conn.close()
    assert compact_rows >= 1
    assert legacy_rows > 0

    # 失败前已提交的换报计数仍生效
    month, resource = _group_key(0)
    before = _expected_max(db, month, resource)
    assert db.get_merged(month, resource)["registers"] == before

    result = db.compact_counts(batch_size=2)
    assert result.completed is True
    assert db.counts_format() == "compact"
    for group in range(5):
        month, resource = _group_key(group)
        assert db.get_merged(month, resource)["registers"] == _expected_max(
            db, month, resource
        )


def test_compaction_counts_changes_even_if_merged_value_unchanged(tmp_path):
    """最大值由别馆保持时，本馆换报/撤回的计数变动仍要纳入整理。"""
    path = str(tmp_path / "legacy.sqlite3")
    _build_legacy_database(path, groups=1)
    db = Database(path)
    month, resource = _group_key(0)

    # 位置 0 的最大值由某馆保持；给一个原本 0 贡献的馆报送较低值，
    # 合并值不变，但直方图必须记录该馆的贡献，撤回后也不能残留。
    before = db.get_merged(month, resource)["registers"]
    db.upsert_report(month, resource, "LIB-LOW", regs({50: 1}))
    db.upsert_report(month, resource, "LIB-LOW2", regs({51: 1}))
    db.withdraw_report(month, resource, "LIB-LOW")
    assert db.get_merged(month, resource)["registers"] == [
        max(a, b) for a, b in zip(before, regs({51: 1}))
    ]

    # 另取一个旧库副本，仅在整理前做同样操作，再整理，结果应一致
    path2 = str(tmp_path / "legacy2.sqlite3")
    _build_legacy_database(path2, groups=1)
    db2 = Database(path2)
    db2.upsert_report(month, resource, "LIB-LOW", regs({50: 1}))
    db2.upsert_report(month, resource, "LIB-LOW2", regs({51: 1}))
    db2.withdraw_report(month, resource, "LIB-LOW")
    db2.compact_counts()
    assert db2.get_merged(month, resource)["registers"] == db.get_merged(
        month, resource
    )["registers"]


def test_http_compact_endpoint(client):
    assert client.get("/maintenance/status").get_json()["counts_format"] == "compact"
    r = client.post("/maintenance/compact", json={})
    assert r.status_code == 200
    body = r.get_json()
    assert body["completed"] is True
    assert body["counts_format"] == "compact"
    assert body["legacy_remaining"] == 0
