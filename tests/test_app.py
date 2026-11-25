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
