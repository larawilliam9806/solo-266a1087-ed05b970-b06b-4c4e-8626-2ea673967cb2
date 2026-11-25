"""SQLite 存档与合并维护（紧凑格式）。

表结构
------
meta
    库级元数据。``compact_counts = 1`` 表示旧计数已全部整理完毕，
    库中只保留紧凑格式；为 0 或缺失表示仍有旧格式计数待整理。

branch_reports
    各馆按 (月份, 资源号, 馆号) 保存的现行 HLL 摘要（128 个寄存器）。
    换报即整行覆盖，撤回即删除。这是各馆唯一的权威副本。

register_counts
    按 (月份, 资源号) 每个键一行，``registers`` 列保存该键逐位置取值
    直方图的紧凑 BLOB（见 ``app.compact``）。它是各馆现行摘要逐位置
    最大值的增量表示：换报/撤回时只读取本馆旧摘要做差分，改完本键的
    BLOB 再逐位置取最大取值得到合并摘要，全程不遍历其他馆的摘要。
    最大值馆撤回或降值时，次大值可直接从直方图恢复。

register_counts_legacy
    仅在旧库升级过程中存在：旧版按 (位置, 取值) 逐行保存的直方图。
    整理程序按 (月份, 资源号) 把它合并成一行紧凑 BLOB，成功后删除
    旧行；全部整理完毕后删除该表。

merged_summaries
    按 (月份, 资源号) 保存合并摘要的 6 bit 紧凑编码，与馆报在同一事务
    内一并生效。旧库中的 JSON 文本行由整理程序改写为紧凑编码。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any, NamedTuple

from .compact import (
    decode_histogram,
    decode_merged,
    encode_histogram,
    encode_merged,
)
from .errors import NotFoundError
from .hll import (
    MAX_REGISTER_VALUE,
    REGISTER_COUNT,
    validate_identifier,
    validate_month,
    validate_registers,
)

_LEGACY_TABLE = "register_counts_legacy"

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS branch_reports (
    month      TEXT NOT NULL,
    resource   TEXT NOT NULL,
    branch     TEXT NOT NULL,
    registers  TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (month, resource, branch)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS register_counts (
    month      TEXT NOT NULL,
    resource   TEXT NOT NULL,
    registers  BLOB NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (month, resource)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS merged_summaries (
    month      TEXT NOT NULL,
    resource   TEXT NOT NULL,
    registers  BLOB NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (month, resource)
) WITHOUT ROWID;
"""

COMPACT_META_KEY = "compact_counts"

_BUSY_RETRIES = 8
_BUSY_BACKOFF = 0.02  # 秒，指数退避：0.02, 0.04, ...
_COMPACT_BATCH = 200  # 每批提交的最大键数


def _schema_statements() -> list[str]:
    """SCHEMA 拆成单条 DDL，供显式事务内逐条执行（executescript 会隐式提交）。

    PRAGMA 由 ``_connect`` 负责，事务内不能再切换 journal_mode，故跳过。
    """
    statements: list[str] = []
    for chunk in SCHEMA.split(";"):
        statement = chunk.strip()
        if statement and not statement.upper().startswith("PRAGMA"):
            statements.append(statement)
    return statements


class CompactResult(NamedTuple):
    """``compact_counts`` 的返回值，便于接口原样输出。"""

    completed: bool  # 本次请求范围内是否已无旧格式数据
    counts_converted: int
    merged_converted: int
    legacy_remaining: int  # 全库尚未整理的旧计数键数

    def as_dict(self) -> dict[str, Any]:
        result = self._asdict()
        result["completed"] = bool(result["completed"])
        return result


def _empty_histogram() -> list[dict[int, int]]:
    return [{} for _ in range(REGISTER_COUNT)]


def _histogram_from_legacy(rows: list[sqlite3.Row]) -> list[dict[int, int]]:
    histogram = _empty_histogram()
    for row in rows:
        position = row["position"]
        value = row["value"]
        cnt = row["cnt"]
        if not 0 <= position < REGISTER_COUNT:
            raise ValueError(f"旧格式计数行位置 {position} 非法")
        if not 1 <= value <= MAX_REGISTER_VALUE:
            raise ValueError(f"旧格式计数行取值 {value} 非法")
        if cnt <= 0:
            raise ValueError(f"旧格式计数行计数 {cnt} 非法")
        counts = histogram[position]
        counts[value] = counts.get(value, 0) + cnt
    return histogram


def _decode_merged_value(value: Any) -> list[int]:
    """合并摘要历史行可能是紧凑 BLOB，也可能是旧版 JSON 文本。"""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return decode_merged(value)
    return json.loads(value)


class Database:
    def __init__(self, path: str):
        self.path = path
        self._init_lock = threading.Lock()
        self._initialized = False
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,  # 事务完全由显式 BEGIN/COMMIT 控制
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _ensure_schema(self) -> None:
        """建表；旧库升级时把旧计数表改名保留，等待整理。"""
        with self._init_lock:
            if self._initialized:
                return
            conn = self._connect()
            try:
                self._transaction(conn)
                try:
                    columns = self._table_columns(conn, "register_counts")
                    if columns and "registers" not in columns:
                        # 旧结构 (position, value, cnt)：保留数据，改名待整理。
                        # 必须在建新表之前改名：CREATE IF NOT EXISTS 不会
                        # 改动已存在的旧表。
                        conn.execute(
                            f"ALTER TABLE register_counts RENAME TO {_LEGACY_TABLE}"
                        )
                    for statement in _schema_statements():
                        conn.execute(statement)
                    legacy = self._table_exists(conn, _LEGACY_TABLE)
                    self._set_compact_flag(conn, not legacy)
                    self._commit(conn)
                except BaseException:
                    conn.rollback()
                    raise
            finally:
                conn.close()
            self._initialized = True

    # -- 事务工具 -------------------------------------------------------

    @staticmethod
    def _commit(conn: sqlite3.Connection) -> None:
        """单独抽出便于测试注入提交失败；正常路径即 SQLite 提交。"""
        conn.commit()

    def _transaction(self, conn: sqlite3.Connection) -> sqlite3.Cursor:
        """开启写事务，遇锁按退避重试；BEGIN IMMEDIATE 保证写操作串行化。"""
        last_error: Exception | None = None
        for attempt in range(_BUSY_RETRIES):
            try:
                return conn.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:  # database is locked / busy
                last_error = exc
                time.sleep(_BUSY_BACKOFF * (2**attempt))
        assert last_error is not None
        raise last_error

    # -- 表结构探测 -----------------------------------------------------

    @staticmethod
    def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
        if not Database._table_exists(conn, table):
            return set()
        return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
        return row is not None

    @classmethod
    def _legacy_pending(cls, conn: sqlite3.Connection) -> bool:
        return cls._table_exists(conn, _LEGACY_TABLE)

    @staticmethod
    def _set_compact_flag(conn: sqlite3.Connection, compact: bool) -> None:
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            (COMPACT_META_KEY, "1" if compact else "0"),
        )

    def counts_format(self) -> str:
        """返回当前计数格式：``compact`` 或 ``legacy``。

        只有旧计数全部迁走、且合并摘要都已是紧凑编码时才报告 compact。
        """
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT value FROM meta WHERE key = ?", (COMPACT_META_KEY,)
            ).fetchone()
            compact = row is not None and row["value"] == "1"
            if (
                not compact
                or self._legacy_pending(conn)
                or len(self._text_merged_keys(conn, None, None, 1))
            ):
                return "legacy"
            return "compact"
        finally:
            conn.close()

    # -- 内部：单键直方图读写（兼容旧格式） ------------------------------

    @classmethod
    def _load_histogram(
        cls, conn: sqlite3.Connection, month: str, resource: str
    ) -> list[dict[int, int]]:
        """读取某键直方图；旧格式行会在本事务内即时迁移为紧凑格式。"""
        row = conn.execute(
            "SELECT registers FROM register_counts WHERE month = ? AND resource = ?",
            (month, resource),
        ).fetchone()
        if row is not None:
            return decode_histogram(row["registers"])
        if cls._legacy_pending(conn):
            rows = conn.execute(
                "SELECT position, value, cnt FROM "
                f"{_LEGACY_TABLE} WHERE month = ? AND resource = ?",
                (month, resource),
            ).fetchall()
            if rows:
                return _histogram_from_legacy(rows)
        return _empty_histogram()

    @classmethod
    def _drop_legacy_key(
        cls, conn: sqlite3.Connection, month: str, resource: str
    ) -> None:
        if cls._legacy_pending(conn):
            conn.execute(
                f"DELETE FROM {_LEGACY_TABLE} WHERE month = ? AND resource = ?",
                (month, resource),
            )

    @staticmethod
    def _store_histogram(
        conn: sqlite3.Connection,
        month: str,
        resource: str,
        histogram: list[dict[int, int]],
    ) -> None:
        """写入紧凑 BLOB；直方图全空时删除本键行，不占任何页。"""
        if any(histogram):
            conn.execute(
                "INSERT INTO register_counts "
                "(month, resource, registers, updated_at) "
                "VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) "
                "ON CONFLICT (month, resource) DO UPDATE SET "
                "registers = excluded.registers, updated_at = excluded.updated_at",
                (month, resource, sqlite3.Binary(encode_histogram(histogram))),
            )
        else:
            conn.execute(
                "DELETE FROM register_counts WHERE month = ? AND resource = ?",
                (month, resource),
            )

    @staticmethod
    def _apply_changes(
        histogram: list[dict[int, int]], deltas: dict[int, dict[int, int]]
    ) -> None:
        """就地施加 {位置: {取值: 增减量}}；只改动传入的直方图。"""
        for position, changes in deltas.items():
            counts = histogram[position]
            for value, delta in changes.items():
                if value == 0 or delta == 0:
                    continue  # 取值 0 不占用计数，全馆无贡献时该位置自然为 0
                new_cnt = counts.get(value, 0) + delta
                if new_cnt < 0:
                    raise ValueError(
                        f"位置 {position} 取值 {value} 的计数将为负，"
                        "直方图与馆报不一致"
                    )
                if new_cnt == 0:
                    counts.pop(value, None)
                else:
                    counts[value] = new_cnt

    @classmethod
    def _refresh_merged(
        cls,
        conn: sqlite3.Connection,
        month: str,
        resource: str,
        histogram: list[dict[int, int]],
    ) -> list[int]:
        """从直方图逐位置取最大值，落盘紧凑合并摘要，与馆报同事务生效。"""
        merged = [max(counts) if counts else 0 for counts in histogram]
        cls._store_merged(conn, month, resource, merged)
        return merged

    @staticmethod
    def _store_merged(
        conn: sqlite3.Connection,
        month: str,
        resource: str,
        merged: list[int],
    ) -> None:
        conn.execute(
            "INSERT INTO merged_summaries (month, resource, registers, updated_at) "
            "VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) "
            "ON CONFLICT (month, resource) DO UPDATE SET "
            "registers = excluded.registers, updated_at = excluded.updated_at",
            (month, resource, sqlite3.Binary(encode_merged(merged))),
        )

    # -- 对外操作 --------------------------------------------------------

    def upsert_report(
        self, month: str, resource: str, branch: str, registers: list[int]
    ) -> dict[str, Any]:
        """上传或换报：整行覆盖本馆摘要，并增量维护合并结果。"""
        month = validate_month(month)
        resource = validate_identifier("resource", resource)
        branch = validate_identifier("branch", branch)
        registers = validate_registers(registers)

        conn = self._connect()
        try:
            self._transaction(conn)
            try:
                old_row = conn.execute(
                    "SELECT registers FROM branch_reports "
                    "WHERE month = ? AND resource = ? AND branch = ?",
                    (month, resource, branch),
                ).fetchone()
                old_registers = (
                    json.loads(old_row["registers"]) if old_row is not None else None
                )

                deltas: dict[int, dict[int, int]] = {}
                for position, new_value in enumerate(registers):
                    old_value = (
                        old_registers[position] if old_registers is not None else 0
                    )
                    if old_value == new_value:
                        continue
                    change: dict[int, int] = {}
                    if old_value:
                        change[old_value] = -1
                    if new_value:
                        change[new_value] = change.get(new_value, 0) + 1
                    if change:
                        deltas[position] = change

                conn.execute(
                    "INSERT INTO branch_reports (month, resource, branch, registers, updated_at) "
                    "VALUES (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) "
                    "ON CONFLICT (month, resource, branch) DO UPDATE SET "
                    "registers = excluded.registers, updated_at = excluded.updated_at",
                    (
                        month,
                        resource,
                        branch,
                        json.dumps(registers, separators=(",", ":")),
                    ),
                )

                # 不论合并值是否变化，本馆这次提交的计数变动都要入账：
                # 直方图更新与馆报、合并摘要处于同一事务。
                histogram = self._load_histogram(conn, month, resource)
                self._apply_changes(histogram, deltas)
                self._drop_legacy_key(conn, month, resource)  # 本键转为紧凑格式
                self._store_histogram(conn, month, resource, histogram)
                merged = self._refresh_merged(conn, month, resource, histogram)
                self._commit(conn)
            except BaseException:
                conn.rollback()
                raise
        finally:
            conn.close()

        return {
            "month": month,
            "resource": resource,
            "branch": branch,
            "registers": registers,
            "merged": merged,
            "replaced": old_registers is not None,
        }

    def withdraw_report(
        self, month: str, resource: str, branch: str
    ) -> dict[str, Any]:
        """撤回本馆摘要：删除本馆行并增量维护合并结果，不触碰其他馆摘要。"""
        month = validate_month(month)
        resource = validate_identifier("resource", resource)
        branch = validate_identifier("branch", branch)

        conn = self._connect()
        try:
            self._transaction(conn)
            try:
                old_row = conn.execute(
                    "SELECT registers FROM branch_reports "
                    "WHERE month = ? AND resource = ? AND branch = ?",
                    (month, resource, branch),
                ).fetchone()
                if old_row is None:
                    raise NotFoundError(
                        f"馆 {branch} 在 {month} 对资源 {resource} 没有已保存的报送，无法撤回"
                    )
                old_registers = json.loads(old_row["registers"])

                deltas: dict[int, dict[int, int]] = {}
                for position, old_value in enumerate(old_registers):
                    if old_value:
                        deltas[position] = {old_value: -1}

                conn.execute(
                    "DELETE FROM branch_reports "
                    "WHERE month = ? AND resource = ? AND branch = ?",
                    (month, resource, branch),
                )
                histogram = self._load_histogram(conn, month, resource)
                self._apply_changes(histogram, deltas)
                self._drop_legacy_key(conn, month, resource)
                self._store_histogram(conn, month, resource, histogram)
                merged = self._refresh_merged(conn, month, resource, histogram)
                self._commit(conn)
            except BaseException:
                conn.rollback()
                raise
        finally:
            conn.close()

        return {
            "month": month,
            "resource": resource,
            "branch": branch,
            "merged": merged,
        }

    def get_merged(self, month: str, resource: str) -> dict[str, Any]:
        """读取已保存的合并摘要（无任何报送记录时 404）。"""
        month = validate_month(month)
        resource = validate_identifier("resource", resource)

        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT registers, updated_at FROM merged_summaries "
                "WHERE month = ? AND resource = ?",
                (month, resource),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise NotFoundError(f"{month} 资源 {resource} 尚无任何报送，没有合并摘要")
        return {
            "month": month,
            "resource": resource,
            "registers": _decode_merged_value(row["registers"]),
            "updated_at": row["updated_at"],
        }

    def get_report(
        self, month: str, resource: str, branch: str
    ) -> dict[str, Any]:
        """读取某馆的现行报送（供换报前确认，可选辅助接口）。"""
        month = validate_month(month)
        resource = validate_identifier("resource", resource)
        branch = validate_identifier("branch", branch)

        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT registers, updated_at FROM branch_reports "
                "WHERE month = ? AND resource = ? AND branch = ?",
                (month, resource, branch),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise NotFoundError(
                f"馆 {branch} 在 {month} 对资源 {resource} 没有已保存的报送"
            )
        return {
            "month": month,
            "resource": resource,
            "branch": branch,
            "registers": json.loads(row["registers"]),
            "updated_at": row["updated_at"],
        }

    # -- 旧计数整理 ------------------------------------------------------

    def compact_counts(
        self,
        month: str | None = None,
        resource: str | None = None,
        batch_size: int = _COMPACT_BATCH,
    ) -> CompactResult:
        """把旧格式计数整理为紧凑格式；可按月份、资源号限定范围。

        - 每批是独立事务：失败的一批整体回滚，此前已提交的批次保留，
          届时已提交的计数与生效格式不变，可直接重新调用继续整理；
        - 整理期间针对同一月份、资源的换报/撤回照常入账（写入路径会把
          本键即时转为紧凑格式），再次整理会纳入这些已提交的变动；
        - 不限定范围且旧行全部整理完毕后删除旧表，库中只留新格式。
        """
        if batch_size <= 0:
            raise ValueError("batch_size 必须为正整数")
        if month is not None:
            month = validate_month(month)
        if resource is not None:
            resource = validate_identifier("resource", resource)
        if resource is not None and month is None:
            raise ValueError("按资源号整理时请同时给出月份")

        counts_converted = 0
        merged_converted = 0
        conn = self._connect()
        try:
            while True:
                self._transaction(conn)
                try:
                    legacy_pending = self._legacy_pending(conn)
                    if not legacy_pending and not self._has_text_merged(
                        conn, month, resource
                    ):
                        self._set_compact_flag(conn, True)
                        self._commit(conn)
                        return CompactResult(
                            True, counts_converted, merged_converted, 0
                        )

                    progress = 0
                    if legacy_pending:
                        keys = self._legacy_keys(
                            conn, month, resource, batch_size
                        )
                        for key_month, key_resource in keys:
                            rows = conn.execute(
                                "SELECT position, value, cnt FROM "
                                f"{_LEGACY_TABLE} "
                                "WHERE month = ? AND resource = ?",
                                (key_month, key_resource),
                            ).fetchall()
                            histogram = _histogram_from_legacy(rows)
                            # 旧合并摘要若是文本，本次重写即完成格式切换；
                            # 同时保留换报/撤回期间已提交的最新合并值。
                            if self._merged_is_text(
                                conn, key_month, key_resource
                            ):
                                merged_converted += 1
                            conn.execute(
                                f"DELETE FROM {_LEGACY_TABLE} "
                                "WHERE month = ? AND resource = ?",
                                (key_month, key_resource),
                            )
                            self._store_histogram(
                                conn, key_month, key_resource, histogram
                            )
                            self._refresh_merged(
                                conn, key_month, key_resource, histogram
                            )
                            counts_converted += 1
                        progress += len(keys)

                    # 同一范围里已切成紧凑格式、但合并摘要仍是旧 JSON 的行，
                    # 即使逐位置最大值没变，也统一改写为新格式。
                    converted = self._convert_text_merged(
                        conn, month, resource, batch_size
                    )
                    merged_converted += converted
                    progress += converted

                    # 只要全库旧计数行都已迁走，就删除旧表并标记
                    # 「只保留紧凑格式」；分范围整理也不会把格式改回去。
                    if self._legacy_key_count(conn, None, None) == 0:
                        conn.execute(f"DROP TABLE IF EXISTS {_LEGACY_TABLE}")
                        self._set_compact_flag(conn, True)

                    legacy_remaining = self._legacy_key_count(conn, None, None)
                    scoped_legacy = self._legacy_key_count(conn, month, resource)
                    scoped_text = len(
                        self._text_merged_keys(conn, month, resource, 1)
                    )
                    global_text = (
                        len(self._text_merged_keys(conn, None, None, 1))
                        if month is None and resource is None
                        else 0
                    )
                    self._commit(conn)
                except BaseException:
                    conn.rollback()
                    raise

                if scoped_legacy == 0 and scoped_text == 0 and global_text == 0:
                    return CompactResult(
                        True, counts_converted, merged_converted, legacy_remaining
                    )
                if progress == 0:  # pragma: no cover - 防御性兜底
                    raise RuntimeError("整理未取得进展，请检查旧计数数据")
        finally:
            conn.close()

    @staticmethod
    def _merged_is_text(
        conn: sqlite3.Connection, month: str, resource: str
    ) -> bool:
        row = conn.execute(
            "SELECT typeof(registers) AS kind FROM merged_summaries "
            "WHERE month = ? AND resource = ?",
            (month, resource),
        ).fetchone()
        return row is not None and row["kind"] == "text"

    @staticmethod
    def _legacy_keys(
        conn: sqlite3.Connection,
        month: str | None,
        resource: str | None,
        limit: int,
    ) -> list[tuple[str, str]]:
        sql = f"SELECT DISTINCT month, resource FROM {_LEGACY_TABLE}"
        clauses: list[str] = []
        params: list[Any] = []
        if month is not None:
            clauses.append("month = ?")
            params.append(month)
        if resource is not None:
            clauses.append("resource = ?")
            params.append(resource)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY month, resource LIMIT ?"
        params.append(limit)
        return [
            (row["month"], row["resource"]) for row in conn.execute(sql, params)
        ]

    @classmethod
    def _legacy_key_count(
        cls,
        conn: sqlite3.Connection,
        month: str | None,
        resource: str | None,
    ) -> int:
        if not cls._legacy_pending(conn):
            return 0
        inner = f"SELECT month, resource FROM {_LEGACY_TABLE}"
        clauses: list[str] = []
        params: list[Any] = []
        if month is not None:
            clauses.append("month = ?")
            params.append(month)
        if resource is not None:
            clauses.append("resource = ?")
            params.append(resource)
        if clauses:
            inner += " WHERE " + " AND ".join(clauses)
        return int(
            conn.execute(f"SELECT COUNT(*) FROM ({inner} GROUP BY month, resource)", params)
            .fetchone()[0]
        )

    @staticmethod
    def _has_text_merged(
        conn: sqlite3.Connection,
        month: str | None,
        resource: str | None,
    ) -> bool:
        return Database._text_merged_keys(conn, month, resource, 1) != []

    @staticmethod
    def _text_merged_keys(
        conn: sqlite3.Connection,
        month: str | None,
        resource: str | None,
        limit: int,
    ) -> list[tuple[str, str]]:
        sql = (
            "SELECT month, resource FROM merged_summaries "
            "WHERE typeof(registers) = 'text'"
        )
        params: list[Any] = []
        if month is not None:
            sql += " AND month = ?"
            params.append(month)
        if resource is not None:
            sql += " AND resource = ?"
            params.append(resource)
        sql += " ORDER BY month, resource LIMIT ?"
        params.append(limit)
        return [
            (row["month"], row["resource"]) for row in conn.execute(sql, params)
        ]

    @classmethod
    def _convert_text_merged(
        cls,
        conn: sqlite3.Connection,
        month: str | None,
        resource: str | None,
        limit: int,
    ) -> int:
        keys = cls._text_merged_keys(conn, month, resource, limit)
        for key_month, key_resource in keys:
            row = conn.execute(
                "SELECT registers FROM merged_summaries "
                "WHERE month = ? AND resource = ?",
                (key_month, key_resource),
            ).fetchone()
            merged = _decode_merged_value(row["registers"])
            cls._store_merged(conn, key_month, key_resource, merged)
        return len(keys)
