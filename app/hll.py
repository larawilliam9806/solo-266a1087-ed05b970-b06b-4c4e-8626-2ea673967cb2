"""HyperLogLog-128 寄存器口径与入参校验。

各分馆必须使用同一哈希口径，本模块给出约定的参考实现：
对读者唯一标识做 SHA-256，取前 8 字节（64 bit），低 7 bit 选寄存器
（共 128 个），其余 57 bit 从最低位起计算前导 1 的个数（rank，1～58）。
寄存器取值范围为 0～58，0 表示该位置尚无贡献。
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable

from .errors import ValidationError

REGISTER_COUNT = 128
MAX_REGISTER_VALUE = 58

_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
# 馆号 / 资源号只允许字母数字及 : _ -，长度受限，避免被注入类字符污染。
_ID_RE = re.compile(r"^[A-Za-z0-9:_-]{1,64}$")


def hll_register(reader_id: str) -> tuple[int, int]:
    """按统一哈希口径计算读者标识对应的 (寄存器位置, rank)。"""
    digest = hashlib.sha256(reader_id.encode("utf-8")).digest()
    word = int.from_bytes(digest[:8], "big")
    position = word & (REGISTER_COUNT - 1)  # 低 7 bit
    remaining = word >> 7  # 剩余 57 bit
    rank = 1
    while rank <= MAX_REGISTER_VALUE and (remaining & 1) == 0:
        rank += 1
        remaining >>= 1
    return position, rank


def build_summary(reader_ids: Iterable[str]) -> list[int]:
    """把一批读者标识汇总成 128 个寄存器的 HLL 摘要（供示例/测试使用）。"""
    registers = [0] * REGISTER_COUNT
    for reader_id in reader_ids:
        position, rank = hll_register(reader_id)
        if rank > registers[position]:
            registers[position] = rank
    return registers


def validate_month(value: object) -> str:
    if not isinstance(value, str) or not _MONTH_RE.match(value):
        raise ValidationError("month 必须是 YYYY-MM 形式的月份字符串，例如 2026-11")
    return value


def validate_identifier(field: str, value: object) -> str:
    if not isinstance(value, str) or not _ID_RE.match(value):
        raise ValidationError(
            f"{field} 必须是 1～64 位字符串，只允许字母、数字、冒号、下划线和短横线"
        )
    return value


def validate_registers(value: object) -> list[int]:
    if not isinstance(value, list) or len(value) != REGISTER_COUNT:
        raise ValidationError(f"registers 必须是长度恰好为 {REGISTER_COUNT} 的数组")
    for index, item in enumerate(value):
        # bool 是 int 的子类，显式排除，避免 True/False 混入。
        if isinstance(item, bool) or not isinstance(item, int):
            raise ValidationError(f"registers[{index}] 必须是整数")
        if not 0 <= item <= MAX_REGISTER_VALUE:
            raise ValidationError(
                f"registers[{index}]={item} 超出允许范围 0～{MAX_REGISTER_VALUE}"
            )
    return list(value)
