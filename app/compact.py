"""寄存器计数与合并摘要的紧凑二进制编码。

SQLite 中不再为每个 (位置, 取值) 保存一行，而是把整组
``(月份, 资源号)`` 的取值直方图编码成一个 BLOB：

- 直方图：先写 16 字节位图，标记哪些位置有非零取值；再按位置升序
  写该位置的非零取值个数，然后按取值升序写 ``取值(1 字节) +
  馆数(varint)``。全零摘要只有版本号 + 全零位图，共 17 字节。
- 合并摘要：128 个取值 0～58 的寄存器按 6 bit 定长打包（96 字节），
  前缀 1 字节版本号。

两种编码都带版本号；解码时做完整边界检查，损坏数据直接报错，而不是
静默产生错误的统计。
"""

from __future__ import annotations

from .hll import MAX_REGISTER_VALUE, REGISTER_COUNT

HISTOGRAM_VERSION = 1
MERGED_VERSION = 1
MERGED_PAYLOAD_SIZE = (REGISTER_COUNT * 6 + 7) // 8  # 96
_BITMAP_SIZE = REGISTER_COUNT // 8  # 16


def _write_varint(out: bytearray, value: int) -> None:
    if value < 0:
        raise ValueError("varint 不能为负数")
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        if offset >= len(data):
            raise ValueError("紧凑直方图数据被截断")
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, offset
        shift += 7
        if shift > 63:
            raise ValueError("紧凑直方图的 varint 过长")


def encode_histogram(histogram: list[dict[int, int]]) -> bytes:
    """把逐位置取值直方图编码为紧凑 BLOB。

    ``histogram[position]`` 是 ``{取值: 馆数}``，取值 0 不占存储。
    """
    if len(histogram) != REGISTER_COUNT:
        raise ValueError(f"直方图必须包含 {REGISTER_COUNT} 个位置")

    bitmap = bytearray(_BITMAP_SIZE)
    body = bytearray()
    for position, counts in enumerate(histogram):
        entries = sorted(
            (int(value), int(cnt))
            for value, cnt in counts.items()
            if value != 0 and cnt != 0
        )
        if not entries:
            continue
        if len(entries) > MAX_REGISTER_VALUE:
            raise ValueError(f"位置 {position} 的取值种类超过上限")
        bitmap[position // 8] |= 1 << (position % 8)
        _write_varint(body, len(entries))
        for value, cnt in entries:
            if not 1 <= value <= MAX_REGISTER_VALUE:
                raise ValueError(f"位置 {position} 的取值 {value} 超出范围")
            if cnt <= 0:
                raise ValueError(f"位置 {position} 取值 {value} 的计数必须为正")
            body.append(value)
            _write_varint(body, cnt)

    return bytes(bytearray([HISTOGRAM_VERSION]) + bitmap + body)


def decode_histogram(blob: bytes | memoryview) -> list[dict[int, int]]:
    data = bytes(blob)
    minimum = 1 + _BITMAP_SIZE
    if len(data) < minimum or data[0] != HISTOGRAM_VERSION:
        raise ValueError("不支持的紧凑直方图版本或长度")
    bitmap = data[1:minimum]
    offset = minimum
    histogram: list[dict[int, int]] = []
    for position in range(REGISTER_COUNT):
        if not bitmap[position // 8] & (1 << (position % 8)):
            histogram.append({})
            continue
        entry_count, offset = _read_varint(data, offset)
        if not 1 <= entry_count <= MAX_REGISTER_VALUE:
            raise ValueError(f"位置 {position} 的取值种类非法")
        counts: dict[int, int] = {}
        previous = 0
        for _ in range(entry_count):
            if offset >= len(data):
                raise ValueError("紧凑直方图数据被截断")
            value = data[offset]
            offset += 1
            if not previous < value <= MAX_REGISTER_VALUE:
                raise ValueError(f"位置 {position} 的取值顺序或范围非法")
            previous = value
            cnt, offset = _read_varint(data, offset)
            if cnt <= 0:
                raise ValueError(f"位置 {position} 取值 {value} 的计数必须为正")
            counts[value] = cnt
        histogram.append(counts)
    if offset != len(data):
        raise ValueError("紧凑直方图带有无法识别的多余数据")
    return histogram


def encode_merged(registers: list[int]) -> bytes:
    """把 128 个 0～58 的寄存器按 6 bit 定长打包。"""
    if len(registers) != REGISTER_COUNT:
        raise ValueError(f"合并摘要必须包含 {REGISTER_COUNT} 个寄存器")
    out = bytearray([MERGED_VERSION])
    accumulator = 0
    bits = 0
    for value in registers:
        if isinstance(value, bool) or not 0 <= value <= MAX_REGISTER_VALUE:
            raise ValueError(f"寄存器取值 {value} 超出范围")
        accumulator |= value << bits
        bits += 6
        while bits >= 8:
            out.append(accumulator & 0xFF)
            accumulator >>= 8
            bits -= 8
    if bits:
        out.append(accumulator & 0xFF)
    return bytes(out)


def decode_merged(blob: bytes | memoryview) -> list[int]:
    data = bytes(blob)
    expected = 1 + MERGED_PAYLOAD_SIZE
    if len(data) != expected or data[0] != MERGED_VERSION:
        raise ValueError("紧凑合并摘要的长度或版本不正确")
    registers: list[int] = []
    accumulator = 0
    bits = 0
    offset = 1
    for _ in range(REGISTER_COUNT):
        while bits < 6:
            accumulator |= data[offset] << bits
            offset += 1
            bits += 8
        value = accumulator & 0x3F
        if value > MAX_REGISTER_VALUE:
            raise ValueError(f"紧凑合并摘要含越界寄存器值 {value}")
        registers.append(value)
        accumulator >>= 6
        bits -= 6
    return registers
