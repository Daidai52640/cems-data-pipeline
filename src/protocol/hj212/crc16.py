"""ANSI CRC16（HJ 212—2025 附录 A.1）。

标准原文（附录 A.1，PDF 第 31–32 页）给出的 C 实现：

    crc_reg = 0xFFFF;
    for (i = 0; i < usDataLen; i++) {
        crc_reg = (crc_reg >> 8) ^ puchMsg[i];
        for (j = 0; j < 8; j++) {
            check = crc_reg & 0x0001;
            crc_reg >>= 1;
            if (check == 0x0001) crc_reg ^= 0xA001;
        }
    }

即：初值 ``0xFFFF``、反射多项式 ``0xA001``、输入输出均反射、无最终异或。
本模块只实现这一个算法，不做"可配置多项式"的通用 CRC 框架——
HJ 212 只用这一种，且附录 A.1 明确规定"本标准采用 ANSI CRC16"。

校验码在报文中的存放顺序为**高字节到低字节**（附录 A.1），
即 ``"2200"`` 表示寄存器值 ``0x2200``。
"""

from __future__ import annotations

from typing import Final

CRC16_INIT: Final[int] = 0xFFFF
CRC16_POLY: Final[int] = 0xA001
CRC16_MASK: Final[int] = 0xFFFF
CRC_HEX_WIDTH: Final[int] = 4


def crc16(data: bytes) -> int:
    """返回 ANSI CRC16 校验寄存器值（0..0xFFFF）。"""
    crc = CRC16_INIT
    for byte in data:
        crc = ((crc >> 8) ^ byte) & CRC16_MASK
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ CRC16_POLY
            else:
                crc >>= 1
    return crc & CRC16_MASK


def crc16_hex(data: bytes) -> str:
    """返回报文里实际书写形式的 4 位大写十六进制校验码（高字节在前）。"""
    return f"{crc16(data):04X}"


def crc16_verify(data: bytes, expected_hex: str) -> bool:
    """校验 ``data`` 的 CRC 是否等于报文里携带的 ``expected_hex``。

    大小写不敏感；长度不是 4 位十六进制时直接判失败（不抛异常，
    便于解码器把"CRC 错误"当作一种可报告的结果而不是崩溃）。
    """
    if len(expected_hex) != CRC_HEX_WIDTH:
        return False
    try:
        expected = int(expected_hex, 16)
    except ValueError:
        return False
    return crc16(data) == expected
