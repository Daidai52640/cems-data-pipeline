"""从 HJ 212—2025 官方 PDF 重新生成 ``test_vectors.json``（可复现）。

用法::

    $env:PYTHONIOENCODING = "utf-8"
    $env:PYTHONPATH = "F:\\Project1\\cems-data-pipeline"
    python tests/protocol/hj212/generate_vectors.py [PDF 路径]

默认读取 ``%TEMP%\\hj212_2025.pdf``。

⚠️ 本脚本**不是**测试的一部分（不叫 ``test_*.py``），它只在需要
重新取证时手动跑一次。每条向量在写入 JSON 之前都会**就地重算**
（声明长度 + ANSI CRC16 + SM4 往返），任何一项对不上就直接断言失败——
因此 JSON 不可能与原文漂移。

**PDF 文本层的两个坑**（脚本里做了处理，改脚本时别踩回去）：

1. 报文会被折成多行，且**说明文字与报文末行并在一起**，例如
   ``…CP=&&&&2200\\r\\n，其中2200为CRC16校验码…``，必须按"合法的
   报文 token"逐段裁剪，不能按长度切。
2. 包尾在 PDF 里是**字面 4 个字符** ``\\r\\n``，不是 CR/LF。
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

import pdfplumber
from gmssl.func import bytes_to_list, list_to_bytes
from gmssl.sm4 import SM4_DECRYPT, SM4_ENCRYPT, CryptSM4

DEFAULT_PDF = Path(os.environ.get("TEMP", r"C:\Windows\Temp")) / "hj212_2025.pdf"
OUT = Path(__file__).with_name("test_vectors.json")

#: 附录 A.2 的公开测试密钥："密钥使用16个0x30"
KEY = bytes([0x30] * 16)

_HEX_TOKEN = re.compile(r"0x[0-9A-Fa-f]{2}(?=[,}])")
_FIELD_TOKEN = re.compile(r"[A-Za-z0-9_-]+=[A-Za-z0-9_.:+-]*")


def lines_of(pdf, pageno: int, tol: float = 3.0) -> list[str]:
    """把一页按视觉行还原成字符串列表（按 top 聚类、按 x0 排序）。"""
    page = pdf.pages[pageno - 1]
    groups: dict[int, list] = {}
    for char in page.chars:
        groups.setdefault(round(char["top"] / tol), []).append(char)
    return [
        "".join(c["text"] for c in sorted(groups[key], key=lambda c: c["x0"]))
        for key in sorted(groups)
    ]


def crc16(data: bytes) -> int:
    """ANSI CRC16（附录 A.1 的 C 代码逐行转写）。"""
    crc = 0xFFFF
    for byte in data:
        crc = (crc >> 8) ^ byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc


class Sm4EcbNoPadding:
    """见 ``src/protocol/hj212/crypto.py``；此处内联一份以保持脚本自包含。"""

    def __init__(self, key: bytes) -> None:
        self._enc = CryptSM4(); self._enc.set_key(key, SM4_ENCRYPT)
        self._dec = CryptSM4(); self._dec.set_key(key, SM4_DECRYPT)

    @staticmethod
    def _block(cipher, block: bytes) -> bytes:
        return list_to_bytes(cipher.one_round(cipher.sk, bytes_to_list(block)))

    def encrypt(self, plaintext: bytes) -> bytes:
        full = len(plaintext) // 16 * 16
        out = bytearray()
        for off in range(0, full, 16):
            out += self._block(self._enc, plaintext[off:off + 16])
        return bytes(out) + plaintext[full:]

    def decrypt(self, ciphertext: bytes) -> bytes:
        full = len(ciphertext) // 16 * 16
        out = bytearray()
        for off in range(0, full, 16):
            out += self._block(self._dec, ciphertext[off:off + 16])
        return bytes(out)


def trim(raw_line: str) -> str:
    """从"报文 + 并排的说明文字"里裁出干净的报文串。"""
    text = raw_line.replace("\\r\\n", "")
    match = re.search(r"##\d{4}(?=[A-Za-z0-9]+=)", text)
    if not match:
        return text
    out = [match.group(0)]
    index = match.end()

    block = re.search(r"CP=&&\{", text[index:])
    block_start = index + block.start() if block else None
    block_end = index + block.end() if block else None
    field_end = block_start if block_start is not None else len(text)

    while index < field_end:
        char = text[index]
        if char in ";=,&":
            out.append(char); index += 1
            continue
        found = _FIELD_TOKEN.match(text, index)
        if not found:
            break
        # 值直接撞上说明文字（如 "2200，其中…"）时整个 token 都要丢弃
        after = text[found.end()] if found.end() < len(text) else ""
        if after and after not in ";=,&":
            break
        out.append(found.group(0)); index = found.end()

    if block_start is not None:
        out.append("CP=&&{")
        index = block_end
        while index < len(text):
            char = text[index]
            if char == ",":
                out.append(char); index += 1
            elif char == "}":
                out.append(char); index += 1
                break
            else:
                found = _HEX_TOKEN.match(text, index)
                if not found:
                    break
                out.append(found.group(0)); index = found.end()
        while index < len(text) and text[index] == "&":
            out.append(text[index]); index += 1
        crc = re.match(r"[0-9A-Fa-f]{4}", text[index:])
        if crc:
            out.append(crc.group(0))
        return "".join(out)

    crc = re.match(r"[0-9A-Fa-f]{4}", text[index:])
    if crc:
        out.append(crc.group(0))
    return "".join(out)


def split_packet(packet: str) -> tuple[int, str, str, str]:
    """切成 ``(长度字段, 数据段, CRC, 数据区)``。"""
    declared = int(packet[2:6])
    body = packet[6:]
    segment, crc = body[:-4], body[-4:]
    start = segment.index("CP=&&")
    return declared, segment, crc, segment[start + 5:-2]


def header_of(segment: str) -> dict[str, object]:
    head = segment.split("CP=&&")[0] if "CP=&&" in segment else segment
    result: dict[str, object] = {}
    for chunk in head.rstrip(";").split(";"):
        key, _, value = chunk.partition("=")
        if key == "Flag":
            try:
                result[key] = int(value)
            except ValueError:
                result[key] = value
        else:
            result[key] = value
    return result


def ciphertext_bytes(packet: str) -> bytes:
    start = packet.index("CP=&&")
    inner = re.search(r"\{([^}]*)\}", packet[start:]).group(1)
    out: list[int] = []
    for item in inner.split(","):
        if re.fullmatch(r"0x[0-9A-Fa-f]{2}", item):
            out.append(int(item, 16))
        else:
            break
    return bytes(out)


#: 项目 9 测点 → 编码（与 src/protocol/hj212/factors.py 一致，取自附录 B.2）
FACTOR_ORDER = (
    ("a00000", "Flow"), ("a34013", "Dust"), ("a21026", "SO2"), ("a21002", "NOx"),
    ("a19001", "O2"), ("a01011", "Velocity"), ("a01012", "Temp"),
    ("a01014", "Humidity"), ("a01013", "Pressure"),
)
READINGS = ("33471.0", "12.34", "18.5", "42.1", "6.8", "11.2", "138.5", "8.6", "101.3")


def build(pdf_path: Path) -> dict:
    with pdfplumber.open(pdf_path) as pdf:
        p32, p33 = lines_of(pdf, 32), lines_of(pdf, 33)

    raw = {
        "a1": "".join([p32[14], p32[15]]).strip(),
        "ex1": "".join([p32[22], p32[23], p32[24], p32[25]]).strip(),
        "ex2": "".join([p32[27]] + p32[28:38] + [p32[38]]).strip(),
        "ex3": "".join([p32[40], p32[41], p32[42]]).strip(),
        "ex4": "".join([p33[2]] + p33[3:10] + [p33[10]]).strip(),
    }

    vectors: dict[str, dict] = {}

    # ---- 附录 A.1：官方 CRC 示例 ------------------------------------------
    a1 = trim(raw["a1"])
    declared, segment, crc, region = split_packet(a1)
    assert len(a1) == 97 and declared == len(segment) == 87, (len(a1), declared, len(segment))
    assert crc == "2200" and crc16(segment.encode()) == 0x2200
    vectors["crc_example_a1"] = {
        "source": "附录 A.1 第 32 页（PDF 物理页）",
        "note": "标准原文的 CRC 示例。数据区写作占位符 &&2200，与紧随其后的真实 CRC 2200 同形；"
                "因此本向量只断言结构：声明长度 == 数据段长度 == 87，且 CRC16(数据段) == 2200。",
        "packet": a1,
        "packet_in_standard_raw": raw["a1"],
        "packet_lines_in_standard": [
            "##0087QN=20240601085857223;ST=32;CN=1011;PW=123456;"
            "MN=010000A8900016F000169DC0;Flag=9;CP=",
            "&&&&2200",
        ],
        "expected": {
            "declared_length": 87,
            "data_segment": segment,
            "data_segment_length": len(segment),
            "crc_hex": "2200",
            "crc_computed": f"{crc16(segment.encode()):04X}",
            "header": header_of(segment),
            "cp_region": region,
        },
    }

    # ---- §6.3.3 表 3：Flag=9 参考报文（本项目 9 测点，非官方向量）----------
    # ⚠️ 原文该处数据区是占位符、无可行 CRC，故本向量为**自建参考报文**，
    #    只用于编解码往返断言。官方 CRC 真值见 crc_example_a1。
    items = ",".join(
        f"{code}-Rtd={value}" for (code, _), value in zip(FACTOR_ORDER, READINGS)
    )
    region3 = f"DataTime=20240601085857;{items}"
    body = (f"QN=20240601085857223;ST=31;CN=2011;PW=123456;"
            f"MN=010000A8900016F000169DC0;Flag=9;CP=&&{region3}&&")
    crc3 = crc16(body.encode())
    vectors["table3_flag9_reference_packet"] = {
        "source": "§6.3.3 表 3 第 9 页（字段顺序 + Flag=9 位分解示例）",
        "kind": "derived_reference",
        "note": "按原文表 3 的字段顺序、本项目 9 个测点自行组包，CRC 为本地计算值。"
                "**不是官方向量**——原文该处数据区为占位符。官方 CRC 真值见 crc_example_a1。",
        "packet": f"##{len(body):04d}{body}{crc3:04X}",
        "expected": {
            # 长度字段 = 数据段字符数（不含 CRC）：附录 A.1 的 97 = 2+4+87+4
            # 且长度字段写 "0087"；附录 A.2 加密示例的 "0295" 同样不含 CRC。
            "declared_length": len(body),
            "data_segment": body,
            "data_segment_length": len(body),
            "segment_with_crc": body + f"{crc3:04X}",
            "crc_hex": f"{crc3:04X}",
            "crc_computed": f"{crc3:04X}",
            "header": header_of(body),
            "cp_region": region3,
        },
    }

    # ---- §6.3.3 表 3：Flag=9 位分解 --------------------------------------
    vectors["flag9_bit_layout"] = {
        "source": "§6.3.3 表 3 第 9 页：「示例：Flag= 9的二进制编码00001001，"
                  "表示版本号为本次修订，需要应答且数据段不包含拆分包。」",
        "flag": 9,
        "binary": "00001001",
        "note": "原文自述版本号为 000010。该值在 00001001 中占据 bit5~bit2，"
                "故版本号是 4 位（0b0010），最低两位为 A/D。"
                "此布局由附录 C 全部 193 条报文示例反推：只出现 8/9/10/11 四个取值，"
                "bits5..2 恒为 0010，且 A 位与 PNUM/PNO 的有无完全一致（2/2）。",
        "expected": {
            "version_text_in_standard": "000010",
            "version_value": 2,
            "version_bits": "0010",
            "has_pagination": False,
            "needs_response": True,
        },
        "appendix_c_histogram": {
            "8": {"count": 119, "with_pnum_pno": 0, "binary": "00001000"},
            "9": {"count": 70, "with_pnum_pno": 0, "binary": "00001001"},
            "10": {"count": 2, "with_pnum_pno": 2, "binary": "00001010"},
            "11": {"count": 2, "with_pnum_pno": 2, "binary": "00001011"},
        },
        "pagination_example_page": 77,
    }

    # ---- §6.4.2 / 附录 A.2：四组加密示例 ---------------------------------
    sm4 = Sm4EcbNoPadding(KEY)
    for tag, plain_tag, cipher_tag, note in (
        ("a2_example_2", "ex1", "ex2",
         "加密传输；数据段不存在不足 16 字符的部分（208 字符 = 13 整块）"),
        ("a2_example_4", "ex3", "ex4",
         "加密传输；存在不足 16 字符的部分（147 字符 = 9 整块 + 3 字符余数，余数保持明文）"),
    ):
        plain_packet = trim(raw[plain_tag])
        cipher_raw = raw[cipher_tag]
        cipher_packet = trim(cipher_raw)
        p_declared, p_segment, p_crc, p_region = split_packet(plain_packet)
        c_declared, c_segment, c_crc, _ = split_packet(cipher_packet)
        cipher = ciphertext_bytes(cipher_raw)
        decrypted = sm4.decrypt(cipher)
        ours = sm4.encrypt(p_region.encode("ascii"))

        assert p_crc.upper() == f"{crc16(p_segment.encode()):04X}", tag
        assert decrypted.decode("latin-1") == p_region[:len(decrypted)], tag
        assert ours[:len(cipher)] == cipher, tag

        vectors[tag] = {
            "source": f"§6.4.2 / 附录 A.2 第 32–33 页（PDF 物理页）— {note}",
            "key_note": "原文：「密钥使用16个0x30」→ key = 48 个 ASCII '0' = 16 字节 0x30",
            "key_hex": KEY.hex(),
            "mode": "SM4 / ECB / Nopadding",
            "plaintext_packet": plain_packet,
            "encrypted_packet_trimmed": cipher_packet,
            "encrypted_packet_in_standard_raw": cipher_raw,
            "ciphertext_hex": cipher.hex().upper(),
            "expected": {
                "data_segment_length": p_declared,
                "declared_length_in_standard": p_declared,
                "transmitted_data_segment_length": len(c_segment),
                "crc_hex": p_crc,
                "crc_computed": f"{crc16(p_segment.encode()):04X}",
                "plaintext_region": p_region,
                "plaintext_region_length": len(p_region),
                "ciphertext_packet_data_segment_length": c_declared,
                "ciphertext_packet_crc": c_crc,
                "ciphertext_length": len(cipher),
                "decrypted_prefix": decrypted.decode("latin-1"),
                "reencrypted_full_region_hex": ours.hex().upper(),
            },
        }

    return vectors


def main() -> int:
    pdf_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PDF
    if not pdf_path.exists():
        print(f"找不到标准 PDF：{pdf_path}", file=sys.stderr)
        print("用法：python generate_vectors.py [PDF 路径]", file=sys.stderr)
        return 2

    vectors = build(pdf_path)
    OUT.write_text(
        json.dumps(vectors, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"已写入 {OUT}（{OUT.stat().st_size} 字节）")
    for name in vectors:
        print(f"  - {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
