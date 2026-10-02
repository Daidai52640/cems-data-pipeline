"""本项目 9 个测点 → HJ 212 参数编码（因子编码）。

**权威来源**：HJ 212—2025 附录 B.2「气监测参数编码表」（PDF 第 35–38 页，即 PDF 物理页
35、36、37、38；页脚 32、33、34、35）。本模块**只收录本项目实际用到的 9 个编码**，
不复制整张附录表（该附录 80+ 页，见 ADR-0006 §8 第 3 条）。

⚠️ **本项目测点契约（``src/common/points.py``）的 ``code`` 字段曾在 2026-10-02 之前 9 个全错**
（按顺序往下抄、整体抄错一位，其中 ``Dust`` 被写成 ``a21001`` = 原文的「氨（氨气）」）。
**已在契约侧修正**，历史与逐条对照见 :data:`CODE_REVIEW`；
两处取值的一致性由 :func:`check_consistency` 断言（同一条信息存在两处，这是本模块唯一被保留的冗余）。

单位差异同样留痕：原文的缺省计量单位与本项目的工程单位不同（例：``a01012`` 废气温度
原文缺省 ``℃``，本项目 ``degC``；``a00000`` 废气流量原文缺省 ``m3/s``，本项目 ``m3/h``）。
**本模块不做单位换算**——换算是出口层的业务决策，不是编解码层的职责。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

#: 编码来源（页码为 PDF 物理页）。
SOURCE_APPENDIX_B = "HJ 212—2025 附录 B.2 气监测参数编码表"
PDF_PAGES: Final[tuple[int, ...]] = (35, 36, 37, 38)


@dataclass(frozen=True)
class Factor:
    """一个测点对应的 HJ 212 参数编码及其标准属性。"""

    #: 本项目测点名（与 ``src/common/points.py`` 的 ``Point.name`` 一致）
    point: str
    #: HJ 212 参数编码（附录 B.2 原值）
    code: str
    #: 原文中文名称
    name: str
    #: 原文缺省计量单位（浓度列）
    unit_std: str
    #: 原文缺省数据类型（浓度列）
    dtype: str
    #: 原文「原编码」列（旧版 HJ/T 212 编码，无则 ``None``）
    legacy_code: str | None
    #: 编码在附录 B.2 中的物理页
    pdf_page: int
    #: 本项目工程单位（来自测点契约，仅作对照）
    unit_project: str
    #: 备注：单位换算 / 口径差异等
    note: str = ""


#: 本项目 9 个测点 → 标准编码。顺序与 ``src/common/points.py`` 的 ``POINTS`` 一致。
FACTORS: Final[tuple[Factor, ...]] = (
    Factor(
        point="Flow", code="a00000", name="废气流量",
        unit_std="m3/s", dtype="N6.1", legacy_code="B02", pdf_page=35,
        unit_project="m3/h",
        note="原文缺省单位 m3/s，本项目 m3/h（1 m3/s = 3600 m3/h）。待与平台确认上报口径。",
    ),
    Factor(
        point="Dust", code="a34013", name="颗粒物（烟尘）",
        unit_std="mg/m3", dtype="N4.1", legacy_code="01", pdf_page=37,
        unit_project="mg/m3",
        note="原文另有 a34001/a34002/a34004/a34005（TSP/PM10/PM2.5/PM1.0，单位 ng/m3），"
             "本项目烟气烟尘取 a34013。",
    ),
    Factor(
        point="SO2", code="a21026", name="二氧化硫",
        unit_std="mg/m3", dtype="N5.2", legacy_code="02", pdf_page=36,
        unit_project="mg/m3",
    ),
    Factor(
        point="NOx", code="a21002", name="氮氧化物",
        unit_std="mg/m3", dtype="N4.2", legacy_code="03", pdf_page=36,
        unit_project="mg/m3",
        note="原文 a21003 是「一氧化氮」、a21004 是「二氧化氮」，均非本项目测点口径。",
    ),
    Factor(
        point="O2", code="a19001", name="氧含量",
        unit_std="%", dtype="N2.2", legacy_code="S01", pdf_page=36,
        unit_project="%",
    ),
    Factor(
        point="Velocity", code="a01011", name="废气流速",
        unit_std="m/s", dtype="N2.2", legacy_code="S02", pdf_page=36,
        unit_project="m/s",
    ),
    Factor(
        point="Temp", code="a01012", name="废气温度",
        unit_std="℃", dtype="N3.1", legacy_code="S03", pdf_page=36,
        unit_project="degC",
        note="仅单位书写差异（℃ / degC），数值口径一致。",
    ),
    Factor(
        point="Humidity", code="a01014", name="废气含湿量",
        unit_std="%", dtype="N3.2", legacy_code="S05", pdf_page=36,
        unit_project="%",
    ),
    Factor(
        point="Pressure", code="a01013", name="废气压力",
        unit_std="kPa", dtype="N2.3", legacy_code="S08", pdf_page=36,
        unit_project="kPa",
    ),
)

#: 测点名 → :class:`Factor`
BY_POINT: Final[dict[str, Factor]] = {f.point: f for f in FACTORS}

#: 标准编码 → :class:`Factor`（供解码时反查）
BY_CODE: Final[dict[str, Factor]] = {f.code: f for f in FACTORS}


@dataclass(frozen=True)
class CodeReview:
    """测点契约里的既有 ``code`` 与标准原文的对照结论。"""

    point: str
    project_code: str
    standard_code: str
    verdict: str
    detail: str


#: ⚠️ 历史留痕：``src/common/points.py`` 的 ``Point.code`` 曾在 2026-10-02 之前**9 个全错**
#: （错法：按顺序往下抄、整体抄错一位）。当时本模块不改 ``points.py``（不在协议层职责内），
#: 只如实登记；**2026-10-02 已在 ``points.py`` 侧修正**，现两边取值一致（见 :func:`check_consistency`）。
CODE_REVIEW: Final[tuple[CodeReview, ...]] = (
    CodeReview("Flow", "B01 -> a00000", "a00000", "已修正",
               "B01 是附录 B.1（水）废水流量 a00000 的「原编码」列值，被误当成气监测编码。"),
    CodeReview("Dust", "a21001 -> a34013", "a34013", "已修正",
               "原文 a21001 是「氨（氨气）」；颗粒物（烟尘）是 a34013。此为最严重的一处错配。"),
    CodeReview("SO2", "a21002 -> a21026", "a21026", "已修正",
               "原文 a21002 是「氮氧化物」；二氧化硫是 a21026。"),
    CodeReview("NOx", "a21003 -> a21002", "a21002", "已修正",
               "原文 a21003 是「一氧化氮」；氮氧化物是 a21002。"),
    CodeReview("O2", "a21008 -> a19001", "a19001", "已修正",
               "附录 B.2 无 a21008；氧含量是 a19001（原编码 S01）。"),
    CodeReview("Velocity", "B02 -> a01011", "a01011", "已修正",
               "B02 是原文 a00000 的「原编码」列值；废气流速是 a01011。"),
    CodeReview("Temp", "B03 -> a01012", "a01012", "已修正",
               "附录 B.2 无 B03；废气温度是 a01012（原编码 S03）。"),
    CodeReview("Humidity", "B04 -> a01014", "a01014", "已修正",
               "附录 B.2 无 B04；废气含湿量是 a01014（原编码 S05）。"),
    CodeReview("Pressure", "B05 -> a01013", "a01013", "已修正",
               "附录 B.2 无 B05；废气压力是 a01013（原编码 S08）。"),
)


def check_consistency() -> tuple[str, ...]:
    """核对测点契约的 ``code`` 与本模块的 ``FACTORS`` 是否一致，返回不一致的测点名。

    ⚠️ 保留这个检查的原因：**同一条信息存在两处**（契约里的 ``code`` + 本模块的权威表），
    历史上正是"两处各自维护"导致了 9 个编码全错却长期无人发现。
    现在把它变成可断言的检查——**任何一侧被改而另一侧没跟，都会在检查里暴露。**
    """
    from src.common.points import CODES

    mismatched: list[str] = []
    for point, code in CODES.items():
        factor = BY_POINT.get(point)
        if factor is None or factor.code != code:
            mismatched.append(point)
    return tuple(mismatched)


class UnknownFactorError(KeyError):
    """查不到对应的 HJ 212 参数编码。"""


def factor_of(point: str) -> Factor:
    """按本项目测点名取 :class:`Factor`。"""
    try:
        return BY_POINT[point]
    except KeyError as exc:
        raise UnknownFactorError(
            f"测点 {point!r} 没有登记 HJ 212 参数编码（已登记：{sorted(BY_POINT)}）；"
            f"附录 B 未收录的编码一律标注「待确认」，不得臆造"
        ) from exc


def code_of(point: str) -> str:
    """按本项目测点名取 HJ 212 参数编码字符串。"""
    return factor_of(point).code


def point_of(code: str) -> str | None:
    """按 HJ 212 参数编码反查本项目测点名；不属于本项目 9 测点返回 ``None``。"""
    found = BY_CODE.get(code)
    return found.point if found else None


def data_field(code: str, value: str, field: str = "Rtd") -> str:
    """组装一个数据区字段，如 ``a34013-Rtd=12.3``。

    ``field`` 取标准表 5 的字段后缀（``Rtd`` / ``Avg`` / ``Min`` / ``Max`` / ``Flag`` …）。
    """
    if not code:
        raise UnknownFactorError("参数编码不能为空")
    return f"{code}-{field}={value}"
