# -*- coding: utf-8 -*-
"""报表导出：把 src/web/report.py 的聚合结果写成 Excel（数据 + 说明两个 sheet）。

聚合与参数校验一律复用 report.py，导出路径不重写 SQL 和口径 ——
否则"页面上看到的"和"导出文件里的"会各算各的，这是报表最忌讳的事。
"""

from __future__ import annotations

import io
import logging
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Final, Mapping, Optional

from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.styles import Alignment, Border, Font, Side
from openpyxl.utils import get_column_letter

# 让 src/common 与 src/web 能被导入：三种启动方式都能工作
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.points import COLUMNS, POINTS, to_reference_o2   # noqa: E402
from src.web import report   # noqa: E402

# ==================== 1. 配置区（要改参数只动这里） ====================

# type 参数 -> 中文报表名（同时也是文件名里那一段）
REPORT_TYPES: Final[dict[str, str]] = {
    "minute": "分钟报表",
    "day": "日报表",
    "month": "月报表",
    "custom": "自由报表",
    "range": "区间报表",
}

SHEET_DATA: Final[str] = "数据"
SHEET_INFO: Final[str] = "说明"

HEADER_TIME: Final[str] = "时间"
HEADER_COUNT: Final[str] = "采样条数"
AGGREGATION_NOTE: Final[str] = (
    "各时间窗口内原始采样的算术平均值（TDengine INTERVAL + AVG，聚合在库侧完成）；"
    f"“{HEADER_COUNT}”为该窗口内的原始采样条数"
)
EMPTY_NOTE: Final[str] = "空白 = 该窗口内没有采样（不是 0）；用 0 表示会把“无数据”和“值就是 0”混为一谈"

NUMBER_FORMAT: Final[str] = "0.00"                 # 聚合值保留 2 位小数
TIME_COLUMN_WIDTH: Final[float] = 20.0
VALUE_COLUMN_WIDTH: Final[float] = 14.0
INFO_KEY_WIDTH: Final[float] = 14.0
INFO_VALUE_WIDTH: Final[float] = 88.0

# ---- 报表版式（平台口径）：标题行 + 信息行 + 三行合并表头 ----
TITLE_TEXT: Final[str] = "CEMS 烟气排放连续监测报表"
DATA_TYPE_LABEL: Final[dict[str, str]] = {"1m": "分钟数据", "1h": "小时数据", "1d": "日数据"}
#: 污染物列组：每组两列（标干值 / 折算值）
GROUPS: Final[tuple[dict[str, str], ...]] = (
    {"key": "dust", "label": "颗粒物(毫克/立方米)"},
    {"key": "so2", "label": "二氧化硫(毫克/立方米)"},
    {"key": "nox", "label": "氮氧化物(毫克/立方米)"},
)
#: 其余测点：一列监测值（列序与 points.py 契约一致）
PLAIN_KEYS: Final[tuple[str, ...]] = ("o2", "velocity", "temp", "humidity", "pressure")
PLAIN_LABEL: Final[dict[str, str]] = {
    "o2": "氧含量(百分比)", "velocity": "烟气流速(米/秒)", "temp": "烟气温度(摄氏度)",
    "humidity": "烟气湿度(百分比)", "pressure": "烟气压力(千帕)",
}
FLOW_LABEL: Final[str] = "流量(立方米/小时)"
FLOW_LABEL_HOURLY: Final[str] = "累计流量(立方米)"
TOTAL_COLUMNS: Final[int] = 2 + len(GROUPS) * 2 + len(PLAIN_KEYS)      # = 13
TITLE_FONT: Final[Font] = Font(bold=True, size=13)
BOLD_FONT_HEADER: Final[Font] = Font(bold=True)
CENTER: Final[Alignment] = Alignment(horizontal="center", vertical="center")
THIN_BORDER: Final[Border] = Border(
    left=Side(style="thin", color="B0B0B0"), right=Side(style="thin", color="B0B0B0"),
    top=Side(style="thin", color="B0B0B0"), bottom=Side(style="thin", color="B0B0B0"),
)


def reference_value(measured: Any, o2: Any) -> Optional[float]:
    """折算值（导出用）：**直接调契约函数** `points.to_reference_o2`，不在这里另写公式。"""
    if measured is None or o2 is None:
        return None
    try:
        value = to_reference_o2(float(measured), float(o2))
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def time_label(ts: Any, unit: str) -> str:
    """「监测时间」列的写法按粒度走（平台报表口径，三种粒度一眼可辨）：

        分钟 2026-10-03 15:18 ｜ 小时 2026-10-03 14~15 ｜ 日 2026-10-02

    写成**文本**而不是日期单元格：小时行本来就是"一个区间"，不是某个时刻，
    用日期格式会被 Excel 显示成 14:00 而丢掉"这个数覆盖 14~15 点"这层意思。
    """
    text = str(ts)[:19].replace("T", " ")
    if unit == "1d":
        return text[:10]
    if unit == "1m":
        return text[:16]
    hour = int(text[11:13])
    return f"{text[:10]} {hour:02d}~{hour + 1:02d}"


# Excel 公式注入防护：这些字符开头的文本会被 Excel 当公式，前面补 ' 变成纯文本
RISKY_PREFIXES: Final[tuple[str, ...]] = ("=", "+", "-", "@")

BOLD_FONT: Final[Font] = Font(bold=True)

# 数据来源写进说明页；与 report.py 用同一份库表配置，避免第三处副本
TD_SOURCE: Final[str] = f"{report.TD_DB}.{report.TD_STABLE}"

# ---- 日志 ----
LOG_LEVEL: Final[int] = logging.INFO
LOG_FORMAT: Final[str] = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT: Final[str] = "%Y-%m-%d %H:%M:%S"

LOGGER: Final[logging.Logger] = logging.getLogger("web.report_export")


class ExportParamError(ValueError):
    """导出参数不合法（type 缺失/非法）—— 接口层转 400。"""


# ==================== 2. 小工具 ====================

def as_datetime(text: Any) -> Any:
    """把 'YYYY-MM-DD HH:MM:SS' 转成 datetime（Excel 里可排序可筛选）；转不动就原样写文本。"""
    try:
        return datetime.strptime(str(text), report.TS_FORMAT)
    except ValueError:
        return as_text(text)


def as_number(value: Any) -> Any:
    """数值保留 2 位小数；None 保持 None —— 在 Excel 里就是空白，绝不能写成 0。"""
    if isinstance(value, (int, float)):
        return round(float(value), report.ROUND_DIGITS)
    return None


def as_text(value: Any) -> str:
    """文本单元格：挡住以 = + - @ 开头的值，避免被 Excel 当公式执行。"""
    text = "" if value is None else str(value)
    return f"'{text}" if text[:1] in RISKY_PREFIXES else text


def build_filename(report_type: str, envelope: dict[str, Any]) -> str:
    """生成中文文件名，例如 CEMS_日报表_2026-09-11.xlsx。"""
    start = str(envelope["start"])
    if report_type == "day":
        stamp = start[:10]                                   # 2026-09-11
    elif report_type == "month":
        stamp = start[:7]                                    # 2026-09
    else:
        stamp = start[:16].replace(" ", "_").replace(":", "")  # 2026-09-11_2229
    return f"CEMS_{REPORT_TYPES[report_type]}_{stamp}.xlsx"


# ==================== 3. 工作簿组装 ====================

def run_report(report_type: str, query: Mapping[str, str], scope: str = "") -> dict[str, Any]:
    """按 type 调 report.py 里对应的报表函数（复用同一套校验与聚合口径）。"""
    if report_type == "minute":
        return report.minute_report(query.get("start"), query.get("end"), scope)
    if report_type == "day":
        return report.day_report(query.get("date"), scope)
    if report_type == "range":
        return report.range_report(query.get("start"), query.get("end"),
                                   query.get("unit") or "1h", scope)
    if report_type == "month":
        return report.month_report(query.get("year"), query.get("month"), scope)
    return report.custom_report(query.get("start"), query.get("end"), scope)


def fill_data_sheet(sheet: Any, envelope: dict[str, Any]) -> None:
    """数据 sheet：标题行 + 信息行 + **3 行合并表头** + 窗口数据（共 13 列 A~M）。

    表头结构（三种粒度完全同构，只有列名与时间写法随粒度变）：
        监测时间 ｜ 流量列 ｜ 颗粒物(标干值/折算值) ｜ 二氧化硫(同) ｜ 氮氧化物(同)
                 ｜ 氧含量 ｜ 流速 ｜ 温度 ｜ 湿度 ｜ 压力

    ★ 列名与单位取自 src/common/points.py 契约；污染物用中文名，其余测点用"烟气+中文名"。
    ★ **没有"设备标记"列**：项目里不存在逐点有效性字段（接入层对整条报文收/拒，
      TDengine 每个测点只有一列数值），所以不凭空造一列。
    """
    # 列 2 的列名随粒度变：小时表的同一个数按平台口径叫"累计流量"，其余叫"流量"。
    flow_key = FLOW_LABEL_HOURLY if envelope.get("unit") == "1h" else FLOW_LABEL
    sheet.column_dimensions["A"].width = TIME_COLUMN_WIDTH
    for index in range(2, len(GROUPS) * 2 + 3):
        sheet.column_dimensions[get_column_letter(index)].width = VALUE_COLUMN_WIDTH
    for index in range(len(GROUPS) * 2 + 3, 14):
        sheet.column_dimensions[get_column_letter(index)].width = VALUE_COLUMN_WIDTH
    sheet.column_dimensions["A"].width = TIME_COLUMN_WIDTH
    sheet.freeze_panes = "A6"          # 冻结标题+表头+首列，翻长报表不丢列名

    last_column = get_column_letter(TOTAL_COLUMNS)
    sheet.merge_cells(f"A1:{last_column}1")
    title_cell = sheet.cell(row=1, column=1, value=as_text(TITLE_TEXT))
    title_cell.font = TITLE_FONT
    sheet.merge_cells(f"A2:{last_column}2")
    info_cell = sheet.cell(row=2, column=1, value=as_text(
        f"数据类型：{DATA_TYPE_LABEL[envelope['unit']]}"
        f"　　设备：{envelope.get('device_label', '')}"
        f"　　时间：{envelope['start']} 至 {envelope['end']}"))
    info_cell.font = BOLD_FONT

    # ---- 3 行合并表头 ----
    sheet.merge_cells("A3:A5")
    sheet.merge_cells("B3:B5")
    sheet.merge_cells(f"I3:I5")
    for offset in range(2, 6):                       # J..M：流速/温度/湿度/压力
        column = get_column_letter(9 + offset - 1)
        sheet.merge_cells(f"{column}3:{column}5")
    sheet.cell(row=3, column=1, value=as_text(HEADER_TIME))
    sheet.cell(row=3, column=2, value=as_text(flow_key))
    for index, group in enumerate(GROUPS):
        first = 3 + index * 2
        second = first + 1
        letter_a, letter_b = get_column_letter(first), get_column_letter(second)
        sheet.merge_cells(f"{letter_a}3:{letter_b}3")
        sheet.cell(row=3, column=first, value=as_text(group["label"]))
        sheet.merge_cells(f"{letter_a}4:{letter_b}4")
        sheet.cell(row=4, column=first, value=as_text("浓度"))
        sheet.cell(row=5, column=first, value=as_text("标干值"))
        sheet.cell(row=5, column=second, value=as_text("折算值"))
    for index, key in enumerate(PLAIN_KEYS):
        sheet.cell(row=3, column=9 + index, value=as_text(PLAIN_LABEL[key]))
    for row in (3, 4, 5):
        for column in range(1, TOTAL_COLUMNS + 1):
            cell = sheet.cell(row=row, column=column)
            cell.font = BOLD_FONT
            cell.alignment = CENTER
            cell.border = THIN_BORDER

    # ---- 数据行 ----
    for row_index, point in enumerate(envelope["points"], start=6):
        # 时间列按粒度写成文本（分钟 15:18 / 小时 14~15 / 日 2026-10-02），见 time_label
        time_cell = sheet.cell(row=row_index, column=1,
                               value=as_text(time_label(point["ts"], envelope["unit"])))
        time_cell.alignment = CENTER
        # 第 2 列：小时表叫"累计流量"、其余叫"流量"，**取的是同一个值**（不做累加）
        flow_cell = sheet.cell(row=row_index, column=2, value=as_number(point.get("flow")))
        flow_cell.number_format = NUMBER_FORMAT
        for index, group in enumerate(GROUPS):
            key = group["key"]
            measured = point.get(key)
            zs = reference_value(measured, point.get("o2"))
            left = sheet.cell(row=row_index, column=3 + index * 2, value=as_number(measured))
            right = sheet.cell(row=row_index, column=4 + index * 2, value=as_number(zs))
            left.number_format = NUMBER_FORMAT
            right.number_format = NUMBER_FORMAT
        for index, key in enumerate(PLAIN_KEYS):
            cell = sheet.cell(row=row_index, column=9 + index, value=as_number(point.get(key)))
            cell.number_format = NUMBER_FORMAT


def fill_info_sheet(sheet: Any, report_type: str, envelope: dict[str, Any]) -> None:
    """说明 sheet：报表类型/口径/时间范围/生成时间/数据来源/空值含义。

    报表拿去存档或上报时，这页是刚需 —— 否则没人知道数字是怎么算出来的。
    """
    sheet.column_dimensions["A"].width = INFO_KEY_WIDTH
    sheet.column_dimensions["B"].width = INFO_VALUE_WIDTH
    rows = (
        ("报表类型", REPORT_TYPES[report_type]),
        ("统计口径", AGGREGATION_NOTE),
        ("时间范围", f"{envelope['start']} ~ {envelope['end']}（窗口单位 {envelope['unit']}）"),
        ("窗口数", str(len(envelope["points"]))),
        ("生成时间", datetime.now().strftime(report.TS_FORMAT)),
        ("数据来源", f"{TD_SOURCE}（TDengine）"),
        ("空值含义", EMPTY_NOTE),
    )
    for key, value in rows:
        key_cell = WriteOnlyCell(sheet, value=as_text(key))
        key_cell.font = BOLD_FONT
        sheet.append([key_cell, WriteOnlyCell(sheet, value=as_text(value))])


def build_workbook(report_type: str, envelope: dict[str, Any]) -> bytes:
    """生成 xlsx 字节。

    ⚠️ 这里**不能**用 `write_only=True`：它不支持合并单元格，而本报表的表头是
    "颗粒物(毫克/立方米) → 浓度 → 标干值/折算值"的三行合并表头（平台报表口径）。
    报表窗口数有上限（CUSTOM_MAX_DAYS × 每窗口数），普通模式的内存占用可控。
    """
    workbook = Workbook()
    workbook.remove(workbook.active)      # 普通模式会自带一个空 sheet，报表只要"数据/说明"两页
    fill_data_sheet(workbook.create_sheet(SHEET_DATA), envelope)
    fill_info_sheet(workbook.create_sheet(SHEET_INFO), report_type, envelope)
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def export_workbook(
    query: Mapping[str, str], scope: str = "", device_label: str = "",
) -> tuple[bytes, str]:
    """导出入口：返回 (xlsx 字节, 中文文件名)。

    参数非法抛 ExportParamError / report.ReportParamError，查库失败抛 report.ReportQueryError，
    由接口层分别转 400 与 503（错误不会写进 xlsx 里让用户下载）。
    """
    report_type = (query.get("type") or "").strip()
    if report_type not in REPORT_TYPES:
        raise ExportParamError(
            f"type 应为 {'/'.join(REPORT_TYPES)} 之一，收到 {report_type!r}"
        )
    envelope = run_report(report_type, query, scope)
    # 报表标题行的"设备"用的是页面上的展示名（1号炉 / 2号炉），由调用方算好传进来；
    # 这里不重复实现命名规则（唯一实现在 web_dashboard.device_label）。
    envelope["device_label"] = device_label or scope
    content = build_workbook(report_type, envelope)
    filename = build_filename(report_type, envelope)
    LOGGER.info(
        "导出 %s：%d 个窗口，%d 字节，文件名 %s",
        REPORT_TYPES[report_type], len(envelope["points"]), len(content), filename,
    )
    return content, filename
