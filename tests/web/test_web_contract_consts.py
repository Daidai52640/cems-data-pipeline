# -*- coding: utf-8 -*-
"""展示层"折算常量注入"的测试。

守的纪律：折算相关的数字（限值 / 基准氧 / 折算分母下限）只能来自契约层，
前端不许手抄。抄一份就不会跟着契约变 —— 改基准氧或改判定门限时，
后端改了、前端还留着老数字，结果就是"后端判超标、前端显示达标"。

分两层：
  1. 静态层：直接读源码文本，前端 JS 里不许再出现写死的折算式或常量。
     不导入 web 层，毫秒级。
  2. 注入层：导入 web_dashboard，核对注入的 JSON 与契约逐项相等。
     导入会尝试连 Redis，连不上时缓存层自动降级并打一条日志（约 0.6 秒）。
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DASHBOARD = REPO_ROOT / "src" / "web" / "web_dashboard.py"


# ---- 1. 静态层：前端不许再写死 ----


def test_no_hardcoded_conversion_in_js():
    """JS 里不许出现写死的折算倍数、干基氧或分母门限。

    这三样都是"改了契约就得跟着改"的数字：15 是 (21-6)、21 是干基氧、
    19 是 (21-2.0)。写成字面量就等于把契约抄了一份。
    """
    src = DASHBOARD.read_text(encoding="utf-8")
    banned = {
        r"\*\s*15\s*/": "写死的折算倍数 15（应为 (o2DryBasis - o2Reference)）",
        r"/\s*\(\s*21\s*-\s*o2\s*\)": "写死的 (21 - o2)（应为 (o2DryBasis - o2)）",
        r"O2_FOR_ZS_MAX": "残留的门限常量 O2_FOR_ZS_MAX（应为 CC.o2DenomMin）",
        r"var\s+ZS_LIMITS": "残留的限值表 ZS_LIMITS（应为 CC.zsLimits）",
        r"var\s+ZS_MAP": "残留的 ZS_MAP",
    }
    hits = []
    for pattern, why in banned.items():
        for m in re.finditer(pattern, src):
            line = src[: m.start()].count("\n") + 1
            hits.append(f"L{line}: {why}")
    assert not hits, "前端仍有写死的折算常量：\n  " + "\n  ".join(hits)


def test_injection_function_reads_from_contract():
    """注入函数必须从契约取值，不许自己写字面量。"""
    src = DASHBOARD.read_text(encoding="utf-8")
    body = src[src.index("def build_contract_consts_json"):]
    body = body[: body.index("\nCONTRACT_CONSTS_JSON")]
    for needed in ("p.limit", "ZS_TARGETS", "O2_DRY_BASIS", "O2_REFERENCE", "o2_denominator_min"):
        assert needed in body, f"注入函数没有用到 {needed}"


def test_placeholder_is_replaced_in_render():
    """两个页面模板里都不能留下未替换的占位符。"""
    src = DASHBOARD.read_text(encoding="utf-8")
    assert ".replace(\"__CONTRACT_CONSTS_JSON__\", CONTRACT_CONSTS_JSON)" in src


# ---- 2. 注入层：注入值与契约逐项相等 ----


@pytest.fixture(scope="module")
def web():
    return pytest.importorskip("src.web.web_dashboard")


def test_injected_limits_equal_contract(web):
    """注入的限值必须等于契约里那三个折算对象的 limit。"""
    from src.common.points import POINTS, ZS_TARGETS

    cc = json.loads(web.CONTRACT_CONSTS_JSON)
    expected = {p.column: p.limit for p in POINTS if p.column in ZS_TARGETS}
    assert cc["zsLimits"] == expected
    assert tuple(cc["zsKeys"]) == ZS_TARGETS


def test_injected_o2_constants_equal_contract(web):
    """注入的干基氧 / 基准氧必须等于契约常量。"""
    from src.common.points import O2_DRY_BASIS, O2_REFERENCE

    cc = json.loads(web.CONTRACT_CONSTS_JSON)
    assert cc["o2DryBasis"] == O2_DRY_BASIS
    assert cc["o2Reference"] == O2_REFERENCE


def test_injected_denominator_gate_matches_alarm_layer(web):
    """注入的分母下限必须与判定层读的是同一个值。

    判定层的判据是 (O2_DRY_BASIS - o2) < o2_denominator_min；
    展示层用同一个数才可能得出同一结论。
    """
    from src.common.alarm_judge import AlarmConfig

    cc = json.loads(web.CONTRACT_CONSTS_JSON)
    assert cc["o2DenomMin"] == AlarmConfig.from_env().o2_denominator_min


def test_rendered_pages_have_no_placeholder_left(web):
    """渲染后占位符必须被替换，且注入的 JSON 能解析。"""
    for attr in ("HTML_PAGE", "TABLE_PAGE"):
        resp = web._render_page(getattr(web, attr))
        html = resp.get_data(as_text=True) if hasattr(resp, "get_data") else str(resp)
        assert "__CONTRACT_CONSTS_JSON__" not in html
        assert not re.search(r"__[A-Z_]+__", html), f"{attr} 还有未替换的占位符"


def test_table_page_uses_injected_values(web):
    """表格页的折算计算必须走注入常量，不是字面量。"""
    resp = web._render_page(web.TABLE_PAGE)
    html = resp.get_data(as_text=True) if hasattr(resp, "get_data") else str(resp)
    assert "var CC = __CONTRACT_CONSTS_JSON__" not in html
    assert "CC.o2DryBasis" in html
    assert "CC.o2DenomMin" in html
    assert "measured * (CC.o2DryBasis - CC.o2Reference)" in html
