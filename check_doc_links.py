"""校验仓库内文档的相对链接：文件是否存在、锚点是否落在目标文件的标题上。

用法（在仓库根目录执行）：
    python check_doc_links.py

只读，不修改任何文件。跳过 http(s):// 与 mailto: 链接。

⚠️ 输出里有 ✅/❌，而 Windows 控制台默认代码页是 GBK —— 直接
`print("❌")` 会抛 `UnicodeEncodeError: 'gbk' codec can't encode character`，
脚本在**能报错的那一行崩掉**，反而看不到有哪些断链（实测踩过）。
所以下面显式把 stdout 重配成 UTF-8（errors="replace" 兜底，不因编码再崩）。
"""
from __future__ import annotations

import pathlib
import re
import sys
import unicodedata

# 必须在任何 print 之前执行：见文件头说明
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):  # 极老的解释器 / 被重定向到不可重配的流
    pass

ROOT = pathlib.Path(__file__).resolve().parent
LINK_RE = re.compile(r"!?\[([^\]]*)\]\(([^)\s]+)\)")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*$", re.MULTILINE)


def slugify(text: str) -> str:
    """近似 GitHub 的锚点算法：去内联标记 -> 转小写 -> 去标点 -> 空格转连字符。"""
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(r"\*\*([^*]*)\*\*", r"\1", text)
    text = re.sub(r"\*([^*]*)\*", r"\1", text)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = unicodedata.normalize("NFKC", text).lower()
    out = []
    for ch in text:
        if ch.isalnum() or ch in "-_" or unicodedata.category(ch).startswith("L"):
            out.append(ch)
        elif ch.isspace():
            out.append("-")
    return "".join(out)


def anchors_of(path: pathlib.Path) -> set[str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    return {slugify(m.group(2)) for m in HEADING_RE.finditer(text)}


def main() -> int:
    md_files = sorted(
        [p for p in ROOT.rglob("*.md") if ".git" not in p.parts and p.is_file()]
    )
    broken: list[str] = []
    checked = 0
    with_anchor = 0
    for md in md_files:
        rel = md.relative_to(ROOT).as_posix()
        text = md.read_text(encoding="utf-8", errors="replace")
        for m in LINK_RE.finditer(text):
            target = m.group(2).strip()
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            if target.startswith("<") and target.endswith(">"):
                target = target[1:-1]
            checked += 1
            path_part, _, anchor = target.partition("#")
            line_no = text[: m.start()].count("\n") + 1
            if path_part:
                dest = (md.parent / path_part).resolve()
                try:
                    dest.relative_to(ROOT)
                except ValueError:
                    broken.append(f"{rel}:{line_no} 越出仓库: {target}")
                    continue
                if not dest.exists():
                    broken.append(f"{rel}:{line_no} 文件不存在: {target}")
                    continue
                if anchor:
                    with_anchor += 1
                    if dest.suffix.lower() != ".md":
                        continue
                    if slugify(anchor) not in anchors_of(dest):
                        broken.append(f"{rel}:{line_no} 锚点未命中: {target}")
            else:
                # 同文件锚点
                with_anchor += 1
                if slugify(anchor) not in anchors_of(md):
                    broken.append(f"{rel}:{line_no} 本文件锚点未命中: {target}")

    print(f"扫描 Markdown 文件: {len(md_files)}")
    print(f"检查相对链接: {checked}（其中带锚点的 {with_anchor}）")
    if broken:
        print(f"❌ 失效链接 {len(broken)} 条:")
        for item in broken:
            print("   " + item)
        return 1
    print("[OK] 全部相对链接均指向仓库内存在的文件（带锚点的也已逐条命中标题）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
