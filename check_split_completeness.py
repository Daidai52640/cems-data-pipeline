"""一次性核对：docs/数据可靠性.md 拆成两份新文档后，内容有没有丢。

做法：从 HEAD 取原文件（已被 git rm），把两半的正文归一化（去空白、去 Markdown 标记）
后按行匹配；报告逐字命中数。未命中的行再做人工分类（章节重编号 / 旧路径改写 / 表格重排）。
只读，不修改任何文件。
"""
from __future__ import annotations

import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(r"F:\Project1\cems-data-pipeline")
SOURCE_REV = "HEAD^:docs/数据可靠性.md"   # 该文件在本次提交中被 git rm，故取父提交的版本
PARTS = [
    "docs/adr/0005-备份策略与保留期.md",
    "docs/runbooks/恢复演练与对账口径.md",
]


def normalize(text: str) -> str:
    text = re.sub(r"\s+", "", text)
    for ch in "*`#|>-[]()^~":
        text = text.replace(ch, "")
    return text


def main() -> int:
    source = subprocess.run(
        ["git", "-C", str(ROOT), "show", SOURCE_REV],
        capture_output=True,
        check=True,
    ).stdout.decode("utf-8")
    merged = "\n".join((ROOT / part).read_text(encoding="utf-8") for part in PARTS)
    merged_norm = normalize(merged)

    total = matched = 0
    unmatched: list[str] = []
    for line in source.splitlines():
        stripped = line.strip()
        if len(stripped) < 8 or re.fullmatch(r"\|?[-:| ]+", stripped):
            continue
        norm = normalize(stripped)
        if len(norm) < 6:
            continue
        total += 1
        if norm in merged_norm:
            matched += 1
        else:
            unmatched.append(stripped)

    print(f"源文件: {SOURCE_REV}")
    print(f"目标: {', '.join(PARTS)}")
    print(f"源文档有效内容行: {total}")
    print(f"逐字命中: {matched}")
    print(f"未逐字命中: {len(unmatched)}（分类见下方逐条）")
    for item in unmatched:
        print("  - " + item[:110])
    return 0


if __name__ == "__main__":
    sys.exit(main())
