"""校验仓库内文档的两件事（只读，不修改任何文件）：

1. **相对链接**：链接指向的文件是否存在、锚点是否落在目标文件的标题上；
2. **索引完整性**：`docs/README.md` 是文档导航页，它自己写着"新增文档后请同步更新这里" ——
   这条约定原来只靠自觉，现在可执行：
   a) `docs/` 下每一份 `.md`（**排除** `docs/legacy/**` 归档与索引页自身）
      **必须**在 `docs/README.md` 里登记；
   b) `docs/README.md` 里提到的 `.md` 路径**必须**真实存在。

用法（在仓库根目录执行）：
    python check_doc_links.py

跳过 http(s):// 与 mailto: 链接。返回码：0 = 两项全过；1 = 有失效项（链接或索引）。

⚠️ 索引里"登记一份文档"的两种写法都会被认：
    a) 带目录的路径 `` `reference/y.md` ``（也可写成 `docs/reference/y.md`）；
    b) 索引按目录分节，小节表内惯例**只写文件名**（`` `y.md` ``，目录由小节标题 `` `reference/` `` 给出）——
       此时要求"该目录已被索引覆盖"**且**"文件名出现在索引里"两者同时成立。
⚠️ 索引里提到一个 `.md` 的两种写法都会被认：Markdown 链接目标 `[x](reference/y.md)`
与反引号里的行内代码（含 `路径.md:120` 这种带行号的写法，行号会被剥掉）。
解析一个提到的路径时按顺序试三处，命中其一即算存在：
    a) `docs/<提到的路径>`      —— 索引页里的路径默认相对 `docs/`
    b) `<仓库根>/<提到的路径>`  —— 跨目录引用（如 `scripts/README.md`、根 `ACCEPTANCE.md`）
    c) 提到的路径**不含 "/"** 时，仓库内存在同名文件即可 —— 正文按名字引用，不可能唯一解析
       （如 `DEPRECATED.md`，`docs/legacy/` 下有两份）
⚠️ **模板/占位写法不按真实路径校验**（表格里教"该新建什么文件"时用的），判据是含
`NNNN` / `xxx` / `*` / `<...>`，或整段就是一个 `.md`；这类写法跳过，不报"文件不存在"。

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
DOCS = ROOT / "docs"
DOC_INDEX = DOCS / "README.md"

#: 归档目录（相对 docs/ 的第一段）：里面是"某一刻的快照"，只要求按目录级别登记，不逐份登记
INDEX_EXEMPT_DIRS = ("legacy",)

LINK_RE = re.compile(r"!?\[([^\]]*)\]\(([^)\s]+)\)")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*$", re.MULTILINE)
#: 反引号里的行内代码（索引页里的路径习惯写成 `reference/xxx.md`）
CODE_RE = re.compile(r"`([^`\n]+)`")
#: 行内代码里常见的 `路径.md:120` / `路径.md:120-130`，把行号后缀剥掉
LINE_SUFFIX_RE = re.compile(r":\d+(?:-\d+)?$")
#: 模板/占位写法（"该新建什么文件"的示例），不作为真实路径校验
PLACEHOLDER_RE = re.compile(r"NNNN|xxx|XXX|\*|<[^>]*>")


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


def normalize_mention(raw: str) -> str | None:
    """把索引页里提到的一段文本规整成 `.md` 路径；不是真实路径就返回 None。"""
    token = raw.strip().strip("`").strip()
    if not token:
        return None
    token = token.partition("#")[0].replace("\\", "/")
    token = LINE_SUFFIX_RE.sub("", token).strip().rstrip("。，；、")
    while token.startswith("./"):
        token = token[2:]
    if not token.lower().endswith(".md") or len(token) <= len(".md"):
        return None  # 整段就是 ".md" 的，是口头说法不是路径
    if PLACEHOLDER_RE.search(token):
        return None  # 模板/占位
    return token


def registered_in_index(doc: pathlib.Path, index_text: str) -> bool:
    """索引里怎么才算"登记了这份文档"：见文件头两种写法。"""
    rel_docs = doc.relative_to(DOCS).as_posix()
    if rel_docs in index_text or f"docs/{rel_docs}" in index_text:
        return True
    parent = doc.parent.relative_to(DOCS).as_posix()
    if parent != "." and f"{parent}/" not in index_text:
        return False
    return doc.name in index_text


def index_mentions(index_text: str) -> dict[str, int]:
    """抽出索引页提到的 `.md` 路径 -> 该路径首次出现的行号。"""
    mentions: dict[str, int] = {}
    for pattern, group in ((CODE_RE, 1), (LINK_RE, 2)):
        for m in pattern.finditer(index_text):
            target = m.group(group).strip()
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            if target.startswith("<") and target.endswith(">"):
                target = target[1:-1]
            token = normalize_mention(target)
            if token:
                mentions.setdefault(token, index_text[: m.start()].count("\n") + 1)
    return mentions


def mention_exists(token: str, basenames: set[str]) -> bool:
    """见文件头「解析一个提到的路径时按顺序试三处」。"""
    if (DOCS / token).is_file() or (ROOT / token).is_file():
        return True
    return "/" not in token and token in basenames


def index_required_docs() -> list[pathlib.Path]:
    """`docs/` 下"必须在索引里登记"的文档：排除 `legacy/` 归档与索引页自身。"""
    required = []
    for doc in sorted(DOCS.rglob("*.md")):
        if not doc.is_file():
            continue
        if doc == DOC_INDEX or doc.relative_to(DOCS).parts[0] in INDEX_EXEMPT_DIRS:
            continue
        required.append(doc)
    return required


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

    # ---- 索引完整性（docs/README.md 的「现有文档清单」）----
    required = index_required_docs()
    if DOC_INDEX.is_file():
        index_text = DOC_INDEX.read_text(encoding="utf-8", errors="replace")
    else:
        index_text = ""
    unregistered: list[str] = []
    for doc in required:
        if not registered_in_index(doc, index_text):
            unregistered.append(doc.relative_to(DOCS).as_posix())

    basenames = {md.name for md in md_files}
    mentions = index_mentions(index_text)
    dangling = [
        (token, line)
        for token, line in sorted(mentions.items())
        if not mention_exists(token, basenames)
    ]

    print(f"扫描 Markdown 文件: {len(md_files)}")
    print(f"检查相对链接: {checked}（其中带锚点的 {with_anchor}）")
    print(
        f"索引完整性: docs/ 下应登记 {len(required)} 份"
        "（已排除 docs/legacy/ 归档与 docs/README.md 自身）"
    )
    print(f"检查索引提到的 .md 路径: {len(mentions)}")

    if broken or unregistered or dangling:
        if broken:
            print(f"❌ 失效链接 {len(broken)} 条:")
            for item in broken:
                print("   " + item)
        if unregistered:
            print(
                f"❌ 索引漏登记 {len(unregistered)} 份"
                "（请补进 docs/README.md 的「二·五、现有文档清单」）:"
            )
            for item in unregistered:
                print("   - docs/" + item)
        if dangling:
            print(f"❌ 索引提到的路径不存在 {len(dangling)} 条:")
            for token, line in dangling:
                print(f"   - docs/README.md:{line} 提到的文件不存在: {token}")
        return 1

    print("✅ 全部相对链接均指向仓库内存在的文件（带锚点的也已逐条命中标题）")
    print("✅ 索引完整：docs/ 下每份 .md 都已在 docs/README.md 登记，且索引提到的 .md 路径全部存在")
    return 0


if __name__ == "__main__":
    sys.exit(main())
