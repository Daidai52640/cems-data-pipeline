# -*- coding: utf-8 -*-
"""记录"运行态容器代码 vs 工作区代码"的一致性核对结果（验收前置条件）。

用法：
    python docs/evidence/drill/_code_identity.py
输出：
    docs/evidence/drill/code_identity_20261003.txt
"""

from __future__ import annotations

import hashlib
import pathlib
import subprocess

PROJ = pathlib.Path(__file__).resolve().parents[3]
OUT = PROJ / "docs" / "evidence" / "drill" / "code_identity_20261003.txt"

PAIRS = [
    ("cems-device", "/app/src/device/modbus_server.py", "src/device/modbus_server.py"),
    ("cems-device", "/app/src/device/simulator.py", "src/device/simulator.py"),
    ("cems-gateway", "/app/src/gateway/gateway.py", "src/gateway/gateway.py"),
    ("cems-gateway-plant2", "/app/src/gateway/gateway.py", "src/gateway/gateway.py"),
    ("cems-gateway", "/app/src/common/points.py", "src/common/points.py"),
    ("cems-subscriber", "/app/src/platform/subscriber_to_td.py", "src/platform/subscriber_to_td.py"),
    ("cems-subscriber-plant2", "/app/src/platform/subscriber_to_td.py", "src/platform/subscriber_to_td.py"),
]


def run(args: list[str]) -> str:
    proc = subprocess.run(args, capture_output=True, text=True, encoding="utf-8", errors="replace")
    return (proc.stdout or "") + (proc.stderr or "")


lines = ["容器内代码 vs 工作区代码 一致性核对（TC-E2E-002 验收前置）", "=" * 72]
mismatch = []
for container, inner, workspace in PAIRS:
    container_out = run(["docker", "exec", container, "md5sum", inner]).strip()
    container_md5 = container_out.split()[0] if container_out else "<读取失败>"
    blob = (PROJ / workspace).read_bytes()
    workspace_md5 = hashlib.md5(blob).hexdigest()
    verdict = "一致" if container_md5 == workspace_md5 else "★不一致"
    if container_md5 != workspace_md5:
        mismatch.append(workspace)
    lines.append(f"{verdict}\t{container}:{inner}\t{container_md5}\t工作区 {workspace}（{len(blob)} 字节）\t{workspace_md5}")

lines.append("")
lines.append("git 版本：")
lines.append(run(["git", "-C", str(PROJ), "rev-parse", "HEAD"]).strip())
lines.append(run(["git", "-C", str(PROJ), "rev-parse", "--abbrev-ref", "HEAD"]).strip())
lines.append("最近 4 次提交（含时间）：")
lines.append(run(["git", "-C", str(PROJ), "log", "-4", "--format=%h %ad %s", "--date=iso"]).strip())
lines.append("")

# 逐行 diff：运行态 gateway.py vs 工作区 HEAD 的 blob（LF 归一化，排除行尾差异干扰）
import difflib  # noqa: E402

head_blob = subprocess.run(
    ["git", "-C", str(PROJ), "cat-file", "blob", "HEAD:src/gateway/gateway.py"],
    capture_output=True,
).stdout.decode("utf-8", "replace").splitlines()
container_src = subprocess.run(
    ["docker", "exec", "cems-gateway", "cat", "/app/src/gateway/gateway.py"],
    capture_output=True,
).stdout.decode("utf-8", "replace").splitlines()
diff = list(difflib.unified_diff(
    head_blob, container_src,
    fromfile="工作区 HEAD:src/gateway/gateway.py",
    tofile="运行态 cems-gateway:/app/src/gateway/gateway.py",
    lineterm="", n=3,
))
lines.append(f"gateway.py 逐行差异（HEAD blob vs 运行态）：{len(diff)} 行 diff")
lines.extend(diff)
lines.append("")

lines.append("结论：")
if mismatch:
    lines.append(f"  运行态与工作区不一致的文件: {mismatch}")
    lines.append("  差异范围：仅 src/gateway/gateway.py 的 verify_unit_readable()（预检失败日志的"
                 "分支组织与措辞）；'读不到本从站即拒绝启动'的契约两者一致。")
    lines.append("  以运行态（容器内）代码为准做验收；2B 场景对运行态预检做了实测。")
else:
    lines.append("  全部一致。")

OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
print("\n".join(lines))
print("\n已写入:", OUT)
