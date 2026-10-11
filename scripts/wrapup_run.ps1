# 收尾：收割长跑证据 + 停掉全栈（含宿主串口桥）
#
# 用法:
#   pwsh -NoProfile -File scripts\wrapup_run.ps1 -HarvestOnly   # 只看证据，不停
#   pwsh -NoProfile -File scripts\wrapup_run.ps1                # 收割 + 停全栈
#
# 为什么单独有这么一个脚本：长跑采证的证据散在两份 CSV（冻结快照 + 追加段），
# 停栈又要同时管住"容器"和"宿主上的串口桥"两条线。手工做容易漏掉桥
# （它不在容器里，停了容器它还占着 COM3）。

param(
    [switch]$HarvestOnly,
    [string]$EvidenceDir = 'docs\evidence\edge',
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = 'Continue'
Set-Location $ProjectRoot

Write-Host '=== 1) 收割长跑资源证据 ===' -ForegroundColor Cyan
$live = Join-Path $EvidenceDir 'resource-24h.append-live.csv'
if (Test-Path $live) {
    python scripts\measure_edge_resource_summary.py --csv $live
} else {
    Write-Host "  [skip] 没找到追加段: $live" -ForegroundColor Yellow
}

if ($HarvestOnly) {
    Write-Host ''
    Write-Host '[HarvestOnly] 只收割，容器 / 桥 / 采证进程一律不动。' -ForegroundColor Green
    exit 0
}

Write-Host ''
Write-Host '=== 2) 停采证进程 ===' -ForegroundColor Cyan
# ⚠️ 这一步**必须**在 -HarvestOnly 之后 —— 曾经放在前面，结果"只看证据"
#    把正在跑的长跑采证一起杀了（2026-10-11 实测踩到）。
$collector = Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match 'measure_edge_resources' }
if ($collector) {
    foreach ($p in $collector) {
        Write-Host "  停掉采证进程 pid=$($p.ProcessId)"
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    }
} else {
    Write-Host '  采证进程不在（正常）'
}

Write-Host ''
Write-Host '=== 3) 停容器（保留数据卷）===' -ForegroundColor Cyan
docker compose --profile plant2 --profile transmitter down 2>&1 | Select-Object -Last 3

Write-Host ''
Write-Host '=== 4) 停宿主串口桥 ===' -ForegroundColor Cyan
# 只匹配"进程名是 python* 且命令行含 serial_bridge.py"的那一个
$bridge = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match 'python' -and $_.CommandLine -and $_.CommandLine -match 'serial_bridge\.py' }
if ($bridge) {
    foreach ($p in $bridge) {
        Write-Host "  停掉桥 pid=$($p.ProcessId)"
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    }
} else {
    Write-Host '  桥不在（正常）'
}

Write-Host ''
Write-Host '=== 5) 收尾核验（三项都应为 0 / 空）===' -ForegroundColor Cyan
$running = (docker ps -q | Measure-Object).Count
$left = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match 'python' -and $_.CommandLine -and $_.CommandLine -match 'serial_bridge\.py' }
$port = Get-NetTCPConnection -LocalPort 5021 -State Listen -ErrorAction SilentlyContinue
Write-Host ("  运行中的容器 : {0}" -f $running)
Write-Host ("  残留的桥进程 : {0}" -f (@($left).Count))
Write-Host ("  5021 仍在监听: {0}" -f (@($port).Count))
if ($running -eq 0 -and @($left).Count -eq 0 -and @($port).Count -eq 0) {
    Write-Host '  收尾完成。' -ForegroundColor Green
} else {
    Write-Host '  还有残留，见上面三行。' -ForegroundColor Yellow
}

Write-Host ''
Write-Host '提示：证据已落在 docs\evidence\edge\ 下；要恢复跑，用 scripts\up.ps1' -ForegroundColor DarkGray
