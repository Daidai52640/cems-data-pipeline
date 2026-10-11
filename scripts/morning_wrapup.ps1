# 早间自动收尾：收割 -> 归档 -> 提交推送 -> 停全栈 -> 写报告
#
# 由 Windows 计划任务在 07:55 调用（见 scripts\register_morning_task.ps1）。
# 设计目标：**无人值守也必须跑完**，任何一步失败都不许让后面的步骤停摆。
#
# 用法:
#   pwsh -NoProfile -File scripts\morning_wrapup.ps1                    # 完整流程
#   pwsh -NoProfile -File scripts\morning_wrapup.ps1 -SkipStop          # 不碰容器/桥（安全试跑）
#   pwsh -NoProfile -File scripts\morning_wrapup.ps1 -SkipPush          # 只本地提交，不推

param(
    [switch]$SkipStop,
    [switch]$SkipPush,
    [string]$EvidenceDir = 'docs\evidence\edge',
    [string]$ReportPath = 'data\morning_report.txt',
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = 'Continue'
Set-Location $ProjectRoot

$log = New-Object System.Collections.Generic.List[string]
function Say([string]$m) {
    $line = "$(Get-Date -Format 'HH:mm:ss')  $m"
    Write-Host $line
    $log.Add($line) | Out-Null
}

Say '================ morning wrapup start ================'
Say "cwd = $ProjectRoot"
$py = 'python'

# ---------------------------------------------------------------- 1) 收割
Say '[1/6] harvest resource evidence'
$live = Join-Path $EvidenceDir 'resource-24h.append-live.csv'
$frozen = Join-Path $EvidenceDir 'resource-24h.csv'
try {
    if (Test-Path $live) {
        $out = & $py scripts\measure_edge_resource_summary.py --csv $live 2>&1
        $out | ForEach-Object { Say "    $_" }
    } else {
        Say '    [skip] append-live csv not found'
    }
} catch { Say "    [ERROR] $($_.Exception.Message)" }

# ---------------------------------------------------------------- 2) 归档
Say '[2/6] archive the long-run segment into the tracked snapshot'
try {
    if (Test-Path $live) {
        $n = (Get-Content $live | Measure-Object -Line).Lines
        Copy-Item $live $frozen -Force
        Say "    copied append-live -> resource-24h.csv ($n lines)"
    }
} catch { Say "    [ERROR] $($_.Exception.Message)" }

# ---------------------------------------------------------------- 3) 停采证
Say '[3/6] stop the collector process'
try {
    $c = Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -match 'measure_edge_resources' }
    if ($c) {
        foreach ($p in $c) { Say "    stopping pid=$($p.ProcessId)"; Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue }
    } else { Say '    not running (ok)' }
} catch { Say "    [ERROR] $($_.Exception.Message)" }

# ---------------------------------------------------------------- 4) 提交推送
Say '[4/6] commit + push evidence'
try {
    & git add -A 2>&1 | Out-Null
    $msg = "chore(evidence): 早间自动收尾 —— 长跑采证归档至 $(Get-Date -Format 'MM-dd HH:mm')"
    & git -c http.proxy= -c https.proxy= commit -q -m $msg 2>&1 | ForEach-Object { Say "    $_" }
    if (-not $SkipPush) {
        & git push origin feat/hj212-outlet 2>&1 | Select-Object -Last 1 | ForEach-Object { Say "    origin: $_" }
        & git push gitee  feat/hj212-outlet 2>&1 | Select-Object -Last 1 | ForEach-Object { Say "    gitee : $_" }
    } else { Say '    [SkipPush]' }
} catch { Say "    [ERROR] $($_.Exception.Message)" }

# ---------------------------------------------------------------- 5) 停全栈
if ($SkipStop) {
    Say '[5/6] [SkipStop] containers and bridge left running'
} else {
    Say '[5/6] stop stack (volumes kept)'
    try {
        $down = & docker compose --profile plant2 down 2>&1
        $down | Select-Object -Last 2 | ForEach-Object { Say "    $_" }
    } catch { Say "    [ERROR] $($_.Exception.Message)" }

    Say '      stop the RS485 bridge'
    try {
        $b = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -match 'python' -and $_.CommandLine -and $_.CommandLine -match 'serial_bridge\.py' }
        if ($b) { foreach ($p in $b) { Say "    stopping bridge pid=$($p.ProcessId)"; Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue } }
        else { Say '    bridge not running (ok)' }
    } catch { Say "    [ERROR] $($_.Exception.Message)" }
}

# ---------------------------------------------------------------- 6) 核验 + 报告
Say '[6/6] verify + write report'
try {
    $running = @(docker ps -q).Count
    $leftBridge = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -match 'python' -and $_.CommandLine -and $_.CommandLine -match 'serial_bridge\.py' }).Count
    $leftCollector = @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -match 'measure_edge_resources' }).Count
    $port = @(Get-NetTCPConnection -LocalPort 5021 -State Listen -ErrorAction SilentlyContinue).Count
    Say "    containers running : $running"
    Say "    bridge processes   : $leftBridge"
    Say "    collector processes: $leftCollector"
    Say "    port 5021 listening: $port"
} catch { Say "    [ERROR] $($_.Exception.Message)" }

try {
    $dir = Split-Path -Parent $ReportPath
    if ($dir -and -not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
    $log | Out-File -FilePath $ReportPath -Encoding utf8
    Say "    report written to $ReportPath"
} catch { Write-Host "    [ERROR] $($_.Exception.Message)" }

Say '================ morning wrapup end =================='
# 退出码永远 0：这是无人值守脚本，报错信息已在报告里
exit 0
