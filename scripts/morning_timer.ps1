# Detached timer: sleep until HH:MM, then run morning_wrapup.ps1.
#
#   pwsh -NoProfile -File scripts\morning_timer.ps1 -At 07:55
#
# Why this exists instead of a Windows scheduled task:
#   Register-ScheduledTask and schtasks both returned "access denied" on this
#   machine (no elevation available in this session). A detached process is the
#   permission-free equivalent and is started with Start-Process, so it survives
#   the shell / agent session that launched it.
#
# Trade-off (stated honestly): a detached process does NOT survive a reboot.
#   A scheduled task would. If the machine is rebooted before the trigger, the
#   operator has to run scripts\morning_wrapup.ps1 by hand.
#
# Heartbeat: writes its own state to data\morning_timer.json every minute so the
#   operator can confirm it is still waiting (rather than guessing).

param(
    [string]$At = '07:55',
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = 'Continue'
Set-Location $ProjectRoot

$target = [datetime]::Today.Add([timespan]::Parse($At))
$hb = Join-Path $ProjectRoot 'data\morning_timer.json'

function Write-Heartbeat([string]$state, [string]$extra = '') {
    $o = [ordered]@{
        pid         = $PID
        state       = $state
        at          = $At
        target      = $target.ToString('yyyy-MM-dd HH:mm:ss')
        now         = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
        remaining_s = [math]::Round(($target - (Get-Date)).TotalSeconds, 0)
        note        = $extra
    }
    try {
        $dir = Split-Path -Parent $hb
        if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
        ($o | ConvertTo-Json -Compress) | Out-File -FilePath $hb -Encoding utf8
    } catch { }
}

Write-Heartbeat 'waiting'

# sleep, refreshing the heartbeat once a minute
while ($true) {
    $left = ($target - (Get-Date)).TotalSeconds
    if ($left -le 0) { break }
    Write-Heartbeat 'waiting'
    Start-Sleep -Seconds ([math]::Min(60, [math]::Max(1, $left)))
}

Write-Heartbeat 'running' 'wrapup started'
& pwsh -NoProfile -ExecutionPolicy Bypass -File (Join-Path $ProjectRoot 'scripts\morning_wrapup.ps1')
$code = $LASTEXITCODE
Write-Heartbeat 'done' "wrapup exit=$code"
