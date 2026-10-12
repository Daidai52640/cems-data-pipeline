# =============================================================================
#  Bring down the WHOLE stack -- INCLUDING the host-side RS485 bridge.
#
#  Companion of scripts/up.ps1.  Read that file's header first: it explains why
#  the bridge cannot be a container (WSL2 sees no COM port AND its kernel has no
#  ch341 module).
#
#  Safety: the bridge is killed by matching BOTH its image name (python*) and its
#  command line, never by a guessed PID or a wildcard kill.
#
#  MEASURED 2026-10-11 -- matching on the command line ALONE is NOT enough:
#  when the bridge is launched from an agent/shell wrapper, THREE processes carry
#  `serial_bridge.py` in their command line:
#       pid 22152  DeepSeek Harness.exe ...                <- the runner
#       pid 23212  pwsh.exe -Command "..."                 <- the shell
#       pid  3844  python.exe scripts\serial_bridge.py     <- THE ACTUAL BRIDGE
#  Killing the first two would tear down the caller.  Hence the name filter.
#  (up.ps1 uses the same predicate -- keep the two in sync.)
#
#  Usage:
#     pwsh -NoProfile -File scripts/down.ps1                # containers + bridge
#     pwsh -NoProfile -File scripts/down.ps1 -StopContainersOnly
#     pwsh -NoProfile -File scripts/down.ps1 -StopBridgeOnly
#     pwsh -NoProfile -File scripts/down.ps1 -KeepContainers  # bridge only, keep stack
#
#  NOTE: `docker compose down` removes containers + networks but KEEPS VOLUMES,
#  so TDengine data survives.  Pass -PurgeVolumes if you really want a wipe.
# =============================================================================
param(
    [switch]$StopBridgeOnly,
    [switch]$StopContainersOnly,
    [switch]$KeepContainers,
    [switch]$PurgeVolumes
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot

Write-Host '=== CEMS stack down ===' -ForegroundColor Cyan

# ---------------------------------------------------------------- 1. bridge
if ($StopContainersOnly) {
    Write-Host '[1/2] bridge: skipped (-StopContainersOnly)' -ForegroundColor DarkGray
}
else {
    # BOTH conditions are required -- see the header. Command line alone also
    # matches the shell/agent wrappers that launched the bridge.
    $bridges = Get-CimInstance Win32_Process -Filter "Name like '%python%'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -and $_.CommandLine -match 'serial_bridge\.py' }

    if (-not $bridges) {
        Write-Host '[1/2] bridge: not running' -ForegroundColor DarkGray
    }
    else {
        foreach ($b in $bridges) {
            # Show WHAT we are about to kill, so a wrong match is visible.
            Write-Host "[1/2] bridge: stopping pid $($b.ProcessId)  ($($b.Name))" -ForegroundColor Yellow
            Write-Host "      $($b.CommandLine)"
            Stop-Process -Id $b.ProcessId -Force -ErrorAction SilentlyContinue
        }
        Start-Sleep -Milliseconds 800
        $left = Get-CimInstance Win32_Process -Filter "Name like '%python%'" -ErrorAction SilentlyContinue |
            Where-Object { $_.CommandLine -and $_.CommandLine -match 'serial_bridge\.py' }
        if ($left) {
            Write-Host '[1/2] bridge: WARNING -- still running after kill' -ForegroundColor Red
        }
        else {
            Write-Host '[1/2] bridge: stopped (COM port released)' -ForegroundColor Green
        }
    }
}

# ---------------------------------------------------------------- 2. compose
if ($StopBridgeOnly -or $KeepContainers) {
    Write-Host '[2/2] containers: kept' -ForegroundColor DarkGray
}
else {
    $composeArgs = @('compose', '--profile', 'plant2', 'down')
    if ($PurgeVolumes) { $composeArgs += '-v'; $composeArgs += '--remove-orphans' }
    Write-Host "[2/2] docker $($composeArgs -join ' ')" -ForegroundColor Cyan
    & docker @composeArgs
    if ($LASTEXITCODE -ne 0) { throw "docker compose down failed (exit $LASTEXITCODE)" }
    if ($PurgeVolumes) {
        Write-Host '[2/2] containers + VOLUMES removed (data is gone)' -ForegroundColor Red
    }
    else {
        Write-Host '[2/2] containers removed, volumes kept (data intact)' -ForegroundColor Green
    }
}

Write-Host ''
Write-Host 'Start again: pwsh -NoProfile -File scripts/up.ps1' -ForegroundColor DarkGray
