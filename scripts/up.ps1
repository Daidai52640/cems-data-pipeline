# =============================================================================
#  Bring up the WHOLE stack -- including the host-side RS485 bridge.
#
#  Why this script exists (the question it answers):
#     "Why is the bridge a loose script and not a container?"
#
#     Because Docker containers on Windows (WSL2 backend) CANNOT see a COM port.
#     Measured 2026-10-11 on this machine, so nobody has to re-litigate it:
#         * `wsl -d Ubuntu-24.04 ... ls /dev/ttyUSB*`  -> No such file or directory
#         * the WSL2 kernel (6.18.33.2-microsoft-standard-WSL2) has NO ch341
#           module -> even with usbipd-win passing the USB device through,
#           there would be NO DRIVER to bind it.  Fixing that means building a
#           custom WSL2 kernel -- far more fragile than one host process.
#         * a container sees no serial device at all (`ls /dev/ttyS*` -> nothing)
#
#     So the bridge MUST run on the Windows host.  This script makes it come up
#     TOGETHER WITH the containers, in one command -- which is what "bring it up
#     with the other containers" actually means here.
#
#  Deliberate non-goal: this is NOT a boot-time service.
#     The user does not want the bridge running when the transmitter is
#     unplugged.  Start it on demand; stop it with scripts/down.ps1.
#
#  Usage:
#     pwsh -NoProfile -File scripts/up.ps1
#     pwsh -NoProfile -File scripts/up.ps1 -ComPort COM4
#     pwsh -NoProfile -File scripts/up.ps1 -NoBuild      # skip image rebuild
#     pwsh -NoProfile -File scripts/up.ps1 -SkipBridge   # containers only
# =============================================================================
param(
    [string]$ComPort = 'COM3',
    [int]$TcpPort = 5021,
    [switch]$NoBuild,
    [switch]$SkipBridge
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$bridgeScript = Join-Path $PSScriptRoot 'serial_bridge.py'
$logDir = Join-Path $projectRoot 'data'
$logFile = Join-Path $logDir 'bridge.log'
$errFile = Join-Path $logDir 'bridge.err.log'

function Get-PythonExe {
    $candidates = @(
        (Join-Path $env:LOCALAPPDATA 'Programs\Python\Python312\python.exe'),
        (Join-Path $env:LOCALAPPDATA 'Programs\Python\Python311\python.exe')
    )
    foreach ($c in $candidates) { if (Test-Path $c) { return $c } }
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    throw "python.exe not found. Install Python 3.11+ or edit scripts/up.ps1."
}

function Test-PortListening([int]$Port) {
    # MUST be a direct TCP connect, NOT `Get-NetTCPConnection`.
    # MEASURED 2026-10-11: `Get-NetTCPConnection` takes tens of seconds on this
    # box, and this function is called up to 40 times in the wait loop below --
    # the script hung for >4 minutes with no output.  A raw connect is ~1 ms.
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $iar = $client.BeginConnect('127.0.0.1', $Port, $null, $null)
        $ok = $iar.AsyncWaitHandle.WaitOne(300)
        if ($ok) { $client.EndConnect($iar) }
        return $ok
    }
    catch { return $false }
    finally { $client.Close() }
}

function Get-SerialPortNames {
    # MUST use SerialPort.GetPortNames(), NOT `Get-CimInstance Win32_PnPEntity`.
    # Win32_PnPEntity enumerates the whole PnP tree and is a well-known slow WMI
    # class -- MEASURED 2026-10-11: it hung this script for over 5 minutes before
    # any progress output appeared.  GetPortNames() is instant.
    [System.IO.Ports.SerialPort]::GetPortNames()
}

function Find-BridgeProcess {
    Get-CimInstance Win32_Process -Filter "Name like '%python%'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -and $_.CommandLine -match 'serial_bridge\.py' }
}

Write-Host '=== CEMS stack up ===' -ForegroundColor Cyan

# ---------------------------------------------------------------- 1. bridge
if ($SkipBridge) {
    Write-Host '[1/3] bridge: skipped (-SkipBridge)' -ForegroundColor DarkGray
}
elseif (Test-PortListening $TcpPort) {
    # Already up (maybe started by an earlier run, or manually).  Do NOT start a
    # second one -- two processes cannot own the same COM port anyway.
    $existing = Find-BridgeProcess
    $who = if ($existing) { "pid " + ($existing.ProcessId -join ',') } else { 'unknown owner' }
    Write-Host "[1/3] bridge: ALREADY listening on $TcpPort ($who) -- reusing" -ForegroundColor Yellow
}
else {
    if (-not (Test-Path $bridgeScript)) { throw "bridge script not found: $bridgeScript" }

    $comExists = (Get-SerialPortNames) -contains $ComPort
    if (-not $comExists) {
        $avail = (Get-SerialPortNames) -join ', '
        Write-Host "[1/3] bridge: WARNING -- '$ComPort' not present. Available: $avail" -ForegroundColor Yellow
        Write-Host '      The bridge will start anyway and retry forever;' -ForegroundColor Yellow
        Write-Host '      cems-device will REFUSE to start until the device answers' -ForegroundColor Yellow
        Write-Host '      (that is deliberate -- it will not publish fake data).' -ForegroundColor Yellow
    }

    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $py = Get-PythonExe
    # Detached on purpose: the process must survive THIS shell and the agent
    # session.  A background job would die with them.
    $proc = Start-Process -FilePath $py `
        -ArgumentList @($bridgeScript, '--port', $ComPort, '--tcp-port', "$TcpPort", '--interval', '1.0') `
        -WorkingDirectory $projectRoot `
        -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput $logFile -RedirectStandardError $errFile
    Write-Host "[1/3] bridge: started pid $($proc.Id) -> $ComPort, TCP $TcpPort" -ForegroundColor Green
    Write-Host "      log: $logFile"

    # Wait for the port to accept, so that cems-device's startup precheck passes.
    $waited = 0
    while (-not (Test-PortListening $TcpPort) -and $waited -lt 20) {
        Start-Sleep -Milliseconds 500
        $waited++
    }
    if (-not (Test-PortListening $TcpPort)) {
        Write-Host "[1/3] bridge: FAILED to listen on $TcpPort within 10s -- see $errFile" -ForegroundColor Red
        exit 1
    }
    Write-Host "[1/3] bridge: listening on $TcpPort (after $([math]::Round($waited*0.5,1))s)" -ForegroundColor Green
}

# ---------------------------------------------------------------- 2. compose
$env:DOCKER_BUILDKIT = '0'
# NOTE: do NOT name this `$args` -- that is a PowerShell automatic variable.
$composeArgs = @('compose', '--profile', 'sim', '--profile', 'plant2', 'up', '-d')
if (-not $NoBuild) { $composeArgs += '--build' }
Write-Host "[2/3] docker $($composeArgs -join ' ')" -ForegroundColor Cyan
& docker @composeArgs
if ($LASTEXITCODE -ne 0) { throw "docker compose up failed (exit $LASTEXITCODE)" }

# ---------------------------------------------------------------- 3. report
Start-Sleep -Seconds 5
Write-Host '[3/3] status' -ForegroundColor Cyan
& docker compose --profile sim --profile plant2 ps --format '{{.Name}}|{{.Status}}'
$unhealthy = @(& docker ps --format '{{.Names}}' --filter health=unhealthy)
if ($unhealthy.Count -gt 0) {
    Write-Host "UNHEALTHY: $($unhealthy -join ', ')" -ForegroundColor Red
}
else {
    Write-Host 'All containers healthy.' -ForegroundColor Green
}
Write-Host ''
Write-Host 'Dashboard: http://127.0.0.1:5001/   EMQX: http://127.0.0.1:18083/' -ForegroundColor DarkGray
Write-Host 'Stop everything (including the bridge): pwsh -NoProfile -File scripts/down.ps1' -ForegroundColor DarkGray
