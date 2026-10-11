# Register (or remove) the 07:55 morning wrapup scheduled task.
#
#   pwsh -NoProfile -File scripts\register_morning_task.ps1            # register/update
#   pwsh -NoProfile -File scripts\register_morning_task.ps1 -Remove    # remove
#   pwsh -NoProfile -File scripts\register_morning_task.ps1 -Show      # show status
#
# Why a scheduled task and not "the agent watches it":
#   an AI session only acts when it is spoken to; it cannot wake itself at 08:00.
#   Task Scheduler runs independently of any chat session, so this is the only
#   thing that can actually finish the run while the operator is asleep.
#
# LogonType is Interactive on purpose: Docker Desktop's engine lives in the
# logged-on user's session, so a task running in another session may not be able
# to reach `docker` at all.

param(
    [switch]$Remove,
    [switch]$Show,
    [string]$TaskName = 'cems-morning-wrapup',
    [string]$At = '07:55',
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = 'Stop'

if ($Show) {
    $t = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($t) {
        $t | Select-Object TaskName, State | Format-List
        $t.Triggers | Select-Object StartBoundary, Enabled | Format-List
        ($t.Actions | Select-Object -First 1) | Select-Object Execute, Arguments, WorkingDirectory | Format-List
        Get-ScheduledTaskInfo -TaskName $TaskName |
            Select-Object LastRunTime, LastTaskResult, NextRunTime | Format-List
    } else {
        Write-Host "task '$TaskName' not found"
    }
    exit 0
}

if ($Remove) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "removed task '$TaskName'"
    exit 0
}

$script = Join-Path $ProjectRoot 'scripts\morning_wrapup.ps1'
if (-not (Test-Path $script)) { throw "script not found: $script" }

$pwshExe = (Get-Command pwsh).Source
Write-Host "pwsh : $pwshExe"
Write-Host "script: $script"
Write-Host "at   : $At"

$action = New-ScheduledTaskAction -Execute $pwshExe `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$script`"" `
    -WorkingDirectory $ProjectRoot

$trigger = New-ScheduledTaskTrigger -Once -At ([datetime]::Today.Add([timespan]::Parse($At)))

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 20) `
    -MultipleInstances IgnoreNew

$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
    -Settings $settings -Principal $principal -Force | Out-Null

Write-Host ''
Write-Host "registered '$TaskName'."
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State | Format-List
Get-ScheduledTaskInfo -TaskName $TaskName | Select-Object NextRunTime | Format-List
