# 临时值守：emqx 若退出则拉起（本次验收期间观测到第三方反复 stop emqx，见 并发与负载指标.md §10.6）
# 用法：pwsh -NoProfile -File scripts/_emqx_watchdog.ps1
# 退出：Ctrl+C 或删除 $flag 文件
$flag = "F:\Project1\cems-data-pipeline\docs\evidence\backup\_emqx_watchdog.stop"
if (Test-Path $flag) { Remove-Item $flag -Force }
$log = "F:\Project1\cems-data-pipeline\docs\evidence\backup\emqx_watchdog_20261003.log"
"watchdog start $(Get-Date -Format 'HH:mm:ss')" | Out-File -Encoding utf8 $log
for ($i = 0; $i -lt 2000; $i++) {
  if (Test-Path $flag) { break }
  $s = docker inspect emqx --format '{{.State.Status}}' 2>$null
  if ($s -ne "running") {
    "$(Get-Date -Format 'HH:mm:ss') emqx=$s -> docker start emqx" | Add-Content -Path $log
    docker start emqx | Out-Null
    Start-Sleep -Seconds 20
  }
  Start-Sleep -Seconds 10
}
"watchdog stop $(Get-Date -Format 'HH:mm:ss')" | Add-Content -Path $log
