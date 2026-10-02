# =============================================================================
# 并发压测编排（正式验收复测）：N 台设备 × 每台一个接入实例
# -----------------------------------------------------------------------------
# 做三件事：① 起 N 个一次性接入实例（独立 client_id、独立子表 <plant>_lt<i>）
#           ② 起负载发生器（scripts/loadgen_devices.py，逐条对账 + 吞吐 vs N）
#           ③ 压测期间按固定间隔抓 docker stats 资源快照
# 跑完**不自动清理**：清理由 loadgen_devices.py --cleanup 单独做并留证据。
# 全部容器名带 -<tag> 后缀，与生产容器（cems-subscriber / cems-subscriber-plant2）不重名。
# =============================================================================
param(
  [Parameter(Mandatory = $true)][int]$Devices,
  [Parameter(Mandatory = $true)][int]$Duration,
  [Parameter(Mandatory = $true)][string]$Tag,
  [double]$Interval = 1.0,
  [int]$ParallelPublishers = 16,
  [string]$OutDir = "docs/evidence/perf"
)

$ErrorActionPreference = "Stop"
$repo = "F:\Project1\cems-data-pipeline"
Set-Location $repo
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONPATH = $repo
$py = "C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe"

$containerNames = @()
Write-Host "==== [$Tag] 起 $Devices 个接入实例 ===="
for ($i = 1; $i -le $Devices; $i++) {
  $name = "cems-lgsub-$Tag-$i"
  $containerNames += $name
  docker rm -f $name 2>&1 | Out-Null
  $topic = "cems/loadtest/device$i/data"
  docker run -d --name $name --network cems-net `
    -e TZ=Asia/Shanghai `
    -e MQTT_HOST=emqx -e MQTT_PORT=1883 `
    -e "MQTT_TOPIC=$topic" -e "MQTT_CLIENT_ID=$name" `
    -e TD_PLANT=loadtest -e "TD_DEVICE=lt$i" `
    -e TD_URL=http://tdengine:6041 -e ALARM_ENABLE=0 `
    cems-pipeline:latest python src/platform/subscriber_to_td.py 2>&1 | Out-Null
}
Write-Host "已起 $($containerNames.Count) 个容器，等 25 s 让 MQTT 订阅就绪…"
Start-Sleep -Seconds 25
$up = (docker ps --filter "name=cems-lgsub-$Tag-" --format "{{.Names}}" | Measure-Object).Count
Write-Host "Running 接入实例数 = $up / $Devices"
if ($up -ne $Devices) {
  Write-Host "!! 接入实例没起全，压测会少入库通道，先排查"
  docker ps -a --filter "name=cems-lgsub-$Tag-" --format "{{.Names}}\t{{.Status}}"
  exit 2
}

# ---- 资源快照（压测期间并行抓） ----
$statsFile = Join-Path $repo "$OutDir/loadgen_stats_$Tag.csv"
if (Test-Path $statsFile) { Remove-Item $statsFile -Force }
$statsJob = Start-Job -ScriptBlock {
  param($out, $watch)
  $first = $true
  for ($k = 0; $k -lt 12; $k++) {
    $rows = docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}" 2>$null
    foreach ($r in $rows) {
      $n = ($r -split ",")[0]
      if ($watch -contains $n -or $n -in @("emqx", "tdengine", "cems-gateway", "cems-gateway-plant2", "cems-subscriber", "cems-subscriber-plant2", "cems-device")) {
        $line = "$(Get-Date -Format 'HH:mm:ss'),$r"
        if ($first) { Add-Content -Path $out -Value "sample_time,container,cpu_perc,mem_usage,mem_perc" }
        Add-Content -Path $out -Value $line
      }
    }
    $first = $false
    Start-Sleep -Seconds 5
  }
} -ArgumentList $statsFile, $containerNames

Write-Host "==== [$Tag] 开始压测：N=$Devices 周期=$Interval 时长=$Duration ===="
$t0 = Get-Date
& $py scripts\loadgen_devices.py --devices $Devices --interval $Interval --duration $Duration `
  --tag $Tag --ingest-channels $Devices --parallel-publishers $ParallelPublishers `
  --out-dir $OutDir 2>&1 | Tee-Object -FilePath "docs/evidence/perf/loadgen_$Tag.log"
$rc = $LASTEXITCODE
$t1 = Get-Date
Write-Host "loadgen 退出码 = $rc；墙钟耗时 = $([math]::Round(($t1-$t0).TotalSeconds,1)) s"

Write-Host "==== [$Tag] 生产链路复核 ===="
& $py "docs\evidence\backup\_probe_prod_after_load.py"

Receive-Job -Job $statsJob -Wait | Out-Null
Remove-Job -Job $statsJob -Force
Write-Host "资源快照 -> $statsFile"
Write-Host "==== [$Tag] 压测结束（容器仍在，待清理） ===="
