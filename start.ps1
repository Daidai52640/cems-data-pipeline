<#
.SYNOPSIS
    CEMS 数据采集全链路 —— 一键启动（服务真正就绪后再打开浏览器）
.DESCRIPTION
    Docker 模式（默认）：构建并启动 emqx / tdengine / device / gateway / subscriber / web，
    轮询 Web 的 /api/health，确认 Web 能响应之后再打开浏览器，避免打开的是错误页。
    Local 模式：起 EMQX + TDengine 容器，再用 4 个 cmd 窗口跑 4 个 Python 服务，适合逐层看日志。
    启动失败时会把原因、占用端口的进程、排查命令直接打在屏幕上。
.PARAMETER Mode
    Docker（默认）| Local
.PARAMETER Timeout
    等 Web 就绪的最长秒数，默认 120
.PARAMETER NoBuild
    Docker 模式跳过镜像重建（刚改过 src/ 代码时不要加）
.PARAMETER ClassicBuild
    Docker 模式直接用传统构建器（DOCKER_BUILDKIT=0）构建，跳过 BuildKit
.PARAMETER NoBrowser
    只启动服务，不打开浏览器
.PARAMETER DryRun
    只打印将要执行的动作，不实际启动
.PARAMETER Stop
    Docker 模式：docker compose down（数据卷保留）
.EXAMPLE
    .\start.ps1
    .\start.ps1 -Mode Local
    .\start.ps1 -Stop
#>
[CmdletBinding()]
param(
    [ValidateSet('Docker', 'Local')][string]$Mode = 'Docker',
    [int]$Timeout = 120,
    [switch]$NoBuild,
    [switch]$ClassicBuild,
    [switch]$NoBrowser,
    [switch]$DryRun,
    [switch]$Stop
)

$ErrorActionPreference = 'Stop'
$Root = $PSScriptRoot
if (-not $Root) { $Root = (Get-Location).Path }
Set-Location -LiteralPath $Root

function Write-Info([string]$msg) { Write-Host "[信息] $msg" -ForegroundColor Cyan }
function Write-Ok([string]$msg)   { Write-Host "[ OK ] $msg" -ForegroundColor Green }
function Write-Note([string]$msg) { Write-Host "[注意] $msg" -ForegroundColor Yellow }
function Write-Bad([string]$msg)  { Write-Host "[失败] $msg" -ForegroundColor Red }

# ---- Web 地址：默认 5000，若 .env 里改过 WEB_PORT 则以 .env 为准 ----
$WebPort = 5000
$EnvFile = Join-Path $Root '.env'
if (Test-Path -LiteralPath $EnvFile) {
    $m = Select-String -LiteralPath $EnvFile -Pattern '^\s*WEB_PORT\s*=\s*(\d+)' -ErrorAction SilentlyContinue |
         Select-Object -First 1
    if ($m) { $WebPort = [int]$m.Matches[0].Groups[1].Value }
}
$WebUrl = "http://localhost:$WebPort"
$HealthUrl = "$WebUrl/api/health"

# ---- docker compose 命令探测：优先 v2 插件，其次老版 docker-compose ----
$script:ComposeExe = 'docker'
$script:ComposeArgs = @('compose')
function Initialize-Compose {
    try { $null = & docker compose version 2>$null; if ($LASTEXITCODE -eq 0) { return } } catch { }
    if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
        $script:ComposeExe = 'docker-compose'
        $script:ComposeArgs = @()
        return
    }
    Write-Bad '找不到 docker compose（需要 Docker Desktop 或 docker compose 插件）'
    exit 1
}

function Get-ComposeText { return ($script:ComposeExe + ' ' + ($script:ComposeArgs -join ' ')).Trim() }

function Test-DockerEngine {
    try { $null = & docker info --format '{{.ServerVersion}}' 2>$null } catch { return $false }
    return ($LASTEXITCODE -eq 0)
}

function Start-DockerDesktop {
    $candidates = @(
        "$env:ProgramFiles\Docker\Docker\Docker Desktop.exe",
        "$env:LOCALAPPDATA\Programs\DockerDesktop\Docker Desktop.exe"
    )
    $exe = $candidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
    if (-not $exe) { return $false }
    Write-Info 'Docker 引擎未运行，正在启动 Docker Desktop ...'
    if (-not $DryRun) { Start-Process -FilePath $exe | Out-Null }
    for ($i = 1; $i -le 45; $i++) {
        if (Test-DockerEngine) { Write-Host ''; return $true }
        Start-Sleep -Seconds 2
        Write-Host -NoNewline '.'
    }
    Write-Host ''
    return (Test-DockerEngine)
}

# 用 .NET 直接发请求：PS5.1 / PS7 行为一致，也不受系统代理影响
function Test-Web {
    $result = @{ Up = $false; Code = 0; Body = '' }
    try {
        $req = [System.Net.HttpWebRequest]::Create($HealthUrl)
        $req.Method = 'GET'
        $req.Timeout = 3000
        $req.Proxy = $null
        $resp = $req.GetResponse()
        $result.Code = [int]$resp.StatusCode
        $result.Body = (New-Object System.IO.StreamReader($resp.GetResponseStream())).ReadToEnd()
        $result.Up = $true
        $resp.Close()
    } catch [System.Net.WebException] {
        $resp = $_.Exception.Response
        if ($null -ne $resp) {
            # 只要有 HTTP 响应就算 Web 已起来（503 = Web 起了但查库失败）
            $result.Code = [int]$resp.StatusCode
            try { $result.Body = (New-Object System.IO.StreamReader($resp.GetResponseStream())).ReadToEnd() } catch { }
            $result.Up = $true
        } else {
            $result.Body = $_.Exception.Message
        }
    } catch {
        $result.Body = $_.Exception.Message
    }
    return $result
}

function Wait-Web {
    param([int]$Seconds)
    $deadline = (Get-Date).AddSeconds($Seconds)
    Write-Host '[等待] Web 服务就绪' -NoNewline
    while ((Get-Date) -lt $deadline) {
        $r = Test-Web
        if ($r.Up) { Write-Host ''; return $r }
        Write-Host -NoNewline '.'
        Start-Sleep -Seconds 2
    }
    Write-Host ''
    return $null
}

function Get-PortOwner {
    param([int]$Port)
    try {
        $conn = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction Stop | Select-Object -First 1
    } catch { return $null }
    if ($null -eq $conn) { return $null }
    $name = '未知进程'
    $p = Get-Process -Id $conn.OwningProcess -ErrorAction SilentlyContinue
    if ($p) { $name = $p.ProcessName }
    return @{
        Port     = $Port
        Pid      = $conn.OwningProcess
        Name     = $name
        IsDocker = ($name -match 'wslrelay|docker|vpnkit')
    }
}

function Open-Browser {
    param([string]$Url)
    if ($NoBrowser) { Write-Info "已按 -NoBrowser 跳过打开浏览器：$Url"; return }
    if ($DryRun) { Write-Info "（DryRun）将用默认浏览器打开 $Url"; return }
    try { Start-Process -FilePath $Url; Write-Ok "已用默认浏览器打开 $Url"; return } catch { }
    try { & cmd /c start "" $Url; Write-Ok "已通过 cmd start 打开 $Url"; return } catch { }
    try { Start-Process -FilePath 'explorer.exe' -ArgumentList $Url; Write-Ok "已通过 explorer 打开 $Url"; return } catch { }
    Write-Note "自动打开浏览器失败，请手动复制到浏览器：$Url"
}

function Get-LanUrl {
    # 优先用默认路由所在网卡（真正连局域网/路由器的那块），避免拿到 WSL / Hyper-V 虚拟网卡
    try {
        $route = Get-NetRoute -DestinationPrefix '0.0.0.0/0' -ErrorAction Stop |
                 Sort-Object RouteMetric | Select-Object -First 1
        if ($route) {
            $ip = Get-NetIPAddress -AddressFamily IPv4 -InterfaceIndex $route.InterfaceIndex -ErrorAction Stop |
                  Where-Object { $_.IPAddress -notlike '169.254.*' } | Select-Object -First 1
            if ($ip) { return "http://$($ip.IPAddress):$WebPort" }
        }
        $ip = Get-NetIPAddress -AddressFamily IPv4 -ErrorAction Stop |
              Where-Object {
                  $_.IPAddress -notlike '127.*' -and
                  $_.IPAddress -notlike '169.254.*' -and
                  $_.PrefixOrigin -ne 'WellKnown' -and
                  $_.InterfaceAlias -notmatch 'vEthernet|WSL|Hyper-V|Docker|Loopback|VMware|VirtualBox'
              } | Select-Object -First 1
        if ($ip) { return "http://$($ip.IPAddress):$WebPort" }
    } catch { }
    return $null
}

# ==================== 主流程 ====================
Write-Host '============================================' -ForegroundColor DarkCyan
Write-Host ' CEMS 数据采集全链路 - 一键启动' -ForegroundColor DarkCyan
Write-Host " 模式: $Mode    项目目录: $Root" -ForegroundColor DarkCyan
if ($DryRun) { Write-Host ' （DryRun：只打印动作，不实际启动）' -ForegroundColor DarkCyan }
Write-Host '============================================' -ForegroundColor DarkCyan

if (-not (Test-Path -LiteralPath (Join-Path $Root 'docker-compose.yml'))) {
    Write-Bad "当前目录没有 docker-compose.yml：$Root"
    exit 1
}

Initialize-Compose

if ($Stop) {
    Write-Info "停止服务：$(Get-ComposeText) down"
    if (-not $DryRun) {
        & $script:ComposeExe @script:ComposeArgs down
        if ($LASTEXITCODE -ne 0) { Write-Bad "停止失败（退出码 $LASTEXITCODE）"; exit 1 }
    }
    Write-Ok '已停止（数据卷保留，下次启动数据还在）'
    exit 0
}

# ---- 已经在运行？给出提示（仍继续执行，保证跑的是最新代码）----
$already = Test-Web
if ($already.Up) { Write-Info "检测到 Web 已在响应（HTTP $($already.Code)），继续执行以确保运行的是最新代码" }

# ---- 端口冲突检查：5000 被别的程序占着，容器也会起不来 ----
if (-not $already.Up) {
    $owner = Get-PortOwner -Port $WebPort
    if ($owner) {
        if ($owner.IsDocker) {
            Write-Bad "端口 $WebPort 被 Docker 端口转发占用，但 Web 没有响应（pid=$($owner.Pid) $($owner.Name)）"
            Write-Note "先清理再重试：$(Get-ComposeText) down   或者   .\start.ps1 -Stop"
        } else {
            Write-Bad "端口 $WebPort 被进程占用：$($owner.Name) (pid=$($owner.Pid))"
            Write-Note '关掉这个进程，或在 .env 里改 WEB_PORT 后重试'
        }
        exit 1
    }
}

if ($Mode -eq 'Docker') {
    # ---- Docker 模式：一条 compose 拉起全部服务 ----
    if (-not (Test-DockerEngine)) {
        if (-not (Start-DockerDesktop)) {
            Write-Bad 'Docker 引擎未运行，也找不到 Docker Desktop 可执行文件'
            Write-Note '请先打开 Docker Desktop，等状态变成 Running 再运行本脚本'
            exit 1
        }
    }
    Write-Ok 'Docker 引擎就绪'

    # ---- 构建与启动分两步：构建失败才需要换构建器，启动失败是另一类问题 ----
    if (-not $DryRun -and -not $NoBuild) {
        if ($ClassicBuild) {
            Write-Info '按 -ClassicBuild 使用传统构建器（DOCKER_BUILDKIT=0）'
            $env:DOCKER_BUILDKIT = '0'
        }
        try {
            Write-Info "构建镜像：$(Get-ComposeText) build"
            & $script:ComposeExe @script:ComposeArgs build
            if ($LASTEXITCODE -ne 0) {
                # Docker Desktop 开了 containerd 镜像存储时，构建最后一步会去 registry 拉基础层，
                # 网络被拒（registry-1.docker.io / failed to copy）时改用传统构建器即可绕过。
                if ($env:DOCKER_BUILDKIT -eq '0') {
                    Write-Bad "构建失败（退出码 $LASTEXITCODE，已在使用传统构建器）"
                    Write-Note '手动排查：docker compose build'
                    exit 1
                }
                Write-Note '构建失败：若是导出/拉取基础镜像层被网络拒绝（报 registry-1.docker.io、failed to copy），改用传统构建器重试 ...'
                $env:DOCKER_BUILDKIT = '0'
                & $script:ComposeExe @script:ComposeArgs build
                if ($LASTEXITCODE -ne 0) {
                    Write-Bad "构建仍然失败（退出码 $LASTEXITCODE）"
                    Write-Note '手动排查：$env:DOCKER_BUILDKIT=0; docker compose build'
                    exit 1
                }
            }
        } finally {
            Remove-Item Env:DOCKER_BUILDKIT -ErrorAction SilentlyContinue
        }
        Write-Ok '镜像构建完成'
    }

    if (-not $DryRun) {
        Write-Info "启动容器：$(Get-ComposeText) up -d"
        & $script:ComposeExe @script:ComposeArgs up -d
        if ($LASTEXITCODE -ne 0) {
            Write-Bad "容器启动失败（退出码 $LASTEXITCODE）"
            Write-Note "端口被占时：docker compose ps 和 netstat -ano | findstr :$WebPort"
            Write-Note "看日志：$(Get-ComposeText) logs --tail 50 web"
            exit 1
        }
    } else {
        Write-Info "（DryRun）将构建镜像并执行 $(Get-ComposeText) up -d"
    }
} else {
    # ---- Local 模式：中间件走容器，4 个服务走本机 Python 窗口 ----
    if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
        Write-Bad '找不到 python；Local 模式需要本机 Python 3.12 + requirements.txt 依赖'
        Write-Note '或者直接用 Docker 模式：.\start.ps1'
        exit 1
    }
    if (-not (Test-DockerEngine)) {
        if (-not (Start-DockerDesktop)) { Write-Bad 'EMQX / TDengine 需要 Docker，请先启动 Docker Desktop'; exit 1 }
    }

    foreach ($port in @(5020, $WebPort)) {
        $owner = Get-PortOwner -Port $port
        if ($owner) {
            if ($owner.IsDocker) {
                Write-Bad "端口 $port 已被本项目的 Docker 容器占用，Local 模式会和它冲突"
                Write-Note "二选一：用 .\start.ps1（Docker 模式）；或先 $(Get-ComposeText) stop device web"
            } else {
                Write-Bad "端口 $port 已被 $($owner.Name) (pid=$($owner.Pid)) 占用"
            }
            exit 1
        }
    }

    Write-Info '启动中间件容器：emqx + tdengine'
    if (-not $DryRun) {
        & $script:ComposeExe @script:ComposeArgs up -d emqx tdengine
        if ($LASTEXITCODE -ne 0) { Write-Bad "EMQX / TDengine 启动失败（退出码 $LASTEXITCODE）"; exit 1 }
    }

    $services = @(
        @{ Title = 'CEMS-1-Device';     Script = 'src\device\modbus_server.py';      Desc = '仿真设备' },
        @{ Title = 'CEMS-2-Gateway';    Script = 'src\gateway\gateway.py';           Desc = '数采网关' },
        @{ Title = 'CEMS-3-Subscriber'; Script = 'src\platform\subscriber_to_td.py'; Desc = '数据入库' },
        @{ Title = 'CEMS-4-Web';        Script = 'src\web\web_dashboard.py';         Desc = 'Web 大屏' }
    )
    foreach ($s in $services) {
        Write-Info "打开窗口 $($s.Title)（$($s.Desc)）：python $($s.Script)"
        if (-not $DryRun) {
            Start-Process -FilePath 'cmd.exe' `
                -ArgumentList '/k', "title $($s.Title) && python $($s.Script)" `
                -WorkingDirectory $Root | Out-Null
            Start-Sleep -Milliseconds 500
        }
    }
}

# ---- 等 Web 真正能响应，再开浏览器 ----
$health = Wait-Web -Seconds $Timeout
if ($null -eq $health) {
    Write-Bad "等了 $Timeout 秒，Web 仍没有响应（$HealthUrl）"
    Write-Note "排查：$(Get-ComposeText) ps   以及   $(Get-ComposeText) logs --tail 50 web"
    if (-not $DryRun) { Write-Host ''; & $script:ComposeExe @script:ComposeArgs ps }
    Open-Browser -Url $WebUrl
    exit 1
}

Write-Ok "Web 已就绪（HTTP $($health.Code)）"
if ($health.Body -match '"ok"\s*:\s*false') {
    Write-Note 'Web 起来了，但 /api/health 报查库失败：大屏会显示“查库失败”，通常是 TDengine 还在初始化'
    Write-Note "日志：$(Get-ComposeText) logs --tail 50 subscriber"
}

Open-Browser -Url $WebUrl

$lan = Get-LanUrl
Write-Host ''
Write-Host '----------- 访问地址 -----------' -ForegroundColor DarkCyan
Write-Host " Web 大屏    : $WebUrl"
if ($lan) { Write-Host " 局域网访问  : $lan" }
Write-Host ' EMQX 管理台 : http://localhost:18083  (默认 admin/public)'
Write-Host ' 停止服务    : .\start.ps1 -Stop'
Write-Host '--------------------------------' -ForegroundColor DarkCyan
