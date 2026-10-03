# =============================================================================
#  Import the image bundle produced by scripts\export_images.ps1
#  Use this on the CLEAN / OFFLINE machine.
#
#  Usage (repo root, or anywhere with -From pointing at the tar folder):
#     pwsh -NoProfile -File scripts\import_images.ps1 -From D:\dist
#     pwsh -NoProfile -File scripts\import_images.ps1 -From D:\dist -WhatIf
#
#  Then start the stack:
#     docker compose up -d --build          # --build needs no network: base layers are local
#     docker compose --profile plant2 up -d # if you also want the 2nd device
#
#  Why BY TAG (and never by digest) -> see the header of export_images.ps1.
# =============================================================================
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$From,
    [switch]$WhatIf
)

$ErrorActionPreference = 'Stop'

$expectedImages = @(
    'emqx/emqx:6.3.0',
    'tdengine/tdengine:3.3.6.13',
    'redis:7.4.11-alpine',
    'nginx:1.31.6-alpine',
    'cems-pipeline:latest'
)

function Write-Step([string]$t) { Write-Host "`n==> $t" -ForegroundColor Cyan }
function Write-Ok([string]$t)   { Write-Host "    $t" -ForegroundColor Green }
function Write-Warn2([string]$t){ Write-Host "    $t" -ForegroundColor Yellow }

if (-not (Test-Path $From)) { throw "folder not found: $From" }
$tars = Get-ChildItem -Path $From -Filter '*.tar' | Sort-Object Name
if (-not $tars) { throw "no .tar found in $From" }

Write-Step "Bundle"
Write-Host "    folder: $From"
foreach ($file in $tars) {
    Write-Host ("      {0}  ({1} MB)" -f $file.Name, [math]::Round($file.Length / 1MB, 1))
}

# ---- 1. verify checksum if the .sha256 file travelled along ------------------
$sumFiles = Get-ChildItem -Path $From -Filter '*.sha256'
foreach ($sumFile in $sumFiles) {
    Write-Step "Verifying checksum: $($sumFile.Name)"
    foreach ($line in (Get-Content $sumFile.FullName)) {
        if ($line -notmatch '^([0-9a-f]{64})\s+(.+)$') { continue }
        $want = $Matches[1]
        $name = $Matches[2].Trim()
        $path = Join-Path $From $name
        if (-not (Test-Path $path)) { Write-Warn2 "missing: $name (skipped)"; continue }
        $got = (Get-FileHash -Algorithm SHA256 -Path $path).Hash.ToLower()
        if ($got -ne $want) { throw "CHECKSUM MISMATCH for $name`n  want $want`n  got  $got" }
        Write-Ok "$name  OK"
    }
}

# ---- 2. read image names out of each tar (source of truth) -------------------
Write-Step "Images carried by this bundle"
$carried = New-Object System.Collections.Generic.List[string]
foreach ($file in $tars) {
    $probe = Join-Path $env:TEMP ("cems-import-probe-" + [guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Force -Path $probe | Out-Null
    try {
        tar -xf $file.FullName -C $probe
        $manifestPath = Join-Path $probe 'manifest.json'
        if (-not (Test-Path $manifestPath)) { Write-Warn2 "$($file.Name): no manifest.json (skipped)"; continue }
        $manifest = Get-Content $manifestPath -Raw | ConvertFrom-Json
        foreach ($entry in $manifest) {
            if ($null -eq $entry.RepoTags -or $entry.RepoTags.Count -eq 0) {
                throw ("$($file.Name) carries an image with RepoTags=null -- it was exported BY DIGEST. " +
                       "That image cannot be used by docker-compose.yml. Re-export BY TAG on the source machine.")
            }
            foreach ($tag in $entry.RepoTags) {
                if ($tag -eq '<none>:<none>') {
                    throw "$($file.Name) carries an unnamed image (<none>:<none>). Re-export BY TAG."
                }
                $carried.Add($tag)
                Write-Ok "$tag"
            }
        }
    } finally {
        Remove-Item -Recurse -Force $probe -ErrorAction SilentlyContinue
    }
}

# ---- 3. compare with what docker-compose.yml expects -------------------------
Write-Step "Checking against what docker-compose.yml expects"
$missing = @()
foreach ($image in $expectedImages) {
    if ($carried -contains $image) { Write-Ok "$image  present in bundle" }
    else { $missing += $image; Write-Warn2 "$image  NOT in bundle" }
}
if ($missing.Count -gt 0) {
    Write-Warn2 "compose needs these but the bundle does not carry them:"
    $missing | ForEach-Object { Write-Host "      $_" -ForegroundColor Yellow }
    Write-Warn2 "Either re-export with export_images.ps1, or those services will fail to start offline."
}

if ($WhatIf) { Write-Warn2 "-WhatIf: nothing was loaded."; exit 0 }

# ---- 4. load ------------------------------------------------------------------
Write-Step "Loading images (docker load)"
foreach ($file in $tars) {
    docker load -i $file.FullName
    if ($LASTEXITCODE -ne 0) { throw "docker load failed for $($file.Name)" }
}

# ---- 5. verify: every expected reference must now resolve locally ------------
Write-Step "Verifying (these must all resolve WITHOUT network)"
$bad = 0
foreach ($image in $expectedImages) {
    docker image inspect $image *> $null
    if ($LASTEXITCODE -eq 0) {
        $id = docker image inspect $image --format '{{.Id}}'
        Write-Ok "$image  ->  $($id.Substring(0, 19))..."
    } else {
        Write-Warn2 "$image  ->  NOT FOUND"
        $bad++
    }
}
if ($bad -gt 0) { throw "$bad image(s) still missing. Do not expect compose to start offline." }

Write-Step "Done -- start the stack with:"
Write-Host "    docker compose up -d --build" -ForegroundColor Cyan
Write-Host "    docker compose --profile plant2 up -d      # optional 2nd device" -ForegroundColor Cyan
Write-Host "    docker compose ps                          # expect healthy" -ForegroundColor Cyan
Write-Host ""
Write-Host "  If compose still tries to reach the network, the images above are fine but a" -ForegroundColor Yellow
Write-Host "  service may reference something not in the bundle -- re-run export_images.ps1." -ForegroundColor Yellow
