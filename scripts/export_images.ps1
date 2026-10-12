# =============================================================================
#  Export all images of this project into a portable tar bundle.
#
#  Why this exists (the interview question it answers):
#     "I give you a clean machine with no network. Make it run."
#     `docker compose up` needs images FIRST, and a clean machine has NONE.
#     So: export -> carry (USB / intranet) -> import -> compose up.
#
#  CRITICAL RULE (measured 2026-10-03, do not "optimize" this):
#     ALWAYS export BY TAG.  NEVER by digest.
#       docker save nginx:1.31.6-alpine   -> tar manifest RepoTags = ["nginx:1.31.6-alpine"]  OK
#       docker save nginx@sha256:df221d.. -> tar manifest RepoTags = null                    BROKEN
#     A tar exported by digest carries NO NAME.  After `docker load` the image sits there
#     unnamed, and `docker compose up` cannot resolve the digest reference at all:
#       Unable to find image 'nginx@sha256:...' locally
#       docker.io/library/nginx@sha256:...: Pulling from library/nginx
#     -> on a machine WITHOUT network that is a hard failure.
#     This is exactly why docker-compose.yml pins infrastructure images by
#     version tag (6.3.0 / 3.3.6.13 / 7.4.11-alpine / 1.31.6-alpine) and not by digest.
#
#  Usage (run in the repo root):
#     pwsh -NoProfile -File scripts\export_images.ps1                      # export to .\dist
#     pwsh -NoProfile -File scripts\export_images.ps1 -Out D:\cems-offline # export elsewhere
#     pwsh -NoProfile -File scripts\export_images.ps1 -DryRun              # list only
#     pwsh -NoProfile -File scripts\export_images.ps1 -SkipBuild           # reuse current app image
#     pwsh -NoProfile -File scripts\export_images.ps1 -Split              # one tar per image
#
#  Output: dist\cems-images-<stamp>.tar  (+  dist\cems-images-<stamp>.sha256)
#  Import on the target machine: see scripts\import_images.ps1
# =============================================================================
[CmdletBinding()]
param(
    [string]$Out = (Join-Path (Get-Location) 'dist'),
    [switch]$DryRun,
    [switch]$SkipBuild,
    [switch]$Split
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot

# All image references MUST match docker-compose.yml exactly.
$infraImages = @(
    'emqx/emqx:6.3.0',
    'tdengine/tdengine:3.3.6.13',
    'redis:7.4.11-alpine',
    'nginx:1.31.6-alpine'
)
$appImage = 'cems-pipeline:latest'

function Write-Step([string]$text) { Write-Host "`n==> $text" -ForegroundColor Cyan }
function Write-Ok([string]$text)   { Write-Host "    $text" -ForegroundColor Green }
function Write-Warn2([string]$text){ Write-Host "    $text" -ForegroundColor Yellow }

# ---- 0. sanity: refuse to export a digest reference --------------------------
foreach ($image in ($infraImages + $appImage)) {
    if ($image -match '@sha256:') {
        throw "REFUSING: '$image' is a digest reference. Export BY TAG (see header)."
    }
}

Write-Step "Plan"
Write-Host "    repo      : $repoRoot"
Write-Host "    output dir: $Out"
Write-Host "    images    : $($infraImages.Count) infra + 1 app"
foreach ($image in $infraImages) { Write-Host "      - $image" }
Write-Host "      - $appImage  (built from this repo)"

if ($DryRun) {
    Write-Warn2 "DryRun: nothing was built, pulled, saved."
    exit 0
}

# ---- 1. build the application image -----------------------------------------
if (-not $SkipBuild) {
    Write-Step "Building application image (single build entry point)"
    Push-Location $repoRoot
    try {
        # DOCKER_BUILDKIT=0 is required on this host: BuildKit cannot reach docker.io
        # to fetch base layers (documented in docs/runbooks, same as local dev flow).
        $env:DOCKER_BUILDKIT = '0'
        docker compose build device
        if ($LASTEXITCODE -ne 0) { throw "docker compose build device failed" }
    } finally {
        Pop-Location
    }
    Write-Ok "application image built"
} else {
    Write-Warn2 "-SkipBuild: reusing the existing $appImage (make sure it is current!)"
}

# ---- 2. make sure the four infra images are present locally -----------------
Write-Step "Checking / pulling infra images"
foreach ($image in $infraImages) {
    docker image inspect $image *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Warn2 "$image not present locally -> docker pull"
        docker pull $image
        if ($LASTEXITCODE -ne 0) { throw "docker pull $image failed" }
    }
    $digest = docker image inspect $image --format '{{index .RepoDigests 0}}'
    Write-Ok "$image  ->  $digest"
}

# ---- 3. export ---------------------------------------------------------------
New-Item -ItemType Directory -Force -Path $Out | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$allImages = $infraImages + $appImage

if ($Split) {
    Write-Step "Exporting one tar per image (BY TAG)"
    foreach ($image in $allImages) {
        $safe = ($image -replace '[/:]', '_')
        $target = Join-Path $Out "cems-$safe-$stamp.tar"
        docker save -o $target $image
        if ($LASTEXITCODE -ne 0) { throw "docker save $image failed" }
        Write-Ok "$([IO.Path]::GetFileName($target))  ($([math]::Round((Get-Item $target).Length/1MB,1)) MB)"
    }
} else {
    $target = Join-Path $Out "cems-images-$stamp.tar"
    Write-Step "Exporting $($allImages.Count) images into one tar (BY TAG)"
    docker save -o $target @allImages
    if ($LASTEXITCODE -ne 0) { throw "docker save failed" }
    Write-Ok "$([IO.Path]::GetFileName($target))  ($([math]::Round((Get-Item $target).Length/1MB,1)) MB)"
}

# ---- 4. checksum (so the target machine can prove the copy is intact) --------
Write-Step "Writing checksum"
$tars = Get-ChildItem -Path $Out -Filter '*.tar' | Where-Object { $_.Name -notlike '*cems-images-*' -or $true }
$lines = @()
foreach ($file in (Get-ChildItem -Path $Out -Filter "*-$stamp.tar")) {
    $hash = (Get-FileHash -Algorithm SHA256 -Path $file.FullName).Hash.ToLower()
    $lines += "$hash  $($file.Name)"
    Write-Ok "$($file.Name)  sha256=$($hash.Substring(0,16))..."
}
$sumFile = Join-Path $Out "cems-images-$stamp.sha256"
Set-Content -Path $sumFile -Value $lines -Encoding ASCII

# ---- 5. self-check: the tar MUST carry image names ---------------------------
Write-Step "Self-check: does the tar carry names? (RepoTags must not be null)"
$probeImage = $infraImages[-1]        # nginx, the smallest one
$probeTar = Join-Path $env:TEMP "cems-probe-$stamp.tar"
docker save -o $probeTar $probeImage
$probeDir = Join-Path $env:TEMP "cems-probe-$stamp"
New-Item -ItemType Directory -Force -Path $probeDir | Out-Null
tar -xf $probeTar -C $probeDir
$manifest = Get-Content (Join-Path $probeDir 'manifest.json') -Raw
Remove-Item -Recurse -Force $probeDir, $probeTar -ErrorAction SilentlyContinue
if ($manifest -match '"RepoTags":\s*null') {
    throw "SELF-CHECK FAILED: the tar has RepoTags=null -> the target machine cannot use it."
}
Write-Ok "RepoTags present: $(($manifest | Select-String -Pattern '"RepoTags":\[[^\]]*\]').Matches.Value)"

Write-Step "Done"
Write-Host "    bundle : $Out"
Write-Host "    checksum file: $sumFile"
Write-Host ""
Write-Host "  Next (on the clean machine, no network):" -ForegroundColor Cyan
Write-Host "    1) copy dist\*.tar and dist\*.sha256 over (USB / intranet)"
Write-Host "    2) copy the repo over too (or git clone from an intranet mirror)"
Write-Host "    3) pwsh -NoProfile -File scripts\import_images.ps1 -From <folder with tars>"
Write-Host "    4) docker compose up -d --build     # --build works offline: all layers are local"
Write-Host ""
