<#
.SYNOPSIS
  Write a Raspberry Pi OS image to a USB SD-card reader and pre-enable SSH + login user.

.DESCRIPTION
  Safety first: refuses to touch any disk that is not USB, is boot/system, is read-only,
  or is smaller than 6 GB. Asks for explicit confirmation unless -Force is given.

  Requires: elevated PowerShell, bsdtar (ships with Windows 10+), openssl (Git for Windows).
  Example:
    .\_pi_write_card.ps1 -ImageFile C:\tmp\2026-10-06-raspios-bookworm-armhf-lite.img.xz `
        -DiskNumber 4 -UserName ddf -Password 'TempPass123' `
        -ExpectedImageSha256 340d17e344724aebe42f5ca7d16fa569d99a64ceb9304942b551defed4edc4b9

  What it does, in order:
    1. checks the image (.xz is decompressed with bsdtar, extract sha256 verified when supplied)
    2. checks the target disk (USB only, not boot/system, >= 6 GB)
    3. takes the disk offline, writes the raw image, brings it back online
    4. finds the FAT boot partition, creates an empty "ssh" file and "userconf.txt"
       (userconf.txt = <user>:<sha512-crypt password hash>) so the Pi boots headless
       with SSH enabled and the requested login user
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$ImageFile,
    [Parameter(Mandatory = $true)][int]$DiskNumber,
    [Parameter(Mandatory = $true)][string]$UserName,
    [Parameter(Mandatory = $true)][string]$Password,
    [string]$ExpectedImageSha256 = '',
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

function Fail([string]$Message) {
    Write-Host ("ERROR: " + $Message) -ForegroundColor Red
    exit 1
}

function Get-FreeDriveLetter {
    $used = (Get-Volume | Where-Object { $_.DriveLetter } | ForEach-Object { $_.DriveLetter })
    foreach ($c in [char[]]'ZYXWVUTSRQPONMLKJIHGFED') {
        if ($used -notcontains $c) { return $c }
    }
    throw 'no free drive letter'
}

# ---- 0) elevation ----------------------------------------------------------
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) { Fail 'run this from an elevated PowerShell (Run as administrator)' }
Write-Host 'admin: ok'

# ---- 1) image --------------------------------------------------------------
if (-not (Test-Path -LiteralPath $ImageFile)) { Fail ("image not found: " + $ImageFile) }
$ImageFile = (Resolve-Path -LiteralPath $ImageFile).Path
$imgFile = $ImageFile
$tmpDir = $null

if ($ImageFile -like '*.xz') {
    $xzHash = (Get-FileHash -LiteralPath $ImageFile -Algorithm SHA256).Hash.ToLower()
    Write-Host ("xz file sha256: " + $xzHash)
    $tmpDir = Join-Path $env:TEMP ('piimg_' + [guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $tmpDir | Out-Null
    Write-Host 'decompressing with bsdtar (may take 1-2 min) ...'
    & tar -xf $ImageFile -C $tmpDir
    if ($LASTEXITCODE -ne 0) { Fail 'bsdtar failed to decompress the image' }
    $imgFile = (Get-ChildItem -Path $tmpDir -Filter '*.img' | Select-Object -First 1).FullName
    if (-not $imgFile) { Fail 'no .img found after decompression' }
}

$imgSize = (Get-Item -LiteralPath $imgFile).Length
Write-Host ("image: " + $imgFile)
Write-Host ("image size: " + [math]::Round($imgSize / 1GB, 2) + " GB")

if ($ExpectedImageSha256) {
    Write-Host 'verifying extract sha256 ...'
    $imgHash = (Get-FileHash -LiteralPath $imgFile -Algorithm SHA256).Hash.ToLower()
    if ($imgHash -ne $ExpectedImageSha256.ToLower()) {
        Fail ("extract sha256 mismatch: got " + $imgHash + " expected " + $ExpectedImageSha256)
    }
    Write-Host 'extract sha256: ok'
}

# ---- 2) target disk --------------------------------------------------------
$disk = Get-Disk -Number $DiskNumber -ErrorAction SilentlyContinue
if (-not $disk) { Fail ("disk " + $DiskNumber + " not found") }
if ($disk.BusType -ne 'USB') { Fail ("disk " + $DiskNumber + " BusType=" + $disk.BusType + "; only USB cards are allowed") }
if ($disk.IsBoot -or $disk.IsSystem) { Fail 'refusing to write the boot/system disk' }
if ($disk.IsReadOnly) { Fail 'target disk is read-only' }
if ($disk.Size -lt 6GB) { Fail ("target too small: " + [math]::Round($disk.Size / 1GB, 2) + " GB") }
if ($disk.Size -lt $imgSize) { Fail 'target smaller than the image' }

Write-Host ''
Write-Host '==== TARGET DISK ====' -ForegroundColor Yellow
$disk | Format-List Number, FriendlyName, BusType, @{ n = 'SizeGB'; e = { [math]::Round($_.Size / 1GB, 2) } }, IsBoot, IsSystem, PartitionStyle
Write-Host '==== IMAGE ====' -ForegroundColor Yellow
Write-Host ('  ' + $imgFile + '  (' + [math]::Round($imgSize / 1GB, 2) + ' GB)')
Write-Host ''
Write-Host 'ALL DATA ON THAT DISK WILL BE DESTROYED.' -ForegroundColor Red
if (-not $Force) {
    $answer = Read-Host 'Type YES (uppercase) to continue'
    if ($answer -cne 'YES') { Fail 'aborted by user' }
}

# ---- 3) write --------------------------------------------------------------
Write-Host 'taking disk offline ...'
$wentOffline = $true
try {
    Set-Disk -Number $DiskNumber -IsOffline $true
}
catch {
    $wentOffline = $false
    Write-Host 'offline failed, dismounting volumes instead ...'
    foreach ($p in (Get-Partition -DiskNumber $DiskNumber -ErrorAction SilentlyContinue)) {
        if ($p.DriveLetter) {
            Remove-PartitionAccessPath -DiskNumber $DiskNumber -PartitionNumber $p.PartitionNumber `
                -AccessPath ([string]$p.DriveLetter + ':\') -ErrorAction SilentlyContinue
        }
    }
}
try {
    $devicePath = '\\.\PhysicalDrive' + $DiskNumber
    Write-Host ('writing to ' + $devicePath + ' ...')
    $src = [IO.File]::OpenRead($imgFile)
    $dst = [IO.File]::Open($devicePath, 'Open', 'Write', 'None')
    try {
        $buf = New-Object byte[] (4 * 1024 * 1024)
        $done = 0
        $sw = [Diagnostics.Stopwatch]::StartNew()
        while (($read = $src.Read($buf, 0, $buf.Length)) -gt 0) {
            $dst.Write($buf, 0, $read)
            $done += $read
            if ($sw.ElapsedMilliseconds -gt 5000) {
                $pct = [math]::Round(100.0 * $done / $imgSize, 1)
                Write-Host ("  ... " + $pct + " % (" + [math]::Round($done / 1MB) + " MB)")
                $sw.Restart()
            }
        }
        $dst.Flush()
    }
    finally {
        $dst.Dispose()
        $src.Dispose()
    }
    Write-Host 'image written'
}
finally {
    if ($wentOffline) {
        Set-Disk -Number $DiskNumber -IsOffline $false
    }
}

Write-Host 'refreshing storage cache ...'
Update-HostStorageCache
Start-Sleep -Seconds 3

# ---- 4) verify partition layout --------------------------------------------
$parts = Get-Partition -DiskNumber $DiskNumber -ErrorAction SilentlyContinue
if (-not $parts) { Fail 'no partitions found after writing; card may be fake or reader faulty' }
Write-Host ''
Write-Host 'partitions:'
$parts | Select-Object PartitionNumber, DriveLetter, PartitionStyle, @{ n = 'SizeMB'; e = { [math]::Round($_.Size / 1MB) } } | Format-Table -AutoSize
if (@($parts).Count -lt 2) { Fail 'expected at least 2 partitions (boot + rootfs)' }

# ---- 5) headless customization --------------------------------------------
$boot = $parts | Where-Object { $_.Size -lt 1GB } | Select-Object -First 1
if (-not $boot) { Fail 'boot (FAT) partition not found' }
if (-not $boot.DriveLetter) {
    $letter = Get-FreeDriveLetter
    Set-Partition -DiskNumber $DiskNumber -PartitionNumber $boot.PartitionNumber -NewDriveLetter $letter
    Start-Sleep -Seconds 2
    $boot = Get-Partition -DiskNumber $DiskNumber -PartitionNumber $boot.PartitionNumber
}
$bootRoot = [string]$boot.DriveLetter + ':\'
Write-Host ('boot partition: ' + $bootRoot)

if (-not (Test-Path -LiteralPath (Join-Path $bootRoot 'cmdline.txt'))) {
    Fail 'cmdline.txt not found on the boot partition; unexpected image layout'
}

# empty "ssh" file enables the SSH server on first boot
New-Item -ItemType File -Path (Join-Path $bootRoot 'ssh') -Force | Out-Null

# userconf.txt = <user>:<sha512-crypt hash>
$openssl = @(
    'C:\Program Files\Git\usr\bin\openssl.exe',
    'C:\Program Files\Git\mingw64\bin\openssl.exe'
) | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
if (-not $openssl) { Fail 'openssl.exe not found (install Git for Windows)' }

$salt = (New-Guid).ToString('N').Substring(0, 16)
$hash = & $openssl passwd -6 -salt $salt $Password
if ($LASTEXITCODE -ne 0 -or -not $hash) { Fail 'openssl passwd failed' }
$hash = $hash.Trim()
$userconf = $UserName + ':' + $hash
Set-Content -LiteralPath (Join-Path $bootRoot 'userconf.txt') -Value $userconf -Encoding ascii -NoNewline

Write-Host ''
Write-Host 'headless customization written:' -ForegroundColor Green
Write-Host ('  ' + $bootRoot + 'ssh        (enables sshd on first boot)')
Write-Host ('  ' + $bootRoot + 'userconf.txt  (user "' + $UserName + '", sha512-crypt hash)')

if ($tmpDir -and (Test-Path -LiteralPath $tmpDir)) {
    Remove-Item -LiteralPath $tmpDir -Recurse -Force
}

Write-Host ''
Write-Host 'DONE. Remove the card safely (eject it) and put it into the Raspberry Pi.' -ForegroundColor Green
Write-Host 'First boot: takes 1-3 minutes, may reboot once (root filesystem resize).'
