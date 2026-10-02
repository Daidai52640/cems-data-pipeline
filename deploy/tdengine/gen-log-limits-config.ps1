# ============================================================================
#  Regenerate TDengine "log limits" config files FROM THE IMAGE, then patch.
#
#  Why generate-from-image instead of hand-copying taos.cfg into the repo:
#    taos.cfg contains many image defaults; a hand copy silently goes stale on
#    image upgrade. We take the file from the image and only patch the
#    log-related parameters, so re-running after an image bump resyncs it.
#    The patch is idempotent (regex replace, not blind append).
#
#  Usage (repo root):  pwsh -NoProfile -File deploy\tdengine\gen-log-limits-config.ps1
#
#  NOTE: this file is pure ASCII on purpose. Chinese explanations are written
#        INTO the generated config files at runtime (as data), not into this
#        script. See ~/.dsh/AGENTS.md: .ps1/.bat must stay ASCII.
# ============================================================================

$ErrorActionPreference = 'Stop'
$here  = $PSScriptRoot
$image = 'tdengine/tdengine:latest'
$utf8  = [Text.UTF8Encoding]::new($false)   # no BOM: docker/TOML parsers reject BOM

function Get-FromImage([string]$path) {
    $tmp = [IO.Path]::GetTempFileName()
    & docker run --rm --entrypoint cat $image $path | Set-Content -Path $tmp -Encoding utf8
    if ($LASTEXITCODE -ne 0) { throw "cannot read $path from image $image" }
    return [IO.File]::ReadAllText($tmp)
}

# ---------------------------------------------------------------- taos.cfg
Write-Host '[1/2] taos-log-limits.cfg'
$cfg = Get-FromImage '/etc/taos/taos.cfg'
$append = @(
    ''
    '# ---- project append: log limits (stop /var/log/taos growing forever) ----'
    '# measured on this host: /var/log/taos = 658 MB while business data is 91 MB (7.2x),'
    '# and the oldest taosAdapter log was 3 weeks old (logKeepDays=0 => never delete).'
    '# three layers together; any one alone is not enough:'
    '#   numOfLogLines    cap on lines per file (prevents one file growing forever)'
    '#   minimalLogDirGB  start cleaning once the log dir reaches this size'
    '#   logKeepDays      delete logs older than N days'
    'numOfLogLines            2000000'
    'minimalLogDirGB          1.0'
    'logKeepDays              7'
) -join "`n"
[IO.File]::WriteAllText((Join-Path $here 'taos-log-limits.cfg'), ($cfg.TrimEnd() + "`n" + $append + "`n"), $utf8)

# ------------------------------------------------------- taosadapter.toml
# This one ALREADY has a [log] section with rotationCount/rotationSize.
# Appending root-level duplicates would be a TOML duplicate key and the
# adapter would fail to start, so we patch the values in place.
Write-Host '[2/2] taosadapter-log-limits.toml (patch inside [log])'
$toml = Get-FromImage '/etc/taos/taosadapter.toml'

$toml = [regex]::Replace($toml, '(?m)^\s*rotationCount\s*=.*$',     'rotationCount = 10')
$toml = [regex]::Replace($toml, '(?m)^\s*rotationSize\s*=.*$',      'rotationSize = "100MB"')
# retention key is named keepDays in this image (NOT logKeepDays -- an earlier
# attempt added logKeepDays, which the image ignores entirely).
$toml = [regex]::Replace($toml, '(?m)^\s*keepDays\s*=.*$',          'keepDays = 7')
# drop any logKeepDays we may have written in an earlier run of this script
$toml = [regex]::Replace($toml, '(?m)^\s*logKeepDays\s*=.*\r?\n',   '')
[IO.File]::WriteAllText((Join-Path $here 'taosadapter-log-limits.toml'), $toml, $utf8)

Write-Host ''
Write-Host 'done. generated:'
Write-Host ('  ' + (Join-Path $here 'taos-log-limits.cfg'))
Write-Host ('  ' + (Join-Path $here 'taosadapter-log-limits.toml'))
Write-Host ''
Write-Host 'next: docker compose up -d --force-recreate tdengine'
