[CmdletBinding()]
param(
    [string]$ReleaseTag = "offline-arm64-py310-20260721-r7"
)

$ErrorActionPreference = "Stop"
$Repo = "intellistream/forL0-state-backend"
$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$BundleName = "forl0-offline-linux-arm64-py310-20260721.tar.gz"
$BundlePath = Join-Path $RepoRoot $BundleName
$ChecksumPath = "$BundlePath.sha256"

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    throw "GitHub CLI (gh) is required. Install it and run: gh auth login"
}

& gh auth status
if ($LASTEXITCODE -ne 0) {
    throw "GitHub CLI is not authenticated. Run: gh auth login"
}

Write-Host "Downloading private Release assets into: $RepoRoot"
& gh release download $ReleaseTag `
    --repo $Repo `
    --pattern $BundleName `
    --pattern "$BundleName.sha256" `
    --dir $RepoRoot `
    --clobber
if ($LASTEXITCODE -ne 0) {
    throw "Release download failed: $ReleaseTag"
}

$Expected = (Get-Content $ChecksumPath -Raw).Trim().Split()[0].ToLowerInvariant()
$Actual = (Get-FileHash $BundlePath -Algorithm SHA256).Hash.ToLowerInvariant()
if ($Actual -ne $Expected) {
    throw "SHA256 mismatch: expected $Expected, got $Actual"
}

Write-Host "SHA256 OK: $Actual"
Write-Host ""
Write-Host "IDEA Deployment should upload the complete repository, including:"
Write-Host "  $BundleName"
Write-Host "  $BundleName.sha256"
Write-Host "  run-forl0-offline.sh"
Write-Host ""
Write-Host "Then run on the target server:"
Write-Host "  cd /root/forl0"
Write-Host "  FORL0_OFFLINE_ONLY=true bash ./run-forl0-offline.sh"
