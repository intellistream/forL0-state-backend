[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("arm64", "x64")]
    [string]$TargetArch,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^3\.[0-9]+$')]
    [string]$PythonVersion,

    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$OutputDir = ""
)

$ErrorActionPreference = "Stop"

$resolvedRepoRoot = (Resolve-Path $RepoRoot).Path
$requirementsFile = Join-Path $resolvedRepoRoot "benchmark\requirements.txt"
if (-not (Test-Path -LiteralPath $requirementsFile -PathType Leaf)) {
    throw "requirements file not found: $requirementsFile"
}

if ([string]::IsNullOrWhiteSpace($OutputDir)) {
    $OutputDir = Join-Path $resolvedRepoRoot "offline-packages"
}
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
$resolvedOutputDir = (Resolve-Path $OutputDir).Path

if (Get-Command py -ErrorAction SilentlyContinue) {
    $pythonExe = (Get-Command py).Source
    $pythonPrefix = @("-3")
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $pythonExe = (Get-Command python).Source
    $pythonPrefix = @()
} else {
    throw "Python 3 was not found. Install Python 3 on this Windows machine and retry."
}

$versionDigits = $PythonVersion.Replace(".", "")
$abi = "cp$versionDigits"
$platform = switch ($TargetArch) {
    "arm64" { "manylinux2014_aarch64" }
    "x64" { "manylinux2014_x86_64" }
}

Write-Host "Downloading wheels for Linux $TargetArch / CPython $PythonVersion"
Write-Host "Requirements: $requirementsFile"
Write-Host "Output:       $resolvedOutputDir"

& $pythonExe @pythonPrefix -m pip --version
if ($LASTEXITCODE -ne 0) {
    throw "pip is unavailable for the selected Windows Python."
}

$pipArgs = @(
    "-m", "pip", "download",
    "--only-binary=:all:",
    "--platform", $platform,
    "--implementation", "cp",
    "--python-version", $versionDigits,
    "--abi", $abi,
    "--dest", $resolvedOutputDir,
    "--requirement", $requirementsFile
)
& $pythonExe @pythonPrefix @pipArgs
if ($LASTEXITCODE -ne 0) {
    throw "Wheel download failed. Check that pip is current and that the requested Python version is supported."
}

Write-Host ""
Write-Host "Done. Copy this entire directory to the offline Linux server:"
Write-Host "  $resolvedOutputDir"
Write-Host "Place it as <ForL0 root>/offline-packages, then rerun forl0-offline-app.sh."
