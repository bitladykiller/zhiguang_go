param(
    [string]$Version = "1.26.4"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$toolsDir = Join-Path $repoRoot ".tools"
$tmpDir = Join-Path $repoRoot ".tmp"
$archivePath = Join-Path $tmpDir ("go{0}.windows-amd64.zip" -f $Version)
$downloadUrl = "https://go.dev/dl/go$Version.windows-amd64.zip"
$installDir = Join-Path $toolsDir "go"

New-Item -ItemType Directory -Force -Path $toolsDir | Out-Null
New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null

if (-not (Test-Path $archivePath)) {
    & curl.exe -L -o $archivePath $downloadUrl
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to download $downloadUrl"
    }
}

if (Test-Path $installDir) {
    $resolvedInstallDir = (Resolve-Path $installDir).Path
    $resolvedToolsDir = (Resolve-Path $toolsDir).Path

    if (-not $resolvedInstallDir.StartsWith($resolvedToolsDir)) {
        throw "Refusing to remove unexpected install path: $resolvedInstallDir"
    }

    Remove-Item -LiteralPath $resolvedInstallDir -Recurse -Force
}

Expand-Archive -Path $archivePath -DestinationPath $toolsDir -Force

Write-Host "Installed Go $Version to .tools\go"
Write-Host "Use .\scripts\run-local.ps1 to run the backend with the local toolchain."
