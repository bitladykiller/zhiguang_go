param(
    [switch]$ForceConfig,
    [switch]$OverwriteKeys
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$configExample = Join-Path $repoRoot "config\config-local.yaml.example"
$configLocal = Join-Path $repoRoot "config\config-local.yaml"
$keysDir = Join-Path $repoRoot "config\keys"
$privateKey = Join-Path $keysDir "private.pem"
$publicKey = Join-Path $keysDir "public.pem"

if (-not (Test-Path $configLocal) -or $ForceConfig) {
    Copy-Item -LiteralPath $configExample -Destination $configLocal -Force
    Write-Host "Prepared config\config-local.yaml"
} else {
    Write-Host "Kept existing config\config-local.yaml"
}

New-Item -ItemType Directory -Force -Path $keysDir | Out-Null

$missingKeys = -not (Test-Path $privateKey) -or -not (Test-Path $publicKey)
if ($missingKeys -or $OverwriteKeys) {
    & (Join-Path $PSScriptRoot "gen-jwt-keys.ps1")
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to generate JWT keys"
    }
    Write-Host "Generated config\keys\private.pem and config\keys\public.pem"
} else {
    Write-Host "Kept existing JWT keys"
}

Write-Host ""
Write-Host "Next steps:"
Write-Host "0. If local Go is broken, run .\scripts\install-go-local.ps1 once"
Write-Host "1. docker compose up -d mysql redis zookeeper kafka kafka2 kafka3 kafka-init canal elasticsearch"
Write-Host "2. Get-Content db/schema.sql -Raw | docker compose exec -T mysql mysql -uroot -proot123 zhiguang"
Write-Host "3. .\scripts\run-local.ps1"
Write-Host "4. cd frontend; npm install; npm run dev"
