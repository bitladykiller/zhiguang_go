param(
    [string]$Config = "config/config-local.yaml"
)

$ErrorActionPreference = "Stop"
$goScript = Join-Path $PSScriptRoot "go-local.ps1"

& $goScript run ./cmd/server -config $Config
exit $LASTEXITCODE
