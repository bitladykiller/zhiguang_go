$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$schema = Join-Path $repoRoot "db\schema.sql"

Get-Content -LiteralPath $schema -Raw | docker compose exec -T mysql mysql -uroot -proot123 zhiguang
