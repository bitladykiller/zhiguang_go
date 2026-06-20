param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$GoArgs
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$workspaceGo = Join-Path $repoRoot ".tools\go\bin\go.exe"
$goExe = if (Test-Path $workspaceGo) { $workspaceGo } else { "go" }

$env:GOCACHE = Join-Path $repoRoot ".gocache"
$env:GOPATH = Join-Path $repoRoot ".gopath"
$env:GOMODCACHE = Join-Path $env:GOPATH "pkg\mod"
$env:GOTMPDIR = Join-Path $repoRoot ".tmp\go-build"

if (Test-Path $workspaceGo) {
    $env:GOROOT = Join-Path $repoRoot ".tools\go"
}

New-Item -ItemType Directory -Force -Path $env:GOCACHE | Out-Null
New-Item -ItemType Directory -Force -Path $env:GOMODCACHE | Out-Null
New-Item -ItemType Directory -Force -Path $env:GOTMPDIR | Out-Null

& $goExe @GoArgs
exit $LASTEXITCODE
