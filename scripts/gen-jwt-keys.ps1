param(
    [string]$PrivateKeyPath = "config/keys/private.pem",
    [string]$PublicKeyPath = "config/keys/public.pem",
    [int]$KeySize = 2048
)

$ErrorActionPreference = "Stop"

if ($KeySize -lt 2048) {
    throw "RSA key size must be at least 2048 bits."
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$privatePath = Join-Path $repoRoot $PrivateKeyPath
$publicPath = Join-Path $repoRoot $PublicKeyPath
$generatedPublicPath = "$privatePath.pub"

New-Item -ItemType Directory -Force -Path (Split-Path -Parent $privatePath) | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $publicPath) | Out-Null

if (Test-Path $privatePath) {
    Remove-Item -LiteralPath $privatePath -Force
}
if (Test-Path $publicPath) {
    Remove-Item -LiteralPath $publicPath -Force
}
if (Test-Path $generatedPublicPath) {
    Remove-Item -LiteralPath $generatedPublicPath -Force
}

$keygenCommand = "ssh-keygen -q -t rsa -b $KeySize -m PEM -N `"`" -f `"$privatePath`""
& cmd.exe /c $keygenCommand | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "ssh-keygen failed while creating the private key."
}

$publicPem = & ssh-keygen -e -m PKCS8 -f $generatedPublicPath
if ($LASTEXITCODE -ne 0) {
    throw "ssh-keygen failed while exporting the public key."
}

Set-Content -LiteralPath $publicPath -Value $publicPem -Encoding ascii
Remove-Item -LiteralPath $generatedPublicPath -Force
