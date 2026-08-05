$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$Venv = Join-Path $Root ".venv"
$Python = Join-Path $Venv "Scripts\python.exe"

if (-not (Test-Path $Python)) { python -m venv $Venv }
& $Python -m pip install --upgrade pip
& $Python -m pip install -r (Join-Path $Root "backend\requirements.txt")
if (-not (Test-Path (Join-Path $Root ".env"))) {
    Copy-Item (Join-Path $Root ".env.sample") (Join-Path $Root ".env")
}
Write-Host "Setup complete. Edit .env, then run .\start_jarvis.ps1"
