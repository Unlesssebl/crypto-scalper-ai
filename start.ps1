#!/usr/bin/env pwsh
# Launcher: native HyperData API + external screener.

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Write-Step { param($Message) Write-Host "==> $Message" -ForegroundColor Cyan }
function Write-Ok { param($Message) Write-Host "OK  $Message" -ForegroundColor Green }
function Write-Warn { param($Message) Write-Host "!!  $Message" -ForegroundColor Yellow }
function Show-Usage {
    Write-Host @"
Usage: .\start.ps1 [options]

Starts the complete project: native API, screener, Market Pulse and PostgreSQL.
  --stop              stop all project services, retaining data
  --restart           restart all project services
  --status            show combined service status

  --demo              synthetic data; disables AI, Telegram and Market Pulse
  --api-port N        native API port (default 8420)
  --port N            screener web port (default 8421)
  --tui               also show the Rich Market Overview
  --tui-page NAME     auto|all|market|risk|vault (default: auto)
  --tui-rotate N      seconds between rotated pages (default: 0 = keys only)
  --notify            enable Telegram delivery
  --journal PATH      screener SQLite journal
  --record PATH       append every scan to JSONL
  --help              this help

Examples:
  .\start.ps1
  .\start.ps1 --demo
  .\start.ps1 --tui --notify
"@
}

if ($args -contains "--help" -or $args -contains "-h") {
    Show-Usage
    exit 0
}

function Test-Python {
    param($Command, [string[]]$Prefix)
    if (-not (Get-Command $Command -ErrorAction SilentlyContinue)) { return $false }
    & $Command @Prefix -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)" 2>$null
    return $LASTEXITCODE -eq 0
}

function Find-Python {
    foreach ($candidate in @(
        @{ Command = "py"; Prefix = @("-3.12") },
        @{ Command = "py"; Prefix = @("-3") },
        @{ Command = "python"; Prefix = @() },
        @{ Command = "python3"; Prefix = @() }
    )) {
        if (Test-Python -Command $candidate.Command -Prefix $candidate.Prefix) {
            return $candidate
        }
    }
    return $null
}

$uv = Get-Command uv -ErrorAction SilentlyContinue
$venvPython = $null
if ($uv) {
    Write-Ok "Using $(& uv --version)"
} else {
    $python = Find-Python
    if (-not $python) {
        Write-Warn "Python 3.12+ not found and uv is not installed."
        Write-Host "    Install uv (https://docs.astral.sh/uv/) or Python 3.12+ and run this script again."
        exit 1
    }
    Write-Ok "Using $(& $python.Command @($python.Prefix + @('--version')))"
    $onWindows = ($env:OS -eq "Windows_NT")
    $venvPython = if ($onWindows) {
        Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
    } else {
        Join-Path $PSScriptRoot ".venv/bin/python"
    }
    if (-not (Test-Path $venvPython)) {
        Write-Step "Creating the virtual environment in .venv"
        & $python.Command @($python.Prefix + @("-m", "venv", ".venv"))
        if ($LASTEXITCODE -ne 0) { throw "Could not create .venv" }
    }
    & $venvPython -c "import hyperdata_terminal, extensions.scalper, apps.screener_web, windows_toasts" 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Step "Installing dependencies (the first run can take a minute)"
        & $venvPython -m pip install --upgrade pip
        if ($LASTEXITCODE -ne 0) { throw "Could not upgrade pip" }
        & $venvPython -m pip install -e ".[desktop]"
        if ($LASTEXITCODE -ne 0) { throw "Could not install the project" }
    }
}

$envFile = Join-Path $PSScriptRoot ".env"
if (-not (Test-Path $envFile)) {
    Copy-Item (Join-Path $PSScriptRoot ".env.example") $envFile
    Write-Warn "Created .env from .env.example."
    Write-Host "    Add GEMINI_API_KEY (or configure OpenRouter) to enable AI analysis."
}

# Keep the --port spelling for the screener web port.
$serveArgs = @()
for ($i = 0; $i -lt $args.Count; $i++) {
    if ($args[$i] -eq "--port") {
        $serveArgs += "--web-port"
        if ($i + 1 -lt $args.Count) { $serveArgs += $args[$i + 1]; $i++ }
    } else {
        $serveArgs += $args[$i]
    }
}

if ($uv) {
    & uv run python scripts/project.py @serveArgs
} else {
    & $venvPython scripts/project.py @serveArgs
}
exit $LASTEXITCODE
