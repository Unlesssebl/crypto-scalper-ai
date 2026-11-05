#!/usr/bin/env pwsh
# One-command launcher for the Crypto Scalper fork.
# Usage: .\start.ps1 [--demo] [--tui] [--port 8420] [--notify] [--journal PATH]

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Write-Step { param($Message) Write-Host "==> $Message" -ForegroundColor Cyan }
function Write-Ok { param($Message) Write-Host "OK  $Message" -ForegroundColor Green }
function Write-Warn { param($Message) Write-Host "!!  $Message" -ForegroundColor Yellow }

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

    $version = & $python.Command @($python.Prefix + @("--version"))
    Write-Ok "Using $version"

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

    & $venvPython -c "import hyperdata_terminal, extensions.scalper, apps.scalper_web" 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Step "Installing dependencies (the first run can take a minute)"
        & $venvPython -m pip install --upgrade pip
        if ($LASTEXITCODE -ne 0) { throw "Could not upgrade pip" }
        & $venvPython -m pip install -e .
        if ($LASTEXITCODE -ne 0) { throw "Could not install the project" }
    }
}

$envFile = Join-Path $PSScriptRoot ".env"
if (-not (Test-Path $envFile)) {
    Copy-Item (Join-Path $PSScriptRoot ".env.example") $envFile
    Write-Warn "Created .env from .env.example."
    Write-Host "    Add GEMINI_API_KEY (or configure OpenRouter) to enable AI analysis."
    Write-Host "    Without a key the dashboard still runs; signals stay HYPERDATA_ONLY."
}

$port = 8420
for ($i = 0; $i -lt $args.Count; $i++) {
    if ($args[$i] -eq "--port" -and $i + 1 -lt $args.Count) {
        $port = $args[$i + 1]
    } elseif ($args[$i] -like "--port=*") {
        $port = $args[$i].Substring("--port=".Length)
    }
}

$helpRequested = $args -contains "--help" -or $args -contains "-h"
if (-not $helpRequested) {
    Write-Step "Starting Crypto Scalper"
    Write-Host "    Dashboard: http://127.0.0.1:$port/scalper"
    Write-Host "    Stop with Ctrl+C"
    Write-Host "    Other modes: .\start.ps1 --demo | --tui | --port 8500 | --notify"
}

if ($uv) {
    & uv run python -m apps.scalper_web @args
} else {
    & $venvPython -m apps.scalper_web @args
}
exit $LASTEXITCODE
