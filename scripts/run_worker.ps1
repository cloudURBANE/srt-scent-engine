<#
.SYNOPSIS
    Launch the local enrichment worker with your worker token set.

.DESCRIPTION
    Sets ENRICHMENT_WORKER_TOKEN (and SCENT_API_BASE_URL) for this process and
    starts scripts/enrichment_worker.py.

    Token resolution order:
      1. -Token argument
      2. Saved token file:  %USERPROFILE%\.scentcast\enrichment_token.txt
      3. Interactive prompt

    The token is never stored inside the repository, so it cannot end up in a
    git commit. Use -Save once to remember it for future runs.

.EXAMPLE
    .\scripts\run_worker.ps1 -Token 11008855334400 -Save
    First run: use this token and remember it.

.EXAMPLE
    .\scripts\run_worker.ps1
    Every run after that: token is read from the saved file, dashboard starts.

.EXAMPLE
    .\scripts\run_worker.ps1 -Mode pending -- --limit 20 --delay 45
    Process pending jobs, passing extra flags straight to the worker.
#>
[CmdletBinding(PositionalBinding = $false)]
param(
    [string]$Token = "",
    [switch]$Save,
    [ValidateSet("dashboard", "management", "pending", "auto")]
    [string]$Mode = "dashboard",
    [string]$ApiBaseUrl = "",
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$WorkerArgs = @()
)

$ErrorActionPreference = "Stop"

$tokenDir  = Join-Path $env:USERPROFILE ".scentcast"
$tokenFile = Join-Path $tokenDir "enrichment_token.txt"
$repoRoot  = Split-Path -Parent $PSScriptRoot   # scripts/ -> repo root

# --- Resolve the token -------------------------------------------------------
if (-not $Token -and (Test-Path $tokenFile)) {
    $Token = (Get-Content $tokenFile -TotalCount 1).Trim()
    if ($Token) { Write-Host "Using saved token from $tokenFile" -ForegroundColor DarkGray }
}
if (-not $Token) {
    $Token = (Read-Host "Enter your enrichment worker token").Trim()
}
if (-not $Token) {
    Write-Host "No token provided; the worker cannot authenticate." -ForegroundColor Red
    exit 1
}

if ($Save) {
    if (-not (Test-Path $tokenDir)) { New-Item -ItemType Directory -Path $tokenDir | Out-Null }
    Set-Content -Path $tokenFile -Value $Token -Encoding ascii
    Write-Host "Token saved to $tokenFile (outside the repo; future runs pick it up automatically)." -ForegroundColor Green
}

# --- Environment for the worker process --------------------------------------
$env:ENRICHMENT_WORKER_TOKEN = $Token
if ($ApiBaseUrl) {
    $env:SCENT_API_BASE_URL = $ApiBaseUrl
} elseif (-not $env:SCENT_API_BASE_URL) {
    $env:SCENT_API_BASE_URL = "https://srt-scent-engine-production.up.railway.app"
}

[string[]]$modeFlag = switch ($Mode) {
    "dashboard"  { "--dashboard" }
    "management" { "--management" }
    "pending"    { "--process-pending" }
    "auto"       { "--dashboard", "--auto-approve" }
}
[string[]]$argList = @($modeFlag) + @($WorkerArgs | Where-Object { $_ })

$masked = if ($Token.Length -gt 4) { ("*" * ($Token.Length - 4)) + $Token.Substring($Token.Length - 4) } else { "****" }
Write-Host "API:   $($env:SCENT_API_BASE_URL)"
Write-Host "Token: $masked"
Write-Host "Mode:  $Mode"
Write-Host ""

& python (Join-Path $repoRoot "scripts\enrichment_worker.py") @argList
exit $LASTEXITCODE
