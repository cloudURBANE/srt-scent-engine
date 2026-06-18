<#
.SYNOPSIS
    Read-only fact-completeness audit over the engine's fragrance_records.
    Pages GET /api/enrichment/completeness (token-authed) and prints the
    aggregate missing-fact counts plus a sample of incomplete fragrances.

.DESCRIPTION
    Answers "which fragrances are still missing year/gender/concentration/etc,
    and how many" using the SAME granular contract (enrichment_facts.FACT_FIELDS)
    the heal sweep requeues on -- so Unknown / Universal / default-gender count
    as MISSING, not present.

    Launch under railway run so the worker token + API base are injected:
        railway run -s lively-adaptation -- powershell -File scripts\audit_completeness.ps1

.PARAMETER Sample
    How many incomplete fragrances to list with their missing facts. Default 40.

.PARAMETER OutFile
    Optional path to dump the full incomplete list as JSON (every record + its
    missing facts), for the next agent to work from.
#>
[CmdletBinding()]
param(
    [int]$Sample = 40,
    [string]$OutFile
)

$ErrorActionPreference = "Stop"
$ApiBase = if ($env:SCENT_API_BASE_URL) { $env:SCENT_API_BASE_URL.TrimEnd('/') } else { "https://srt-scent-engine-production.up.railway.app" }
$Token   = $env:ENRICHMENT_WORKER_TOKEN
if (-not $Token) { throw "No ENRICHMENT_WORKER_TOKEN in env. Launch under ``railway run -s lively-adaptation -- ...``." }
$headers = @{ Authorization = "Bearer $Token" }

# Page through every record (API clamps limit to 500/page).
$all = @()
$totals = $null
$offset = 0
do {
    $uri = "$ApiBase/api/enrichment/completeness?limit=500&offset=$offset&include_complete=false"
    $r = Invoke-RestMethod -Headers $headers -Uri $uri -TimeoutSec 60
    if (-not $totals) { $totals = $r }
    if ($r.items) { $all += $r.items }
    $offset += 500
} while ($offset -lt [int]$totals.total_records)

# actionable = rows with >=1 gap a re-scrape could still resolve (raw missing
# minus facts the source provably can't supply). The rest are structurally
# complete and will NEVER clear -- chasing them is the forever queue-churn.
$actionable = @($all | Where-Object { $_.actionable_missing -and $_.actionable_missing.Count -gt 0 })

Write-Host "`n=== Completeness audit ($ApiBase) ===" -ForegroundColor Cyan
[pscustomobject]@{
    total_records         = $totals.total_records
    complete              = [int]$totals.total_records - $all.Count
    incomplete_raw        = $all.Count
    incomplete_actionable = $actionable.Count
    non_actionable        = $all.Count - $actionable.Count
} | Format-List

Write-Host "Missing-fact counts (raw, across all records):" -ForegroundColor Cyan
$totals.missing_field_counts | ConvertTo-Json -Compress | Write-Host
Write-Host "Missing-fact counts (ACTIONABLE -- what a drain can target):" -ForegroundColor Cyan
$totals.actionable_missing_field_counts | ConvertTo-Json -Compress | Write-Host

# Which single fact is most often the lone gap? (the "passes as complete but
# missing one thing" bucket)
$byCount = $all | Group-Object { $_.missing.Count } | Sort-Object Name
Write-Host "`nIncomplete records grouped by # of missing facts:" -ForegroundColor Cyan
$byCount | ForEach-Object { "  {0} missing fact(s): {1} records" -f $_.Name, $_.Count } | Write-Host

Write-Host "`nSample of incomplete fragrances (name -- missing facts -- source):" -ForegroundColor Cyan
$all | Select-Object -First $Sample | ForEach-Object {
    "  {0,-45} | {1,-7} | missing: {2}" -f `
        ($_.name + ($(if ($_.house) { " ($($_.house))" } else { "" }))), `
        ($(if ($_.source) { $_.source } else { "fg" })), `
        ($_.missing -join ", ")
} | Write-Host

if ($OutFile) {
    $all | ConvertTo-Json -Depth 6 | Out-File -FilePath $OutFile -Encoding utf8
    Write-Host "`nFull incomplete list -> $OutFile" -ForegroundColor DarkGray
}
