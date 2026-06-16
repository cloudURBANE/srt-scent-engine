<#
.SYNOPSIS
    Fast parallel enrichment-queue drain. Same heal/scrape/project cycle as
    drain_queue.ps1, but tuned for throughput:

      * runs the worker in PARALLEL (--workers N) over the Decodo egress path
        instead of one serial, easily-blocked IP, and
      * defers the heavy heal_offline re-projection to the END instead of
        running it after every batch (the main "why is it so slow" cost).

.DESCRIPTION
    Parallelism is only active when the Decodo egress path is on (rotating
    premium-proxy fetches + browserless concentration). The clean way to get
    those creds into the loop without leaking them onto a command line is to
    launch THIS script under `railway run`, which injects the service env into
    the process; every child `python` then inherits it:

        railway run -s lively-adaptation -- pwsh -File scripts\drain_fast.ps1

    Per iteration it just processes up to -BatchLimit pending jobs and re-reads
    the public status endpoint. No per-batch heal. A single heal_offline runs
    once up front (cheap offline wins) and once at the very end (project the
    freshly-scraped facts into the wardrobe).

    A stall guard stops the loop if pending fails to drop across two
    consecutive iterations (remaining jobs are unresolvable misses).

.PARAMETER Token
    ENRICHMENT_WORKER_TOKEN. Defaults to the existing $env value (railway run
    injects it on the engine service, so usually you don't pass this).

.PARAMETER Workers
    Parallel job workers. 0 = auto (4 when Decodo is active). 6 is a good
    aggressive default; push to 8 and watch for climbing "universal URL fetch"
    failures = you've hit Decodo's concurrency cap, step back one.

.PARAMETER BatchLimit
    Jobs per process-pending pass. Capped server-/client-side at 100.

.PARAMETER MaxIterations
    Hard cap on scrape iterations. Default 20 (20 x 100 = 2000 jobs).

.PARAMETER SkipHeal
    Skip both the up-front and final heal_offline passes (pure scrape drain).

.PARAMETER Seed
    Before the drain, enqueue an enrichment job for every wardrobe fragrance with
    no matching engine record (the heal's no_engine_match bucket). These rows
    cannot be healed by offline projection -- there is nothing to project from --
    so they must be scraped first. With -Seed this run seeds, scrapes, and then
    projects them into the wardrobe in one pass. Some seeded rows are name-mangled
    wardrobe entries (e.g. "Michael | Jordan") that will never resolve; those just
    fail their job and the stall guard ends the loop -- harmless.

.EXAMPLE
    railway run -s lively-adaptation -- pwsh -File scripts\drain_fast.ps1 -Workers 6

.EXAMPLE
    # Heal the leftover no_engine_match wardrobe rows end-to-end:
    railway run -s lively-adaptation -- pwsh -File scripts\drain_fast.ps1 -Seed -Workers 6
#>
[CmdletBinding()]
param(
    [string]$Token = $env:ENRICHMENT_WORKER_TOKEN,
    [int]$Workers = 8,
    [int]$BatchLimit = 100,
    [int]$MaxIterations = 20,
    [switch]$SkipHeal,
    # One-shot before the drain: reopen every fact-incomplete stored row as a
    # pending job (POST /api/enrichment/heal across the whole DB) so this drain
    # re-verifies them. Idempotent -- pending/processing/ignored rows untouched.
    [switch]$Reopen,
    # One-shot before the drain: enqueue an enrichment job for every WARDROBE
    # fragrance that has NO matching engine record yet (heal_offline.py --steps
    # seed). Offline projection can only fill wardrobe rows that already exist in
    # fragrance_records; the rest (the heal's no_engine_match bucket) must be
    # scraped first. This closes the loop -- seed enqueues them, the drain loop
    # scrapes them, and the final heal projects year/gender/family into the
    # wardrobe. Idempotent (dedupes on the same identity job key the SPA writes).
    [switch]$Seed,
    # Verbose per-job engine output: resolution path, concentration source,
    # fetch diagnostics (passes --debug to the worker).
    [switch]$WorkerDebug,
    # Tee a timestamped transcript here so you can scroll back / grep later.
    [string]$LogFile
)

$ErrorActionPreference = "Stop"

# Prefix every streamed line with a wall-clock time so the parallel bursts are
# readable (you can see which jobs resolved in the same second).
filter Add-Stamp { "{0:HH:mm:ss}  {1}" -f (Get-Date), $_ }

# --- Resolve paths (script lives in search_engine/scripts) -------------------
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir
$Python    = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$Heal      = Join-Path $ScriptDir "heal_offline.py"
$Worker    = Join-Path $ScriptDir "enrichment_worker.py"
$ApiBase   = if ($env:SCENT_API_BASE_URL) { $env:SCENT_API_BASE_URL.TrimEnd('/') } else { "https://srt-scent-engine-production.up.railway.app" }

if (-not (Test-Path $Python)) { throw "venv python not found at $Python -- create the venv first (see dev-commands skill)." }
if (-not $Token)              { throw "No worker token. Pass -Token, set `$env:ENRICHMENT_WORKER_TOKEN, or launch under ``railway run``." }

$env:ENRICHMENT_WORKER_TOKEN = $Token

if (-not $LogFile) {
    $LogFile = Join-Path $RepoRoot ("drain_{0:yyyyMMdd_HHmmss}.log" -f (Get-Date))
}
Write-Host "Transcript -> $LogFile" -ForegroundColor DarkGray

function Write-Step($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }

function Get-PendingCount {
    try {
        $r = Invoke-RestMethod -Uri "$ApiBase/api/enrichment/status" -TimeoutSec 25
        return [int]$r.counts.pending
    } catch {
        Write-Warning "status read failed: $($_.Exception.Message)"
        return -1
    }
}

# --- 0. Sanity: is the Decodo egress path actually on? -----------------------
# If not, --workers silently falls back to 1 (serial) and this is no faster.
$decodoOn = & $Python -c "import sys; sys.path.insert(0,r'$RepoRoot'); from concentration_grabber import _decodo_concentration_enabled as e; print('1' if e() else '0')"
if ($decodoOn -ne "1") {
    Write-Warning "Decodo egress path is OFF -- workers will fall back to serial (1)."
    Write-Warning "Launch under ``railway run -s lively-adaptation -- pwsh -File scripts\drain_fast.ps1`` so the Decodo creds are injected, or unset SCENT_CONCENTRATION_DISABLE_DECODO."
} else {
    Write-Host "Decodo egress path: ON (parallel workers active)." -ForegroundColor Green
}

# --- 1. Cheap offline heal up front (no scraping) ----------------------------
if (-not $SkipHeal) {
    Write-Step "Up-front heal (offline projection, no scrape)"
    & $Python $Heal
    if ($LASTEXITCODE -ne 0) { Write-Warning "heal_offline.py exited $LASTEXITCODE (continuing)" }
}

# --- 1a. Optional: seed wardrobe rows absent from the engine into the queue ---
# Without this, the heal's no_engine_match rows (e.g. Black Opium, Ramz Gold)
# can never heal: offline projection has no engine record to copy from, so they
# must be scraped first. Seeding here puts them in the queue BEFORE the drain
# loop reads pending, so this same run scrapes them and the final heal projects.
if ($Seed) {
    Write-Step "Seed wardrobe rows missing from engine (enqueue for scrape)"
    & $Python $Heal --steps seed | Add-Stamp | Tee-Object -FilePath $LogFile -Append
    if ($LASTEXITCODE -ne 0) { Write-Warning "seed exited $LASTEXITCODE (continuing)" }
}

# --- 1b. Optional: reopen all incomplete rows so this drain re-verifies them -
if ($Reopen) {
    Write-Step "Reopen incomplete rows (one-shot heal sweep -> pending)"
    & $Python $Worker --reopen-incomplete | Add-Stamp | Tee-Object -FilePath $LogFile -Append
    if ($LASTEXITCODE -ne 0) { Write-Warning "reopen exited $LASTEXITCODE (continuing)" }
}

# --- 2. Parallel drain loop (NO per-batch heal) ------------------------------
$start = Get-Date
$prevPending = [int]::MaxValue
$stall = 0
for ($i = 1; $i -le $MaxIterations; $i++) {
    $pending = Get-PendingCount
    $elapsed = [int]((Get-Date) - $start).TotalSeconds
    Write-Step "Iteration $i/$MaxIterations  (pending=$pending, ${elapsed}s elapsed)"
    if ($pending -eq 0) { Write-Host "Queue drained." -ForegroundColor Green; break }
    if ($pending -lt 0) { Write-Warning "Cannot read status; aborting loop."; break }

    if ($pending -ge $prevPending) {
        $stall++
        if ($stall -ge 2) {
            Write-Warning "Pending stopped decreasing across 2 iterations ($pending left). Remaining are likely unresolvable misses -- stopping."
            break
        }
    } else {
        $stall = 0
    }
    $prevPending = $pending

    $wArgs = @("--process-pending", "--limit", $BatchLimit, "--workers", $Workers, "--delay", "0", "--jitter", "0.9")
    if ($WorkerDebug) { $wArgs += "--debug" }
    Write-Host "-> $($wArgs -join ' ')" -ForegroundColor DarkGray
    # Stream stdout live: timestamp each line, echo to console AND append to log.
    & $Python $Worker @wArgs | Add-Stamp | Tee-Object -FilePath $LogFile -Append
    if ($LASTEXITCODE -ne 0) { Write-Warning "worker exited $LASTEXITCODE (continuing)" }
}

# --- 3. ONE final heal: project everything just scraped into the wardrobe ----
if (-not $SkipHeal) {
    Write-Step "Final heal (project scraped facts into wardrobe)"
    & $Python $Heal
    if ($LASTEXITCODE -ne 0) { Write-Warning "heal_offline.py exited $LASTEXITCODE" }
}

Write-Step "Final status"
$final = Get-PendingCount
$totalSec = [int]((Get-Date) - $start).TotalSeconds
$finalColor = if ($final -eq 0) { "Green" } else { "Yellow" }
Write-Host "pending=$final   (drain loop took ${totalSec}s)" -ForegroundColor $finalColor
if ($final -gt 0) {
    Write-Host "Remaining pending are most likely genuine resolver misses or transient blocks. Inspect with:" -ForegroundColor DarkYellow
    Write-Host "  $Python $Worker --dashboard   (press [1] List pending jobs)"
}
