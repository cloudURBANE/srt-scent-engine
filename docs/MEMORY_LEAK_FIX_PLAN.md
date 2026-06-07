# Railway memory-leak fix plan — SRT scent engine

## Symptom
Memory graph climbs steadily over hours/days on a 512MB Hobby instance until OOM.

## Root cause
Failed Chromium clearance mints leak orphaned browser processes, on a 5-minute timer.

1. Mints never succeed on Railway (DrissionPage 4.1.1.2 WS-404 upstream incompat), so
   `_BASENOTES_SESSION` / `_FRAGRANTICA_SESSION` stay `None` permanently.
2. `RoutedScraper._for_url()` (`fragrance_parser_full_rewrite_fixed.py:3242`) sets
   `mint_clearance=True` whenever `_SESSION is None and monotonic() >= _NEXT_MINT_AFTER`.
   With cooldown defaulting to 300s (`_clearance_mint_cooldown_seconds`), a search request
   re-fires a mint every ~5 minutes, indefinitely.
3. Each failed mint spawns 2–3 Chromium processes: DrissionPage `ChromiumPage`, then the
   raw-CDP fallback, then the `xvfb-run` fallback (`:2911-2938`) — the last is non-headless
   + an Xvfb X server.
4. `proc.terminate()` signals only the parent Chromium; renderer/GPU/zygote children orphan
   to PID 1. The backstop `_kill_chromiums_for_user_data_dir()` (`:2470`) is a silent no-op
   on Railway — it `return`s early when `pgrep` is absent, and `nixpacks.toml` `nixPkgs`
   ships no `procps`.

Net: ~2–3 leaked Chromium process trees every 5 minutes → steady climb → OOM.
This is a leak, not a sizing problem; raising the plan only delays it.

## Fix — Part 1: stop minting in-process on the web service (primary)

The web service should never spawn a browser. Clearance is minted offline and injected
via env (`_load_clearance_from_env`, `:2451`).

- [ ] Add a `DISABLE_CHROMIUM_MINT` env flag (truthy = `1/true/yes`).
- [ ] In `RoutedScraper._for_url()` (`:3228`), when the flag is set, force
      `mint_clearance = False` for both branches. Engine still uses any env/cache
      clearance and falls back to the default scraper otherwise — no browser launch.
- [ ] Optionally short-circuit `_mint_basenotes_clearance` / `_mint_fragrantica_clearance`
      to return `None` immediately under the flag (defense in depth).
- [ ] Set `DISABLE_CHROMIUM_MINT=1` on the Railway API service.
- [ ] Mint clearance offline; publish to Railway env vars:
      - `BASENOTES_CLEARANCE_UA` + `BASENOTES_CLEARANCE_COOKIES_JSON`
        (or `BASENOTES_CLEARANCE_COOKIE_HEADER`)
      - `FRAGRANTICA_CLEARANCE_UA` + `FRAGRANTICA_CLEARANCE_COOKIES_JSON`
      Format consumed by `_load_clearance_from_env` (`:2451-2467`).
- [ ] Note: env clearance still runs `_validate_*_session`. If cookies are stale,
      validation fails and the engine falls back to the default scraper — it must NOT
      resume minting. The flag guarantees that.

## Fix — Part 2: make Chromium cleanup actually work (defense in depth)

For any environment where minting still runs (offline worker, local).

- [ ] Launch every Chromium `subprocess.Popen` with `start_new_session=True` so the
      browser + all children share one process group. Affected sites:
      `_mint_clearance_with_raw_cdp` (`:2588`), the api.py raw-CDP probe (`:1832`).
- [ ] On teardown, replace bare `proc.terminate()` with `os.killpg(os.getpgid(proc.pid),
      SIGTERM)` then `SIGKILL` after a short wait — kills renderers/GPU/zygote too.
- [ ] Rewrite `_kill_chromiums_for_user_data_dir()` (`:2470`) to scan `/proc/*/cmdline`
      in pure Python (match the `--user-data-dir=` path) instead of shelling to `pgrep`,
      so it works without `procps`. Keep the `os.name == "nt"` guard.
- [ ] (Alternative/extra) add `procps` to `nixpacks.toml` `nixPkgs`.
- [ ] For DrissionPage's `ChromiumPage`, after `page.quit()` also run the rewritten
      `_kill_chromiums_for_user_data_dir()` (already called at `:2954`) — it becomes
      effective once pgrep-free.

## Fix — Part 3: observability

- [ ] Add a `/api/diagnostics/memory` endpoint reading `/proc/self/status` (VmRSS) and
      counting live `chromium`/`Xvfb` processes via `/proc`. Lets the leak be watched
      directly and confirms the graph flattens post-fix.

## Verification
- After deploy with `DISABLE_CHROMIUM_MINT=1`: RSS should be flat (~150–250MB baseline),
  no `chromium` processes in `/proc`.
- `/api/fragrances/search?q=xerjoff` still returns engine results via env clearance or
  cache fallback.
- Watch the Railway memory graph for 24h — should be flat, not sawtooth-climbing.

## Files touched
- `fragrance_parser_full_rewrite_fixed.py` — `_for_url`, `_kill_chromiums_for_user_data_dir`,
  `_mint_clearance_with_raw_cdp`, mint helpers.
- `api.py` — raw-CDP probe teardown, new memory diagnostic endpoint.
- `nixpacks.toml` — optional `procps`.
- Railway env — `DISABLE_CHROMIUM_MINT`, `*_CLEARANCE_*`.
