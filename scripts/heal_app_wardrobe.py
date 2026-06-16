"""Project engine facts into the APP wardrobe (the monorepo Supabase DB).

WHY THIS EXISTS
---------------
`heal_offline.py` writes the wardrobe to whatever `DATABASE_URL` points at. When
you launch a drain via `railway run -s lively-adaptation` from a shell that does
NOT already export `DATABASE_URL`, Railway injects the ENGINE service's
`DATABASE_URL` -- the viaduct cache (`fragrance_records`) -- which has **no**
`user_fragrances` table. The drain's final heal then projects against viaduct and
silently no-ops for the app: the owner's phone never improves even though every
drain "succeeds". (The worker's per-row cache upload DOES still reach Supabase
`global_fragrances` via the production API, so the catalog moves but the user's
own rows go stale.)

This wrapper fixes the wiring deterministically: it remaps the railway-injected
viaduct DSN to `ENGINE_DATABASE_URL` (folded read-only for fresh facts) and points
`DATABASE_URL` at the monorepo Supabase wardrobe -- the DB the app actually reads.
heal_offline then folds the full viaduct engine cache and projects into the real
`user_fragrances` / `global_fragrances`.

USAGE
-----
    railway run -s lively-adaptation -- <abs venv python> scripts/heal_app_wardrobe.py [--dry-run ...]

Any extra args pass straight through to heal_offline.py. Run a `--dry-run` first;
the `engine_db+<N>` fold count in the output proves the viaduct facts were picked
up (N should be the full engine row count, ~1300+, not 0).
"""
import os
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def _wardrobe_dsn_from_env_file() -> str:
    for candidate in (REPO / ".env", REPO.parent / "huge_monorepo" / ".env"):
        if not candidate.exists():
            continue
        for line in candidate.read_text(encoding="utf-8", errors="ignore").splitlines():
            m = re.match(r"\s*DATABASE_URL\s*=\s*(.*)\s*$", line)
            if m:
                return m.group(1).strip().strip('"').strip("'")
    raise SystemExit("No wardrobe DATABASE_URL found in search_engine/.env or huge_monorepo/.env")


def _mask(dsn: str) -> str:
    return re.sub(r"://[^@]+@", "://****@", dsn)[:72]


def main() -> int:
    injected = os.environ.get("DATABASE_URL", "").strip()
    if not injected:
        raise SystemExit(
            "DATABASE_URL not injected -- launch under `railway run -s lively-adaptation` so "
            "the viaduct engine DSN is available to fold."
        )
    wardrobe = _wardrobe_dsn_from_env_file()
    if wardrobe == injected:
        raise SystemExit(
            "Injected DATABASE_URL equals the wardrobe DSN; nothing to remap. Are you sure "
            "Railway injected the viaduct engine DSN (not the app DB)?"
        )

    print("wardrobe (write) DATABASE_URL        ->", _mask(wardrobe))
    print("engine   (fold)  ENGINE_DATABASE_URL ->", _mask(injected))

    env = os.environ.copy()
    env["DATABASE_URL"] = wardrobe
    env["ENGINE_DATABASE_URL"] = injected
    return subprocess.run(
        [sys.executable, str(REPO / "scripts" / "heal_offline.py"), *sys.argv[1:]],
        env=env,
        cwd=str(REPO),
    ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
