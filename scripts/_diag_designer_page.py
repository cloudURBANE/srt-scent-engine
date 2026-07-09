"""Fetch a Fragrantica designer page raw and grep for expected perfumes."""
import re
import sys

import fragrance_parser_full_rewrite_fixed as engine

slug = sys.argv[1] if len(sys.argv) > 1 else "Clive-Christian"
terms = [t.lower() for t in sys.argv[2:]] or ["miami", "geranium", "tarocco", "twist", "crown"]

scraper = engine.get_scraper()
fg = engine.get_fragrantica_scraper(scraper.default_scraper, mint_clearance=not engine._chromium_mint_disabled())
if not engine._validate_fragrantica_session(fg):
    raise SystemExit("clearance preflight failed")

url = f"{engine.FragranticaEngine.BASE_URL}/designers/{slug}.html"
res = engine.Http.get(scraper, url, timeout=15, referer=engine.FragranticaEngine.BASE_URL, attempts=2)
if not res:
    raise SystemExit(f"fetch failed for {url}")
body = res.text
print(f"page bytes: {len(body)}")
hrefs = sorted(set(re.findall(r"/perfume/[^\"'\s>]+\.html", body)))
print(f"distinct perfume hrefs on page: {len(hrefs)}")
for t in terms:
    hits = [h for h in hrefs if t in h.lower()]
    print(f"  term {t!r}: {len(hits)}")
    for h in hits[:10]:
        print(f"    {h}")
