
import sys
import re
import time
import json
import hashlib
import os
import tempfile
from urllib.parse import quote, urlparse, unquote
from dataclasses import dataclass, field, asdict
from DrissionPage import ChromiumPage, ChromiumOptions

# ---------------------------------------------------------------- Colors
C, G, Y, E, M, Z = '\033[36m', '\033[32m', '\033[33m', '\033[31m', '\033[35m', '\033[0m'
B, W, D = '\033[34m', '\033[37m', '\033[90m'


def _env_float(name, default, *, minimum=0.0):
    try:
        value = float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return float(default)
    if value < minimum:
        return float(minimum)
    return value


def _decodo_concentration_enabled() -> bool:
    """True when the SERP consensus tier should fetch through the engine's
    Decodo structured-search client instead of booting a local headless
    Chromium DuckDuckGo browser.

    Defaults to on whenever Decodo credentials are configured (the structured
    provider is faster, runs over rotating premium-proxy egress, and needs no
    browser — which is also what makes the enrichment worker safe to
    parallelize). Set ``SCENT_CONCENTRATION_DISABLE_DECODO=1`` to fall back to
    the legacy Chromium SERP (e.g. local runs without Decodo creds).
    """
    if (os.environ.get("SCENT_CONCENTRATION_DISABLE_DECODO") or "").strip().lower() in {
        "1", "true", "yes", "on"
    }:
        return False
    try:
        from fragrance_parser_full_rewrite_fixed import DecodoScraperClient
    except Exception:
        return False
    try:
        return bool(DecodoScraperClient.enabled())
    except Exception:
        return False


@dataclass
class ScentProfile:
    query: str
    is_explicit: bool
    primary_concentration: str
    primary_confidence: int
    second_concentration: str
    second_confidence: int
    second_source: str
    flankers_detected: list
    scoring_matrix: dict
    sources_consulted: list = field(default_factory=list)
    brand_prior: str = ""
    pillar_votes: int = 0
    total_votes: int = 0
    elapsed_seconds: float = 0.0


class SemanticScentEngine:

    # Patch 1: Token Modifiers definition
    VARIANT_MODIFIERS = {
        "intense", "exclusif", "exclusive", "elixir", "absolu", "absolue",
        "profumo", "profondo", "sport", "extreme", "fraiche", "fresh",
        "parfum", "le parfum", "edt", "edp", "edc", "extrait",
    }

    # Patch 6: Source Tier Pricing
    SOURCE_TIER = {
        "official": 100.0,
        "fragrantica": 75.0,
        "parfumo": 75.0,
        "basenotes": 65.0,
        "luckyscent": 60.0,
        "sephora": 45.0,
        "ulta": 45.0,
        "nordstrom": 45.0,
        "openweb": 20.0,
    }

    # Patch 6: Field weight granular distribution
    FIELD_WEIGHT = {
        "official_product_title": 1.00,
        "official_h1": 1.00,
        "product_spec_concentration": 1.00,
        "canonical_url": 0.90,
        "serp_title": 0.55,
        "serp_url": 0.50,
        "snippet": 0.15,
    }

    TAXONOMY = {
        "Elixir":                       r"(?i)\belixirs?\b",
        "Extrait / Parfum Extract":     r"(?i)\b(extraits?(?:\s+de\s+parfum)?|pure\s+parfum|parfum\s+extract)\b",
        "Le Parfum":                    r"(?i)\ble\s+parfum\b",
        "EDP Intense":                  r"(?i)\b(edp\s*intense|eau\s+de\s+parfum\s+intense|intense\s+eau\s+de\s+parfum)\b",
        "EDT Intense":                  r"(?i)\b(edt\s*intense|eau\s+de\s+toilette\s+intense|intense\s+eau\s+de\s+toilette)\b",
        "EDP (Eau de Parfum)":          r"(?i)\b(edp|e\.?d\.?p\.?|eau\s+de\s+parfum)\b",
        "EDT (Eau de Toilette)":        r"(?i)\b(edt|e\.?d\.?t\.?|eau\s+de\s+toilette)\b",
        "EDC (Cologne)":                r"(?i)\b(edc|e\.?d\.?c\.?|eau\s+de\s+cologne)\b",
        "Eau Fraiche":                  r"(?i)\b(eau\s+fra[iî]che)\b",
        "Body Mist / Hair Mist":        r"(?i)\b(body\s+mist|hair\s+mist|fragrance\s+mist)\b",
        "Aftershave":                   r"(?i)\b(after[-\s]?shave|a\/s)\b",
        "Parfum (Profumo)":             r"(?i)\b(parfum|profumo)\b",
    }

    URL_SLUG_MARKERS = [
        ("EDP Intense",                 r"eau[- ]de[- ]parfum[- ]intense|edp[- ]intense"),
        ("EDT Intense",                 r"eau[- ]de[- ]toilette[- ]intense|edt[- ]intense"),
        ("Elixir",                      r"elixir"),
        ("Extrait / Parfum Extract",    r"extrait[- ]de[- ]parfum|parfum[- ]extract|pure[- ]parfum"),
        ("Le Parfum",                   r"le[- ]parfum"),
        ("EDP (Eau de Parfum)",         r"eau[- ]de[- ]parfum|\bedp\b"),
        ("EDT (Eau de Toilette)",       r"eau[- ]de[- ]toilette|\bedt\b"),
        ("EDC (Cologne)",               r"eau[- ]de[- ]cologne"),
        ("Eau Fraiche",                 r"eau[- ]fra[iî]che"),
        ("Parfum (Profumo)",            r"(?<!eau[- ]de[- ])parfum|profumo"),
    ]

    SIBLING_FALLBACK = {
        "Elixir":                    "EDP (Eau de Parfum)",
        "Extrait / Parfum Extract":  "EDP (Eau de Parfum)",
        "Parfum (Profumo)":          "EDP (Eau de Parfum)",
        "Le Parfum":                 "EDP (Eau de Parfum)",
        "EDP Intense":               "EDP (Eau de Parfum)",
        "EDP (Eau de Parfum)":       "EDT (Eau de Toilette)",
        "EDT Intense":               "EDT (Eau de Toilette)",
        "EDT (Eau de Toilette)":     "EDP (Eau de Parfum)",
        "EDC (Cologne)":             "EDT (Eau de Toilette)",
        "Eau Fraiche":               "EDT (Eau de Toilette)",
        "Body Mist / Hair Mist":     "EDT (Eau de Toilette)",
        "Aftershave":                "EDT (Eau de Toilette)",
    }

    BRAND_PRIORS = {
        "amouage":                  "EDP (Eau de Parfum)",
        "hermes":                   "EDT (Eau de Toilette)",
        "yves saint laurent":       "EDP (Eau de Parfum)",
        "ysl":                      "EDP (Eau de Parfum)",
        "xerjoff":                  "Parfum (Profumo)",
        "casamorati":               "EDP (Eau de Parfum)",
        "roja":                     "Parfum (Profumo)",
        "roja dove":                "Parfum (Profumo)",
        "clive christian":          "Parfum (Profumo)",
        "nishane":                  "Extrait / Parfum Extract",
        "bortnikoff":               "Extrait / Parfum Extract",
        "areej le dore":            "Extrait / Parfum Extract",
        "ensar oud":                "Extrait / Parfum Extract",
        "henry jacques":            "Parfum (Profumo)",
        "maison francis kurkdjian": "EDP (Eau de Parfum)",
        "francis kurkdjian":        "EDP (Eau de Parfum)",
        "mfk":                      "EDP (Eau de Parfum)",
        "parfums de marly":         "EDP (Eau de Parfum)",
        "initio":                   "EDP (Eau de Parfum)",
        "kilian":                   "EDP (Eau de Parfum)",
        "by kilian":                "EDP (Eau de Parfum)",
        "memo paris":               "EDP (Eau de Parfum)",
        "frederic malle":           "EDP (Eau de Parfum)",
        "le labo":                  "EDP (Eau de Parfum)",
        "diptyque":                 "EDP (Eau de Parfum)",
        "byredo":                   "EDP (Eau de Parfum)",
        "creed":                    "EDP (Eau de Parfum)",
        "tom ford private blend":   "EDP (Eau de Parfum)",
        "ormonde jayne":            "EDP (Eau de Parfum)",
        "mancera":                  "EDP (Eau de Parfum)",
        "montale":                  "EDP (Eau de Parfum)",
    }

    KNOWN_EDT_PILLARS = {
        "sauvage", "aqua di gio", "acqua di gio", "acqua di gio profumo",
        "le male", "eros", "light blue", "cool water", "drakkar noir",
        "fahrenheit", "polo", "polo blue", "polo black", "polo red",
        "azzaro pour homme", "stronger with you", "armani code", "1 million",
        "invictus", "212", "bvlgari pour homme", "pi", "kouros", "obsession",
        "issey miyake pour homme", "lacoste pour homme", "boss bottled",
        "boss the scent", "joop homme", "chrome", "ck one", "ck be",
        "allure homme", "terre d'hermes", "vetiver",
        "happy", "ck eternity", "burberry brit", "dylan blue", "blue jeans",
        "l eau d issey pour homme", "l eau d issey",
        "egoiste", "egoiste legoiste",
    }
    KNOWN_EDP_PILLARS = {
        "aventus", "bleu de chanel", "la nuit de l'homme", "y", "myself",
        "valentino uomo", "spicebomb", "santal 33", "baccarat rouge 540",
        "tobacco vanille", "oud wood", "fucking fabulous", "ombre nomade",
        "layton", "delina", "another 13", "oud for greatness", "side effect",
        "rehab", "psychedelic love", "high frequency", "musk therapy",
        "atomic rose", "good girl", "scandal", "la vie est belle",
        "black opium", "libre", "mon guerlain", "olympea", "alien",
        "angel", "j'adore", "jadore", "miss dior", "coco mademoiselle", "no 5",
        "chance", "gabrielle", "narciso", "narciso rouge", "for her",
    }

    DOMAIN_MAP = {
        "fragrantica.com": "fragrantica",
        "parfumo.net": "parfumo",
        "parfumo.com": "parfumo",
        "basenotes.com": "basenotes",
        "basenotes.net": "basenotes",
        "luckyscent.com": "luckyscent",
        "sephora.com": "sephora",
        "ulta.com": "ulta",
        "nordstrom.com": "nordstrom",
    }

    POSITIONAL = [10, 4, 3, 2, 2, 1, 1, 1, 1, 1]

    JUNK_TOKENS = re.compile(
        r"\b("
        r"for\s+men|for\s+women|pour\s+homme|pour\s+femme|"
        r"men|women|man|woman|unisex|"
        r"original|new|limited\s+edition|special\s+edition|"
        r"reviews?|review|notes?|perfume|fragrance|"
        r"\d{4}|"
        r"\([^)]*\)"
        r")\b",
        re.IGNORECASE,
    )

    CACHE_PATH = os.path.join(tempfile.gettempdir(), "scent_engine_cache_v6.json")
    CACHE_TTL_SEC = 60 * 60 * 24 * 7
    # Hard ceilings for the SERP consensus pass. Without them a blocked or
    # stalling DuckDuckGo leaves the enrichment worker wedged inside analyze()
    # with no feedback. Page-level timeouts bound each fetch; the budget bounds
    # the whole multi-source loop.
    SERP_PAGE_LOAD_TIMEOUT_SEC = _env_float("SCENT_SERP_PAGE_TIMEOUT_SECONDS", 20, minimum=1.0)
    SERP_TOTAL_BUDGET_SEC = _env_float("SCENT_SERP_BUDGET_SECONDS", 75, minimum=1.0)
    SERP_RESULT_WAIT_TIMEOUT_SEC = _env_float("SCENT_SERP_RESULT_WAIT_SECONDS", 0.75, minimum=0.1)

    @staticmethod
    def _normalize(s):
        if not s: return ""
        s = s.replace("’", "'").replace("–", "-").replace("—", "-")
        return re.sub(r"\s+", " ", s).strip()

    # Patch 1: Query variant modifier parsing
    @staticmethod
    def _query_has_variant_modifier(query):
        if not query: return False
        q = query.lower()
        tokens = set(re.findall(r"\w+", q))
        return any(m in tokens for m in SemanticScentEngine.VARIANT_MODIFIERS)

    # Patch 2: Query canonicalization framework helper
    @staticmethod
    def canonicalize_query_without_brand(query):
        q = SemanticScentEngine._normalize(query).lower()
        brand_hit, _ = SemanticScentEngine._brand_prior(q)
        if brand_hit:
            q = re.sub(r'\b' + re.escape(brand_hit) + r'\b', '', q)
        return SemanticScentEngine._normalize(q)

    # Patch 2: Exact canonical match pillar prior rule
    @staticmethod
    def _query_pillar_prior(query):
        q = SemanticScentEngine.canonicalize_query_without_brand(query).strip()
        if SemanticScentEngine._query_has_variant_modifier(q):
            return None
        if q in SemanticScentEngine.KNOWN_EDP_PILLARS:
            return "EDP (Eau de Parfum)"
        if q in SemanticScentEngine.KNOWN_EDT_PILLARS:
            return "EDT (Eau de Toilette)"
        # Brand not in BRAND_PRIORS (e.g. "Chanel") — try stripping 1–2 leading words
        words = q.split()
        for n in range(1, min(5, len(words))):
            q_try = " ".join(words[n:]).strip()
            if not q_try or SemanticScentEngine._query_has_variant_modifier(q_try):
                continue
            if q_try in SemanticScentEngine.KNOWN_EDP_PILLARS:
                return "EDP (Eau de Parfum)"
            if q_try in SemanticScentEngine.KNOWN_EDT_PILLARS:
                return "EDT (Eau de Toilette)"
        return None

    # Patch 3: Modifier token retrieval extraction
    @staticmethod
    def _modifier_tokens(query):
        q = query.lower()
        tokens = set(re.findall(r"\w+", q))
        return {m for m in SemanticScentEngine.VARIANT_MODIFIERS if m in tokens}

    # Patch 3: Result preservation check verification gate
    @staticmethod
    def _result_preserves_modifiers(query, title, url):
        mods = SemanticScentEngine._modifier_tokens(query)
        if not mods:
            return True
        hay = f"{title} {url}".lower()
        hay_tokens = set(re.findall(r"\w+", hay))
        return all(m in hay_tokens for m in mods)

    @staticmethod
    def _explicit_intent(query):
        q = SemanticScentEngine._normalize(query)
        for key, pattern in SemanticScentEngine.TAXONOMY.items():
            if re.search(pattern, q):
                return key
        return None

    @staticmethod
    def _brand_prior(query):
        q = SemanticScentEngine._normalize(query).lower()
        padded = f" {q} "
        for brand in sorted(SemanticScentEngine.BRAND_PRIORS.keys(), key=len, reverse=True):
            if padded.startswith(f" {brand} ") or f" {brand} " in padded:
                return brand, SemanticScentEngine.BRAND_PRIORS[brand]
        return None, None

    @staticmethod
    def _domain_from(text):
        if not text: return ""
        t = text.strip().lower()
        if t.startswith("http"):
            try: return urlparse(t).netloc.lower().lstrip("www.")
            except Exception: return ""
        host = t.split("/", 1)[0]
        return host[4:] if host.startswith("www.") else host

    # Patch 6: Source Tier categorization mapper
    @staticmethod
    def _get_source_tier_key(domain, url):
        d = domain.lower()
        for known, key in SemanticScentEngine.DOMAIN_MAP.items():
            if known in d:
                return key
        # Analyze if url belongs to the query manufacturer/official landing site
        return "openweb"

    @staticmethod
    def _strip_junk(text):
        if not text: return ""
        return SemanticScentEngine.JUNK_TOKENS.sub(" ", text)

    @staticmethod
    def _strip_query_brand(text, query, brand):
        if not text: return ""
        q_tokens = [t for t in re.findall(r"\w+", query.lower()) if len(t) > 1]
        b_tokens = [t for t in re.findall(r"\w+", (brand or "").lower()) if len(t) > 1]
        kill = set(q_tokens + b_tokens)
        out_tokens = []
        for tok in re.findall(r"[\w./'-]+", text):
            if tok.lower() in kill: continue
            out_tokens.append(tok)
        return " ".join(out_tokens)

    @staticmethod
    def _detect_concentration_in(text):
        if not text: return None
        for key, pattern in SemanticScentEngine.TAXONOMY.items():
            if re.search(pattern, text):
                return key
        return None

    @staticmethod
    def _detect_url_concentration(url_text):
        if not url_text: return None
        slug = unquote(url_text).lower()
        for key, pattern in SemanticScentEngine.URL_SLUG_MARKERS:
            if re.search(pattern, slug):
                return key
        return None

    @staticmethod
    def _pillar_default(query, brand_prior):
        qp = SemanticScentEngine._query_pillar_prior(query)
        if qp:
            return qp
        if brand_prior:
            return brand_prior
        return "EDT (Eau de Toilette)"

    # Modified via patches 3, 4, 5, 6
    @staticmethod
    def _classify_result(row, query, brand, brand_prior):
        title = SemanticScentEngine._normalize(row.get("title", ""))
        url   = row.get("url", "")

        # Patch 3: Validate modifier preservation rule
        if not SemanticScentEngine._result_preserves_modifiers(query, title, url):
            return None, "modifier-mismatch", 0.0

        # Patch 6: Tier Mapping selection logic initialization
        tier_key = SemanticScentEngine._get_source_tier_key(row.get("domain", ""), url)
        if tier_key == "openweb" and brand and brand in url.lower():
            tier_key = "official"
            
        base_tier_score = SemanticScentEngine.SOURCE_TIER.get(tier_key, 20.0)

        # 1. URL slug parse (Patch 5 / 6 priority combination strategy)
        url_conc = SemanticScentEngine._detect_url_concentration(url)
        if url_conc:
            fw = SemanticScentEngine.FIELD_WEIGHT["canonical_url"] if "serp" not in url else SemanticScentEngine.FIELD_WEIGHT["serp_url"]
            return url_conc, "url", (base_tier_score * fw)

        # 2. Title modifier parse
        title_no_junk = SemanticScentEngine._strip_junk(title)
        modifier = SemanticScentEngine._strip_query_brand(title_no_junk, query, brand)
        title_conc = SemanticScentEngine._detect_concentration_in(modifier)
        if title_conc:
            fw = SemanticScentEngine.FIELD_WEIGHT["official_product_title"] if tier_key == "official" else SemanticScentEngine.FIELD_WEIGHT["serp_title"]
            return title_conc, "title-modifier", (base_tier_score * fw)

        # 3. Pillar Default mapping detection logic
        q_tokens = set(t for t in re.findall(r"\w+", query.lower()) if len(t) > 2)
        title_tokens = set(re.findall(r"\w+", title.lower()))
        url_tokens   = set(re.findall(r"\w+", row.get("url", "").lower()))
        haystack = title_tokens | url_tokens
        
        if q_tokens:
            overlap = len(q_tokens & haystack)
            need = max(1, (len(q_tokens) + 1) // 2)
            if overlap >= need:
                # Patch 4: Block pillar default computation parsing on variant modified expressions
                if SemanticScentEngine._query_has_variant_modifier(query):
                    return None, "blocked-pillar-modified-query", 0.0
                
                fw = SemanticScentEngine.FIELD_WEIGHT["serp_title"]
                return SemanticScentEngine._pillar_default(query, brand_prior), "pillar", (base_tier_score * fw)

        return None, "unclassified", 0.0

    @staticmethod
    def _cache_load():
        try:
            with open(SemanticScentEngine.CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    @staticmethod
    def _cache_save(cache):
        try:
            with open(SemanticScentEngine.CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(cache, f)
        except Exception:
            pass

    @staticmethod
    def _cache_key(query):
        return hashlib.sha1(query.lower().strip().encode("utf-8")).hexdigest()

    @staticmethod
    def _fetch_serp(page, query, site_filter="", timeout=None):
        """Fetch one DDG SERP. Returns rows, [] for a loaded-but-empty SERP,
        or None when the fetch/parse itself failed (the only retryable case)."""
        q = f"{query} {site_filter}".strip()
        safe_q = quote(q)
        page_timeout = SemanticScentEngine.SERP_PAGE_LOAD_TIMEOUT_SEC
        if timeout is not None:
            page_timeout = max(1.0, min(page_timeout, float(timeout)))
        try:
            ok = page.get(
                f"https://html.duckduckgo.com/html/?q={safe_q}",
                retry=0,
                interval=0,
                timeout=page_timeout,
            )
            if ok is False:
                return None
            page.wait.load_start(timeout=min(SemanticScentEngine.SERP_RESULT_WAIT_TIMEOUT_SEC, page_timeout))
            time.sleep(0.4)
        except Exception:
            return None
        rows = []
        try:
            dom_timeout = min(SemanticScentEngine.SERP_RESULT_WAIT_TIMEOUT_SEC, page_timeout)
            title_elements = page.eles('css:.result__title', timeout=dom_timeout)[:10]
            if not title_elements:
                return []
            titles   = [el.text for el in title_elements]
            snippets = [el.text for el in page.eles('css:.result__snippet', timeout=dom_timeout)[:10]]
            urls_txt = [el.text for el in page.eles('css:.result__url', timeout=dom_timeout)[:10]]
        except Exception:
            return None
        n = min(len(titles), len(snippets), len(urls_txt))
        for i in range(n):
            url_text = (urls_txt[i] or "").strip()
            domain = SemanticScentEngine._domain_from(url_text)
            url_clean = unquote(url_text).replace('-', ' ').replace('_', ' ').replace('/', ' ')
            rows.append({
                "title":   titles[i] or "",
                "snippet": snippets[i] or "",
                "url":     url_clean,
                "url_raw": url_text,
                "domain":  domain,
            })
        return rows

    @staticmethod
    def _retry_fetch(page, query, site_filter, attempts=3, deadline=None):
        """Retry only on fetch/parse failures (None). A SERP that loaded fine
        with zero results is a real answer -- retrying it just burns the job's
        time budget and looks like a hang from the worker dashboard."""
        for attempt in range(attempts):
            timeout = None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining < 1.0:
                    break
                timeout = remaining
            rows = SemanticScentEngine._fetch_serp(page, query, site_filter, timeout=timeout)
            if rows is not None:
                return rows
            if attempt + 1 < attempts:
                sleep_for = 0.8 * (attempt + 1)
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    sleep_for = min(sleep_for, remaining)
                if sleep_for > 0:
                    time.sleep(sleep_for)
        return []

    @staticmethod
    def _decodo_serp_rows(query, site_filter, deadline=None):
        """Structured-SERP equivalent of ``_retry_fetch``.

        Fetches organic results for ``"<query> <site_filter>"`` through the
        engine's Decodo client and maps each entry into the exact
        ``{title, snippet, url, url_raw, domain}`` row shape ``_classify_result``
        consumes. Transport only: the voting, tiering, and concentration
        scoring downstream are unchanged. Returns ``[]`` on any failure or an
        exhausted budget, mirroring ``_retry_fetch``'s loaded-but-empty
        contract so the caller treats a missing provider exactly like an empty
        SERP rather than an error.
        """
        try:
            from fragrance_parser_full_rewrite_fixed import DecodoScraperClient
        except Exception:
            return []
        if not DecodoScraperClient.enabled():
            return []
        search_query = f"{query} {site_filter}".strip()
        timeout = DecodoScraperClient.MAX_TIMEOUT
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining < DecodoScraperClient.MIN_VIABLE_BUDGET:
                return []
            timeout = min(timeout, remaining)
        try:
            payload = DecodoScraperClient._post_google(search_query, timeout)
            entries = DecodoScraperClient._search_entries(payload)
        except Exception as exc:
            DecodoScraperClient._log_request_failure("concentration SERP", exc)
            return []
        rows = []
        seen = set()
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            link = DecodoScraperClient._entry_link(entry)
            if not link or link in seen:
                continue
            seen.add(link)
            # Mirror _fetch_serp's url normalization so _detect_url_concentration
            # and _get_source_tier_key see the same token-friendly form.
            url_clean = unquote(link).replace('-', ' ').replace('_', ' ').replace('/', ' ')
            rows.append({
                "title":   str(entry.get("title") or "").strip(),
                "snippet": str(
                    entry.get("snippet")
                    or entry.get("description")
                    or entry.get("desc")
                    or ""
                ).strip(),
                "url":     url_clean,
                "url_raw": link,
                "domain":  SemanticScentEngine._domain_from(link),
            })
            if len(rows) >= 10:
                break
        return rows

    @staticmethod
    def _new_serp_page(deadline=None):
        co = ChromiumOptions()
        co.set_argument('--window-position=-2000,-2000')
        co.set_argument('--mute-audio')
        co.set_argument('--disable-blink-features=AutomationControlled')
        co.incognito()
        page = ChromiumPage(co)
        try:
            # Bound every navigation; DrissionPage's defaults can leave a
            # stalled DDG request blocking the worker far longer than the
            # whole SERP pass is worth.
            page.set.timeouts(
                base=10,
                page_load=SemanticScentEngine.SERP_PAGE_LOAD_TIMEOUT_SEC,
                script=10,
            )
            page.set.load_mode.eager()
        except Exception:
            pass

        try:
            remaining = (
                SemanticScentEngine.SERP_PAGE_LOAD_TIMEOUT_SEC
                if deadline is None
                else max(1.0, deadline - time.monotonic())
            )
            warmup_timeout = min(
                SemanticScentEngine.SERP_PAGE_LOAD_TIMEOUT_SEC,
                remaining,
            )
            page.get(
                "https://html.duckduckgo.com/html/",
                retry=0,
                interval=0,
                timeout=warmup_timeout,
            )
            page.wait.load_start(
                timeout=min(SemanticScentEngine.SERP_RESULT_WAIT_TIMEOUT_SEC, warmup_timeout)
            )
            time.sleep(0.3)
        except Exception:
            pass
        return page

    @staticmethod
    def analyze(fragrance_name, use_cache=True, page=None):
        fragrance_name = SemanticScentEngine._normalize(fragrance_name)
        print(f"{C}[SYS] Analyzing: '{fragrance_name}'...{Z}")

        # 1. Explicit concentration matching setup
        explicit_match = SemanticScentEngine._explicit_intent(fragrance_name)
        if explicit_match:
            print(f"{M}[BRAIN] Explicit concentration detected. Short-circuiting.{Z}")
            second = SemanticScentEngine.SIBLING_FALLBACK.get(explicit_match, "EDP (Eau de Parfum)")
            return ScentProfile(
                query=fragrance_name, is_explicit=True,
                primary_concentration=explicit_match, primary_confidence=100,
                second_concentration=second, second_confidence=0,
                second_source="sibling-fallback",
                flankers_detected=[], scoring_matrix={explicit_match: "EXPLICIT OVERRIDE"},
                sources_consulted=["lexical"], brand_prior="",
            )

        # 2. Cache resolution validation
        cache = SemanticScentEngine._cache_load() if use_cache else {}
        ck = SemanticScentEngine._cache_key(fragrance_name)
        if ck in cache:
            entry = cache[ck]
            if time.time() - entry.get("_ts", 0) < SemanticScentEngine.CACHE_TTL_SEC:
                print(f"{B}[CACHE] Hit. Returning prior consensus.{Z}")
                data = {k: v for k, v in entry.items() if not k.startswith("_")}
                return ScentProfile(**data)

        # 3. Brand prior resolution extraction
        brand_hit, brand_prior = SemanticScentEngine._brand_prior(fragrance_name)
        if brand_hit:
            print(f"{M}[BRAIN] Brand prior: '{brand_hit}' -> {brand_prior}{Z}")

        # 4. Multi-source engine data pulling loops
        use_decodo = page is None and _decodo_concentration_enabled()
        if use_decodo:
            print(f"{Y}[SYS] Fetching SERP sources via Decodo (no browser)...{Z}")
        else:
            print(f"{Y}[SYS] Booting browser & fetching sources...{Z}")
        t0 = time.monotonic()
        deadline = t0 + SemanticScentEngine.SERP_TOTAL_BUDGET_SEC

        source_queries = [
            ("fragrantica", "site:fragrantica.com"),
            ("parfumo",     "site:parfumo.net"),
            ("basenotes",   "site:basenotes.com"),
            ("openweb",     ""),
        ]

        sources_consulted = []
        per_source_rows = {}

        owns_page = page is None and not use_decodo
        try:
            if page is None and not use_decodo:
                page = SemanticScentEngine._new_serp_page(deadline)

            for label, site_filter in source_queries:
                elapsed = time.monotonic() - t0
                if elapsed >= SemanticScentEngine.SERP_TOTAL_BUDGET_SEC:
                    print(
                        f"  {E}[SYS] SERP budget exhausted "
                        f"({elapsed:.0f}s >= {SemanticScentEngine.SERP_TOTAL_BUDGET_SEC:.0f}s); "
                        f"skipping {label} and remaining sources.{Z}"
                    )
                    break
                print(f"  {C}[FETCH] {label:11s}...{Z}", end="")
                sys.stdout.flush()
                if use_decodo:
                    rows = SemanticScentEngine._decodo_serp_rows(
                        fragrance_name, site_filter, deadline=deadline
                    )
                else:
                    rows = SemanticScentEngine._retry_fetch(
                        page,
                        fragrance_name,
                        site_filter,
                        deadline=deadline,
                    )
                per_source_rows[label] = rows
                if rows:
                    sources_consulted.append(label)
                    print(f" {G}{len(rows)} rows{Z}")
                else:
                    print(f" {E}empty{Z}")
        except Exception as e:
            print(f"  {E}[ERR] Browser failure: {e}{Z}")
        finally:
            if owns_page:
                try:
                    if page: page.quit()
                except Exception: pass

        # 5. Patch 5 & 6: Source Tier Score + Decay resolution map matrix
        votes = {k: 0.0 for k in SemanticScentEngine.TAXONOMY.keys()}
        pillar_vote_count = 0
        total_vote_count = 0
        breakdown = []

        for label, rows in per_source_rows.items():
            for i, row in enumerate(rows):
                if i >= len(SemanticScentEngine.POSITIONAL): break
                conc, source_kind, computed_score = SemanticScentEngine._classify_result(
                    row, fragrance_name, brand_hit, brand_prior
                )
                if not conc:
                    continue
                
                pos_decay_mult = SemanticScentEngine.POSITIONAL[i] / 10.0
                weight = computed_score * pos_decay_mult
                
                if conc in votes:
                    votes[conc] += weight
                else:
                    votes[conc] = weight
                if source_kind == "pillar":
                    pillar_vote_count += 1
                total_vote_count += 1
                breakdown.append((label, i, row["domain"], source_kind, conc, round(weight, 2)))

        active = {k: round(v, 3) for k, v in votes.items() if v > 0}
        ranked = sorted(active.items(), key=lambda kv: kv[1], reverse=True)
        dt = time.monotonic() - t0
        print(f"{G}[SYS] Voting complete in {dt:.2f}s. {total_vote_count} ballots.{Z}")

        # Precise query-level structural check matching modifications override rules
        query_pillar = SemanticScentEngine._query_pillar_prior(fragrance_name)
        if query_pillar and ranked:
            total = sum(v for _, v in ranked)
            top_share = ranked[0][1] / total if total else 0
            top_key = ranked[0][0]
            if top_key != query_pillar and top_share < 0.80:
                print(f"{M}[BRAIN] Applying structural fallback prior: {query_pillar}{Z}")
                votes[query_pillar] = votes.get(query_pillar, 0) + ranked[0][1] * 1.8
                active = {k: round(v, 3) for k, v in votes.items() if v > 0}
                ranked = sorted(active.items(), key=lambda kv: kv[1], reverse=True)

        # 7. Final assignment profiles structuring metrics configurations mapping pipeline
        if not ranked:
            primary = "AMBIGUOUS_FAMILY" if SemanticScentEngine._query_has_variant_modifier(fragrance_name) else (brand_prior or "Unknown")
            second  = SemanticScentEngine.SIBLING_FALLBACK.get(primary, "EDT (Eau de Toilette)")
            profile = ScentProfile(
                query=fragrance_name, is_explicit=False,
                primary_concentration=primary, primary_confidence=0,
                second_concentration=second, second_confidence=0,
                second_source="none", flankers_detected=[],
                scoring_matrix={"info": "unresolved vector filters applied"},
                sources_consulted=sources_consulted, brand_prior=brand_prior or "",
                pillar_votes=pillar_vote_count, total_votes=total_vote_count,
                elapsed_seconds=round(dt, 2),
            )
        else:
            primary_key, primary_score = ranked[0]
            total = sum(v for _, v in ranked)
            primary_conf = int((primary_score / total) * 100) if total > 0 else 0

            if len(ranked) > 1:
                second_key, second_score = ranked[1]
                second_conf = int((second_score / total) * 100) if total > 0 else 0
                second_source = "consensus"
                if second_conf < 8:
                    fb = SemanticScentEngine.SIBLING_FALLBACK.get(primary_key, "EDP (Eau de Parfum)")
                    if fb != primary_key:
                        second_key, second_conf, second_source = fb, max(second_conf, 0), "sibling-fallback"
            else:
                fb = SemanticScentEngine.SIBLING_FALLBACK.get(primary_key, "EDP (Eau de Parfum)")
                second_key = fb if fb != primary_key else "EDT (Eau de Toilette)"
                second_conf, second_source = 0, "sibling-fallback"

            flankers = [k for k, v in ranked[1:] if total > 0 and (v / total) >= 0.10 and k != second_key]

            profile = ScentProfile(
                query=fragrance_name, is_explicit=False,
                primary_concentration=primary_key, primary_confidence=primary_conf,
                second_concentration=second_key, second_confidence=second_conf,
                second_source=second_source, flankers_detected=flankers,
                scoring_matrix=active, sources_consulted=sources_consulted,
                brand_prior=brand_prior or "", pillar_votes=pillar_vote_count,
                total_votes=total_vote_count, elapsed_seconds=round(dt, 2),
            )

        if use_cache:
            try:
                payload = asdict(profile)
                payload["_ts"] = time.time()
                cache[ck] = payload
                SemanticScentEngine._cache_save(cache)
            except Exception:
                pass

        return profile


def to_display_concentration(engine_label: str) -> str:
    """Map engine taxonomy labels to ScentCast display format."""
    mapping = {
        "EDP (Eau de Parfum)": "Eau de Parfum",
        "EDT (Eau de Toilette)": "Eau de Toilette",
        "EDC (Cologne)": "Eau de Cologne",
        "Extrait / Parfum Extract": "Extrait",
        "Parfum (Profumo)": "Parfum",
        "Le Parfum": "Parfum",
        "Elixir": "Elixir",
        "EDP Intense": "Eau de Parfum Intense",
        "EDT Intense": "Eau de Toilette Intense",
        "Eau Fraiche": "Eau Fraiche",
        "Body Mist / Hair Mist": "Body Mist",
        "Aftershave": "Aftershave",
    }
    return mapping.get(engine_label, engine_label)


_SCENTCAST_MAP = {
    "EDP (Eau de Parfum)": "Eau de Parfum",
    "EDT (Eau de Toilette)": "Eau de Toilette",
    "EDC (Cologne)": "Eau de Cologne",
    "Extrait / Parfum Extract": "Extrait",
    "Parfum (Profumo)": "Parfum",
    "Le Parfum": "Parfum",
    "Elixir": "Parfum",
    "EDP Intense": "Eau de Parfum",
    "EDT Intense": "Eau de Toilette",
    "Body Mist / Hair Mist": "Body Spray",
    "Eau Fraiche": "Eau de Cologne",
    "Aftershave": "Eau de Cologne",
}


def to_scentcast_concentration(engine_label: str) -> str:
    """Map SemanticScentEngine taxonomy label to scentParser.Concentration union value."""
    return _SCENTCAST_MAP.get(engine_label, "Unknown")


def _color_for_conf(conf):
    if conf >= 75: return G
    if conf >= 50: return Y
    return E


def render(result):
    print("-" * 72)
    primary_color = M if result.is_explicit else _color_for_conf(result.primary_confidence)
    second_color  = _color_for_conf(result.second_confidence) if result.second_source == "consensus" else B

    print(f"Target Query:        {C}{result.query}{Z}")
    if result.brand_prior:
        print(f"Brand Prior:         {M}{result.brand_prior}{Z}")
    print(f"PRIMARY:             {primary_color}{result.primary_concentration}{Z}  "
          f"({primary_color}{result.primary_confidence}% dominance{Z})")
    src_label = "consensus" if result.second_source == "consensus" else "sibling-heuristic"
    print(f"SECOND-BEST:         {second_color}{result.second_concentration}{Z}  "
          f"({second_color}{result.second_confidence}%{Z}, via {src_label})")

    if not result.is_explicit:
        print("Resolution Vector:   Source Tier Scored Matrix + Modifier Gate filters")
        print(f"Ballots:             {result.total_votes} total ({result.pillar_votes} pillar)")
        if result.sources_consulted:
            print(f"Sources Consulted:   {', '.join(result.sources_consulted)}")
        if result.elapsed_seconds:
            print(f"Latency:             {result.elapsed_seconds}s")
        if result.flankers_detected:
            print(f"\n{E}[!] FLANKER WARNING: line fragmentation detected.{Z}")
            print(f"Other Variants:      {Y}{', '.join(result.flankers_detected)}{Z}")

    print(f"\nScoring Matrix:      {result.scoring_matrix}")
    print("-" * 72)


def main():
    print(f"\n{G}=== SEMANTIC SCENT ENGINE v6.1 (PATCHED CHANNELS) ==={Z}")
    print(f"{W}Commands: 'q' to quit  |  'nocache <query>' to bypass cache  |  'clearcache'{Z}")
    if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8' and hasattr(sys.stdout, 'reconfigure'):
        try: sys.stdout.reconfigure(encoding='utf-8')
        except Exception: pass
    while True:
        try:
            query = input(f"\n{C}Fragrance > {Z}").strip()
            if not query: continue
            if query.lower() in ('q', 'quit', 'exit'): break
            if query.lower() == 'clearcache':
                try:
                    os.remove(SemanticScentEngine.CACHE_PATH)
                    print(f"{G}[CACHE] Cleared.{Z}")
                except Exception as ex:
                    print(f"{E}[CACHE] Clear failed: {ex}{Z}")
                continue
            use_cache = True
            if query.lower().startswith('nocache '):
                use_cache = False
                query = query[len('nocache '):].strip()
            result = SemanticScentEngine.analyze(query, use_cache=use_cache)
            render(result)
        except KeyboardInterrupt:
            print(); sys.exit(0)
        except Exception as ex:
            print(f"{E}[FATAL] {ex}{Z}")


if __name__ == "__main__":
    main()
