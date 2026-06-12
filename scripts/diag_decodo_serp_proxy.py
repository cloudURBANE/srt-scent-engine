"""Evaluate Decodo residential proxy SERP discovery as a Serper replacement.

Default behavior is intentionally cheap: no network call unless ``--live`` is
passed, and live mode runs only one case unless ``--all-cases`` or ``--case`` is
specified. Credentials are read from environment variables only.
"""

from __future__ import annotations

import argparse
import html as html_lib
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, quote, quote_plus, unquote, urlparse

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import fragrance_parser_full_rewrite_fixed as engine  # noqa: E402


DEFAULT_TIMEOUT = 8.0
DEFAULT_MAX_RESPONSE_BYTES = 900_000
DEFAULT_MAX_TOTAL_BYTES = 2_500_000

BLOCK_MARKERS = (
    "our systems have detected unusual traffic",
    "unusual traffic from your computer network",
    "sorry/index",
    "detected unusual activity",
    "captcha",
    "verify you are human",
    "enable javascript",
    "enablejs",
    "if you're having trouble accessing google search",
)


@dataclass(frozen=True)
class DecodoProxyConfig:
    host: str
    port: int
    username: str
    password: str
    scheme: str = "http"

    @classmethod
    def from_env(cls) -> "DecodoProxyConfig":
        raw_url = (os.environ.get("DECODO_PROXY_URL") or "").strip()
        if raw_url:
            parsed = urlparse(raw_url)
            if not parsed.hostname or parsed.port is None or not parsed.username or parsed.password is None:
                raise ValueError("DECODO_PROXY_URL must include scheme://username:password@host:port")
            return cls(
                host=parsed.hostname,
                port=parsed.port,
                username=unquote(parsed.username),
                password=unquote(parsed.password),
                scheme=parsed.scheme or "http",
            )

        host = (os.environ.get("DECODO_PROXY_HOST") or "gate.decodo.com").strip()
        raw_port = (os.environ.get("DECODO_PROXY_PORT") or "").strip()
        username = (os.environ.get("DECODO_PROXY_USERNAME") or "").strip()
        password = os.environ.get("DECODO_PROXY_PASSWORD") or ""
        scheme = (os.environ.get("DECODO_PROXY_SCHEME") or "http").strip().lower()
        if not raw_port or not username or not password:
            raise ValueError(
                "Set DECODO_PROXY_PORT, DECODO_PROXY_USERNAME, and DECODO_PROXY_PASSWORD "
                "(or set DECODO_PROXY_URL)."
            )
        if scheme not in {"http", "https", "socks5", "socks5h"}:
            raise ValueError("DECODO_PROXY_SCHEME must be http, https, socks5, or socks5h")
        return cls(host=host, port=int(raw_port), username=username, password=password, scheme=scheme)

    def proxy_url(self) -> str:
        user = quote(self.username, safe="")
        password = quote(self.password, safe="")
        return f"{self.scheme}://{user}:{password}@{self.host}:{self.port}"

    def requests_proxies(self) -> dict[str, str]:
        proxy = self.proxy_url()
        return {"http": proxy, "https": proxy}

    def masked(self) -> dict[str, object]:
        return {
            "host": self.host,
            "port": self.port,
            "username": _mask(self.username),
            "password": "********",
            "scheme": self.scheme,
        }


@dataclass(frozen=True)
class SerpCase:
    name: str
    query: str
    target: str
    expected_substring: str


@dataclass
class SerpCaseResult:
    name: str
    query: str
    search_engine: str
    status_code: int | None
    elapsed_ms: int
    response_bytes: int
    blocked: bool
    result_count: int
    accepted_count: int
    first_accepted_url: str | None
    result_sample: list[str]
    accepted_sample: list[str]
    page_title: str | None
    expected_matched: bool
    error: str | None = None


DEFAULT_CASES: tuple[SerpCase, ...] = (
    SerpCase(
        name="fragrantica_naxos",
        query="xerjoff naxos site:fragrantica.com/perfume",
        target="fragrantica",
        expected_substring="/Xerjoff/",
    ),
    SerpCase(
        name="fragrantica_aventus",
        query="creed aventus site:fragrantica.com/perfume",
        target="fragrantica",
        expected_substring="/Creed/Aventus-9828.html",
    ),
    SerpCase(
        name="fragrantica_tempio",
        query="casamorati 1888 tempio d acqua site:fragrantica.com/perfume",
        target="fragrantica",
        expected_substring="/Casamorati-1888/Tempio-d-Acqua-128356.html",
    ),
    SerpCase(
        name="fragrantica_q",
        query="dolce gabbana q site:fragrantica.com/perfume",
        target="fragrantica",
        expected_substring="/Dolce-Gabbana/Q-by-Dolce-Gabbana-78873.html",
    ),
    SerpCase(
        name="parfumo_khamrah",
        query='site:parfumo.com/Perfumes "Lattafa" "Khamrah"',
        target="parfumo",
        expected_substring="Khamrah",
    ),
)


def _mask(value: str) -> str:
    if len(value) <= 4:
        return "****"
    return f"{value[:2]}...{value[-2:]}"


def _search_url(search_engine: str, query: str) -> str:
    encoded = quote_plus(query)
    if search_engine == "google":
        return f"https://www.google.com/search?q={encoded}&num=10&hl=en&gl=us&pws=0"
    if search_engine == "bing":
        return f"https://www.bing.com/search?q={encoded}&count=10&setlang=en-US&cc=US"
    raise ValueError(f"Unsupported search engine: {search_engine}")


def _unwrap_search_href(href: str) -> str | None:
    href = (href or "").strip()
    if not href:
        return None
    if href.startswith("/url?"):
        qs = parse_qs(urlparse(href).query)
        return (qs.get("q") or qs.get("url") or [None])[0]
    if href.startswith("/ck/"):
        qs = parse_qs(urlparse(href).query)
        direct = (qs.get("u") or [None])[0]
        if direct and direct.startswith("a1"):
            return _decode_bing_a1_url(direct)
        return direct
    parsed = urlparse(href)
    host = (parsed.netloc or "").lower()
    if host.endswith("google.com") and parsed.path == "/url":
        qs = parse_qs(parsed.query)
        return (qs.get("q") or qs.get("url") or [None])[0]
    if host.endswith("bing.com") and parsed.path.startswith("/ck/"):
        qs = parse_qs(parsed.query)
        direct = (qs.get("u") or [None])[0]
        if direct and direct.startswith("a1"):
            return _decode_bing_a1_url(direct)
        return direct
    if parsed.scheme in {"http", "https"}:
        return href
    return None


def _decode_bing_a1_url(value: str) -> str | None:
    # Bing sometimes stores redirect targets as "a1" + unpadded URL-safe base64.
    import base64

    raw = value[2:]
    padded = raw + ("=" * ((4 - len(raw) % 4) % 4))
    try:
        decoded = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8", errors="replace")
    except Exception:
        return None
    return decoded if decoded.startswith(("http://", "https://")) else None


def extract_result_urls(html: str) -> list[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    urls: list[str] = []
    seen: set[str] = set()

    def add_url(candidate: str | None) -> None:
        if not candidate:
            return
        candidate = html_lib.unescape(candidate).replace("\\/", "/")
        unwrapped = _unwrap_search_href(candidate) or candidate
        parsed = urlparse(unwrapped)
        host = parsed.netloc.lower()
        if not parsed.scheme.startswith("http") or not host:
            return
        if any(host == blocked or host.endswith(f".{blocked}") for blocked in ("google.com", "bing.com", "microsoft.com")):
            return
        if unwrapped in seen:
            return
        seen.add(unwrapped)
        urls.append(unwrapped)

    for anchor in soup.find_all("a", href=True):
        add_url(str(anchor.get("href") or ""))

    raw_html = html or ""
    for match in re.finditer(r"/url\?[^\"'<>\s]+", raw_html):
        add_url(match.group(0))
    for match in re.finditer(r"https?:(?:\\?/\\?/|//)[^\"'<>\s\\]+", raw_html):
        add_url(match.group(0))
    return urls


def accepted_urls(raw_urls: Iterable[str], target: str) -> list[str]:
    accepted: list[str] = []
    seen: set[str] = set()
    for raw_url in raw_urls:
        if target == "fragrantica":
            canonical = engine.FragranticaEngine.canonical_url(raw_url)
            if not canonical or not engine.FragranticaEngine.is_perfume_url(canonical):
                continue
            dedupe = engine.FragranticaEngine.dedupe_key(canonical)
        elif target == "parfumo":
            canonical = engine.ParfumoEngine.canonical_url(raw_url)
            if not canonical or not engine.ParfumoEngine.is_perfume_url(canonical):
                continue
            dedupe = canonical
        else:
            continue
        if dedupe in seen:
            continue
        seen.add(dedupe)
        accepted.append(canonical)
    return accepted


def looks_blocked(status_code: int | None, html: str) -> bool:
    if status_code in {403, 429, 503}:
        return True
    lower = (html or "").lower()
    return any(marker in lower for marker in BLOCK_MARKERS)


def page_title(html: str) -> str | None:
    match = re.search(r"<title[^>]*>(.*?)</title>", html or "", re.I | re.S)
    if not match:
        return None
    return re.sub(r"\s+", " ", html_lib.unescape(re.sub(r"<[^>]+>", " ", match.group(1)))).strip()[:160]


def fetch_limited(
    session: requests.Session,
    url: str,
    *,
    proxies: dict[str, str],
    timeout: float,
    max_response_bytes: int,
) -> tuple[int, bytes]:
    with session.get(url, proxies=proxies, timeout=timeout, stream=True) as response:
        chunks: list[bytes] = []
        total = 0
        for chunk in response.iter_content(chunk_size=64_000):
            if not chunk:
                continue
            total += len(chunk)
            if total > max_response_bytes:
                raise RuntimeError(f"response exceeded max bytes ({max_response_bytes})")
            chunks.append(chunk)
        return int(response.status_code), b"".join(chunks)


def run_case(
    case: SerpCase,
    *,
    search_engine: str,
    proxies: dict[str, str],
    timeout: float,
    max_response_bytes: int,
    html_output_dir: Path | None = None,
) -> SerpCaseResult:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    started = time.perf_counter()
    status_code: int | None = None
    body = b""
    error: str | None = None
    try:
        status_code, body = fetch_limited(
            session,
            _search_url(search_engine, case.query),
            proxies=proxies,
            timeout=timeout,
            max_response_bytes=max_response_bytes,
        )
        html = body.decode("utf-8", errors="replace")
        if html_output_dir is not None:
            html_output_dir.mkdir(parents=True, exist_ok=True)
            (html_output_dir / f"{search_engine}_{case.name}.html").write_text(html, encoding="utf-8")
        raw_urls = extract_result_urls(html)
        accepted = accepted_urls(raw_urls, case.target)
        blocked = looks_blocked(status_code, html)
        title = page_title(html)
    except Exception as exc:
        html = ""
        raw_urls = []
        accepted = []
        blocked = False
        title = None
        error = f"{type(exc).__name__}: {exc}"
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    first = accepted[0] if accepted else None
    return SerpCaseResult(
        name=case.name,
        query=case.query,
        search_engine=search_engine,
        status_code=status_code,
        elapsed_ms=elapsed_ms,
        response_bytes=len(body),
        blocked=blocked,
        result_count=len(raw_urls),
        accepted_count=len(accepted),
        first_accepted_url=first,
        result_sample=raw_urls[:5],
        accepted_sample=accepted[:5],
        page_title=title,
        expected_matched=any(case.expected_substring in url for url in accepted),
        error=error,
    )


def check_proxy_ip(
    *,
    proxies: dict[str, str],
    timeout: float,
    max_response_bytes: int,
) -> dict[str, object]:
    session = requests.Session()
    status_code, body = fetch_limited(
        session,
        "https://ip.decodo.com/json",
        proxies=proxies,
        timeout=timeout,
        max_response_bytes=max_response_bytes,
    )
    payload = json.loads(body.decode("utf-8", errors="replace"))
    return {"status_code": status_code, "response_bytes": len(body), "payload": payload}


def choose_cases(args: argparse.Namespace) -> list[SerpCase]:
    cases_by_name = {case.name: case for case in DEFAULT_CASES}
    if args.case:
        selected = []
        for name in args.case:
            if name not in cases_by_name:
                raise ValueError(f"Unknown case {name!r}; choose from {', '.join(cases_by_name)}")
            selected.append(cases_by_name[name])
        return selected
    if args.all_cases:
        return list(DEFAULT_CASES)
    return [DEFAULT_CASES[0]]


def _self_test() -> None:
    html = """
    <html><body>
      <a href="/url?q=https%3A%2F%2Fwww.fragrantica.com%2Fperfume%2FCreed%2FAventus-9828.html&sa=U">A</a>
      <a href="https://www.example.com/page">B</a>
    </body></html>
    """
    urls = extract_result_urls(html)
    assert urls[:2] == [
        "https://www.fragrantica.com/perfume/Creed/Aventus-9828.html",
        "https://www.example.com/page",
    ]
    accepted = accepted_urls(urls, "fragrantica")
    assert accepted == ["https://www.fragrantica.com/perfume/Creed/Aventus-9828.html"]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", help="Run Decodo-backed network checks.")
    parser.add_argument("--search-engine", choices=("google", "bing"), default="google")
    parser.add_argument("--case", action="append", help="Case name to run. Repeat for multiple cases.")
    parser.add_argument("--all-cases", action="store_true", help="Run the full default matrix.")
    parser.add_argument("--check-ip", action="store_true", help="Also call ip.decodo.com/json through the proxy.")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--max-response-bytes", type=int, default=DEFAULT_MAX_RESPONSE_BYTES)
    parser.add_argument("--max-total-bytes", type=int, default=DEFAULT_MAX_TOTAL_BYTES)
    parser.add_argument("--json-output", type=Path, help="Optional path to write JSON results.")
    parser.add_argument("--html-output-dir", type=Path, help="Optional directory for raw SERP HTML debug output.")
    parser.add_argument(
        "--allow-misses",
        action="store_true",
        help="Exit 0 even when a live case does not match its expected target.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    _self_test()
    if not args.live:
        print("Local Decodo SERP parser/config self-test passed. Add --live for proxy checks.")
        return 0

    config = DecodoProxyConfig.from_env()
    proxies = config.requests_proxies()
    cases = choose_cases(args)
    output: dict[str, object] = {
        "proxy": config.masked(),
        "search_engine": args.search_engine,
        "cases": [],
        "ip_check": None,
    }
    total_bytes = 0

    if args.check_ip:
        ip_result = check_proxy_ip(
            proxies=proxies,
            timeout=args.timeout,
            max_response_bytes=min(args.max_response_bytes, 100_000),
        )
        total_bytes += int(ip_result.get("response_bytes") or 0)
        output["ip_check"] = ip_result
        print(f"IP check status={ip_result['status_code']} bytes={ip_result['response_bytes']}")

    for case in cases:
        if total_bytes >= args.max_total_bytes:
            raise RuntimeError(f"total byte budget reached before {case.name}")
        result = run_case(
            case,
            search_engine=args.search_engine,
            proxies=proxies,
            timeout=args.timeout,
            max_response_bytes=min(args.max_response_bytes, args.max_total_bytes - total_bytes),
            html_output_dir=args.html_output_dir,
        )
        total_bytes += result.response_bytes
        output["cases"].append(asdict(result))
        status = "PASS" if result.expected_matched and not result.blocked and not result.error else "FAIL"
        print(
            f"{status} {case.name}: status={result.status_code} "
            f"latency={result.elapsed_ms}ms bytes={result.response_bytes} "
            f"results={result.result_count} accepted={result.accepted_count} "
            f"first={result.first_accepted_url} title={result.page_title!r} "
            f"blocked={result.blocked} error={result.error}"
        )

    output["total_response_bytes"] = total_bytes
    if args.json_output:
        args.json_output.write_text(json.dumps(output, indent=2), encoding="utf-8")
        print(f"Wrote {args.json_output}")

    failed = [
        item
        for item in output["cases"]
        if item.get("error") or item.get("blocked") or not item.get("expected_matched")
    ]
    if failed and not args.allow_misses:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
