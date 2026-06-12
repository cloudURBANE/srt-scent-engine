import os

from scripts import diag_decodo_serp_proxy as diag


_SAVED_ENV_KEYS = (
    "DECODO_PROXY_URL",
    "DECODO_PROXY_HOST",
    "DECODO_PROXY_PORT",
    "DECODO_PROXY_USERNAME",
    "DECODO_PROXY_PASSWORD",
    "DECODO_PROXY_SCHEME",
)


def check(label: str, ok: bool, detail: str = "") -> None:
    if ok:
        print(f"[OK] {label}")
        return
    raise AssertionError(f"{label}: {detail}")


def _save_env() -> dict[str, str | None]:
    return {key: os.environ.get(key) for key in _SAVED_ENV_KEYS}


def _restore_env(saved: dict[str, str | None]) -> None:
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def test_decodo_proxy_config_from_env_masks_and_encodes_secret() -> None:
    print("Decodo proxy config checks:")
    saved = _save_env()
    try:
        for key in _SAVED_ENV_KEYS:
            os.environ.pop(key, None)
        os.environ["DECODO_PROXY_PORT"] = "10001"
        os.environ["DECODO_PROXY_USERNAME"] = "proxy-test"
        os.environ["DECODO_PROXY_PASSWORD"] = "fake-pass~with-symbols"

        config = diag.DecodoProxyConfig.from_env()
        proxy_url = config.proxy_url()
        masked = config.masked()
    finally:
        _restore_env(saved)

    check("default host is Decodo residential gateway", config.host == "gate.decodo.com", str(config))
    check("port comes from environment", config.port == 10001, str(config.port))
    check("proxy URL includes encoded credentials", proxy_url.startswith("http://proxy-test:"), proxy_url)
    check("masked output does not expose password", masked["password"] == "********", str(masked))
    check("masked username preserves only edges", masked["username"] == "pr...st", str(masked))


def test_decodo_proxy_config_from_url() -> None:
    print("Decodo proxy URL config checks:")
    saved = _save_env()
    try:
        for key in _SAVED_ENV_KEYS:
            os.environ.pop(key, None)
        os.environ["DECODO_PROXY_URL"] = "http://user%40x:p%40ss@gate.decodo.com:10002"

        config = diag.DecodoProxyConfig.from_env()
    finally:
        _restore_env(saved)

    check("proxy URL parser decodes username", config.username == "user@x", config.username)
    check("proxy URL parser decodes password", config.password == "p@ss", config.password)
    check("proxy URL parser reads port", config.port == 10002, str(config.port))


def test_extract_google_and_bing_result_urls() -> None:
    print("Decodo SERP URL extraction checks:")
    bing_encoded = "a1aHR0cHM6Ly93d3cuZnJhZ3JhbnRpY2EuY29tL3BlcmZ1bWUvQ3JlZWQvQXZlbnR1cy05ODI4Lmh0bWw"
    html = f"""
    <html><body>
      <a href="/url?q=https%3A%2F%2Fwww.fragrantica.com%2Fperfume%2FXerjoff%2FXJ-1861-Naxos-30529.html&sa=U">Naxos</a>
      <a href="https://www.google.com/search?q=ignore">Google</a>
      <a href="/ck/a?u={bing_encoded}">Aventus</a>
      <a href="https://example.com/not-a-perfume">Other</a>
    </body></html>
    """

    urls = diag.extract_result_urls(html)
    accepted = diag.accepted_urls(urls, "fragrantica")

    check("Google redirect URLs are unwrapped", urls[0].endswith("XJ-1861-Naxos-30529.html"), str(urls))
    check("Bing a1 redirect URLs are decoded", any("Aventus-9828.html" in url for url in urls), str(urls))
    check("search engine self links are skipped", not any("google.com/search" in url for url in urls), str(urls))
    check("Fragrantica acceptance keeps perfume URLs only", len(accepted) == 2, str(accepted))


def test_block_marker_detection() -> None:
    print("Decodo block marker checks:")
    check("429 is treated as blocked", diag.looks_blocked(429, "") is True, "")
    check(
        "unusual traffic page is treated as blocked",
        diag.looks_blocked(200, "Our systems have detected unusual traffic") is True,
        "",
    )
    check("normal SERP shell is not blocked", diag.looks_blocked(200, "<html><body>ok</body></html>") is False, "")


def main() -> int:
    test_decodo_proxy_config_from_env_masks_and_encodes_secret()
    test_decodo_proxy_config_from_url()
    test_extract_google_and_bing_result_urls()
    test_block_marker_detection()
    print()
    print("All Decodo SERP proxy checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
