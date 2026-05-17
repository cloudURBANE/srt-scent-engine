#!/usr/bin/env python3
"""
mobile.py

Phone-side surface for the enrichment dashboard. Two route trees:

    /m/*       -- server-rendered HTML pages (login, dashboard)
    /api/m/*   -- JSON endpoints the dashboard JS calls, session-cookie authed

Auth model (two-factor):
    1. Worker enters email at /m/login.
    2. We email a single-use, hashed magic link (TTL 10 min, rate-limited).
    3. Clicking the link opens /m/redeem and reveals a PIN prompt; the magic
       token is NOT a session yet.
    4. Worker submits link token + PIN. Both correct -> mobile_sessions row +
       HttpOnly cookie pinned to a device fingerprint cookie (random per
       device). Compromising the inbox alone is not enough.

Scrape commands (toggle auto-approve, process N, retry failed) cannot run on
Railway because Fragrantica blocks Cloudflare egress. The mobile UI writes
intents to worker_commands; the desktop worker polls and executes. Read-only
metrics and queue-mutation actions that don't need scraping (ignore job) run
directly against the API.
"""
from __future__ import annotations

import hashlib
import hmac
import html
import json
import os
import secrets
from typing import Any

import requests
from fastapi import APIRouter, Cookie, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from pydantic import BaseModel

import db

router = APIRouter()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MAGIC_LINK_TTL_MINUTES = 10
SESSION_TTL_DAYS = 30
PIN_LOCK_MINUTES = 15
PIN_MAX_STRIKES = 5
MAGIC_LINK_RATE_PER_EMAIL_PER_HOUR = 3
MAGIC_LINK_RATE_PER_IP_PER_HOUR = 10

SESSION_COOKIE = "srt_session"
DEVICE_COOKIE = "srt_device"

# Public base URL used to build the magic link. Required for emails to be
# clickable from a phone — when unset, we fall back to the inbound request's
# scheme+host, which works in most deploys but breaks behind some proxies.
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
RESEND_FROM = os.environ.get("RESEND_FROM", "SRT <noreply@srt.local>")
ADMIN_ALERT_EMAIL = os.environ.get("ADMIN_ALERT_EMAIL", "")


def _emit_email(*, to: str, subject: str, html_body: str) -> None:
    """Send via Resend. If RESEND_API_KEY is unset, log to stdout (dev fallback)."""
    if not RESEND_API_KEY:
        print(f"[mobile.email] (no RESEND_API_KEY) to={to} subject={subject}")
        print(f"[mobile.email] body:\n{html_body}\n")
        return
    try:
        requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json={"from": RESEND_FROM, "to": [to], "subject": subject, "html": html_body},
            timeout=10,
        )
    except requests.RequestException as exc:
        # We don't surface delivery failures to the user — the response shape
        # is identical so an attacker can't enumerate accounts by error.
        print(f"[mobile.email] resend send failed: {exc}")


# ---------------------------------------------------------------------------
# PIN hashing -- bcrypt if present, else PBKDF2-SHA256 (stdlib).
# ---------------------------------------------------------------------------

try:
    import bcrypt  # type: ignore
    _HAVE_BCRYPT = True
except Exception:  # pragma: no cover
    _HAVE_BCRYPT = False


def hash_pin(pin: str) -> str:
    pin_bytes = pin.encode("utf-8")
    if _HAVE_BCRYPT:
        return "bcrypt$" + bcrypt.hashpw(pin_bytes, bcrypt.gensalt(rounds=12)).decode("ascii")
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", pin_bytes, salt, 200_000)
    return "pbkdf2$" + salt.hex() + "$" + digest.hex()


def verify_pin(pin: str, stored: str) -> bool:
    if not stored:
        return False
    pin_bytes = pin.encode("utf-8")
    if stored.startswith("bcrypt$") and _HAVE_BCRYPT:
        try:
            return bcrypt.checkpw(pin_bytes, stored[len("bcrypt$"):].encode("ascii"))
        except Exception:
            return False
    if stored.startswith("pbkdf2$"):
        try:
            _, salt_hex, digest_hex = stored.split("$")
            salt = bytes.fromhex(salt_hex)
            expected = bytes.fromhex(digest_hex)
            actual = hashlib.pbkdf2_hmac("sha256", pin_bytes, salt, 200_000)
            return hmac.compare_digest(expected, actual)
        except Exception:
            return False
    return False


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Device fingerprint -- a random opaque cookie per browser. Bound to the
# session row so an attacker with only the session-id cookie still misses one.
# ---------------------------------------------------------------------------


def _ensure_device_cookie(request: Request, response: Response) -> str:
    existing = request.cookies.get(DEVICE_COOKIE)
    if existing and 16 <= len(existing) <= 128:
        return existing
    new_device = secrets.token_urlsafe(24)
    response.set_cookie(
        DEVICE_COOKIE,
        new_device,
        max_age=60 * 60 * 24 * 365 * 2,
        secure=True,
        httponly=True,
        samesite="strict",
        path="/",
    )
    return new_device


def _set_session_cookie(response: Response, session_id: str) -> None:
    response.set_cookie(
        SESSION_COOKIE,
        session_id,
        max_age=60 * 60 * 24 * SESSION_TTL_DAYS,
        secure=True,
        httponly=True,
        samesite="strict",
        path="/",
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(SESSION_COOKIE, path="/")


# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------


def _require_db_enabled() -> None:
    if not db.ENABLED:
        raise HTTPException(status_code=503, detail="Mobile surface requires DATABASE_URL.")


def current_account(
    request: Request,
    session_cookie: str | None = Cookie(default=None, alias=SESSION_COOKIE),
    device_cookie: str | None = Cookie(default=None, alias=DEVICE_COOKIE),
) -> dict[str, Any]:
    _require_db_enabled()
    if not session_cookie or not device_cookie:
        raise HTTPException(status_code=401, detail="not_authenticated")
    sess = db.get_active_session(session_cookie, device_cookie)
    if not sess:
        raise HTTPException(status_code=401, detail="session_invalid")
    account = db.get_worker_account(sess["account_id"])
    if not account or account.get("disabled"):
        raise HTTPException(status_code=401, detail="account_disabled")
    db.touch_session(session_cookie, SESSION_TTL_DAYS)
    return account


# ---------------------------------------------------------------------------
# HTML rendering -- inline templates so this drops in without a templates dir.
# ---------------------------------------------------------------------------

_BASE_CSS = """
:root { color-scheme: dark; }
* { box-sizing: border-box; }
html, body {
  margin: 0; padding: 0;
  background: #07120a; color: #b9f6ca;
  font-family: ui-monospace, "SFMono-Regular", Menlo, Consolas, monospace;
  font-size: 15px; line-height: 1.4;
  min-height: 100vh;
}
.wrap { max-width: 720px; margin: 0 auto; padding: 18px 14px 80px; }
h1, h2 { color: #7CFFB2; letter-spacing: 0.08em; text-transform: uppercase; font-weight: 600; margin: 18px 0 10px; }
h1 { font-size: 17px; } h2 { font-size: 13px; }
.banner { font-size: 11px; opacity: 0.7; letter-spacing: 0.18em; }
.card {
  border: 1px solid #1f4d2b; background: #0a1d12;
  border-radius: 8px; padding: 14px; margin: 12px 0;
}
label { display:block; font-size:11px; letter-spacing: 0.18em; text-transform: uppercase; opacity: 0.7; margin-bottom: 6px; }
input[type=email], input[type=text], input[type=password], input[type=tel] {
  width:100%; padding:12px 14px; border-radius:6px;
  border:1px solid #2d6b3e; background:#04100a; color:#d2ffe1;
  font: inherit; outline: none;
}
input:focus { border-color: #7CFFB2; }
button, .btn {
  display:inline-block; width:100%; padding:12px 14px;
  background:#0f3a1f; color:#a5ffc4;
  border:1px solid #2d6b3e; border-radius:6px;
  font: inherit; cursor: pointer; margin-top: 10px;
  letter-spacing: 0.14em; text-transform: uppercase; font-size: 12px;
}
button:hover, .btn:hover { background:#15532c; color: #d3ffe2; }
button[disabled] { opacity: 0.45; cursor: not-allowed; }
.row { display:flex; gap:10px; flex-wrap: wrap; }
.row > * { flex: 1 1 0; min-width: 120px; }
.metric { font-size: 22px; color: #d2ffe1; }
.metric small { display:block; font-size: 10px; opacity:0.55; letter-spacing:0.18em; text-transform:uppercase; margin-bottom:4px; color:#7CFFB2; }
.stream { font-size: 12px; max-height: 280px; overflow-y: auto; }
.stream div { padding: 3px 0; border-bottom: 1px dashed #143820; }
.stream .ok { color:#82ffb1; } .stream .err { color:#ff7d7d; } .stream .info { opacity:0.75; }
.err-msg { color: #ff8888; font-size: 12px; margin-top: 8px; }
.ok-msg  { color: #82ffb1; font-size: 12px; margin-top: 8px; }
.muted { opacity: 0.55; }
a { color: #7CFFB2; }
.topbar { display:flex; justify-content:space-between; align-items:center; }
.topbar form { display:inline; }
.topbar button { width:auto; margin:0; padding:6px 10px; font-size:10px; }
.kbd { background:#143820; padding:2px 6px; border-radius:4px; font-size:11px; }
"""


def _page(body: str, *, title: str = "SRT Mobile") -> str:
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#07120a">
<title>{html.escape(title)}</title>
<style>{_BASE_CSS}</style>
</head><body><div class="wrap">{body}</div></body></html>"""


def _login_page(*, message: str = "", error: str = "") -> str:
    msg_html = ""
    if message:
        msg_html = f'<div class="ok-msg">{html.escape(message)}</div>'
    if error:
        msg_html = f'<div class="err-msg">{html.escape(error)}</div>'
    body = f"""
<h1>SRT // ACCESS</h1>
<div class="banner">two-factor // email link + pin</div>
<form method="post" action="/m/login" class="card">
  <label for="email">Email</label>
  <input id="email" name="email" type="email" required autocomplete="email" autocapitalize="off" autocorrect="off">
  <button type="submit">Send link</button>
  {msg_html}
</form>
<p class="muted" style="font-size:11px">If the email is on file, a single-use link arrives within seconds. The link alone won't sign you in — it only unlocks the PIN prompt.</p>
"""
    return _page(body, title="SRT // Login")


def _pin_page(token: str, *, error: str = "") -> str:
    err_html = f'<div class="err-msg">{html.escape(error)}</div>' if error else ""
    safe_token = html.escape(token)
    body = f"""
<h1>SRT // PIN</h1>
<div class="banner">step 2 of 2 // enter your 6-digit pin</div>
<form method="post" action="/m/verify" class="card" autocomplete="off">
  <input type="hidden" name="token" value="{safe_token}">
  <label for="pin">PIN</label>
  <input id="pin" name="pin" type="tel" inputmode="numeric" pattern="[0-9]{{4,8}}" maxlength="8" required autofocus>
  <button type="submit">Sign in</button>
  {err_html}
</form>
<p class="muted" style="font-size:11px">5 wrong PINs locks the account for {PIN_LOCK_MINUTES} minutes.</p>
"""
    return _page(body, title="SRT // PIN")


def _dashboard_page(account: dict[str, Any]) -> str:
    label = account.get("label") or account.get("email") or "operator"
    body = f"""
<div class="topbar">
  <h1>SRT // FIELD</h1>
  <form method="post" action="/m/logout"><button type="submit">Sign out</button></form>
</div>
<div class="banner">{html.escape(label)} // session pinned to this device</div>

<div class="card">
  <div class="row">
    <div class="metric" id="m-pending"><small>pending</small>—</div>
    <div class="metric" id="m-processing"><small>processing</small>—</div>
    <div class="metric" id="m-failed"><small>failed</small>—</div>
    <div class="metric" id="m-completed"><small>completed</small>—</div>
  </div>
  <div class="muted" id="worker-liveness" style="font-size:11px;margin-top:10px">checking worker status…</div>
</div>

<h2>Controls (require a desktop worker online)</h2>
<div class="card">
  <div class="row">
    <button data-cmd="toggle_auto_approve">Toggle auto-approve</button>
    <button data-cmd="process_pending" data-prompt="How many jobs to process now?" data-prompt-key="limit" data-prompt-default="5">Process N pending</button>
  </div>
  <div class="row">
    <button data-cmd="retry_failed">Retry failed</button>
    <button data-cmd="set_auto_approve" data-prompt="Set auto-approve to (on/off)?" data-prompt-key="state" data-prompt-default="on">Set auto-approve</button>
  </div>
  <div id="cmd-msg" class="ok-msg"></div>
</div>

<h2>Recent jobs</h2>
<div class="card stream" id="recent-jobs"><div class="muted">loading…</div></div>

<h2>Command queue</h2>
<div class="card stream" id="recent-commands"><div class="muted">loading…</div></div>

<script>
const FAILED_WORKER_THRESHOLD_SECONDS = 120;

async function fetchJSON(path, opts) {{
  const r = await fetch(path, Object.assign({{credentials:'same-origin', headers:{{'Accept':'application/json'}}}}, opts || {{}}));
  if (r.status === 401) {{ location.href = '/m/login'; throw new Error('auth'); }}
  return r.json();
}}

async function refresh() {{
  try {{
    const overview = await fetchJSON('/api/m/overview');
    const counts = overview.counts || {{}};
    document.getElementById('m-pending').lastChild.textContent = counts.pending ?? 0;
    document.getElementById('m-processing').lastChild.textContent = counts.processing ?? 0;
    document.getElementById('m-failed').lastChild.textContent = counts.failed ?? 0;
    document.getElementById('m-completed').lastChild.textContent = counts.completed ?? 0;
    const live = document.getElementById('worker-liveness');
    const secs = overview.worker_last_seen_seconds;
    if (secs === null || secs === undefined) {{
      live.textContent = 'no desktop worker has connected yet — commands will queue';
      live.className = 'muted';
    }} else if (secs > FAILED_WORKER_THRESHOLD_SECONDS) {{
      live.textContent = 'desktop worker last seen ' + Math.round(secs) + 's ago — may be offline';
      live.className = 'err-msg';
    }} else {{
      live.textContent = 'desktop worker live (' + Math.round(secs) + 's ago)';
      live.className = 'ok-msg';
    }}

    const jobsBox = document.getElementById('recent-jobs');
    jobsBox.innerHTML = '';
    const jobs = overview.recent_jobs || [];
    if (!jobs.length) {{ jobsBox.innerHTML = '<div class="muted">no jobs yet</div>'; }}
    for (const job of jobs) {{
      const div = document.createElement('div');
      const cls = job.status === 'failed' ? 'err' : job.status === 'completed' ? 'ok' : 'info';
      div.className = cls;
      const label = (job.house ? job.house + ' — ' : '') + (job.name || job.query || job.job_key || job.id);
      div.textContent = '[' + job.status + '] ' + label;
      jobsBox.appendChild(div);
    }}

    const cmdBox = document.getElementById('recent-commands');
    cmdBox.innerHTML = '';
    const cmds = overview.recent_commands || [];
    if (!cmds.length) {{ cmdBox.innerHTML = '<div class="muted">no commands sent yet</div>'; }}
    for (const cmd of cmds) {{
      const div = document.createElement('div');
      const cls = cmd.status === 'failed' ? 'err' : cmd.status === 'completed' ? 'ok' : 'info';
      div.className = cls;
      const note = cmd.result_text ? ' — ' + cmd.result_text : '';
      div.textContent = '[' + cmd.status + '] ' + cmd.kind + note;
      cmdBox.appendChild(div);
    }}
  }} catch (e) {{ /* swallow; next tick retries */ }}
}}

document.querySelectorAll('button[data-cmd]').forEach(btn => {{
  btn.addEventListener('click', async () => {{
    const kind = btn.getAttribute('data-cmd');
    const payload = {{}};
    const promptText = btn.getAttribute('data-prompt');
    if (promptText) {{
      const key = btn.getAttribute('data-prompt-key');
      const def = btn.getAttribute('data-prompt-default') || '';
      const answer = window.prompt(promptText, def);
      if (answer === null) return;
      payload[key] = answer;
    }}
    btn.disabled = true;
    const msg = document.getElementById('cmd-msg');
    try {{
      const r = await fetchJSON('/api/m/commands', {{
        method:'POST',
        headers:{{'Content-Type':'application/json'}},
        body: JSON.stringify({{kind, payload}})
      }});
      msg.textContent = 'queued: ' + (r.kind || kind);
      msg.className = 'ok-msg';
      refresh();
    }} catch (e) {{
      msg.textContent = 'failed to queue command';
      msg.className = 'err-msg';
    }} finally {{
      btn.disabled = false;
    }}
  }});
}});

refresh();
setInterval(refresh, 4000);
</script>
"""
    return _page(body, title="SRT // Field")


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------


def _base_url(request: Request) -> str:
    if PUBLIC_BASE_URL:
        return PUBLIC_BASE_URL
    return f"{request.url.scheme}://{request.url.netloc}".rstrip("/")


def _client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return (request.client.host if request.client else "") or ""


@router.get("/m", response_class=HTMLResponse)
def m_root(request: Request) -> Response:
    if request.cookies.get(SESSION_COOKIE) and request.cookies.get(DEVICE_COOKIE):
        sess = db.get_active_session(
            request.cookies[SESSION_COOKIE], request.cookies[DEVICE_COOKIE]
        )
        if sess:
            return RedirectResponse("/m/dashboard", status_code=302)
    return RedirectResponse("/m/login", status_code=302)


@router.get("/m/login", response_class=HTMLResponse)
def m_login_get() -> Response:
    return HTMLResponse(_login_page())


@router.post("/m/login", response_class=HTMLResponse)
def m_login_post(request: Request, email: str = Form(...)) -> Response:
    _require_db_enabled()
    normalized = (email or "").strip().lower()
    ip = _client_ip(request)

    # Generic response either way -- never reveal which emails are real.
    generic_msg = "If that email is on file, a link is on the way."

    # Cheap IP rate limit before any DB lookup.
    if db.count_recent_magic_links(account_id=None, ip=ip, minutes=60) >= MAGIC_LINK_RATE_PER_IP_PER_HOUR:
        return HTMLResponse(_login_page(message=generic_msg))

    account = db.get_worker_account_by_email(normalized)
    if account and not account.get("disabled"):
        if db.count_recent_magic_links(
            account_id=account["id"], ip=None, minutes=60
        ) < MAGIC_LINK_RATE_PER_EMAIL_PER_HOUR:
            token = secrets.token_urlsafe(32)
            db.insert_magic_link(
                account_id=account["id"],
                token_hash=_hash_token(token),
                ttl_minutes=MAGIC_LINK_TTL_MINUTES,
                requesting_ip=ip,
            )
            link = f"{_base_url(request)}/m/redeem?t={token}"
            body = (
                "<p>Tap the link below within "
                f"{MAGIC_LINK_TTL_MINUTES} minutes:</p>"
                f'<p><a href="{html.escape(link)}">{html.escape(link)}</a></p>'
                "<p>You'll still need your PIN. If you didn't request this, ignore the email.</p>"
            )
            _emit_email(to=normalized, subject="Your SRT access link", html_body=body)
    return HTMLResponse(_login_page(message=generic_msg))


@router.get("/m/redeem", response_class=HTMLResponse)
def m_redeem_get(t: str = Query(default="")) -> Response:
    if not t:
        return HTMLResponse(_login_page(error="Missing link token."))
    # Do not consume yet; consumption is atomic with PIN verification.
    return HTMLResponse(_pin_page(t))


@router.post("/m/verify", response_class=HTMLResponse)
def m_verify(
    request: Request,
    token: str = Form(...),
    pin: str = Form(...),
) -> Response:
    _require_db_enabled()
    pin = (pin or "").strip()
    if not token or not pin or not pin.isdigit():
        return HTMLResponse(_pin_page(token, error="PIN must be digits."), status_code=400)

    response = HTMLResponse("")  # placeholder; we set cookies on whichever response we return
    device = _ensure_device_cookie(request, response)

    consumed = db.consume_magic_link(_hash_token(token))
    if not consumed:
        return HTMLResponse(
            _login_page(error="Link is invalid or expired. Request a new one."),
            status_code=400,
        )
    account = db.get_worker_account(consumed["account_id"])
    if not account or account.get("disabled"):
        return HTMLResponse(
            _login_page(error="Account is unavailable."), status_code=400
        )

    # Locked accounts cannot attempt PINs at all.
    locked_until = account.get("locked_until")
    if locked_until:
        return HTMLResponse(
            _pin_page(token, error="Account is temporarily locked. Try again later."),
            status_code=429,
        )

    if not verify_pin(pin, account["pin_hash"]):
        updated = db.record_pin_strike(account["id"], PIN_LOCK_MINUTES)
        if updated and updated.get("locked_until") and ADMIN_ALERT_EMAIL:
            _emit_email(
                to=ADMIN_ALERT_EMAIL,
                subject="SRT: account locked after PIN strikes",
                html_body=f"Account {html.escape(account['email'])} locked for {PIN_LOCK_MINUTES} minutes.",
            )
        # Magic link already consumed; user must request a new one. This is
        # intentional: it makes link-stealing-then-guessing the PIN a one-shot.
        return HTMLResponse(
            _login_page(error="PIN incorrect. The link was burned — request a new one."),
            status_code=401,
        )

    db.reset_pin_strikes(account["id"])
    session_id = db.create_session(
        account_id=account["id"],
        device_fingerprint=device,
        ttl_days=SESSION_TTL_DAYS,
    )
    response = RedirectResponse("/m/dashboard", status_code=302)
    _ensure_device_cookie(request, response)
    _set_session_cookie(response, session_id)
    return response


@router.post("/m/logout")
def m_logout(
    request: Request,
    session_cookie: str | None = Cookie(default=None, alias=SESSION_COOKIE),
) -> Response:
    if session_cookie and db.ENABLED:
        db.revoke_session(session_cookie)
    response = RedirectResponse("/m/login", status_code=302)
    _clear_session_cookie(response)
    return response


@router.get("/m/dashboard", response_class=HTMLResponse)
def m_dashboard(account: dict[str, Any] = Depends(current_account)) -> Response:
    return HTMLResponse(_dashboard_page(account))


# ---------------------------------------------------------------------------
# JSON API consumed by the dashboard
# ---------------------------------------------------------------------------


@router.get("/api/m/overview")
def m_overview(account: dict[str, Any] = Depends(current_account)) -> dict[str, Any]:
    counts = db.get_status_counts() if db.ENABLED else {}
    recent_jobs: list[dict[str, Any]] = []
    if db.ENABLED:
        # Surface failed first (operator-relevant), then processing, then pending.
        for status in ("failed", "processing", "pending"):
            for j in db.list_jobs(status=status, limit=5):
                recent_jobs.append(j)
    return {
        "counts": counts,
        "recent_jobs": recent_jobs[:15],
        "recent_commands": db.list_commands(limit=10),
        "worker_last_seen_seconds": db.latest_worker_heartbeat_seconds(),
    }


class CommandRequest(BaseModel):
    kind: str
    payload: dict[str, Any] | None = None


def _normalize_command(req: CommandRequest) -> tuple[str, dict[str, Any]]:
    kind = (req.kind or "").strip()
    if kind not in db.VALID_COMMAND_KINDS:
        raise HTTPException(status_code=400, detail=f"Unknown command kind '{kind}'.")
    raw = dict(req.payload or {})
    payload: dict[str, Any] = {}
    if kind == "process_pending":
        try:
            limit = int(str(raw.get("limit") or 5))
        except ValueError:
            raise HTTPException(status_code=400, detail="limit must be an integer")
        payload["limit"] = max(1, min(limit, 50))
    elif kind == "set_auto_approve":
        state = str(raw.get("state") or "").strip().lower()
        if state not in ("on", "off"):
            raise HTTPException(status_code=400, detail="state must be 'on' or 'off'")
        payload["state"] = state
    # toggle_auto_approve and retry_failed take no payload
    return kind, payload


@router.post("/api/m/commands")
def m_post_command(
    req: CommandRequest, account: dict[str, Any] = Depends(current_account)
) -> dict[str, Any]:
    kind, payload = _normalize_command(req)
    cmd = db.enqueue_command(
        kind=kind, payload=payload, issued_by_account_id=account["id"]
    )
    return cmd


@router.get("/api/m/commands")
def m_list_commands(
    _: dict[str, Any] = Depends(current_account),
    status: str | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=100),
) -> dict[str, Any]:
    if status and status not in db.VALID_COMMAND_STATUSES:
        raise HTTPException(status_code=400, detail="invalid status")
    return {"commands": db.list_commands(status=status, limit=limit)}


# ---------------------------------------------------------------------------
# Desktop-worker-facing command channel (reuses the bearer worker token).
# These are the only routes outside /m/* and /api/m/* added by this module;
# they live here so the command-queue plumbing stays in one file.
# ---------------------------------------------------------------------------


def _require_worker_token(request: Request) -> None:
    expected = os.environ.get("ENRICHMENT_WORKER_TOKEN", "")
    if not expected:
        raise HTTPException(status_code=503, detail="Worker endpoints unconfigured.")
    presented = request.headers.get("authorization", "")
    if not hmac.compare_digest(presented, f"Bearer {expected}"):
        raise HTTPException(status_code=401, detail="Invalid or missing worker token.")


@router.post("/api/enrichment/commands/claim")
def worker_claim_command(request: Request) -> JSONResponse:
    _require_worker_token(request)
    _require_db_enabled()
    cmd = db.claim_next_command()
    if not cmd:
        return JSONResponse({"command": None})
    return JSONResponse({"command": cmd})


class CommandResultRequest(BaseModel):
    ok: bool
    result_text: str | None = None


@router.post("/api/enrichment/commands/{command_id}/finish")
def worker_finish_command(
    command_id: str, payload: CommandResultRequest, request: Request
) -> dict[str, Any]:
    _require_worker_token(request)
    _require_db_enabled()
    db.finish_command(command_id, ok=payload.ok, result_text=payload.result_text)
    return {"ok": True}


@router.post("/api/enrichment/commands/heartbeat")
def worker_heartbeat(request: Request) -> dict[str, Any]:
    """Worker pings this on every poll tick so the dashboard can show liveness."""
    _require_worker_token(request)
    _require_db_enabled()
    db.stamp_worker_heartbeat()
    return {"ok": True}
