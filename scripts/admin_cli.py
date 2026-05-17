#!/usr/bin/env python3
"""
admin_cli.py -- offline admin tool for mobile worker accounts.

Examples:
    python scripts/admin_cli.py create --email you@example.com --label "Phone"
    python scripts/admin_cli.py list
    python scripts/admin_cli.py reset-pin --email you@example.com
    python scripts/admin_cli.py disable --email you@example.com
    python scripts/admin_cli.py enable  --email you@example.com

Requires DATABASE_URL in the environment. PINs are prompted (never argv).
"""
from __future__ import annotations

import argparse
import getpass
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import db  # noqa: E402
import mobile  # noqa: E402


def _prompt_pin() -> str:
    while True:
        pin = getpass.getpass("PIN (6-8 digits): ").strip()
        if not pin.isdigit() or not (4 <= len(pin) <= 8):
            print("PIN must be 4-8 digits.")
            continue
        confirm = getpass.getpass("Confirm PIN: ").strip()
        if pin != confirm:
            print("PINs do not match.")
            continue
        return pin


def cmd_create(args: argparse.Namespace) -> int:
    pin = _prompt_pin()
    try:
        account = db.create_worker_account(
            email=args.email, label=args.label, pin_hash=mobile.hash_pin(pin)
        )
    except Exception as exc:
        print(f"error: {exc}")
        return 1
    print(f"created account {account['id']} for {account['email']}")
    return 0


def cmd_list(_args: argparse.Namespace) -> int:
    rows = db.list_worker_accounts()
    if not rows:
        print("(no accounts)")
        return 0
    for r in rows:
        flags = []
        if r.get("disabled"):
            flags.append("DISABLED")
        if r.get("locked_until"):
            flags.append(f"locked_until={r['locked_until']}")
        print(
            f"{r['email']:<30} label={r.get('label') or '-':<15} "
            f"strikes={r['pin_strikes']} last_login={r.get('last_login_at') or '-'} "
            f"{' '.join(flags)}"
        )
    return 0


def _find_or_die(email: str) -> dict:
    account = db.get_worker_account_by_email(email)
    if not account:
        print(f"no account with email {email!r}")
        sys.exit(1)
    return account


def cmd_reset_pin(args: argparse.Namespace) -> int:
    account = _find_or_die(args.email)
    pin = _prompt_pin()
    db.update_account_pin(account["id"], mobile.hash_pin(pin))
    print(f"pin reset for {account['email']}")
    return 0


def cmd_disable(args: argparse.Namespace) -> int:
    account = _find_or_die(args.email)
    db.set_account_disabled(account["id"], True)
    print(f"disabled {account['email']}")
    return 0


def cmd_enable(args: argparse.Namespace) -> int:
    account = _find_or_die(args.email)
    db.set_account_disabled(account["id"], False)
    print(f"enabled {account['email']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    if not db.ENABLED:
        print("DATABASE_URL is not set; admin CLI requires durable storage.")
        return 2
    db.init_db()

    parser = argparse.ArgumentParser(description="SRT mobile-worker admin CLI.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_create = sub.add_parser("create", help="Create a new worker account.")
    p_create.add_argument("--email", required=True)
    p_create.add_argument("--label", default=None)
    p_create.set_defaults(func=cmd_create)

    p_list = sub.add_parser("list", help="List all worker accounts.")
    p_list.set_defaults(func=cmd_list)

    p_reset = sub.add_parser("reset-pin", help="Reset an account's PIN.")
    p_reset.add_argument("--email", required=True)
    p_reset.set_defaults(func=cmd_reset_pin)

    p_disable = sub.add_parser("disable", help="Disable login for an account.")
    p_disable.add_argument("--email", required=True)
    p_disable.set_defaults(func=cmd_disable)

    p_enable = sub.add_parser("enable", help="Re-enable an account.")
    p_enable.add_argument("--email", required=True)
    p_enable.set_defaults(func=cmd_enable)

    args = parser.parse_args(argv)
    return int(args.func(args) or 0)


if __name__ == "__main__":
    sys.exit(main())
