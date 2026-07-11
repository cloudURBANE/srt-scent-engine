"""Minimal, privacy-bounded Sentry setup for the engine process."""

from __future__ import annotations

import importlib
import logging
import os
from collections.abc import Callable, Mapping
from types import ModuleType

logger = logging.getLogger(__name__)


class SentryReporter:
    """Capture sanitized operational exceptions when Sentry is configured."""

    def __init__(self, sdk: ModuleType | None = None) -> None:
        self._sdk = sdk

    @property
    def enabled(self) -> bool:
        return self._sdk is not None

    def capture_background_exception(self, exc: BaseException, task: str) -> None:
        if self._sdk is None:
            return
        try:
            with self._sdk.new_scope() as scope:
                scope.set_tag("background_task", task)
                self._sdk.capture_exception(exc)
        except Exception:
            # Telemetry must never turn best-effort background work into a crash.
            logger.warning("Sentry background exception capture failed", exc_info=True)


def _load_sdk() -> ModuleType:
    return importlib.import_module("sentry_sdk")


def configure_sentry(
    environ: Mapping[str, str] | None = None,
    sdk_loader: Callable[[], ModuleType] = _load_sdk,
) -> SentryReporter:
    """Initialize Sentry only when a non-empty DSN is present.

    FastAPI and Starlette integrations auto-enable in the SDK. Request bodies and
    default PII are explicitly disabled; background callers add only a static task
    tag and the exception itself.
    """

    env = os.environ if environ is None else environ
    dsn = env.get("SENTRY_DSN", "").strip()
    if not dsn:
        return SentryReporter()

    try:
        sdk = sdk_loader()
        sdk.init(
            dsn=dsn,
            environment=env.get("RAILWAY_ENVIRONMENT_NAME", "production"),
            traces_sample_rate=float(env.get("SENTRY_TRACES_SAMPLE_RATE", "0") or 0),
            send_default_pii=False,
            max_request_body_size="never",
        )
    except Exception:
        logger.exception("Sentry init failed; continuing without it")
        return SentryReporter()

    logger.info("Sentry error tracking enabled")
    return SentryReporter(sdk)
