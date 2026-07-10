from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from sentry_config import configure_sentry


class FakeSdk:
    def __init__(self) -> None:
        self.init_kwargs = None
        self.captured = []
        self.tags = []

    def init(self, **kwargs) -> None:
        self.init_kwargs = kwargs

    @contextmanager
    def new_scope(self):
        scope = SimpleNamespace(set_tag=lambda key, value: self.tags.append((key, value)))
        yield scope

    def capture_exception(self, exc: BaseException) -> None:
        self.captured.append(exc)


def test_no_dsn_is_a_noop_without_importing_sdk() -> None:
    def unexpected_loader():
        raise AssertionError("SDK loader must not run without SENTRY_DSN")

    reporter = configure_sentry({}, sdk_loader=unexpected_loader)

    assert reporter.enabled is False
    reporter.capture_background_exception(RuntimeError("ignored"), "test-worker")


def test_configured_dsn_initializes_privacy_bounded_sdk() -> None:
    sdk = FakeSdk()

    reporter = configure_sentry(
        {
            "SENTRY_DSN": "https://public@example.invalid/1",
            "RAILWAY_ENVIRONMENT_NAME": "production",
            "SENTRY_TRACES_SAMPLE_RATE": "0.1",
        },
        sdk_loader=lambda: sdk,
    )

    assert reporter.enabled is True
    assert sdk.init_kwargs == {
        "dsn": "https://public@example.invalid/1",
        "environment": "production",
        "traces_sample_rate": 0.1,
        "send_default_pii": False,
        "max_request_body_size": "never",
    }


def test_background_capture_uses_only_a_static_task_tag() -> None:
    sdk = FakeSdk()
    reporter = configure_sentry(
        {"SENTRY_DSN": "https://public@example.invalid/1"},
        sdk_loader=lambda: sdk,
    )
    error = RuntimeError("worker failed")

    reporter.capture_background_exception(error, "warm-cache-refresh")

    assert sdk.captured == [error]
    assert sdk.tags == [("background_task", "warm-cache-refresh")]


def test_sdk_initialization_failure_never_breaks_startup() -> None:
    def broken_loader():
        raise RuntimeError("SDK unavailable")

    reporter = configure_sentry(
        {"SENTRY_DSN": "https://public@example.invalid/1"},
        sdk_loader=broken_loader,
    )

    assert reporter.enabled is False
