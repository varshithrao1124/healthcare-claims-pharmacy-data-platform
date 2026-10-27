import os
from decimal import Decimal

import pytest

from hc_platform.config import join_uri, load_settings
from hc_platform.retry import call_with_retry
from hc_platform.secrets import get_secret


def test_join_uri_local_and_abfss():
    assert join_uri("/lake", "bronze", "claim").endswith("bronze/claim")
    assert join_uri("abfss://claims@acct.dfs.core.windows.net/lake", "gold", "fact") == (
        "abfss://claims@acct.dfs.core.windows.net/lake/gold/fact"
    )


def test_dev_defaults_are_local(monkeypatch):
    monkeypatch.delenv("HC_STORAGE_MODE", raising=False)
    monkeypatch.delenv("HC_CATALOG_MODE", raising=False)
    settings = load_settings("dev")
    assert settings.storage_mode == "local"
    assert settings.catalog_mode == "local"
    assert settings.table_id("gold", "fact_pmpm") == "healthcare_dev.gold.fact_pmpm"
    assert settings.table_path("gold", "fact_pmpm").endswith("gold/fact_pmpm")
    assert not settings.use_unity_catalog


def test_env_overrides_switch_to_adls_and_unity(monkeypatch):
    monkeypatch.setenv("HC_STORAGE_MODE", "adls")
    monkeypatch.setenv("HC_CATALOG_MODE", "unity")
    monkeypatch.setenv("HC_CATALOG_NAME", "healthcare_prod")
    monkeypatch.setenv("HC_ADLS_URI", "abfss://claims@example.dfs.core.windows.net/lake")
    settings = load_settings("dev")
    assert settings.storage_mode == "adls"
    assert settings.use_unity_catalog
    assert settings.lake_root.startswith("abfss://")
    assert settings.table_id("silver", "medical_claim") == "healthcare_prod.silver.medical_claim"
    assert settings.amount_tolerance == Decimal("0.01")


def test_retry_succeeds_on_second_attempt():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise TimeoutError("transient")
        return "ok"

    assert (
        call_with_retry(
            flaky, attempts=3, backoff_seconds=0, retry_on=(TimeoutError,), sleep_fn=lambda _: None
        )
        == "ok"
    )
    assert calls["n"] == 2


def test_retry_does_not_swallow_unlisted_errors():
    def boom():
        raise ValueError("nope")

    with pytest.raises(ValueError):
        call_with_retry(
            boom, attempts=3, backoff_seconds=0, retry_on=(TimeoutError,), sleep_fn=lambda _: None
        )


def test_secret_reads_environment(monkeypatch):
    monkeypatch.setenv("HC_STORAGE_MODE", "local")
    monkeypatch.setenv("HC_DEMO_SECRET", "from-env")
    settings = load_settings("dev")
    assert get_secret(None, settings, "HC_DEMO_SECRET") == "from-env"
    assert os.environ["HC_DEMO_SECRET"] == "from-env"
