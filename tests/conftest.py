"""Shared Spark session for unit and integration tests."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from hc_platform.config import load_settings
from hc_platform.spark_session import build_spark


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="hc-warehouse-")
    os.environ.setdefault("HC_LOCAL_ROOT", warehouse)
    os.environ.setdefault("HC_LOG_DIR", str(Path(warehouse) / "logs"))
    settings = load_settings("test")
    session = build_spark(settings, warehouse_dir=warehouse)
    yield session
    session.stop()


@pytest.fixture
def local_settings(tmp_path, monkeypatch):
    landing = tmp_path / "landing"
    lake = tmp_path / "lake"
    logs = tmp_path / "logs"
    landing.mkdir()
    monkeypatch.setenv("HC_LANDING", str(landing))
    monkeypatch.setenv("HC_LOCAL_ROOT", str(lake))
    monkeypatch.setenv("HC_LOG_DIR", str(logs))
    settings = load_settings("test")
    return settings
