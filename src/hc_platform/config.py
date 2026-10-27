"""Environment configuration.

Paths switch between the local filesystem and ``abfss://`` ADLS URIs.
Table identities switch between local Delta paths and Unity Catalog
three-level names (``catalog.schema.table``). Values in YAML may use
``${ENV_VAR:-default}`` placeholders.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import yaml

_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def project_root() -> Path:
    here = Path(__file__).resolve()
    for candidate in [here, *here.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate
    return Path.cwd()


def _expand(value: object) -> object:
    if isinstance(value, str):

        def replacer(match: re.Match[str]) -> str:
            env_value = os.environ.get(match.group(1))
            if env_value is not None and env_value != "":
                return env_value
            return match.group(2) if match.group(2) is not None else ""

        return _PLACEHOLDER.sub(replacer, value)
    if isinstance(value, dict):
        return {key: _expand(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand(item) for item in value]
    return value


def join_uri(root: str, *parts: str) -> str:
    """Join a local path or an abfss/dbfs URI."""

    if root.startswith(("abfss://", "dbfs:", "wasbs://")):
        return root.rstrip("/") + "/" + "/".join(parts)
    return str(Path(root).joinpath(*parts))


def _resolve_local(root: Path, value: str) -> str:
    path = Path(value)
    if not path.is_absolute():
        path = root / path
    return str(path)


@dataclass(frozen=True)
class Settings:
    environment: str
    storage_mode: str
    lake_root: str
    landing_dir: str
    checkpoint_dir: str
    schema_dir: str
    log_dir: str
    catalog_mode: str
    catalog_name: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    audit_schema: str
    spark_app_name: str
    spark_master: str
    shuffle_partitions: int
    driver_memory: str
    pipeline_name: str
    amount_tolerance: Decimal
    dq_fail_rate_alert_threshold: float
    retry_attempts: int
    retry_backoff_seconds: float
    scd2_epoch: str
    brand_awp_discount: Decimal
    alert_webhook_url: str | None
    secret_mode: str
    secret_scope: str
    project_root: str

    def schema_for(self, layer: str) -> str:
        return {
            "bronze": self.bronze_schema,
            "silver": self.silver_schema,
            "gold": self.gold_schema,
            "audit": self.audit_schema,
        }[layer]

    def table_id(self, layer: str, table: str) -> str:
        """Unity Catalog name, or a local two-level name used only as a label."""

        return f"{self.catalog_name}.{self.schema_for(layer)}.{table}"

    def table_path(self, layer: str, table: str) -> str:
        return join_uri(self.lake_root, self.schema_for(layer), table)

    @property
    def use_unity_catalog(self) -> bool:
        return self.catalog_mode == "unity"


def load_settings(environment: str = "dev", root: Path | None = None) -> Settings:
    project = root or project_root()
    config_path = project / "conf" / f"{environment}.yml"
    if not config_path.exists():
        raise FileNotFoundError(f"No config for environment {environment!r} at {config_path}")
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    data = _expand(raw)
    assert isinstance(data, dict)

    storage = data["storage"]
    catalog = data["catalog"]
    spark_cfg = data["spark"]
    pipeline = data["pipeline"]
    alerts = data.get("alerts", {})
    secrets = data.get("secrets", {})
    logging_cfg = data.get("logging", {})

    storage_mode = str(storage["mode"]).lower()
    if storage_mode not in {"local", "adls"}:
        raise ValueError(f"storage.mode must be local or adls, got {storage_mode}")
    catalog_mode = str(catalog["mode"]).lower()
    if catalog_mode not in {"local", "unity"}:
        raise ValueError(f"catalog.mode must be local or unity, got {catalog_mode}")

    if storage_mode == "adls":
        lake_root = str(storage["adls_uri"])
        landing = str(storage["landing_adls"])
    else:
        lake_root = _resolve_local(project, str(storage["local_root"]))
        landing = _resolve_local(project, str(storage["landing"]))

    checkpoint_dir = join_uri(lake_root, "_checkpoints")
    schema_dir = join_uri(lake_root, "_schemas")
    log_dir = _resolve_local(project, str(logging_cfg.get("dir", "data/logs")))
    webhook = str(alerts.get("webhook_url") or "") or None

    return Settings(
        environment=str(data["environment"]),
        storage_mode=storage_mode,
        lake_root=lake_root,
        landing_dir=landing,
        checkpoint_dir=checkpoint_dir,
        schema_dir=schema_dir,
        log_dir=log_dir,
        catalog_mode=catalog_mode,
        catalog_name=str(catalog["name"]),
        bronze_schema=str(catalog["bronze_schema"]),
        silver_schema=str(catalog["silver_schema"]),
        gold_schema=str(catalog["gold_schema"]),
        audit_schema=str(catalog["audit_schema"]),
        spark_app_name=str(spark_cfg["app_name"]),
        spark_master=str(spark_cfg["master"]),
        shuffle_partitions=int(spark_cfg["shuffle_partitions"]),
        driver_memory=str(spark_cfg["driver_memory"]),
        pipeline_name=str(pipeline["name"]),
        amount_tolerance=Decimal(str(pipeline["amount_tolerance"])),
        dq_fail_rate_alert_threshold=float(pipeline["dq_fail_rate_alert_threshold"]),
        retry_attempts=int(pipeline["retry_attempts"]),
        retry_backoff_seconds=float(pipeline["retry_backoff_seconds"]),
        scd2_epoch=str(pipeline["scd2_epoch"]),
        brand_awp_discount=Decimal(str(pipeline["brand_awp_discount"])),
        alert_webhook_url=webhook,
        secret_mode=str(secrets.get("mode", "env")),
        secret_scope=str(secrets.get("scope", "hc-kv-dev")),
        project_root=str(project),
    )
