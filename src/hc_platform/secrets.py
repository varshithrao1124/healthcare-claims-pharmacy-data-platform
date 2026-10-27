"""Secret lookup.

Local and CI runs read environment variables. Databricks jobs read a Key Vault
backed secret scope (``hc-kv-dev`` / ``hc-kv-prod``) created in the deployment
guide. The scope name is configuration; secret values are never hard-coded.
"""

from __future__ import annotations

import os
from typing import Any

from hc_platform.config import Settings


def get_dbutils(spark: Any) -> Any | None:
    try:
        from pyspark.dbutils import DBUtils
    except ImportError:
        return None
    try:
        return DBUtils(spark)
    except Exception:
        return None


def get_secret(spark: Any, settings: Settings, key: str, default: str | None = None) -> str | None:
    if settings.secret_mode == "databricks":
        dbutils = get_dbutils(spark)
        if dbutils is None:
            raise RuntimeError(
                f"secret mode is databricks but dbutils is unavailable for scope "
                f"{settings.secret_scope}"
            )
        return str(dbutils.secrets.get(settings.secret_scope, key))
    value = os.environ.get(key)
    if value is None or value == "":
        return default
    return value
