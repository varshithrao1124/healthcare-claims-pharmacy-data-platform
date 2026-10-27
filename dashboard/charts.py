"""Chart frames shared by the Streamlit app and the screenshot exporter.

Both read Gold Delta tables written by the local pipeline. Power BI is the
production surface; these charts are the local stand-in.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from deltalake import DeltaTable


def lake_root(explicit: str | None = None) -> Path:
    if explicit:
        return Path(explicit)
    from hc_platform.config import load_settings

    return Path(load_settings("dev").lake_root)


def read_gold(table: str, root: Path) -> pd.DataFrame:
    path = root / "gold" / table
    if not path.exists():
        raise FileNotFoundError(f"Gold table not found at {path}. Run `make pipeline` first.")
    frame = DeltaTable(str(path)).to_pandas()
    for column in frame.columns:
        if frame[column].dtype == object:
            converted = pd.to_numeric(frame[column], errors="coerce")
            if converted.notna().any():
                frame[column] = converted
    return frame


def money(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0).astype(float)


def load_frames(root: Path) -> dict[str, pd.DataFrame]:
    names = [
        "fact_pmpm",
        "fact_denial_summary",
        "fact_denial_reason",
        "fact_generic_dispensing",
        "fact_top_drugs",
        "fact_reimbursement_variance",
        "fact_provider_prescribing",
        "fact_member_utilization",
        "fact_drug_spend",
    ]
    return {name: read_gold(name, root) for name in names}
