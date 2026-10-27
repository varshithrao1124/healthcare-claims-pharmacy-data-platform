"""Batch checkpoint.

The checkpoint is written only after a batch has been committed to Delta.
A crash before that write causes the batch to be replayed. Bronze merges,
silver merges, quarantine ``replaceWhere``, and SCD2 overwrites are
idempotent, so the replay does not duplicate current-state rows.
"""

from __future__ import annotations

import json
from pathlib import Path


def checkpoint_path(checkpoint_dir: str, storage_mode: str) -> Path | None:
    if storage_mode != "local":
        return None
    return Path(checkpoint_dir) / "ingestion_checkpoint.json"


def load_checkpoint(checkpoint_dir: str, storage_mode: str) -> dict[str, list[str]]:
    path = checkpoint_path(checkpoint_dir, storage_mode)
    if path is None or not path.exists():
        return {"completed_batches": []}
    payload = json.loads(path.read_text(encoding="utf-8"))
    completed = [str(item) for item in payload.get("completed_batches", [])]
    return {"completed_batches": completed}


def save_checkpoint(checkpoint_dir: str, storage_mode: str, completed_batches: list[str]) -> None:
    path = checkpoint_path(checkpoint_dir, storage_mode)
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps({"completed_batches": completed_batches}, indent=2),
        encoding="utf-8",
    )
    temporary.replace(path)
