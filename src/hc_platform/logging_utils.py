"""Structured logging.

The console formatter is plain text so an operator can read a run.
The file formatter is one JSON object per line for log shipping.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        event = getattr(record, "event", None)
        if event is not None:
            payload["event"] = event
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(log_dir: str, run_id: str) -> logging.Logger:
    path = Path(log_dir)
    path.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("hc_platform")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
    logger.addHandler(console)

    file_handler = logging.FileHandler(path / f"pipeline_{run_id}.jsonl", encoding="utf-8")
    file_handler.setFormatter(JsonFormatter())
    logger.addHandler(file_handler)

    latest = logging.FileHandler(path / "pipeline_latest.jsonl", mode="w", encoding="utf-8")
    latest.setFormatter(JsonFormatter())
    logger.addHandler(latest)
    return logger


def log_event(logger: logging.Logger, message: str, **fields: object) -> None:
    logger.info(message, extra={"event": fields})
