"""Landing file parser with Auto Loader-equivalent schema behavior.

Databricks Auto Loader (``cloudFiles``) owns parsing in the notebook path.
This module is the local and CI implementation of the same contract:

* header row, UTF-8 CSV
* schema location that records columns already seen
* ``schemaEvolutionMode = addNewColumns``
* ``_rescued_data`` for rows that cannot be parsed (``##CORRUPT##`` marker
  or a CSV row the parser rejects)
* stable ``ingest_record_id`` from the relative path and ``source_row_num``

Remote ``abfss://`` files are read through Spark's text data source so the
same function works on a Databricks cluster. Local files use ordinary IO.
"""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import SparkSession


@dataclass
class ParsedFile:
    source_file: str
    header: list[str]
    rows: list[dict[str, str | None]]
    new_columns: list[str] = field(default_factory=list)
    physical_data_lines: int = 0


def count_physical_data_lines(path: Path) -> int:
    text = path.read_text(encoding="utf-8")
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return 0
    return len(lines) - 1


def parse_landing_text(text: str) -> tuple[list[str], list[dict[str, str | None]], int]:
    """Parse CSV text into a header, row dicts, and a physical data-line count."""

    lines = text.splitlines()
    content_lines = [line for line in lines if line.strip()]
    if not content_lines:
        return [], [], 0
    header = next(csv.reader([content_lines[0]]))
    rows: list[dict[str, str | None]] = []
    for physical_line in content_lines[1:]:
        if physical_line.startswith("##CORRUPT##"):
            rows.append(
                {
                    "source_row_num": None,
                    "_rescued_data": physical_line,
                    "_is_rescued": "true",
                }
            )
            continue
        try:
            parsed = next(csv.reader([physical_line]))
        except csv.Error as exc:
            rows.append(
                {
                    "source_row_num": None,
                    "_rescued_data": json.dumps({"error": str(exc), "line": physical_line}),
                    "_is_rescued": "true",
                }
            )
            continue
        row: dict[str, str | None] = {}
        for index, column in enumerate(header):
            if index >= len(parsed):
                row[column] = None
                continue
            value = parsed[index].strip()
            row[column] = value if value != "" else None
        extra = parsed[len(header) :]
        if extra:
            row["_rescued_data"] = json.dumps({"extra_values": extra})
        else:
            row["_rescued_data"] = None
        row["_is_rescued"] = "false"
        rows.append(row)
    for offset, row in enumerate(rows, start=1):
        if row.get("source_row_num") is None:
            row["source_row_num"] = str(offset)
    return header, rows, len(content_lines) - 1


def load_text(spark: SparkSession | None, path: str, storage_mode: str) -> str:
    if storage_mode == "local" or path.startswith("file:"):
        local = path[len("file://") :] if path.startswith("file://") else path
        return Path(local).read_text(encoding="utf-8")
    if spark is None:
        raise RuntimeError(f"Spark is required to read remote landing file {path}")
    collected = spark.read.text(path).collect()
    return "\n".join(row[0] for row in collected)


def load_schema(schema_dir: str, entity: str, storage_mode: str) -> list[str]:
    if storage_mode != "local":
        return []
    path = Path(schema_dir) / f"{entity}.json"
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    columns = payload.get("columns", [])
    return [str(column) for column in columns]


def save_schema(
    schema_dir: str,
    entity: str,
    columns: list[str],
    storage_mode: str,
    updated_at: str,
) -> None:
    if storage_mode != "local":
        return
    path = Path(schema_dir)
    path.mkdir(parents=True, exist_ok=True)
    target = path / f"{entity}.json"
    temporary = path / f".{entity}.json.tmp"
    temporary.write_text(
        json.dumps({"entity": entity, "columns": columns, "updated_at": updated_at}, indent=2),
        encoding="utf-8",
    )
    temporary.replace(target)


def ingest_record_id(source_file: str, source_row_num: str) -> str:
    raw = f"{source_file}|{source_row_num}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_landing_file(
    spark: SparkSession | None,
    path: str,
    *,
    source_file: str,
    storage_mode: str,
    known_columns: list[str],
) -> ParsedFile:
    text = load_text(spark, path, storage_mode)
    header, rows, physical = parse_landing_text(text)
    new_columns = [column for column in header if column not in known_columns]
    # A first observation records the header but is not a schema change.
    if not known_columns:
        new_columns = []
    evolved = list(known_columns) if known_columns else list(header)
    for column in header:
        if column not in evolved:
            evolved.append(column)
    normalized: list[dict[str, str | None]] = []
    for row in rows:
        item: dict[str, str | None] = {column: row.get(column) for column in evolved}
        item["_rescued_data"] = row.get("_rescued_data")
        item["_is_rescued"] = row.get("_is_rescued", "false")
        item["source_row_num"] = row.get("source_row_num")
        item["ingest_record_id"] = ingest_record_id(source_file, str(row.get("source_row_num")))
        normalized.append(item)
    return ParsedFile(
        source_file=source_file,
        header=evolved,
        rows=normalized,
        new_columns=new_columns if known_columns else [],
        physical_data_lines=physical,
    )
