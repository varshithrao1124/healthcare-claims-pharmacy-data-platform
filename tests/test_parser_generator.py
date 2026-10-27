import json
from pathlib import Path

from hc_platform.generator.generate import generate_dataset
from hc_platform.ingestion.parser import parse_landing_file, parse_landing_text
from hc_platform.ingestion.tracker import load_checkpoint, save_checkpoint
from hc_platform.schemas import EVOLVED_COLUMNS


def test_corrupt_line_is_rescued_and_schema_column_is_detected(tmp_path):
    text = "claim_id,paid_amount\nMC1,10.00\n##CORRUPT##not,a,row\nMC2,12.00,extra\n"
    header, rows, physical = parse_landing_text(text)
    assert header == ["claim_id", "paid_amount"]
    assert physical == 3
    assert rows[1]["_is_rescued"] == "true"
    assert rows[1]["_rescued_data"].startswith("##CORRUPT##")
    assert rows[2]["_rescued_data"] is not None

    path = tmp_path / "medical_claims.csv"
    evolved = "claim_id,paid_amount,prior_auth_number\nMC1,10.00,PA1\n"
    path.write_text(evolved, encoding="utf-8")
    first = parse_landing_file(
        None,
        str(path),
        source_file="batch_date=2024-06-03/medical_claims.csv",
        storage_mode="local",
        known_columns=[],
    )
    assert first.new_columns == []
    second = parse_landing_file(
        None,
        str(path),
        source_file="batch_date=2024-06-06/medical_claims.csv",
        storage_mode="local",
        known_columns=["claim_id", "paid_amount"],
    )
    assert second.new_columns == ["prior_auth_number"]
    assert second.rows[0]["ingest_record_id"]


def test_checkpoint_round_trip(tmp_path):
    save_checkpoint(str(tmp_path), "local", ["2024-06-03"])
    assert load_checkpoint(str(tmp_path), "local")["completed_batches"] == ["2024-06-03"]
    assert load_checkpoint(str(tmp_path), "adls")["completed_batches"] == []


def test_tiny_generator_writes_four_batches_and_schema_change(tmp_path):
    manifest_path = generate_dataset(tmp_path, scale="tiny", seed=42)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert [batch["batch_date"] for batch in manifest["batches"]] == [
        "2024-06-03",
        "2024-06-04",
        "2024-06-05",
        "2024-06-06",
    ]
    assert manifest["notes"]["schema_change_column"] == "prior_auth_number"
    assert manifest["batches"][2]["corrupt_rows"]["medical_claims"] == 1
    base = Path(tmp_path) / "batch_date=2024-06-03" / "medical_claims.csv"
    evolved = Path(tmp_path) / "batch_date=2024-06-06" / "medical_claims.csv"
    base_header = base.read_text(encoding="utf-8").splitlines()[0].split(",")
    evolved_header = evolved.read_text(encoding="utf-8").splitlines()[0].split(",")
    for column in EVOLVED_COLUMNS["medical_claims"]:
        assert column not in base_header
        assert column in evolved_header
    corrupt = [
        line
        for line in evolved.read_text(encoding="utf-8").splitlines()
        if line.startswith("##CORRUPT##")
    ]
    assert corrupt == []
    bad_batch = (Path(tmp_path) / "batch_date=2024-06-05" / "medical_claims.csv").read_text(
        encoding="utf-8"
    )
    assert "##CORRUPT##" in bad_batch
