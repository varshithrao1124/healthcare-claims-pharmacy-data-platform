"""Print analytics numbers from a finished local lake for the README."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from deltalake import DeltaTable


def frame(root: Path, layer: str, table: str):
    return DeltaTable(str(root / layer / table)).to_pandas()


def main() -> None:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ROOT / "data" / "lake")
    summary_path = Path(sys.argv[2] if len(sys.argv) > 2 else ROOT / "data" / "logs" / "last_run_summary.json")
    if summary_path.exists():
        print(summary_path.read_text(encoding="utf-8"))
    pmpm = frame(root, "gold", "fact_pmpm")
    print("PMPM")
    print(pmpm.sort_values(["year_month", "line_of_business"]).to_string(index=False))
    denial = frame(root, "gold", "fact_denial_summary")
    print("DENIAL")
    print(denial.sort_values(["year_month", "claim_domain"]).to_string(index=False))
    gdr = frame(root, "gold", "fact_generic_dispensing")
    print("GDR")
    print(gdr.to_string(index=False))
    drugs = frame(root, "gold", "fact_top_drugs").sort_values("spend_rank").head(8)
    print("DRUGS")
    print(drugs[["spend_rank", "drug_name", "brand_generic_flag", "paid_amount", "fill_count"]].to_string(index=False))
    reasons = frame(root, "gold", "fact_denial_reason")
    print("REASONS")
    print(reasons.groupby("denial_reason_code")["denied_claims"].sum().sort_values(ascending=False).head(8))
    q = frame(root, "silver", "quarantine")
    print("QUARANTINE distinct", q["ingest_record_id"].nunique(), "rule rows", len(q))
    print(q["rule_id"].value_counts().head(15).to_string())
    providers = frame(root, "silver", "provider")
    changed = providers.groupby("provider_npi").filter(lambda g: len(g) > 1)
    print("SCD2 providers")
    cols = ["provider_npi", "specialty", "network_status", "credential_status", "effective_start", "effective_end", "is_current"]
    print(changed.sort_values(["provider_npi", "effective_start"])[cols].head(20).to_string(index=False))
    pricing = frame(root, "silver", "drug_pricing")
    mac = pricing[pricing["price_type"] == "MAC"]
    multi = mac.groupby("ndc").filter(lambda g: g["contract_id"].nunique() > 1)
    print("MAC changes")
    print(multi.sort_values(["ndc", "effective_start"])[["ndc", "unit_price", "contract_id", "effective_start", "effective_end", "is_current"]].head(20).to_string(index=False))
    medical = frame(root, "silver", "medical_claim")
    print("prior_auth silver", int(medical["prior_auth_number"].notna().sum()), "of", len(medical))
    adjusted = medical[medical["claim_version"] > 1][["claim_id", "claim_line_number", "claim_version", "adjustment_type", "claim_status", "paid_amount"]].head(12)
    print("ADJUSTED")
    print(adjusted.to_string(index=False))
    recon = frame(root, "audit", "reconciliation")
    print("recon", recon["status"].value_counts().to_dict(), "checks", len(recon))
    alerts = frame(root, "audit", "alert")
    print("ALERTS")
    print(alerts[["severity", "alert_code", "message"]].drop_duplicates().to_string(index=False))
    bronze = frame(root, "bronze", "medical_claim")
    print("bronze medical prior_auth by batch")
    print(bronze.groupby("batch_date")["prior_auth_number"].apply(lambda s: int(s.notna().sum())).to_string())


if __name__ == "__main__":
    main()
