"""Export real Gold-table charts for docs/images.

Run after `make pipeline`. Images are matplotlib renders of the same frames
the Streamlit dashboard displays.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from dashboard.charts import lake_root, load_frames, money

OUT = ROOT / "docs" / "images"
OUT.mkdir(parents=True, exist_ok=True)


def main() -> None:
    root = lake_root()
    frames = load_frames(root)
    pmpm = frames["fact_pmpm"].copy()
    pmpm["pmpm_total"] = money(pmpm["pmpm_total"])
    denial = frames["fact_denial_summary"].copy()
    denial["denial_rate"] = pd.to_numeric(denial["denial_rate"], errors="coerce").fillna(0)
    drugs = frames["fact_top_drugs"].sort_values("paid_amount", ascending=True)
    drugs["paid_amount"] = money(drugs["paid_amount"])
    gdr = frames["fact_generic_dispensing"].copy()
    gdr["generic_dispensing_rate"] = pd.to_numeric(gdr["generic_dispensing_rate"], errors="coerce")
    variance = frames["fact_reimbursement_variance"].copy()
    variance["variance_amount"] = money(variance["variance_amount"])

    fig, ax = plt.subplots(figsize=(10, 5))
    for lob, part in pmpm.groupby("line_of_business"):
        part = part.sort_values("year_month")
        ax.plot(part["year_month"], part["pmpm_total"], marker="o", label=lob)
    ax.set_title("PMPM by line of business (local Gold Delta)")
    ax.set_ylabel("PMPM ($)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT / "dashboard_pmpm.png", dpi=120)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 4.5))
    months = sorted(denial["year_month"].unique())
    domains = sorted(denial["claim_domain"].unique())
    width = 0.35
    for index, domain in enumerate(domains):
        part = denial[denial["claim_domain"] == domain].set_index("year_month").reindex(months)
        positions = [i + (index - 0.5) * width for i in range(len(months))]
        ax.bar(positions, part["denial_rate"].fillna(0), width=width, label=domain)
    ax.set_xticks(range(len(months)))
    ax.set_xticklabels(months)
    ax.set_ylim(0, 1)
    ax.set_title("Denial rate by domain (local Gold Delta)")
    ax.set_ylabel("Denial rate")
    ax.legend()
    fig.tight_layout()
    fig.savefig(OUT / "dashboard_denials.png", dpi=120)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(drugs["drug_name"].astype(str), drugs["paid_amount"])
    ax.set_title("Top drugs by plan paid (local Gold Delta)")
    ax.set_xlabel("Paid amount ($)")
    fig.tight_layout()
    fig.savefig(OUT / "dashboard_top_drugs.png", dpi=120)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 4))
    gdr = gdr.sort_values("year_month")
    ax.plot(gdr["year_month"], gdr["generic_dispensing_rate"], marker="o", color="#2e7d32")
    ax.set_ylim(0, 1)
    ax.set_title("Generic dispensing rate (local Gold Delta)")
    fig.tight_layout()
    fig.savefig(OUT / "dashboard_gdr.png", dpi=120)
    plt.close(fig)

    summary = (
        variance.groupby("pharmacy_name", as_index=False)["variance_amount"]
        .sum()
        .sort_values("variance_amount")
    )
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh(summary["pharmacy_name"].astype(str), summary["variance_amount"])
    ax.set_title("Reimbursement variance vs contracted price (local Gold Delta)")
    ax.set_xlabel("Variance ($)")
    fig.tight_layout()
    fig.savefig(OUT / "dashboard_variance.png", dpi=120)
    plt.close(fig)
    print(f"Wrote chart images to {OUT}")


if __name__ == "__main__":
    main()
