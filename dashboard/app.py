"""Local payer analytics dashboard.

This Streamlit app reads the Gold Delta tables produced by ``make pipeline``.
It is the local stand-in used for screenshots. The production report is the
Power BI semantic model in ``docs/powerbi.md``, which connects to the same
gold star schema.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.charts import lake_root, load_frames, money

st.set_page_config(page_title="Healthcare Claims Analytics (local)", layout="wide")
st.title("Healthcare claims and pharmacy analytics")
st.caption(
    "Local dashboard over Gold Delta tables. Production reporting target is Power BI "
    "on the same star schema (see docs/powerbi.md). All figures are synthetic."
)

try:
    root = lake_root()
    frames = load_frames(root)
except FileNotFoundError as exc:
    st.error(str(exc))
    st.stop()

pmpm = frames["fact_pmpm"].copy()
pmpm["total_paid"] = money(pmpm["total_paid"])
pmpm["pmpm_total"] = money(pmpm["pmpm_total"])
pmpm["pmpm_medical"] = money(pmpm["pmpm_medical"])
pmpm["pmpm_pharmacy"] = money(pmpm["pmpm_pharmacy"])
denial = frames["fact_denial_summary"].copy()
denial["denial_rate"] = pd.to_numeric(denial["denial_rate"], errors="coerce")
drugs = frames["fact_top_drugs"].copy()
drugs["paid_amount"] = money(drugs["paid_amount"])
gdr = frames["fact_generic_dispensing"].copy()
variance = frames["fact_reimbursement_variance"].copy()
variance["variance_amount"] = money(variance["variance_amount"])
prescribing = frames["fact_provider_prescribing"].copy()
prescribing["paid_amount"] = money(prescribing["paid_amount"])
utilization = frames["fact_member_utilization"].copy()
utilization["total_paid"] = money(utilization["total_paid"])

total_paid = float(pmpm["total_paid"].sum())
member_months = int(pd.to_numeric(pmpm["member_months"], errors="coerce").sum())
overall_pmpm = total_paid / member_months if member_months else 0.0
adjudicated = pd.to_numeric(denial["adjudicated_claims"], errors="coerce").sum()
denied = pd.to_numeric(denial["denied_claims"], errors="coerce").sum()
denial_rate = float(denied / adjudicated) if adjudicated else 0.0
generic_fills = pd.to_numeric(gdr["generic_fills"], errors="coerce").sum()
total_fills = pd.to_numeric(gdr["total_fills"], errors="coerce").sum()
gdr_rate = float(generic_fills / total_fills) if total_fills else 0.0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total paid", f"${total_paid:,.2f}")
c2.metric("Overall PMPM", f"${overall_pmpm:,.2f}")
c3.metric("Denial rate", f"{denial_rate:.1%}")
c4.metric("Generic dispensing rate", f"{gdr_rate:.1%}")

page = st.sidebar.radio(
    "Page",
    [
        "PMPM",
        "Denials",
        "Pharmacy",
        "Reimbursement variance",
        "Prescribing",
        "Member utilization",
    ],
)

if page == "PMPM":
    st.subheader("Per member per month allowed-and-paid cost")
    st.plotly_chart(
        px.bar(
            pmpm.sort_values(["year_month", "line_of_business"]),
            x="year_month",
            y="pmpm_total",
            color="line_of_business",
            barmode="group",
            labels={"pmpm_total": "PMPM ($)", "year_month": "Month"},
        ),
        use_container_width=True,
    )
    st.dataframe(pmpm.sort_values(["year_month", "line_of_business"]), use_container_width=True)

elif page == "Denials":
    st.subheader("Claim denial rate by domain")
    st.plotly_chart(
        px.bar(
            denial.sort_values(["year_month", "claim_domain"]),
            x="year_month",
            y="denial_rate",
            color="claim_domain",
            barmode="group",
            labels={"denial_rate": "Denial rate"},
        ),
        use_container_width=True,
    )
    reasons = frames["fact_denial_reason"].copy()
    reasons["denied_claims"] = pd.to_numeric(reasons["denied_claims"], errors="coerce")
    st.plotly_chart(
        px.bar(
            reasons.groupby("denial_reason_code", as_index=False)["denied_claims"].sum(),
            x="denial_reason_code",
            y="denied_claims",
        ),
        use_container_width=True,
    )

elif page == "Pharmacy":
    st.subheader("Top drugs by plan paid amount")
    st.plotly_chart(
        px.bar(
            drugs.sort_values("paid_amount", ascending=False),
            x="paid_amount",
            y="drug_name",
            color="brand_generic_flag",
            orientation="h",
        ),
        use_container_width=True,
    )
    gdr["generic_dispensing_rate"] = pd.to_numeric(gdr["generic_dispensing_rate"], errors="coerce")
    st.plotly_chart(
        px.line(
            gdr.sort_values("year_month"), x="year_month", y="generic_dispensing_rate", markers=True
        ),
        use_container_width=True,
    )

elif page == "Reimbursement variance":
    st.subheader("Pharmacy reimbursement versus contracted price")
    st.caption(
        "Variance = (plan paid + member copay) - (contracted unit price x quantity + dispensing fee)."
    )
    summary = (
        variance.groupby(["pharmacy_name", "network_tier"], as_index=False)["variance_amount"]
        .sum()
        .sort_values("variance_amount")
    )
    st.plotly_chart(
        px.bar(
            summary, x="variance_amount", y="pharmacy_name", color="network_tier", orientation="h"
        ),
        use_container_width=True,
    )

elif page == "Prescribing":
    st.subheader("Prescriber paid amount by specialty")
    specialty = prescribing.groupby("prescriber_specialty", as_index=False)["paid_amount"].sum()
    st.plotly_chart(
        px.bar(specialty, x="prescriber_specialty", y="paid_amount"), use_container_width=True
    )
    st.dataframe(
        prescribing.sort_values("paid_amount", ascending=False).head(25),
        use_container_width=True,
    )

else:
    st.subheader("Member utilization")
    util = utilization.groupby("line_of_business", as_index=False).agg(
        members=("member_id", "nunique"),
        medical_claims=("medical_claims", "sum"),
        pharmacy_claims=("pharmacy_claims", "sum"),
        er_visits=("er_visits", "sum"),
        total_paid=("total_paid", "sum"),
    )
    st.dataframe(util, use_container_width=True)
    st.plotly_chart(px.bar(util, x="line_of_business", y="total_paid"), use_container_width=True)
