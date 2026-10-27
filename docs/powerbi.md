# Power BI semantic model

Power BI Desktop and the Power BI service are not available in this environment, and this repo does not contain a `.pbix`. The production model is the star schema below. The local Streamlit app in `dashboard/app.py` reads the same Gold Delta tables and is what the screenshots show.

## Connection

Connect to the Databricks SQL warehouse (or to the local views `gold_<table>` while developing) and import:

- Facts: `v_fact_medical_claim`, `v_fact_pharmacy_claim`, `v_fact_pmpm`, `v_fact_denial_summary`, `v_fact_denial_reason`, `v_fact_generic_dispensing`, `v_fact_top_drugs`, `v_fact_reimbursement_variance`, `v_fact_provider_prescribing`, `v_fact_member_utilization`
- Dimensions: `dim_member`, `dim_provider`, `dim_pharmacy`, `dim_drug`, `dim_drug_pricing`, `dim_date`, `dim_denial_reason`
- History pages can use `dim_provider_history` and `dim_pharmacy_history` on a separate page so the current-flag filter does not hide closed rows

Relationships (single direction, many facts to one dimension):

- `fact_medical_claim.member_id` to `dim_member.member_id`
- `fact_medical_claim.billing_provider_npi` to `dim_provider.provider_npi`
- `fact_pharmacy_claim.member_id` to `dim_member.member_id`
- `fact_pharmacy_claim.pharmacy_npi` to `dim_pharmacy.pharmacy_npi`
- `fact_pharmacy_claim.prescriber_npi` to `dim_provider.provider_npi` (inactive by default; activate for prescribing pages)
- `fact_pharmacy_claim.ndc` to `dim_drug.ndc`
- `fact_denial_reason.denial_reason_code` to `dim_denial_reason.denial_reason_code`
- date relationships from `service_from_date` and `fill_date` to `dim_date.full_date`

Do not relate both medical and pharmacy paid amounts to the same dimension without a measure that specifies the fact table. PMPM is pre-aggregated in `fact_pmpm` because member months are not claim-grain additive.

## DAX measures

```DAX
Total Paid Medical =
SUM ( fact_medical_claim[paid_amount] )

Total Paid Pharmacy =
SUM ( fact_pharmacy_claim[paid_amount] )

Total Paid =
[Total Paid Medical] + [Total Paid Pharmacy]

Member Months =
SUM ( fact_pmpm[member_months] )

PMPM =
DIVIDE ( SUM ( fact_pmpm[total_paid] ), SUM ( fact_pmpm[member_months] ) )

PMPM Medical =
DIVIDE ( SUM ( fact_pmpm[medical_paid] ), SUM ( fact_pmpm[member_months] ) )

PMPM Pharmacy =
DIVIDE ( SUM ( fact_pmpm[pharmacy_paid] ), SUM ( fact_pmpm[member_months] ) )

Denial Rate =
DIVIDE (
    SUM ( fact_denial_summary[denied_claims] ),
    SUM ( fact_denial_summary[adjudicated_claims] )
)

Generic Dispensing Rate =
DIVIDE (
    SUM ( fact_generic_dispensing[generic_fills] ),
    SUM ( fact_generic_dispensing[total_fills] )
)

Reimbursement Variance =
SUM ( fact_reimbursement_variance[variance_amount] )

Reimbursement Variance % =
DIVIDE (
    SUM ( fact_reimbursement_variance[variance_amount] ),
    SUM ( fact_reimbursement_variance[contracted_amount] )
)

Generic Prescribing Rate =
DIVIDE (
    SUM ( fact_provider_prescribing[generic_fills] ),
    SUM ( fact_provider_prescribing[fill_count] )
)
```

## Pages

1. Executive: PMPM by line of business and month, total paid, denial rate, GDR.
2. Medical denials: denial rate trend and denied billed amount by reason code.
3. Pharmacy: top 15 drugs, GDR trend, brand versus generic paid.
4. Network reimbursement: variance by pharmacy and tier. Positive means the plan paid more than the contracted ingredient-plus-fee amount.
5. Prescribers: paid amount and generic rate by specialty.
6. Members: utilization (ER place of service 23, inpatient place of service 21) and total paid. This page is operational, not a member outreach list; the names are synthetic.

Filters: line of business, plan, state, year-month, network tier, brand/generic.

SQL equivalents are in `sql/analytics/payer_queries.sql`.
