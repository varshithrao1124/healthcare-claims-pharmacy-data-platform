"""Landing-file contracts and medallion table names.

Source columns are the CSV header contract. ``prior_auth_number`` is not part
of the base contract; the 2024-06-06 batch adds it and the bronze layer evolves.
"""

from __future__ import annotations

MEMBER_COLUMNS = [
    "source_row_num",
    "member_id",
    "subscriber_id",
    "first_name",
    "last_name",
    "date_of_birth",
    "gender",
    "plan_id",
    "line_of_business",
    "group_id",
    "coverage_start",
    "coverage_end",
    "state",
    "postal_code",
    "pcp_npi",
]

PROVIDER_COLUMNS = [
    "source_row_num",
    "provider_npi",
    "provider_name",
    "specialty",
    "taxonomy_code",
    "network_status",
    "credential_status",
    "city",
    "state",
    "postal_code",
]

PHARMACY_COLUMNS = [
    "source_row_num",
    "pharmacy_npi",
    "ncpdp_id",
    "pharmacy_name",
    "chain_name",
    "pharmacy_type",
    "network_tier",
    "city",
    "state",
    "postal_code",
]

DRUG_COLUMNS = [
    "source_row_num",
    "ndc",
    "drug_name",
    "generic_name",
    "brand_generic_flag",
    "therapeutic_class",
    "dosage_form",
    "strength",
    "route",
    "manufacturer",
]

DRUG_PRICING_COLUMNS = [
    "source_row_num",
    "ndc",
    "price_type",
    "unit_price",
    "contract_id",
]

PRESCRIPTION_COLUMNS = [
    "source_row_num",
    "rx_id",
    "member_id",
    "prescriber_npi",
    "ndc",
    "written_date",
    "quantity",
    "refills_authorized",
    "days_supply",
    "sig",
]

MEDICAL_CLAIM_COLUMNS = [
    "source_row_num",
    "claim_id",
    "claim_line_number",
    "claim_version",
    "adjustment_type",
    "original_claim_id",
    "member_id",
    "billing_provider_npi",
    "rendering_provider_npi",
    "service_from_date",
    "service_to_date",
    "claim_type",
    "place_of_service",
    "procedure_code",
    "diagnosis_code",
    "billed_amount",
    "allowed_amount",
    "paid_amount",
    "member_responsibility",
    "claim_status",
    "denial_reason_code",
    "received_date",
    "paid_date",
]

PHARMACY_CLAIM_COLUMNS = [
    "source_row_num",
    "claim_id",
    "claim_line_number",
    "claim_version",
    "adjustment_type",
    "original_claim_id",
    "member_id",
    "pharmacy_npi",
    "prescriber_npi",
    "ndc",
    "rx_id",
    "fill_date",
    "days_supply",
    "quantity",
    "daw_code",
    "generic_indicator",
    "ingredient_cost",
    "dispensing_fee",
    "billed_amount",
    "paid_amount",
    "member_copay",
    "claim_status",
    "denial_reason_code",
    "received_date",
    "paid_date",
]

REMITTANCE_COLUMNS = [
    "source_row_num",
    "remittance_id",
    "claim_id",
    "claim_domain",
    "payment_date",
    "paid_amount",
    "adjustment_amount",
    "adjustment_reason_code",
    "payment_method",
    "trace_number",
]

ENTITY_COLUMNS: dict[str, list[str]] = {
    "members": MEMBER_COLUMNS,
    "providers": PROVIDER_COLUMNS,
    "pharmacies": PHARMACY_COLUMNS,
    "drugs": DRUG_COLUMNS,
    "drug_pricing": DRUG_PRICING_COLUMNS,
    "prescriptions": PRESCRIPTION_COLUMNS,
    "medical_claims": MEDICAL_CLAIM_COLUMNS,
    "pharmacy_claims": PHARMACY_CLAIM_COLUMNS,
    "remittances": REMITTANCE_COLUMNS,
}

# Known future column delivered by the schema-change batch. Auto Loader's
# addNewColumns mode (and the local schema registry) must accept it.
EVOLVED_COLUMNS: dict[str, list[str]] = {
    "medical_claims": ["prior_auth_number"],
    "pharmacy_claims": ["prior_auth_number"],
}

DIMENSION_ENTITIES = (
    "members",
    "providers",
    "pharmacies",
    "drugs",
    "drug_pricing",
    "prescriptions",
)
FACT_ENTITIES = ("medical_claims", "pharmacy_claims", "remittances")
ALL_ENTITIES = DIMENSION_ENTITIES + FACT_ENTITIES

SCD2_ENTITIES = ("providers", "pharmacies", "drug_pricing")

BRONZE_TABLES = {
    "members": "member",
    "providers": "provider",
    "pharmacies": "pharmacy",
    "drugs": "drug",
    "drug_pricing": "drug_pricing",
    "prescriptions": "prescription",
    "medical_claims": "medical_claim",
    "pharmacy_claims": "pharmacy_claim",
    "remittances": "remittance",
}

DENIAL_REASONS = [
    ("CO-16", "Claim or service lacks information needed for adjudication"),
    ("CO-197", "Precertification or authorization absent"),
    ("CO-29", "Time limit for filing has expired"),
    ("CO-45", "Charges exceed fee schedule or contracted amount"),
    ("CO-97", "Payment is included in the allowance for another service"),
    ("CO-4", "Procedure code is inconsistent with the modifier or a modifier is missing"),
    ("PR-1", "Deductible amount"),
    ("PR-2", "Coinsurance amount"),
    ("PR-3", "Copay amount"),
    ("NCPDP-70", "Product or service not covered"),
    ("NCPDP-75", "Prior authorization required"),
    ("NCPDP-76", "Plan limitations exceeded"),
    ("NCPDP-79", "Refill too soon"),
    ("NCPDP-88", "DUR reject"),
]
