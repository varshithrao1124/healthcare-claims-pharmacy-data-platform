"""Deterministic synthetic dataset generator.

Four daily batches are always produced:

* 2024-06-03 baseline snapshot and original claims
* 2024-06-04 attribute changes (SCD2) and new claims
* 2024-06-05 late submissions, duplicates, adjustments, and bad records
* 2024-06-06 schema change (``prior_auth_number``), a reversal, and a stale replay

The seed controls numpy and Faker, so the same scale and seed rewrite the
same files. Landing files are immutable batch partitions; corrections are
modeled as later versions, not in-place edits.
"""

from __future__ import annotations

import csv
import json
import shutil
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
from faker import Faker

from hc_platform.generator.reference_data import (
    BATCH_DATES,
    DRUGS,
    ENCOUNTERS,
    LOCATIONS,
    MEDICAL_DENIALS,
    PHARMACY_DENIALS,
    PHARMACY_PROTOTYPES,
    PLANS,
    SCALES,
    SPECIALTIES,
)
from hc_platform.identifiers import is_valid_member_id, is_valid_ndc, is_valid_npi, npi_check_digit
from hc_platform.schemas import ENTITY_COLUMNS, EVOLVED_COLUMNS


def money2(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    cents = abs(int(cents))
    return f"{sign}{cents // 100}.{cents % 100:02d}"


def money4(cents: int) -> str:
    return f"{Decimal(int(cents)) / Decimal(100):.4f}"


def _npi(rng: np.random.Generator, seen: set[str]) -> str:
    while True:
        first = str(int(rng.integers(1, 3)))
        rest = "".join(str(int(rng.integers(0, 10))) for _ in range(8))
        body = first + rest
        npi = body + npi_check_digit(body)
        if npi not in seen and is_valid_npi(npi):
            seen.add(npi)
            return npi


def _weighted(rng: np.random.Generator, items: list, weights: list[int]):
    pick = float(rng.random()) * sum(weights)
    running = 0.0
    for item, weight in zip(items, weights, strict=True):
        running += weight
        if pick <= running:
            return item
    return items[-1]


@dataclass
class _World:
    rng: np.random.Generator
    scale: dict[str, int]
    members: list[dict] = field(default_factory=list)
    providers: list[dict] = field(default_factory=list)
    pharmacies: list[dict] = field(default_factory=list)
    drugs: list[dict] = field(default_factory=list)
    prices: list[dict] = field(default_factory=list)
    medical_versions: list[dict] = field(default_factory=list)
    pharmacy_versions: list[dict] = field(default_factory=list)
    med_seq: int = 0
    pharm_seq: int = 0
    rx_seq: int = 0
    remit_seq: int = 0
    examples: dict = field(default_factory=dict)
    dq_fail_rows: dict[str, dict[str, int]] = field(default_factory=dict)

    def note_dq(self, batch_date: str, entity: str, count: int) -> None:
        self.dq_fail_rows.setdefault(batch_date, {}).setdefault(entity, 0)
        self.dq_fail_rows[batch_date][entity] += count


def _build_providers(world: _World, fake: Faker) -> None:
    seen: set[str] = set()
    for index in range(world.scale["providers"]):
        specialty, taxonomy = SPECIALTIES[index % len(SPECIALTIES)]
        city, state, postal = LOCATIONS[index % len(LOCATIONS)]
        credential = "MD" if index % 5 else "DO"
        world.providers.append(
            {
                "provider_npi": _npi(world.rng, seen),
                "provider_name": f"{fake.last_name()} {credential}, {specialty.split()[0]}",
                "specialty": specialty,
                "taxonomy_code": taxonomy,
                "network_status": "IN" if index % 5 else "OUT",
                "credential_status": "Active",
                "city": city,
                "state": state,
                "postal_code": postal,
            }
        )


def _build_pharmacies(world: _World) -> None:
    seen: set[str] = set()
    for index in range(world.scale["pharmacies"]):
        name, chain, pharmacy_type, tier = PHARMACY_PROTOTYPES[index % len(PHARMACY_PROTOTYPES)]
        city, state, postal = LOCATIONS[(index * 3) % len(LOCATIONS)]
        suffix = "" if index < len(PHARMACY_PROTOTYPES) else f" {index + 1}"
        world.pharmacies.append(
            {
                "pharmacy_npi": _npi(world.rng, seen),
                "ncpdp_id": f"{1000000 + index:07d}",
                "pharmacy_name": f"{name}{suffix}",
                "chain_name": chain,
                "pharmacy_type": pharmacy_type,
                "network_tier": tier,
                "city": city,
                "state": state,
                "postal_code": postal,
            }
        )


def _build_drugs(world: _World) -> None:
    for record in DRUGS[: world.scale["drugs"]]:
        ndc, drug, generic, flag, klass, form, strength, route, manufacturer, awp = record
        world.drugs.append(
            {
                "ndc": ndc,
                "drug_name": drug,
                "generic_name": generic,
                "brand_generic_flag": flag,
                "therapeutic_class": klass,
                "dosage_form": form,
                "strength": strength,
                "route": route,
                "manufacturer": manufacturer,
                "_awp_cents": awp,
            }
        )
        wac = (awp * 83) // 100
        world.prices.append(
            {
                "ndc": ndc,
                "price_type": "AWP",
                "unit_price": money4(awp),
                "contract_id": "AWP-COMM-2024",
                "_cents": awp,
            }
        )
        world.prices.append(
            {
                "ndc": ndc,
                "price_type": "WAC",
                "unit_price": money4(wac),
                "contract_id": "WAC-COMM-2024",
                "_cents": wac,
            }
        )
        if flag == "G":
            mac = (awp * 55) // 100
            world.prices.append(
                {
                    "ndc": ndc,
                    "price_type": "MAC",
                    "unit_price": money4(mac),
                    "contract_id": "MAC-COMM-2024",
                    "_cents": mac,
                }
            )


def _build_members(
    world: _World, fake: Faker, coverage_start: str, count: int | None = None
) -> None:
    plan_rows = [(plan, lob, group) for plan, lob, group, _weight in PLANS]
    weights = [weight for _plan, _lob, _group, weight in PLANS]
    pcp_pool = [
        provider
        for provider in world.providers
        if provider["specialty"] in {"Family Medicine", "Internal Medicine", "Pediatrics"}
    ] or world.providers
    target = count if count is not None else world.scale["members"]
    start_index = len(world.members) + 1
    for offset in range(target):
        index = start_index + offset
        plan, lob, group = _weighted(world.rng, plan_rows, weights)
        if lob == "Medicare Advantage":
            year = int(world.rng.integers(1942, 1960))
        elif "Pediatrics" in pcp_pool[index % len(pcp_pool)]["specialty"] and lob == "Commercial":
            year = int(world.rng.integers(2012, 2020))
        else:
            year = int(world.rng.integers(1958, 2004))
        month = int(world.rng.integers(1, 13))
        day = int(world.rng.integers(1, 28))
        gender_roll = float(world.rng.random())
        gender = "U" if gender_roll > 0.98 else ("F" if gender_roll > 0.49 else "M")
        city, state, postal = LOCATIONS[index % len(LOCATIONS)]
        pcp = pcp_pool[index % len(pcp_pool)]
        member = {
            "member_id": f"M{index:08d}",
            "subscriber_id": f"S{index:08d}",
            "first_name": fake.first_name_female() if gender == "F" else fake.first_name_male(),
            "last_name": fake.last_name(),
            "date_of_birth": date(year, month, day).isoformat(),
            "gender": gender,
            "plan_id": plan,
            "line_of_business": lob,
            "group_id": group,
            "coverage_start": coverage_start,
            "coverage_end": "2024-12-31",
            "state": state,
            "postal_code": postal,
            "pcp_npi": pcp["provider_npi"],
            "_city": city,
        }
        assert is_valid_member_id(member["member_id"])
        assert is_valid_npi(member["pcp_npi"])
        world.members.append(member)


def _price_cents(world: _World, ndc: str, price_type: str) -> int:
    for row in world.prices:
        if row["ndc"] == ndc and row["price_type"] == price_type:
            return int(row["_cents"])
    raise KeyError((ndc, price_type))


def _drug(world: _World, ndc: str) -> dict:
    for row in world.drugs:
        if row["ndc"] == ndc:
            return row
    raise KeyError(ndc)


def _apply_scd_changes(world: _World, batch_index: int) -> dict[str, int]:
    changes = {"providers": 0, "pharmacies": 0, "drug_pricing": 0}
    if batch_index == 1:
        provider = world.providers[0]
        current = provider["specialty"]
        replacement = next(item[0] for item in SPECIALTIES if item[0] != current)
        taxonomy = next(item[1] for item in SPECIALTIES if item[0] == replacement)
        world.examples["changed_provider_npi"] = provider["provider_npi"]
        world.examples["changed_provider_specialty_from"] = current
        world.examples["changed_provider_specialty_to"] = replacement
        provider["specialty"] = replacement
        provider["taxonomy_code"] = taxonomy
        network = world.providers[1]
        world.examples["network_provider_npi"] = network["provider_npi"]
        world.examples["network_from"] = network["network_status"]
        network["network_status"] = "OUT" if network["network_status"] == "IN" else "IN"
        world.examples["network_to"] = network["network_status"]
        changes["providers"] = 2

        pharmacy = world.pharmacies[0]
        world.examples["changed_pharmacy_npi"] = pharmacy["pharmacy_npi"]
        world.examples["changed_pharmacy_tier_from"] = pharmacy["network_tier"]
        pharmacy["network_tier"] = (
            "Standard" if pharmacy["network_tier"] == "Preferred" else "Preferred"
        )
        world.examples["changed_pharmacy_tier_to"] = pharmacy["network_tier"]
        changes["pharmacies"] = 1

        generic_prices = [row for row in world.prices if row["price_type"] == "MAC"]
        target = generic_prices[0]
        old_cents = int(target["_cents"])
        new_cents = max(old_cents + 1, (old_cents * 107) // 100)
        target["_cents"] = new_cents
        target["unit_price"] = money4(new_cents)
        target["contract_id"] = "MAC-COMM-2024-R1"
        world.examples["changed_ndc"] = target["ndc"]
        world.examples["changed_price_type"] = "MAC"
        world.examples["changed_price_from"] = money4(old_cents)
        world.examples["changed_price_to"] = money4(new_cents)
        changes["drug_pricing"] = 1
    if batch_index == 3:
        provider = world.providers[2]
        provider["credential_status"] = "Suspended"
        world.examples["suspended_provider_npi"] = provider["provider_npi"]
        changes["providers"] += 1
        generic_prices = [row for row in world.prices if row["price_type"] == "MAC"]
        target = generic_prices[1]
        old_cents = int(target["_cents"])
        new_cents = max(old_cents + 1, (old_cents * 104) // 100)
        target["_cents"] = new_cents
        target["unit_price"] = money4(new_cents)
        target["contract_id"] = "MAC-COMM-2024-R2"
        changes["drug_pricing"] += 1
    return changes


def _medical_lines(
    world: _World,
    batch_date: str,
    sequence_index: int,
    *,
    late: bool,
    claim_id: str | None = None,
) -> list[dict]:
    batch = date.fromisoformat(batch_date)
    member = world.members[int(world.rng.integers(0, len(world.members)))]
    provider = world.providers[sequence_index % len(world.providers)]
    encounter = ENCOUNTERS[sequence_index % len(ENCOUNTERS)]
    procedure, diagnosis, claim_type, pos, low, high = encounter
    span = max(1, high - low)
    billed = low + int(world.rng.integers(0, span))
    allowed = (billed * int(world.rng.integers(65, 90))) // 100
    status_slot = sequence_index % 11
    if status_slot == 0:
        status = "Denied"
        paid = 0
        member_resp = 0
        denial = MEDICAL_DENIALS[sequence_index % len(MEDICAL_DENIALS)]
        paid_date = ""
    elif status_slot == 1:
        status = "Pending"
        paid = 0
        member_resp = 0
        denial = ""
        paid_date = ""
    else:
        status = "Paid"
        paid = (allowed * int(world.rng.integers(75, 100))) // 100
        member_resp = allowed - paid
        denial = ""
        paid_date = batch_date
    if late:
        service = batch - timedelta(days=int(world.rng.integers(25, 40)))
    else:
        service = batch - timedelta(days=int(world.rng.integers(1, 12)))
    service_to = service if claim_type == "Professional" else service + timedelta(days=2)
    world.med_seq += 1
    claim_id = claim_id or f"MC{batch_date.replace('-', '')}{world.med_seq:05d}"
    line = {
        "claim_id": claim_id,
        "claim_line_number": "1",
        "claim_version": "1",
        "adjustment_type": "Original",
        "original_claim_id": claim_id,
        "member_id": member["member_id"],
        "billing_provider_npi": provider["provider_npi"],
        "rendering_provider_npi": provider["provider_npi"],
        "service_from_date": service.isoformat(),
        "service_to_date": service_to.isoformat(),
        "claim_type": claim_type,
        "place_of_service": pos,
        "procedure_code": procedure,
        "diagnosis_code": diagnosis,
        "billed_amount": money2(billed),
        "allowed_amount": money2(allowed),
        "paid_amount": money2(paid),
        "member_responsibility": money2(member_resp),
        "claim_status": status,
        "denial_reason_code": denial,
        "received_date": batch_date,
        "paid_date": paid_date,
    }
    lines = [line]
    if sequence_index % 5 == 0:
        lab = deepcopy(line)
        lab["claim_line_number"] = "2"
        lab["procedure_code"] = "80053"
        lab["place_of_service"] = "81"
        lab_billed = 4500 + (sequence_index % 7) * 100
        lab_allowed = (lab_billed * 80) // 100
        if status == "Paid":
            lab_paid = (lab_allowed * 90) // 100
            lab_resp = lab_allowed - lab_paid
        else:
            lab_paid = 0
            lab_resp = 0
        lab["billed_amount"] = money2(lab_billed)
        lab["allowed_amount"] = money2(lab_allowed)
        lab["paid_amount"] = money2(lab_paid)
        lab["member_responsibility"] = money2(lab_resp)
        lines.append(lab)
    world.medical_versions.extend(deepcopy(lines))
    return lines


def _adjust_lines(lines: list[dict], batch_date: str, version: int, kind: str) -> list[dict]:
    adjusted = []
    for line in lines:
        row = deepcopy(line)
        row["claim_version"] = str(version)
        row["received_date"] = batch_date
        row["adjustment_type"] = "Reversal" if kind == "reversal" else "Adjustment"
        if kind == "reversal":
            row["claim_status"] = "Reversed"
            row["paid_amount"] = "0.00"
            row["allowed_amount"] = "0.00"
            row["member_responsibility"] = "0.00"
            row["denial_reason_code"] = ""
            row["paid_date"] = batch_date
        else:
            paid_cents = int(Decimal(line["paid_amount"]) * 100)
            billed_cents = int(Decimal(line["billed_amount"]) * 100)
            new_paid = (paid_cents * 115) // 100 if line["claim_status"] == "Paid" else paid_cents
            new_allowed = max(int(Decimal(line["allowed_amount"]) * 100), new_paid)
            new_billed = max(billed_cents, new_allowed)
            row["claim_status"] = (
                "Adjusted" if line["claim_status"] == "Paid" else line["claim_status"]
            )
            row["paid_amount"] = money2(new_paid)
            row["allowed_amount"] = money2(new_allowed)
            row["billed_amount"] = money2(new_billed)
            row["member_responsibility"] = money2(new_allowed - new_paid)
            row["paid_date"] = batch_date if new_paid else ""
        adjusted.append(row)
    return adjusted


def _pharmacy_claim(
    world: _World, batch_date: str, sequence_index: int, *, late: bool
) -> tuple[dict, dict]:
    batch = date.fromisoformat(batch_date)
    member = world.members[(sequence_index * 3) % len(world.members)]
    pharmacy = world.pharmacies[sequence_index % len(world.pharmacies)]
    prescriber = world.providers[(sequence_index + 1) % len(world.providers)]
    drug = world.drugs[sequence_index % len(world.drugs)]
    if drug["dosage_form"] in {"Inhaler", "Injection", "Spray"}:
        quantity = 1 if drug["therapeutic_class"] != "Specialty Immunology" else 2
        days = 30
    else:
        quantity = 30 if sequence_index % 9 else 90
        days = quantity
    if late:
        fill = batch - timedelta(days=int(world.rng.integers(20, 35)))
    else:
        fill = batch - timedelta(days=int(world.rng.integers(0, 8)))
    awp = _price_cents(world, drug["ndc"], "AWP")
    mac = awp if drug["brand_generic_flag"] == "B" else _price_cents(world, drug["ndc"], "MAC")
    full_awp = sequence_index % 7 == 0 and drug["brand_generic_flag"] == "B"
    if drug["brand_generic_flag"] == "G":
        ingredient = mac * quantity
    elif full_awp:
        ingredient = awp * quantity
    else:
        ingredient = (awp * 84 * quantity) // 100
    dispensing_fee = 200
    billed = ingredient + dispensing_fee
    denied = sequence_index % 12 == 0
    if drug["therapeutic_class"] == "Specialty Immunology":
        copay_cap, copay_share = 15000, 10
    elif drug["brand_generic_flag"] == "B":
        copay_cap, copay_share = 4000, 20
    else:
        copay_cap, copay_share = 1000, 20
    if denied:
        paid = 0
        copay = 0
        status = "Denied"
        denial = PHARMACY_DENIALS[sequence_index % len(PHARMACY_DENIALS)]
        paid_date = ""
    else:
        copay = min(copay_cap, (billed * copay_share) // 100)
        paid = billed - copay
        status = "Paid"
        denial = ""
        paid_date = batch_date
    world.pharm_seq += 1
    world.rx_seq += 1
    claim_id = f"PC{batch_date.replace('-', '')}{world.pharm_seq:05d}"
    rx_id = f"RX{world.rx_seq:08d}"
    claim = {
        "claim_id": claim_id,
        "claim_line_number": "1",
        "claim_version": "1",
        "adjustment_type": "Original",
        "original_claim_id": claim_id,
        "member_id": member["member_id"],
        "pharmacy_npi": pharmacy["pharmacy_npi"],
        "prescriber_npi": prescriber["provider_npi"],
        "ndc": drug["ndc"],
        "rx_id": rx_id,
        "fill_date": fill.isoformat(),
        "days_supply": str(days),
        "quantity": money2(quantity * 100),
        "daw_code": "0" if drug["brand_generic_flag"] == "G" else "1",
        "generic_indicator": "Y" if drug["brand_generic_flag"] == "G" else "N",
        "ingredient_cost": money2(ingredient),
        "dispensing_fee": money2(dispensing_fee),
        "billed_amount": money2(billed),
        "paid_amount": money2(paid),
        "member_copay": money2(copay),
        "claim_status": status,
        "denial_reason_code": denial,
        "received_date": batch_date,
        "paid_date": paid_date,
    }
    prescription = {
        "rx_id": rx_id,
        "member_id": member["member_id"],
        "prescriber_npi": prescriber["provider_npi"],
        "ndc": drug["ndc"],
        "written_date": fill.isoformat(),
        "quantity": claim["quantity"],
        "refills_authorized": "2" if drug["brand_generic_flag"] == "G" else "0",
        "days_supply": str(days),
        "sig": "Take as directed by prescriber",
    }
    assert is_valid_ndc(claim["ndc"])
    assert is_valid_npi(claim["pharmacy_npi"])
    world.pharmacy_versions.append(deepcopy(claim))
    return claim, prescription


def _remittance(world: _World, claim: dict, domain: str, batch_date: str) -> dict | None:
    if claim["claim_status"] not in {"Paid", "Adjusted", "Reversed"}:
        return None
    if claim["claim_line_number"] != "1":
        return None
    # Exact replays (duplicates and stale versions) keep the original received
    # date. They must not create a second remittance.
    if claim["adjustment_type"] == "Original" and claim["received_date"] != batch_date:
        return None
    world.remit_seq += 1
    paid = claim["paid_amount"]
    return {
        "remittance_id": f"RM{world.remit_seq:08d}",
        "claim_id": claim["claim_id"],
        "claim_domain": domain,
        "payment_date": batch_date,
        "paid_amount": paid,
        "adjustment_amount": "0.00" if claim["adjustment_type"] == "Original" else paid,
        "adjustment_reason_code": "" if claim["adjustment_type"] == "Original" else "CO-45",
        "payment_method": "EFT" if world.remit_seq % 4 else "CHK",
        "trace_number": f"TR{batch_date.replace('-', '')}{world.remit_seq:06d}",
    }


def _bad_rows(world: _World, batch_date: str) -> dict[str, list[dict]]:
    """Rows that silver DQ must quarantine. Counts are fixed across scales."""

    good_member = world.members[0]["member_id"]
    good_npi = world.providers[0]["provider_npi"]
    bad_check = good_npi[:-1] + ("0" if good_npi[-1] != "0" else "1")
    assert not is_valid_npi(bad_check)
    medical = [
        _defect_medical(
            batch_date, member_id="", billing_provider_npi=good_npi, rendering_provider_npi=good_npi
        ),
        _defect_medical(
            batch_date,
            member_id="NOT-A-MEMBER",
            billing_provider_npi=good_npi,
            rendering_provider_npi=good_npi,
        ),
        _defect_medical(
            batch_date,
            member_id=good_member,
            billing_provider_npi="12345",
            rendering_provider_npi="12345",
        ),
        _defect_medical(
            batch_date,
            member_id=good_member,
            billing_provider_npi=bad_check,
            rendering_provider_npi=bad_check,
        ),
        _defect_medical(
            batch_date,
            member_id=good_member,
            billing_provider_npi=good_npi,
            rendering_provider_npi=good_npi,
            billed_amount="-25.00",
        ),
        _defect_medical(
            batch_date,
            member_id=good_member,
            billing_provider_npi=good_npi,
            rendering_provider_npi=good_npi,
            service_from_date="2099-01-01",
        ),
        _defect_medical(
            batch_date,
            member_id=good_member,
            billing_provider_npi=good_npi,
            rendering_provider_npi=good_npi,
            claim_id="",
        ),
        _defect_medical(
            batch_date,
            member_id=good_member,
            billing_provider_npi=good_npi,
            rendering_provider_npi=good_npi,
            claim_status="GHOST",
            denial_reason_code="",
        ),
        _defect_medical(
            batch_date,
            member_id="M99999999",
            billing_provider_npi=good_npi,
            rendering_provider_npi=good_npi,
        ),
    ]
    pharmacy = [
        _defect_pharmacy(world, batch_date, ndc="123"),
        _defect_pharmacy(world, batch_date, days_supply="0"),
        _defect_pharmacy(world, batch_date, quantity="-1.00"),
        _defect_pharmacy(world, batch_date, pharmacy_npi="999"),
        _defect_pharmacy(world, batch_date, ndc="99999999999"),
        _defect_pharmacy(world, batch_date, member_id=""),
    ]
    providers = [
        {
            "provider_npi": "ABC",
            "provider_name": "Invalid Provider",
            "specialty": "Family Medicine",
            "taxonomy_code": "207Q00000X",
            "network_status": "IN",
            "credential_status": "Active",
            "city": "Austin",
            "state": "TX",
            "postal_code": "78701",
        },
        {
            "provider_npi": good_npi,
            "provider_name": "",
            "specialty": "Family Medicine",
            "taxonomy_code": "207Q00000X",
            "network_status": "IN",
            "credential_status": "Active",
            "city": "Austin",
            "state": "TX",
            "postal_code": "78701",
        },
    ]
    pharmacies = [
        {
            "pharmacy_npi": "12",
            "ncpdp_id": "0000001",
            "pharmacy_name": "",
            "chain_name": "Bad Chain",
            "pharmacy_type": "Retail",
            "network_tier": "Preferred",
            "city": "Austin",
            "state": "TX",
            "postal_code": "78701",
        }
    ]
    drugs = [
        {
            "ndc": "12",
            "drug_name": "",
            "generic_name": "Bad Drug",
            "brand_generic_flag": "X",
            "therapeutic_class": "Unknown",
            "dosage_form": "Tablet",
            "strength": "1 mg",
            "route": "Oral",
            "manufacturer": "None",
        }
    ]
    pricing = [
        {
            "ndc": "00000000000",
            "price_type": "FAKE",
            "unit_price": "-1.00",
            "contract_id": "BAD",
        }
    ]
    members = [
        {
            "member_id": "",
            "subscriber_id": "S00000000",
            "first_name": "Bad",
            "last_name": "Record",
            "date_of_birth": "not-a-date",
            "gender": "Q",
            "plan_id": "PPO-100",
            "line_of_business": "Commercial",
            "group_id": "GRP-100",
            "coverage_start": "2024-01-01",
            "coverage_end": "2024-12-31",
            "state": "TX",
            "postal_code": "78701",
            "pcp_npi": "1",
        }
    ]
    world.note_dq(batch_date, "medical_claims", len(medical))
    world.note_dq(batch_date, "pharmacy_claims", len(pharmacy))
    world.note_dq(batch_date, "providers", len(providers))
    world.note_dq(batch_date, "pharmacies", len(pharmacies))
    world.note_dq(batch_date, "drugs", len(drugs))
    world.note_dq(batch_date, "drug_pricing", len(pricing))
    world.note_dq(batch_date, "members", len(members))
    return {
        "medical_claims": medical,
        "pharmacy_claims": pharmacy,
        "providers": providers,
        "pharmacies": pharmacies,
        "drugs": drugs,
        "drug_pricing": pricing,
        "members": members,
    }


def _defect_medical(batch_date: str, **overrides: str) -> dict:
    billed = overrides.get("billed_amount", "100.00")
    claim_status = overrides.get("claim_status", "Paid")
    claim_id = overrides.get("claim_id", "MC2099010199999")
    service_from = overrides.get("service_from_date", "2024-06-01")
    row = {
        "claim_id": claim_id,
        "claim_line_number": "1",
        "claim_version": "1",
        "adjustment_type": "Original",
        "original_claim_id": claim_id,
        "member_id": overrides.get("member_id", ""),
        "billing_provider_npi": overrides.get("billing_provider_npi", ""),
        "rendering_provider_npi": overrides.get("rendering_provider_npi", ""),
        "service_from_date": service_from,
        "service_to_date": service_from,
        "claim_type": "Professional",
        "place_of_service": "11",
        "procedure_code": "99213",
        "diagnosis_code": "I10",
        "billed_amount": billed,
        "allowed_amount": "0.00" if str(billed).startswith("-") else billed,
        "paid_amount": "0.00" if claim_status != "Paid" else billed,
        "member_responsibility": "0.00",
        "claim_status": claim_status,
        "denial_reason_code": overrides.get("denial_reason_code", ""),
        "received_date": batch_date,
        "paid_date": "",
    }
    return row


def _defect_pharmacy(world: _World, batch_date: str, **overrides: str) -> dict:
    drug = world.drugs[0]
    row = {
        "claim_id": "PC2099010199999",
        "claim_line_number": "1",
        "claim_version": "1",
        "adjustment_type": "Original",
        "original_claim_id": "PC2099010199999",
        "member_id": world.members[0]["member_id"],
        "pharmacy_npi": world.pharmacies[0]["pharmacy_npi"],
        "prescriber_npi": world.providers[0]["provider_npi"],
        "ndc": drug["ndc"],
        "rx_id": "RX00000000",
        "fill_date": "2024-06-01",
        "days_supply": "30",
        "quantity": "30.00",
        "daw_code": "0",
        "generic_indicator": "Y",
        "ingredient_cost": "10.00",
        "dispensing_fee": "2.00",
        "billed_amount": "12.00",
        "paid_amount": "2.00",
        "member_copay": "10.00",
        "claim_status": "Paid",
        "denial_reason_code": "",
        "received_date": batch_date,
        "paid_date": batch_date,
    }
    row.update(overrides)
    return row


def _public_rows(rows: list[dict], columns: list[str]) -> list[dict]:
    cleaned = []
    for row in rows:
        cleaned.append(
            {
                key: row.get(key, "")
                for key in columns
                if not key.startswith("_") and key != "source_row_num"
            }
        )
    return cleaned


def _write_csv(path: Path, columns: list[str], rows: list[dict], corrupt: bool) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=columns, lineterminator="\n", extrasaction="ignore"
        )
        writer.writeheader()
        for index, row in enumerate(rows, start=1):
            payload = {
                column: "" if row.get(column) is None else str(row.get(column, ""))
                for column in columns
            }
            payload["source_row_num"] = str(index)
            writer.writerow(payload)
        if corrupt:
            handle.write("##CORRUPT## intentionally unparseable row for rescued-data handling\n")
    return len(rows) + (1 if corrupt else 0)


def _versions_for(rows: list[dict], claim_id: str, version: str) -> list[dict]:
    return [row for row in rows if row["claim_id"] == claim_id and row["claim_version"] == version]


def generate_dataset(output_dir: str | Path, scale: str = "small", seed: int = 42) -> Path:
    """Write batch partitions and ``manifest.json``. Returns the manifest path."""

    if scale not in SCALES:
        raise ValueError(f"scale must be one of {sorted(SCALES)}")
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    for child in output.glob("batch_date=*"):
        if child.is_dir():
            shutil.rmtree(child)

    rng = np.random.default_rng(seed)
    fake = Faker("en_US")
    Faker.seed(seed)
    fake.seed_instance(seed)
    world = _World(rng=rng, scale=SCALES[scale])
    _build_providers(world, fake)
    _build_pharmacies(world)
    _build_drugs(world)
    _build_members(world, fake, "2024-01-01")

    manifest_batches = []
    for batch_index, batch_date in enumerate(BATCH_DATES):
        if batch_index == 1:
            _build_members(world, fake, batch_date, count=2 if scale != "medium" else 40)
        changes = _apply_scd_changes(world, batch_index)
        medical_rows: list[dict] = []
        pharmacy_rows: list[dict] = []
        prescriptions: list[dict] = []
        remittances: list[dict] = []
        for index in range(world.scale["medical_per_batch"]):
            medical_rows.extend(
                _medical_lines(world, batch_date, batch_index * 1000 + index, late=False)
            )
        for index in range(world.scale["pharmacy_per_batch"]):
            claim, prescription = _pharmacy_claim(
                world, batch_date, batch_index * 1000 + index, late=False
            )
            pharmacy_rows.append(claim)
            prescriptions.append(prescription)

        late_count = 0
        duplicate_count = 0
        adjustment_count = 0
        reversal_count = 0
        stale_count = 0
        corrupt = {"medical_claims": False, "pharmacy_claims": False}
        extras: dict[str, list[dict]] = {entity: [] for entity in ENTITY_COLUMNS}

        if batch_index == 2:
            late_lines = _medical_lines(world, batch_date, 9000, late=True)
            late_lines_b = _medical_lines(world, batch_date, 9001, late=True)
            medical_rows.extend(late_lines + late_lines_b)
            late_claim, late_rx = _pharmacy_claim(world, batch_date, 9000, late=True)
            pharmacy_rows.append(late_claim)
            prescriptions.append(late_rx)
            late_count = 3
            originals = [
                row
                for row in world.medical_versions
                if row["claim_version"] == "1" and row["claim_status"] == "Paid"
            ]
            claim_ids = []
            for row in originals:
                if row["claim_id"] not in claim_ids:
                    claim_ids.append(row["claim_id"])
                if len(claim_ids) == 3:
                    break
            if len(claim_ids) < 3:
                raise RuntimeError("not enough paid medical claims to build adjustments")
            for claim_id in claim_ids:
                source = _versions_for(world.medical_versions, claim_id, "1")
                adjusted = _adjust_lines(source, batch_date, 2, "adjustment")
                medical_rows.extend(adjusted)
                world.medical_versions.extend(deepcopy(adjusted))
                adjustment_count += 1
            first_line = _versions_for(world.medical_versions, claim_ids[0], "2")[0]
            original_line = _versions_for(world.medical_versions, claim_ids[0], "1")[0]
            world.examples["adjusted_claim_id"] = claim_ids[0]
            world.examples["adjusted_paid"] = first_line["paid_amount"]
            world.examples["adjusted_original_paid"] = original_line["paid_amount"]
            world.examples["stale_claim_id"] = claim_ids[1]
            world.examples["reversed_claim_id"] = claim_ids[2]

            duplicates = [
                row
                for row in world.pharmacy_versions
                if row["claim_version"] == "1" and row["received_date"] == BATCH_DATES[0]
            ][:2]
            pharmacy_rows.extend(deepcopy(duplicates))
            duplicate_count = len(duplicates)
            world.examples["duplicate_pharmacy_claim_id"] = duplicates[0]["claim_id"]
            bad = _bad_rows(world, batch_date)
            for entity, rows in bad.items():
                extras[entity].extend(rows)
            corrupt["medical_claims"] = True
            corrupt["pharmacy_claims"] = True
            world.note_dq(batch_date, "medical_claims", 1)
            world.note_dq(batch_date, "pharmacy_claims", 1)

        if batch_index == 3:
            reversal_id = world.examples["reversed_claim_id"]
            source = _versions_for(world.medical_versions, reversal_id, "2")
            reversed_rows = _adjust_lines(source, batch_date, 3, "reversal")
            medical_rows.extend(reversed_rows)
            world.medical_versions.extend(deepcopy(reversed_rows))
            reversal_count = 1
            stale_id = world.examples["stale_claim_id"]
            stale_rows = deepcopy(_versions_for(world.medical_versions, stale_id, "1"))
            medical_rows.extend(stale_rows)
            stale_count = 1
            for row in medical_rows:
                if row["claim_version"] != "1":
                    continue
                if int(row["claim_line_number"]) % 2 == 1 and row["claim_status"] == "Paid":
                    row["prior_auth_number"] = f"PA{row['claim_id'][-6:]}"
                else:
                    row["prior_auth_number"] = ""
            for row in pharmacy_rows:
                if row["claim_status"] == "Paid" and row["generic_indicator"] == "N":
                    row["prior_auth_number"] = f"PA{row['claim_id'][-6:]}"
                else:
                    row["prior_auth_number"] = ""

        for row in medical_rows:
            remit = _remittance(world, row, "medical", batch_date)
            if remit:
                remittances.append(remit)
        for row in pharmacy_rows:
            remit = _remittance(world, row, "pharmacy", batch_date)
            if remit:
                remittances.append(remit)

        columns = {entity: list(ENTITY_COLUMNS[entity]) for entity in ENTITY_COLUMNS}
        if batch_index == 3:
            for entity, extra in EVOLVED_COLUMNS.items():
                columns[entity] = columns[entity] + extra

        payloads = {
            "members": _public_rows(world.members, columns["members"]) + extras["members"],
            "providers": _public_rows(world.providers, columns["providers"]) + extras["providers"],
            "pharmacies": _public_rows(world.pharmacies, columns["pharmacies"])
            + extras["pharmacies"],
            "drugs": _public_rows(world.drugs, columns["drugs"]) + extras["drugs"],
            "drug_pricing": _public_rows(world.prices, columns["drug_pricing"])
            + extras["drug_pricing"],
            "prescriptions": prescriptions,
            "medical_claims": medical_rows + extras["medical_claims"],
            "pharmacy_claims": pharmacy_rows + extras["pharmacy_claims"],
            "remittances": remittances,
        }
        batch_dir = output / f"batch_date={batch_date}"
        file_counts = {}
        for entity, rows in payloads.items():
            path = batch_dir / f"{entity}.csv"
            file_counts[entity] = _write_csv(
                path,
                columns[entity],
                rows,
                corrupt=corrupt.get(entity, False),
            )
        manifest_batches.append(
            {
                "batch_date": batch_date,
                "schema_extra_columns": EVOLVED_COLUMNS if batch_index == 3 else {},
                "file_rows": file_counts,
                "scd2_attribute_changes": changes,
                "late_arrivals": late_count,
                "duplicate_claim_rows": duplicate_count,
                "adjustments": adjustment_count,
                "reversals": reversal_count,
                "stale_versions": stale_count,
                "intentional_dq_rows": world.dq_fail_rows.get(batch_date, {}),
                "corrupt_rows": {
                    "medical_claims": 1 if corrupt["medical_claims"] else 0,
                    "pharmacy_claims": 1 if corrupt["pharmacy_claims"] else 0,
                },
            }
        )

    manifest = {
        "scale": scale,
        "seed": seed,
        "batches": manifest_batches,
        "examples": world.examples,
        "intentional_dq_rows": world.dq_fail_rows,
        "notes": {
            "phi": "synthetic",
            "schema_change_batch": BATCH_DATES[3],
            "schema_change_column": "prior_auth_number",
        },
    }
    manifest_path = output / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest_path
