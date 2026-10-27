"""Validators for synthetic healthcare identifiers.

NPI check digits follow the CMS Luhn algorithm over the prefix ``80840`` plus
the first nine digits. NDC values are validated as the 11-digit HIPAA format
(no hyphens). These are format and check-digit rules only; the platform never
receives real PHI.
"""

from __future__ import annotations

import re

NPI_PREFIX = "80840"
NPI_RE = re.compile(r"^\d{10}$")
NDC_RE = re.compile(r"^\d{11}$")
MEMBER_RE = re.compile(r"^M\d{8}$")


def luhn_check_digit(payload: str) -> str:
    """Return the Luhn check digit for ``payload``.

    The rightmost payload digit is doubled, which is the correct parity when
    the check digit will be appended to the right.
    """

    total = 0
    for index, character in enumerate(reversed(payload)):
        digit = int(character)
        if index % 2 == 0:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return str((10 - (total % 10)) % 10)


def npi_check_digit(first_nine: str) -> str:
    if not re.fullmatch(r"\d{9}", first_nine):
        raise ValueError(f"NPI body must be 9 digits, got {first_nine!r}")
    return luhn_check_digit(NPI_PREFIX + first_nine)


def is_valid_npi(value: str | None) -> bool:
    if value is None or NPI_RE.fullmatch(value) is None:
        return False
    return npi_check_digit(value[:9]) == value[9]


def is_valid_ndc(value: str | None) -> bool:
    return value is not None and NDC_RE.fullmatch(value) is not None


def is_valid_member_id(value: str | None) -> bool:
    return value is not None and MEMBER_RE.fullmatch(value) is not None
