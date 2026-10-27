from hc_platform.identifiers import is_valid_member_id, is_valid_ndc, is_valid_npi, npi_check_digit


def test_known_npi_check_digit():
    assert npi_check_digit("123456789") == "3"
    assert is_valid_npi("1234567893")


def test_npi_rejects_bad_check_digit_and_length():
    assert not is_valid_npi("1234567890")
    assert not is_valid_npi("123")
    assert not is_valid_npi(None)
    assert not is_valid_npi("")


def test_ndc_and_member_formats():
    assert is_valid_ndc("00069015001")
    assert not is_valid_ndc("00069-0150-01")
    assert not is_valid_ndc(None)
    assert is_valid_member_id("M00000001")
    assert not is_valid_member_id("X00000001")
    assert not is_valid_member_id(None)
