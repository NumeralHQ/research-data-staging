"""Tests for the models module."""

import pytest
from decimal import Decimal

from src.models import Record, CustomerType, LookupTables


def test_record_creation():
    """Test creating a valid record."""
    record = Record(
        geocode="US0600000000",
        tax_auth_id="001",
        group="ZZZZ",
        item="1.1.1.1.0.0.0.0",
        customer=CustomerType.BUSINESS.value,
        provider="99",
        transaction="01",
        taxable=1,
        tax_type="01",
        tax_cat="ST",
        effective="20240101",
        per_taxable_type="01",
        percent_taxable=Decimal("1.000000")
    )
    
    assert record.geocode == "US0600000000"
    assert record.customer == "BB"
    assert record.percent_taxable == Decimal("1.000000")


def test_record_csv_output():
    """Test CSV row generation."""
    record = Record(
        geocode="US0100000000",
        item="1.1.1.1.0.0.0.0",
        customer=CustomerType.PERSONAL.value,
        tax_cat="01",
        taxable=1,
        percent_taxable=Decimal("1.000000")
    )
    
    csv_row = record.to_csv_row()
    expected = [
        "US0100000000",  # geocode
        "",              # tax_auth_id
        "ZZZZ",          # group
        "1.1.1.1.0.0.0.0", # item
        "99",            # customer
        "99",            # provider
        "01",            # transaction
        "1",             # taxable
        "01",            # tax_type
        "01",            # tax_cat
        "",              # effective
        "01",            # per_taxable_type
        "1.000000"       # percent_taxable
    ]
    
    assert csv_row == expected


def test_csv_headers():
    """Test CSV headers are in correct order."""
    headers = Record.csv_headers()
    expected = [
        "geocode",
        "tax_auth_id", 
        "group",
        "item",
        "customer",
        "provider",
        "transaction",
        "taxable",
        "tax_type",
        "tax_cat",
        "effective",
        "per_taxable_type",
        "percent_taxable"
    ]
    
    assert headers == expected


if __name__ == "__main__":
    pytest.main([__file__]) 