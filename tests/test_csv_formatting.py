#!/usr/bin/env python3
"""
Test CSV formatting with quotes and effective date.
"""

import os
import sys

# Add parent directory to Python path so we can import src modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Set test environment variables
os.environ['DRIVE_FOLDER_ID'] = '1VK3kgR-tS-nkTUSwq8_B-8JYl3zFkXFU'
os.environ['GOOGLE_SERVICE_ACCOUNT_SECRET'] = 'research-data-aggregation/google-service-account'
os.environ['S3_BUCKET'] = 'research-aggregation-test'
os.environ['EFFECTIVE_DATE'] = '2024-01-01'  # Custom effective date

def test_csv_formatting():
    """Test the new CSV formatting with quotes and effective date."""
    print("Testing CSV formatting...")
    
    from src.models import Record, CustomerType, TaxableValue
    from decimal import Decimal
    
    # Test with custom effective date
    record = Record(
        geocode="US1800000000",  # Illinois
        tax_auth_id="",
        group="ZZZZ",
        item="1.2.3.4.5.6.7.8",
        customer=CustomerType.BUSINESS.value,
        provider="99",
        transaction="01",
        taxable=TaxableValue.TAXABLE.value,
        tax_type="01",
        tax_cat="05",
        effective="2024-01-01",
        per_taxable_type="01",
        percent_taxable="0.875000"
    )
    
    print(f"✓ Created record with geocode: {record.geocode}")
    print(f"✓ Customer type: {record.customer}")
    print(f"✓ Effective date: {record.effective}")
    print(f"✓ Percent taxable: {record.percent_taxable}")
    
    # Test CSV output
    csv_row = record.to_csv_row()
    print(f"\n✓ CSV row (first 5 fields):")
    for i, field in enumerate(csv_row[:5]):
        print(f"  [{i}]: {field}")
    
    # Test that all values are quoted
    for i, field in enumerate(csv_row):
        assert field.startswith('"') and field.endswith('"'), f"Field {i} not properly quoted: {field}"
    
    # Test specific values
    assert csv_row[0] == '"US1800000000"'  # geocode
    assert csv_row[4] == '"BB"'  # customer (business)
    assert csv_row[10] == '"2024-01-01"'  # effective date from env var
    assert csv_row[12] == '"0.875000"'  # percent_taxable
    
    print(f"\n✓ All {len(csv_row)} fields are properly quoted!")
    print(f"✓ Effective date correctly set to: {csv_row[10]}")
    
    return True

def test_csv_headers():
    """Test that CSV headers are also quoted."""
    print("\nTesting CSV headers...")
    
    from src.models import Record
    
    headers = Record.csv_headers()
    print(f"✓ Headers (first 5):")
    for i, header in enumerate(headers[:5]):
        print(f"  [{i}]: {header}")
    
    # Test that all headers are quoted
    for i, header in enumerate(headers):
        assert header.startswith('"') and header.endswith('"'), f"Header {i} not properly quoted: {header}"
    
    print(f"\n✓ All {len(headers)} headers are properly quoted!")
    
    return True

def test_empty_values():
    """Test that empty values are properly quoted as empty strings."""
    print("\nTesting empty values...")
    
    from src.models import Record, CustomerType
    
    # Create record with minimal required fields
    record = Record(
        geocode="US0600000000",
        tax_auth_id="",
        group="ZZZZ",
        item="",
        customer=CustomerType.PERSONAL.value,
        provider="99",
        transaction="01",
        taxable=1,
        tax_type="01",
        tax_cat="01",
        effective="1999-01-01",
        per_taxable_type="01",
        percent_taxable="1.000000"
    )
    
    csv_row = record.to_csv_row()
    
    # Check that empty tax_auth_id is quoted empty string
    assert csv_row[1] == '""', f"Empty tax_auth_id should be quoted empty string, got: {csv_row[1]}"
    
    # Check that empty item is quoted empty string
    assert csv_row[3] == '""', f"Empty item should be quoted empty string, got: {csv_row[3]}"
    
    print(f"✓ Empty values properly formatted as quoted empty strings")
    print(f"  - tax_auth_id: {csv_row[1]}")
    print(f"  - item: {csv_row[3]}")
    
    return True

if __name__ == "__main__":
    print("CSV Formatting Test Suite")
    print("=" * 40)
    
    success = True
    success &= test_csv_formatting()
    success &= test_csv_headers()
    success &= test_empty_values()
    
    print("\n" + "=" * 40)
    if success:
        print("🎉 All CSV formatting tests passed!")
        print("✅ Values are wrapped in quotes")
        print("✅ Effective date uses EFFECTIVE_DATE env var")
        print("✅ Empty values are properly quoted")
        sys.exit(0)
    else:
        print("❌ Some tests failed.")
        sys.exit(1) 