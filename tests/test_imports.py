#!/usr/bin/env python3
"""
Simple test script to verify imports work correctly.
"""

import os
import sys

# Add parent directory to Python path so we can import src modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Set test environment variables
os.environ['DRIVE_FOLDER_ID'] = '1VK3kgR-tS-nkTUSwq8_B-8JYl3zFkXFU'
os.environ['GOOGLE_SERVICE_ACCOUNT_SECRET'] = 'research-data-aggregation/google-service-account'
os.environ['S3_BUCKET'] = 'research-aggregation-test'

def test_imports():
    """Test that all our modules import correctly."""
    print("Testing imports...")
    
    try:
        print("✓ Testing config import...")
        from src.config import config
        print(f"  - Drive folder: {config.drive_folder_id}")
        print(f"  - Service account secret: {config.google_service_account_secret}")
        print(f"  - S3 bucket: {config.s3_bucket}")
        
        print("✓ Testing models import...")
        from src.models import Record, LookupTables, TaxableValue
        
        print("✓ Testing drive client import...")
        from src.drive_client import DriveClient
        
        print("✓ Testing sheets client import...")
        from src.sheets_client import SheetsClient
        
        print("✓ Testing mapper import...")
        from src.mapper import RowMapper
        
        print("✓ Testing worker import...")
        from src.worker import SheetWorker, process_sheets_concurrently
        
        print("✓ Testing orchestrator import...")
        from src.orchestrator import ResearchDataOrchestrator
        
        print("✓ Testing lambda handler import...")
        from src.lambda_handler import lambda_handler
        
        print("\n🎉 All imports successful!")
        return True
        
    except Exception as e:
        print(f"\n❌ Import failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_basic_functionality():
    """Test basic functionality without external dependencies."""
    print("\nTesting basic functionality...")
    
    try:
        from src.models import Record, TaxableValue
        
        # Test Record creation with correct field lengths
        record = Record(
            geocode="US0600000000",  # 12 characters
            tax_auth_id="001",
            group="6030",  # 4 characters minimum
            item="001",
            customer="99", # 2 characters maximum
            provider="99",  # 2 characters maximum
            transaction="01",  # 2 characters maximum
            taxable=TaxableValue.TAXABLE,
            tax_type="01",  # 2 characters maximum
            tax_cat="01",
            effective="20240101",
            per_taxable_type="01",  # 2 characters maximum
            percent_taxable=1.000000
        )
        
        print(f"✓ Created record: {record.geocode}, {record.customer}, {record.percent_taxable}")
        
        # Test CSV output
        csv_row = record.to_csv_row()
        print(f"✓ CSV output: {csv_row[:50]}...")
        
        print("✓ Basic functionality tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Basic functionality test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Research Data Aggregation Service - Test Suite")
    print("=" * 50)
    
    success = True
    success &= test_imports()
    success &= test_basic_functionality()
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 All tests passed! Ready for deployment.")
        sys.exit(0)
    else:
        print("❌ Some tests failed. Please fix issues before deployment.")
        sys.exit(1) 