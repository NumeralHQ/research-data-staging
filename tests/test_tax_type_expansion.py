"""Tests for the new tax type expansion functionality."""

import pytest
from unittest.mock import Mock, patch
from src.mapper import RowMapper
from src.models import LookupTables, CustomerType, TaxableValue


class MockConfig:
    """Mock configuration object for testing."""
    def __init__(self):
        self.admin_filter_value = "Tag Level"
        self.effective_date = "1999-01-01"


class TestTaxTypeExpansion:
    """Test the new tax type expansion feature."""
    
    @pytest.fixture
    def mock_lookup_tables(self):
        """Create mock lookup tables with tax type data."""
        lookup_tables = Mock(spec=LookupTables)
        lookup_tables.get_tax_cat_code = Mock(return_value="05")
        # Mock different tax types for different geocodes
        lookup_tables.get_tax_types_for_geocode_and_tax_cat = Mock(side_effect=lambda geocode, tax_cat: {
            # Different tax types for different geocode+tax_cat combinations
            ("US0600000000", "05"): ["01", "02", "03", "04", "05"],  # 5 tax types for category 05 in California
            ("US0600000000", "03"): ["01", "02"],                    # 2 tax types for category 03 in California
            ("US1200000000", "05"): ["01", "02"],                    # 2 tax types for category 05 in Florida
            ("US1200000000", "03"): ["01"],                          # 1 tax type for category 03 in Florida
            ("US2700000000", "05"): ["01"],                          # 1 tax type for Minnesota
            ("US9999999999", "05"): ["01"]                           # Fallback case
        }.get((geocode, tax_cat), ["01"]))  # Default fallback
        return lookup_tables
    
    @pytest.fixture
    def row_mapper(self, mock_lookup_tables):
        """Create a RowMapper instance with mock dependencies."""
        return RowMapper(mock_lookup_tables)
    
    @pytest.fixture
    def config(self):
        """Create mock config."""
        return MockConfig()
    
    @pytest.fixture
    def header_map(self):
        """Standard header mapping for tests."""
        return {
            'current_id': 0,
            'business_use': 1,
            'business_tax_cat': 2,
            'business_percent_tax': 3,
            'personal_use': 4,
            'personal_tax_cat': 5,
            'personal_percent_tax': 6
        }
    
    def test_identical_treatment_creates_multiple_tax_types(self, row_mapper, config, header_map):
        """Test that identical treatment creates only 99 records but multiplied by tax types."""
        # Row with identical business and personal treatment
        row = [
            "1.1.1.1.0.0.0.0",    # current_id
            "TAXABLE",            # business_use
            "Category",           # business_tax_cat
            "100%",               # business_percent_tax
            "TAXABLE",            # personal_use (same)
            "Category",           # personal_tax_cat (same)
            "100%"                # personal_percent_tax (same)
        ]
        
        geocode = "US0600000000"  # Has 5 tax types: ["01", "02", "03", "04", "05"]
        
        # Get template records first
        business_record, personal_record = row_mapper.convert_row_to_records(
            row, header_map, geocode, config
        )
        
        # Should create only personal template (deduplication logic)
        assert business_record is None
        assert personal_record is not None
        
        # Expand by tax types
        expanded_records = row_mapper._expand_records_by_tax_types(
            [business_record, personal_record], geocode
        )
        
        # Should create 5 records (1 template × 5 tax types)
        assert len(expanded_records) == 5
        
        # All should be personal (99) customer type
        for record in expanded_records:
            assert record.customer == CustomerType.PERSONAL.value  # "99"
            assert record.item == "1.1.1.1.0.0.0.0"
            assert record.taxable == TaxableValue.TAXABLE.value
            assert record.tax_cat == "05"
            assert record.percent_taxable == "1.000000"
        
        # Check that tax_types are correct
        tax_types = [record.tax_type for record in expanded_records]
        assert sorted(tax_types) == ["01", "02", "03", "04", "05"]
    
    def test_different_treatment_creates_multiple_tax_types_for_both(self, row_mapper, config, header_map):
        """Test that different treatment creates both 0B and 99 records multiplied by tax types."""
        # Row with different business and personal treatment
        row = [
            "2.2.2.2.0.0.0.0",    # current_id
            "TAXABLE",            # business_use
            "Category",           # business_tax_cat
            "100%",               # business_percent_tax
            "NOT TAXABLE",        # personal_use (different)
            "Category",           # personal_tax_cat
            "0%"                  # personal_percent_tax (different)
        ]
        
        geocode = "US1200000000"  # Has 2 tax types: ["01", "02"]
        
        # Get template records first
        business_record, personal_record = row_mapper.convert_row_to_records(
            row, header_map, geocode, config
        )
        
        # Should create both templates (different treatment)
        assert business_record is not None
        assert personal_record is not None
        
        # Expand by tax types
        expanded_records = row_mapper._expand_records_by_tax_types(
            [business_record, personal_record], geocode
        )
        
        # Should create 4 records (2 templates × 2 tax types)
        assert len(expanded_records) == 4
        
        # Separate business and personal records
        business_records = [r for r in expanded_records if r.customer == "0B"]
        personal_records = [r for r in expanded_records if r.customer == "99"]
        
        assert len(business_records) == 2  # 2 tax types
        assert len(personal_records) == 2  # 2 tax types
        
        # Check business records
        for record in business_records:
            assert record.customer == CustomerType.BUSINESS.value  # "0B"
            assert record.taxable == TaxableValue.TAXABLE.value
            assert record.percent_taxable == "1.000000"
        
        # Check personal records
        for record in personal_records:
            assert record.customer == CustomerType.PERSONAL.value  # "99"
            assert record.taxable == TaxableValue.NOT_TAXABLE.value
            assert record.percent_taxable == "0.000000"
        
        # Check tax types for both customer types
        business_tax_types = sorted([r.tax_type for r in business_records])
        personal_tax_types = sorted([r.tax_type for r in personal_records])
        
        assert business_tax_types == ["01", "02"]
        assert personal_tax_types == ["01", "02"]
    
    def test_single_tax_type_geocode(self, row_mapper, config, header_map):
        """Test geocode with only one tax type behaves like before."""
        # Row with only business valid
        row = [
            "3.3.3.3.0.0.0.0",    # current_id
            "TAXABLE",            # business_use
            "Category",           # business_tax_cat
            "100%",               # business_percent_tax
            "TO RESEARCH",        # personal_use (uncertain - will be skipped)
            "Category",           # personal_tax_cat
            "50%"                 # personal_percent_tax
        ]
        
        geocode = "US2700000000"  # Has 1 tax type: ["01"]
        
        # Get template records first
        business_record, personal_record = row_mapper.convert_row_to_records(
            row, header_map, geocode, config
        )
        
        # Should create only business template
        assert business_record is not None
        assert personal_record is None
        
        # Expand by tax types
        expanded_records = row_mapper._expand_records_by_tax_types(
            [business_record, personal_record], geocode
        )
        
        # Should create 1 record (1 template × 1 tax type)
        assert len(expanded_records) == 1
        
        record = expanded_records[0]
        assert record.customer == CustomerType.BUSINESS.value  # "0B"
        assert record.tax_type == "01"
        assert record.taxable == TaxableValue.TAXABLE.value
    
    def test_process_sheet_rows_with_tax_type_expansion(self, row_mapper, config):
        """Test that process_sheet_rows correctly uses tax type expansion."""
        header_map = {
            'admin': 0,
            'current_id': 1,
            'business_use': 2,
            'business_tax_cat': 3,
            'business_percent_tax': 4,
            'personal_use': 5,
            'personal_tax_cat': 6,
            'personal_percent_tax': 7
        }
        
        # Two rows: one with identical treatment, one with different treatment
        rows = [
            [
                "Tag Level",          # admin (matches filter)
                "ITEM.1",             # current_id
                "TAXABLE",            # business_use
                "Category",           # business_tax_cat
                "100%",               # business_percent_tax
                "TAXABLE",            # personal_use (same treatment)
                "Category",           # personal_tax_cat
                "100%"                # personal_percent_tax
            ],
            [
                "Tag Level",          # admin (matches filter)
                "ITEM.2",             # current_id
                "TAXABLE",            # business_use
                "Category",           # business_tax_cat
                "100%",               # business_percent_tax
                "NOT TAXABLE",        # personal_use (different treatment)
                "Category",           # personal_tax_cat
                "0%"                  # personal_percent_tax
            ]
        ]
        
        # Mock geocode lookup to return a geocode with 3 tax types
        with patch.object(row_mapper.lookup_tables, 'get_geocode_for_filename', return_value="US0600000000"):
            records, error, processing_errors = row_mapper.process_sheet_rows(
                rows, header_map, "Test File.xlsx", config
            )
        
        assert error is None
        
        # First row: identical treatment = 1 template × 5 tax types = 5 records (all 99)
        # Second row: different treatment = 2 templates × 5 tax types = 10 records (5 0B + 5 99)
        # Total: 5 + 10 = 15 records
        assert len(records) == 15
        
        # Count by customer type
        business_records = [r for r in records if r.customer == "0B"]
        personal_records = [r for r in records if r.customer == "99"]
        
        assert len(business_records) == 5   # Only from second row (different treatment)
        assert len(personal_records) == 10  # From both rows
        
        # Check that all tax types are represented
        all_tax_types = set(r.tax_type for r in records)
        assert all_tax_types == {"01", "02", "03", "04", "05"}
        
        # Check specific items
        item1_records = [r for r in records if r.item == "ITEM.1"]
        item2_records = [r for r in records if r.item == "ITEM.2"]
        
        assert len(item1_records) == 5   # Only personal records (identical treatment)
        assert len(item2_records) == 10  # Both business and personal (different treatment)
    
    def test_fallback_tax_type_for_unknown_geocode(self, row_mapper, config, header_map):
        """Test that unknown geocodes fallback to tax_type '01'."""
        row = [
            "4.4.4.4.0.0.0.0",    # current_id
            "TAXABLE",            # business_use
            "Category",           # business_tax_cat
            "100%",               # business_percent_tax
            "",                   # personal_use (empty)
            "",                   # personal_tax_cat
            ""                    # personal_percent_tax
        ]
        
        unknown_geocode = "US9999999999"  # Not in our mock data
        
        # Get template records first
        business_record, personal_record = row_mapper.convert_row_to_records(
            row, header_map, unknown_geocode, config
        )
        
        # Should create only business template
        assert business_record is not None
        assert personal_record is None
        
        # Expand by tax types (should fallback to ["01"])
        expanded_records = row_mapper._expand_records_by_tax_types(
            [business_record, personal_record], unknown_geocode
        )
        
        # Should create 1 record with fallback tax_type
        assert len(expanded_records) == 1
        
        record = expanded_records[0]
        assert record.tax_type == "01"  # Fallback value
        assert record.customer == CustomerType.BUSINESS.value
    
    def test_no_records_created_returns_empty_list(self, row_mapper, config, header_map):
        """Test that when no records are created, expansion returns empty list."""
        # Row with uncertain values for both business and personal
        row = [
            "5.5.5.5.0.0.0.0",    # current_id
            "TO RESEARCH",        # business_use (uncertain)
            "Category",           # business_tax_cat
            "invalid%",           # business_percent_tax (invalid)
            "DRILL DOWN",         # personal_use (uncertain)
            "Category",           # personal_tax_cat
            "bad_percent"         # personal_percent_tax (invalid)
        ]
        
        geocode = "US0600000000"
        
        # Get template records first
        business_record, personal_record = row_mapper.convert_row_to_records(
            row, header_map, geocode, config
        )
        
        # Should create no templates
        assert business_record is None
        assert personal_record is None
        
        # Expand by tax types
        expanded_records = row_mapper._expand_records_by_tax_types(
            [business_record, personal_record], geocode
        )
        
        # Should create no records
        assert len(expanded_records) == 0

    def test_different_tax_categories_create_different_tax_types(self, row_mapper, config):
        """Test that business and personal records with different tax categories get different tax types."""
        # Mock different tax categories for business vs personal
        row_mapper.lookup_tables.get_tax_cat_code = Mock(side_effect=lambda desc: {
            "Business Category": "05",   # Business gets tax_cat 05
            "Personal Category": "03"    # Personal gets tax_cat 03
        }.get(desc, "00"))
        
        header_map = {
            'current_id': 0,
            'business_use': 1,
            'business_tax_cat': 2,
            'business_percent_tax': 3,
            'personal_use': 4,
            'personal_tax_cat': 5,
            'personal_percent_tax': 6
        }
        
        # Row with different business and personal tax categories
        row = [
            "ITEM.DIFF",           # current_id
            "TAXABLE",            # business_use
            "Business Category",  # business_tax_cat (will map to "05")
            "100%",               # business_percent_tax
            "TAXABLE",            # personal_use 
            "Personal Category",  # personal_tax_cat (will map to "03")
            "100%"                # personal_percent_tax
        ]
        
        geocode = "US0600000000"  # California
        
        # Get template records first
        business_record, personal_record = row_mapper.convert_row_to_records(
            row, header_map, geocode, config, "test_file"
        )
        
        # Should create both templates (same taxable/percent but different tax_cat)
        assert business_record is not None
        assert personal_record is not None
        assert business_record.tax_cat == "05"
        assert personal_record.tax_cat == "03"
        
        # Expand by tax types
        expanded_records = row_mapper._expand_records_by_tax_types(
            [business_record, personal_record], geocode
        )
        
        # Business record (tax_cat=05) should get 5 tax types: ["01", "02", "03", "04", "05"]
        # Personal record (tax_cat=03) should get 2 tax types: ["01", "02"]
        # Total: 5 + 2 = 7 records
        assert len(expanded_records) == 7
        
        # Separate business and personal records
        business_records = [r for r in expanded_records if r.customer == "0B"]
        personal_records = [r for r in expanded_records if r.customer == "99"]
        
        assert len(business_records) == 5  # 5 tax types for tax_cat=05
        assert len(personal_records) == 2  # 2 tax types for tax_cat=03
        
        # Check business tax types (should be all 5)
        business_tax_types = sorted([r.tax_type for r in business_records])
        assert business_tax_types == ["01", "02", "03", "04", "05"]
        
        # Check personal tax types (should be only 2)
        personal_tax_types = sorted([r.tax_type for r in personal_records])
        assert personal_tax_types == ["01", "02"]
        
        # Verify tax_cat values are preserved
        for record in business_records:
            assert record.tax_cat == "05"
        for record in personal_records:
            assert record.tax_cat == "03"


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 