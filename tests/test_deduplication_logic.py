"""Tests for the new deduplication logic in mapper.py"""

import pytest
from unittest.mock import Mock, patch
from decimal import Decimal

from src.mapper import RowMapper
from src.models import LookupTables, CustomerType, TaxableValue


class MockConfig:
    """Mock configuration object for testing."""
    def __init__(self):
        self.admin_filter_value = "Tag Level"
        self.effective_date = "1999-01-01"


class TestDeduplicationLogic:
    """Test the new deduplication logic in convert_row_to_records."""
    
    @pytest.fixture
    def mock_lookup_tables(self):
        """Create mock lookup tables."""
        lookup_tables = Mock(spec=LookupTables)
        lookup_tables.get_tax_cat_code = Mock(return_value="05")
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
    
    def test_identical_tax_treatment_creates_only_personal_record(self, row_mapper, config, header_map):
        """Test that identical tax treatment creates only the general (99) customer record."""
        # Row with identical business and personal tax treatment
        row = [
            "1.1.1.1.0.0.0.0",    # current_id
            "TAXABLE",            # business_use  
            "General Merchandise", # business_tax_cat
            "100%",               # business_percent_tax
            "TAXABLE",            # personal_use (same as business)
            "General Merchandise", # personal_tax_cat (same as business)
            "100%"                # personal_percent_tax (same as business)
        ]
        
        geocode = "US0600000000"
        
        with patch.object(row_mapper, '_get_tax_cat_code', return_value="05"):
            business_record, personal_record = row_mapper.convert_row_to_records(
                row, header_map, geocode, config
            )
        
        # Should create only personal (99) record
        assert business_record is None
        assert personal_record is not None
        assert personal_record.customer == CustomerType.PERSONAL.value  # "99"
        assert personal_record.item == "1.1.1.1.0.0.0.0"
        assert personal_record.taxable == TaxableValue.TAXABLE.value
        assert personal_record.tax_cat == "05"
    
    def test_different_tax_treatment_creates_both_records(self, row_mapper, config, header_map):
        """Test that different tax treatment creates both 0B and 99 customer records."""
        # Row with different business and personal tax treatment
        row = [
            "1.2.3.4.0.0.0.0",    # current_id
            "TAXABLE",            # business_use
            "Business Equipment",  # business_tax_cat
            "100%",               # business_percent_tax
            "NOT TAXABLE",        # personal_use (different from business)
            "Personal Items",     # personal_tax_cat (different from business)
            "0%"                  # personal_percent_tax (different from business)
        ]
        
        geocode = "US1200000000"
        
        def mock_tax_cat_code(desc):
            if desc == "Business Equipment":
                return "10"
            elif desc == "Personal Items":
                return "20"
            return "00"
        
        with patch.object(row_mapper, '_get_tax_cat_code', side_effect=mock_tax_cat_code):
            business_record, personal_record = row_mapper.convert_row_to_records(
                row, header_map, geocode, config
            )
        
        # Should create both records
        assert business_record is not None
        assert personal_record is not None
        
        # Verify business record
        assert business_record.customer == CustomerType.BUSINESS.value  # "0B"
        assert business_record.taxable == TaxableValue.TAXABLE.value
        assert business_record.tax_cat == "10"
        assert business_record.percent_taxable == "1.000000"
        
        # Verify personal record
        assert personal_record.customer == CustomerType.PERSONAL.value  # "99"
        assert personal_record.taxable == TaxableValue.NOT_TAXABLE.value
        assert personal_record.tax_cat == "20"
        assert personal_record.percent_taxable == "0.000000"
    
    def test_only_business_valid_creates_business_record(self, row_mapper, config, header_map):
        """Test that only valid business data creates only business record."""
        # Row with only business data valid
        row = [
            "2.1.1.1.0.0.0.0",    # current_id
            "TAXABLE",            # business_use
            "Software",           # business_tax_cat
            "8.75%",              # business_percent_tax
            "TO RESEARCH",        # personal_use (uncertain - will be skipped)
            "Unknown",            # personal_tax_cat
            "50%"                 # personal_percent_tax
        ]
        
        geocode = "US4800000000"
        
        with patch.object(row_mapper, '_get_tax_cat_code', return_value="15"):
            business_record, personal_record = row_mapper.convert_row_to_records(
                row, header_map, geocode, config
            )
        
        # Should create only business record
        assert business_record is not None
        assert personal_record is None
        
        # Verify business record
        assert business_record.customer == CustomerType.BUSINESS.value  # "0B"
        assert business_record.taxable == TaxableValue.TAXABLE.value
        assert business_record.tax_cat == "15"
        assert business_record.percent_taxable == "0.087500"
    
    def test_only_personal_valid_creates_personal_record(self, row_mapper, config, header_map):
        """Test that only valid personal data creates only personal record."""
        # Row with only personal data valid
        row = [
            "3.1.1.1.0.0.0.0",    # current_id
            "DRILL DOWN",         # business_use (uncertain - will be skipped)
            "Research Needed",    # business_tax_cat
            "invalid%",           # business_percent_tax (invalid)
            "EXEMPT",             # personal_use
            "Food Items",         # personal_tax_cat
            "0%"                  # personal_percent_tax
        ]
        
        geocode = "US3600000000"
        
        with patch.object(row_mapper, '_get_tax_cat_code', return_value="30"):
            business_record, personal_record = row_mapper.convert_row_to_records(
                row, header_map, geocode, config
            )
        
        # Should create only personal record
        assert business_record is None
        assert personal_record is not None
        
        # Verify personal record
        assert personal_record.customer == CustomerType.PERSONAL.value  # "99"
        assert personal_record.taxable == TaxableValue.NOT_TAXABLE.value
        assert personal_record.tax_cat == "30"
        assert personal_record.percent_taxable == "0.000000"
    
    def test_neither_valid_creates_no_records(self, row_mapper, config, header_map):
        """Test that invalid data creates no records."""
        # Row with both business and personal data invalid
        row = [
            "4.1.1.1.0.0.0.0",    # current_id
            "TO RESEARCH",        # business_use (uncertain)
            "Unknown",            # business_tax_cat
            "invalid%",           # business_percent_tax (invalid)
            "DRILL DOWN",         # personal_use (uncertain)
            "Research",           # personal_tax_cat
            "bad_percent"         # personal_percent_tax (invalid)
        ]
        
        geocode = "US0600000000"
        
        with patch.object(row_mapper, '_get_tax_cat_code', return_value="00"):
            business_record, personal_record = row_mapper.convert_row_to_records(
                row, header_map, geocode, config
            )
        
        # Should create no records
        assert business_record is None
        assert personal_record is None
    
    def test_no_current_id_creates_no_records(self, row_mapper, config, header_map):
        """Test that missing current_id creates no records."""
        # Row with missing current_id
        row = [
            "",                   # current_id (empty)
            "TAXABLE",            # business_use
            "Test",               # business_tax_cat
            "100%",               # business_percent_tax
            "TAXABLE",            # personal_use
            "Test",               # personal_tax_cat
            "100%"                # personal_percent_tax
        ]
        
        geocode = "US0600000000"
        
        business_record, personal_record = row_mapper.convert_row_to_records(
            row, header_map, geocode, config
        )
        
        # Should create no records
        assert business_record is None
        assert personal_record is None
    
    def test_same_taxable_different_tax_cat_creates_both_records(self, row_mapper, config, header_map):
        """Test that same taxable status but different tax categories creates both records."""
        # Row with same taxable but different tax categories
        row = [
            "5.1.1.1.0.0.0.0",    # current_id
            "TAXABLE",            # business_use
            "Category A",         # business_tax_cat
            "100%",               # business_percent_tax
            "TAXABLE",            # personal_use (same taxable)
            "Category B",         # personal_tax_cat (different category)
            "100%"                # personal_percent_tax (same percent)
        ]
        
        geocode = "US0600000000"
        
        def mock_tax_cat_code(desc):
            if desc == "Category A":
                return "01"
            elif desc == "Category B":
                return "02"
            return "00"
        
        with patch.object(row_mapper, '_get_tax_cat_code', side_effect=mock_tax_cat_code):
            business_record, personal_record = row_mapper.convert_row_to_records(
                row, header_map, geocode, config
            )
        
        # Should create both records due to different tax categories
        assert business_record is not None
        assert personal_record is not None
        assert business_record.tax_cat == "01"
        assert personal_record.tax_cat == "02"
    
    def test_different_taxable_same_tax_cat_creates_both_records(self, row_mapper, config, header_map):
        """Test that different taxable status but same tax category creates both records."""
        # Row with different taxable but same tax category
        row = [
            "6.1.1.1.0.0.0.0",    # current_id
            "TAXABLE",            # business_use
            "Same Category",      # business_tax_cat
            "100%",               # business_percent_tax
            "NOT TAXABLE",        # personal_use (different taxable)
            "Same Category",      # personal_tax_cat (same category)
            "0%"                  # personal_percent_tax
        ]
        
        geocode = "US0600000000"
        
        with patch.object(row_mapper, '_get_tax_cat_code', return_value="05"):
            business_record, personal_record = row_mapper.convert_row_to_records(
                row, header_map, geocode, config
            )
        
        # Should create both records due to different taxable status
        assert business_record is not None
        assert personal_record is not None
        assert business_record.taxable == TaxableValue.TAXABLE.value
        assert personal_record.taxable == TaxableValue.NOT_TAXABLE.value
        assert business_record.tax_cat == "05"
        assert personal_record.tax_cat == "05"
    
    @patch('src.mapper.logger')
    def test_deduplication_logging(self, mock_logger, row_mapper, config, header_map):
        """Test that appropriate debug logging occurs during deduplication."""
        # Row with identical tax treatment
        row = [
            "7.1.1.1.0.0.0.0",    # current_id
            "TAXABLE",            # business_use
            "Same",               # business_tax_cat
            "100%",               # business_percent_tax
            "TAXABLE",            # personal_use
            "Same",               # personal_tax_cat
            "100%"                # personal_percent_tax
        ]
        
        geocode = "US0600000000"
        
        with patch.object(row_mapper, '_get_tax_cat_code', return_value="05"):
            row_mapper.convert_row_to_records(row, header_map, geocode, config)
        
        # Check that debug logging was called for identical treatment
        mock_logger.debug.assert_called_with(
            "Tax treatment identical for 7.1.1.1.0.0.0.0 - creating only general (99) customer record"
        )
    
    @patch('src.mapper.logger')
    def test_different_treatment_logging(self, mock_logger, row_mapper, config, header_map):
        """Test that appropriate debug logging occurs for different tax treatment."""
        # Row with different tax treatment
        row = [
            "8.1.1.1.0.0.0.0",    # current_id
            "TAXABLE",            # business_use
            "Category A",         # business_tax_cat
            "100%",               # business_percent_tax
            "NOT TAXABLE",        # personal_use (different)
            "Category B",         # personal_tax_cat (different)
            "0%"                  # personal_percent_tax (different)
        ]
        
        geocode = "US0600000000"
        
        def mock_tax_cat_code(desc):
            if desc == "Category A":
                return "01"
            elif desc == "Category B":
                return "02"
            return "00"
        
        with patch.object(row_mapper, '_get_tax_cat_code', side_effect=mock_tax_cat_code):
            row_mapper.convert_row_to_records(row, header_map, geocode, config)
        
        # Check that debug logging was called for different treatment
        mock_logger.debug.assert_called_with(
            "Different tax treatment for 8.1.1.1.0.0.0.0 - creating both 0B and 99 customer records"
        )

    def test_same_taxable_and_tax_cat_different_percent_creates_both_records(self, row_mapper, config, header_map):
        """Test that same taxable status and tax category but different percent_taxable creates both records."""
        # Row with same taxable and tax_cat but different percent_taxable
        row = [
            "9.1.1.1.0.0.0.0",    # current_id
            "TAXABLE",            # business_use
            "General Merchandise", # business_tax_cat
            "8.75%",              # business_percent_tax (different rate)
            "TAXABLE",            # personal_use (same taxable)
            "General Merchandise", # personal_tax_cat (same category)
            "0%"                  # personal_percent_tax (different rate)
        ]
        
        geocode = "US0600000000"
        
        with patch.object(row_mapper, '_get_tax_cat_code', return_value="05"):
            business_record, personal_record = row_mapper.convert_row_to_records(
                row, header_map, geocode, config
            )
        
        # Should create both records due to different percent_taxable values
        assert business_record is not None
        assert personal_record is not None
        
        # Verify both records have same taxable and tax_cat but different percent_taxable
        assert business_record.taxable == personal_record.taxable  # Both TAXABLE
        assert business_record.tax_cat == personal_record.tax_cat  # Both "05"
        assert business_record.percent_taxable != personal_record.percent_taxable  # Different percentages
        assert business_record.percent_taxable == "0.087500"  # 8.75%
        assert personal_record.percent_taxable == "0.000000"  # 0%

    def test_all_three_values_identical_creates_only_personal_record(self, row_mapper, config, header_map):
        """Test that identical taxable, tax_cat, AND percent_taxable creates only personal record."""
        # Row with ALL THREE values identical
        row = [
            "10.1.1.1.0.0.0.0",   # current_id
            "TAXABLE",            # business_use
            "Software",           # business_tax_cat
            "8.25%",              # business_percent_tax (same rate)
            "TAXABLE",            # personal_use (same taxable)
            "Software",           # personal_tax_cat (same category)
            "8.25%"               # personal_percent_tax (same rate)
        ]
        
        geocode = "US0600000000"
        
        with patch.object(row_mapper, '_get_tax_cat_code', return_value="10"):
            business_record, personal_record = row_mapper.convert_row_to_records(
                row, header_map, geocode, config
            )
        
        # Should create only personal (99) record since all tax treatment is identical
        assert business_record is None
        assert personal_record is not None
        assert personal_record.customer == CustomerType.PERSONAL.value  # "99"
        assert personal_record.taxable == TaxableValue.TAXABLE.value
        assert personal_record.tax_cat == "10"
        assert personal_record.percent_taxable == "0.082500"  # 8.25%

    def test_business_higher_rate_than_personal_creates_both_records(self, row_mapper, config, header_map):
        """Test business rate higher than personal rate creates both records."""
        # Row where business has higher tax rate than personal
        row = [
            "11.1.1.1.0.0.0.0",   # current_id
            "TAXABLE",            # business_use
            "Equipment",          # business_tax_cat
            "10.5%",              # business_percent_tax (higher rate)
            "TAXABLE",            # personal_use (same taxable)
            "Equipment",          # personal_tax_cat (same category)
            "5.25%"               # personal_percent_tax (lower rate)
        ]
        
        geocode = "US1200000000"
        
        with patch.object(row_mapper, '_get_tax_cat_code', return_value="15"):
            business_record, personal_record = row_mapper.convert_row_to_records(
                row, header_map, geocode, config
            )
        
        # Should create both records due to different rates
        assert business_record is not None
        assert personal_record is not None
        
        # Verify the specific rates
        assert business_record.percent_taxable == "0.105000"  # 10.5%
        assert personal_record.percent_taxable == "0.052500"  # 5.25%
        assert business_record.customer == CustomerType.BUSINESS.value  # "0B"
        assert personal_record.customer == CustomerType.PERSONAL.value  # "99"

    def test_personal_higher_rate_than_business_creates_both_records(self, row_mapper, config, header_map):
        """Test personal rate higher than business rate creates both records."""
        # Row where personal has higher tax rate than business
        row = [
            "12.1.1.1.0.0.0.0",   # current_id
            "TAXABLE",            # business_use
            "Services",           # business_tax_cat
            "2.5%",               # business_percent_tax (lower rate)
            "TAXABLE",            # personal_use (same taxable)
            "Services",           # personal_tax_cat (same category)
            "7.75%"               # personal_percent_tax (higher rate)
        ]
        
        geocode = "US3600000000"
        
        with patch.object(row_mapper, '_get_tax_cat_code', return_value="20"):
            business_record, personal_record = row_mapper.convert_row_to_records(
                row, header_map, geocode, config
            )
        
        # Should create both records due to different rates
        assert business_record is not None
        assert personal_record is not None
        
        # Verify the specific rates
        assert business_record.percent_taxable == "0.025000"  # 2.5%
        assert personal_record.percent_taxable == "0.077500"  # 7.75%


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 