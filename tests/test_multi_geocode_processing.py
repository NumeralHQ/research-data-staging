"""Tests for multi-geocode record processing functionality."""

import pytest
from unittest.mock import Mock, patch
from src.models import LookupTables, Record, CustomerType, GroupType, ProviderType, TransactionType, TaxType, PerTaxableType
from src.mapper import RowMapper
from src.config import Config


class TestMultiGeocodeProcessing:
    """Test end-to-end record generation for city files with multiple geocodes."""
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration object."""
        config = Mock(spec=Config)
        config.admin_filter_value = "Tag Level"
        config.effective_date = "1999-01-01"
        config.header_row = 4
        config.sheet_name = "Research"
        return config
    
    @pytest.fixture
    def mock_s3_client(self):
        """Mock S3 client for testing."""
        mock_client = Mock()
        return mock_client
    
    @pytest.fixture
    def mock_geo_csv_content(self):
        """Mock geo_state.csv content with multiple geocodes per city."""
        return '''geocode,state,county,city,tax_district,jurisdiction
US1700000000,IL,,,,STATE
US0800000000,CO,,,,STATE
US08013A0025,CO,BOULDER,BOULDER,,CITY
US08031A0002,CO,DENVER,DENVER,,CITY
US17031A0003,IL,COOK,CHICAGO,,CITY
US17031A0047,IL,COOK,CHICAGO,,CITY
US17043A0053,IL,DUPAGE,CHICAGO,,CITY'''
    
    @pytest.fixture
    def mock_tax_type_csv_content(self):
        """Mock unique_tax_type.csv content."""
        return '''geocode,tax_cat,tax_type
US1700000000,01,01
US1700000000,01,02
US0800000000,01,01
US0800000000,01,03
US08013A0025,01,01
US17031A0003,01,47
US17031A0047,01,47
US17043A0053,01,47'''
    
    @pytest.fixture  
    def mock_tax_cat_csv_content(self):
        """Mock tax_cat.csv content."""
        return '''tax_cat,tax_cat_desc
01,General Sales Tax'''
    
    @pytest.fixture
    def lookup_tables(self, mock_s3_client, mock_geo_csv_content, mock_tax_type_csv_content, mock_tax_cat_csv_content):
        """Create LookupTables instance with mocked S3 data."""
        def mock_get_object(Bucket, Key):
            mock_response = {'Body': Mock()}
            if Key == "mapping/geo_state.csv":
                mock_response['Body'].read.return_value = mock_geo_csv_content.encode('utf-8')
            elif Key == "mapping/unique_tax_type.csv":
                mock_response['Body'].read.return_value = mock_tax_type_csv_content.encode('utf-8')
            elif Key == "mapping/tax_cat.csv":
                mock_response['Body'].read.return_value = mock_tax_cat_csv_content.encode('utf-8')
            else:
                raise Exception(f"Unexpected S3 key: {Key}")
            return mock_response
        
        with patch('src.models.boto3.client', return_value=mock_s3_client):
            mock_s3_client.get_object.side_effect = mock_get_object
            
            lookup_tables = LookupTables("test-bucket")
            # Trigger loading of all lookup data
            _ = lookup_tables.geocode_lookup
            _ = lookup_tables.tax_type_lookup  
            _ = lookup_tables.tax_cat_lookup
            return lookup_tables
    
    @pytest.fixture
    def row_mapper(self, lookup_tables):
        """Create RowMapper instance."""
        return RowMapper(lookup_tables)
    
    @pytest.fixture
    def sample_header_map(self):
        """Sample header mapping for tests."""
        return {
            'admin': 10,  # Column K (Admin)
            'current_id': 1,  # Column B (Current ID)
            'business_use': 4,  # Column E (Business Use)
            'business_tax_cat': 5,  # Column F (Business tax_cat)
            'business_percent_tax': 6,  # Column G (Business percent_taxable)
            'personal_use': 7,  # Column H (Personal Use)
            'personal_tax_cat': 8,  # Column I (Personal tax_cat)
            'personal_percent_tax': 9  # Column J (Personal percent_taxable)
        }
    
    @pytest.fixture
    def sample_row_data(self):
        """Sample row data for testing."""
        # Row with business and personal data, admin filter matches
        return [
            "",  # Column A
            "1.1.1.4.3.0.0.0",  # Column B - Current ID
            "L1 Description",  # Column C
            "L2 Description",  # Column D  
            "Taxable",  # Column E - Business Use
            "General Sales Tax",  # Column F - Business tax_cat
            "8.75%",  # Column G - Business percent_taxable
            "Taxable",  # Column H - Personal Use
            "General Sales Tax",  # Column I - Personal tax_cat
            "8.75%",  # Column J - Personal percent_taxable
            "Tag Level"  # Column K - Admin (matches filter)
        ]
    
    def test_state_file_processing(self, row_mapper, sample_header_map, sample_row_data, mock_config):
        """Test processing state-level file returns single geocode records."""
        # State file should return single geocode
        rows = [sample_row_data]
        records, error, processing_errors = row_mapper.process_sheet_rows(
            rows, sample_header_map, "Illinois Sales Tax Research", mock_config
        )
        
        assert error is None
        assert len(records) > 0
        
        # All records should have the same state geocode
        state_geocode = "US1700000000"
        assert all(record.geocode == state_geocode for record in records)
        
        # Should have business and personal records (or just personal if identical treatment)
        unique_customers = {record.customer for record in records}
        assert CustomerType.PERSONAL.value in unique_customers  # "99" should always be present
    
    def test_city_file_single_geocode(self, row_mapper, sample_header_map, sample_row_data, mock_config):
        """Test processing city file with single geocode."""
        # Boulder has only one geocode
        rows = [sample_row_data]
        records, error, processing_errors = row_mapper.process_sheet_rows(
            rows, sample_header_map, "Boulder Sales Tax Research", mock_config
        )
        
        assert error is None
        assert len(records) > 0
        
        # All records should have Boulder's geocode
        boulder_geocode = "US08013A0025"
        assert all(record.geocode == boulder_geocode for record in records)
    
    def test_city_file_multiple_geocodes(self, row_mapper, sample_header_map, sample_row_data, mock_config):
        """Test processing city file with multiple geocodes."""
        # Chicago has 3 geocodes
        rows = [sample_row_data]
        records, error, processing_errors = row_mapper.process_sheet_rows(
            rows, sample_header_map, "Chicago Sales Tax Research", mock_config
        )
        
        assert error is None
        assert len(records) > 0
        
        # Records should be spread across Chicago's 3 geocodes
        chicago_geocodes = {"US17031A0003", "US17031A0047", "US17043A0053"}
        record_geocodes = {record.geocode for record in records}
        assert record_geocodes == chicago_geocodes
        
        # Each geocode should have the same number of records (since same row data)
        geocode_counts = {}
        for record in records:
            geocode_counts[record.geocode] = geocode_counts.get(record.geocode, 0) + 1
        
        # All geocodes should have same count (same source data processed for each)
        counts = list(geocode_counts.values())
        assert all(count == counts[0] for count in counts)
    
    def test_record_multiplication_calculation(self, row_mapper, sample_header_map, sample_row_data, mock_config):
        """Test that record count scales correctly with geocodes and tax types."""
        # Chicago: 3 geocodes, each geocode has 1 tax_type (47) for tax_cat "01"
        # Row creates 1 record template (business and personal identical → collapsed to personal "99")
        # Expected: 1 record template × 3 geocodes × 1 tax_type each = 3 records
        
        rows = [sample_row_data]
        records, error, processing_errors = row_mapper.process_sheet_rows(
            rows, sample_header_map, "Chicago Sales Tax Research", mock_config
        )
        
        assert error is None
        
        # Should have 3 records (1 per geocode, 1 tax_type each)
        assert len(records) == 3
        
        # All records should have tax_type "47" (Chicago-specific)
        assert all(record.tax_type == "47" for record in records)
        
        # All records should be customer "99" (collapsed due to identical treatment)
        assert all(record.customer == CustomerType.PERSONAL.value for record in records)
    
    def test_different_tax_treatment_multiplication(self, row_mapper, sample_header_map, mock_config):
        """Test record multiplication when business and personal have different treatment."""
        # Create row with different business vs personal treatment
        different_treatment_row = [
            "",  # Column A
            "1.1.1.4.3.0.0.0",  # Column B - Current ID
            "L1 Description",  # Column C
            "L2 Description",  # Column D  
            "Taxable",  # Column E - Business Use
            "General Sales Tax",  # Column F - Business tax_cat
            "10.0%",  # Column G - Business percent_taxable (DIFFERENT)
            "Taxable",  # Column H - Personal Use
            "General Sales Tax",  # Column I - Personal tax_cat
            "8.75%",  # Column J - Personal percent_taxable (DIFFERENT)
            "Tag Level"  # Column K - Admin (matches filter)
        ]
        
        rows = [different_treatment_row]
        records, error, processing_errors = row_mapper.process_sheet_rows(
            rows, sample_header_map, "Chicago Sales Tax Research", mock_config
        )
        
        assert error is None
        
        # Should have 6 records: 2 record templates (business + personal) × 3 geocodes × 1 tax_type each
        assert len(records) == 6
        
        # Should have both business and personal customer types
        customer_types = {record.customer for record in records}
        assert CustomerType.BUSINESS.value in customer_types  # "0B"
        assert CustomerType.PERSONAL.value in customer_types  # "99"
        
        # Each geocode should have 2 records (business + personal)
        geocode_counts = {}
        for record in records:
            geocode_counts[record.geocode] = geocode_counts.get(record.geocode, 0) + 1
        
        assert all(count == 2 for count in geocode_counts.values())
    
    def test_tax_type_hierarchy_fallback_in_processing(self, row_mapper, sample_header_map, sample_row_data, mock_config):
        """Test that tax type hierarchy fallback works during record processing."""
        # Denver has no direct tax_type entries, should fall back to Colorado state tax_types
        rows = [sample_row_data]
        records, error, processing_errors = row_mapper.process_sheet_rows(
            rows, sample_header_map, "Denver Sales Tax Research", mock_config
        )
        
        assert error is None
        assert len(records) > 0
        
        # Denver should fall back to Colorado state tax_types: ["01", "03"]
        tax_types = {record.tax_type for record in records}
        assert tax_types == {"01", "03"}
        
        # Should have 2 records (1 template × 1 geocode × 2 tax_types)
        assert len(records) == 2
    
    def test_no_geocode_match_error(self, row_mapper, sample_header_map, sample_row_data, mock_config):
        """Test error handling when no geocodes are found for filename."""
        rows = [sample_row_data]
        records, error, processing_errors = row_mapper.process_sheet_rows(
            rows, sample_header_map, "Unknown City Sales Tax Research", mock_config
        )
        
        # Should return error and empty records
        assert error is not None
        assert "Could not determine geocode(s)" in error
        assert len(records) == 0
    
    def test_tax_type_not_found_exclusion(self, row_mapper, sample_header_map, mock_config):
        """Test that records are excluded when no tax types are found."""
        # Create row with tax_cat that has no match in tax_type lookup
        no_tax_type_row = [
            "",  # Column A
            "1.1.1.4.3.0.0.0",  # Column B - Current ID
            "L1 Description",  # Column C
            "L2 Description",  # Column D  
            "Taxable",  # Column E - Business Use
            "Unknown Tax Cat",  # Column F - Business tax_cat (no match)
            "8.75%",  # Column G - Business percent_taxable
            "Taxable",  # Column H - Personal Use
            "Unknown Tax Cat",  # Column I - Personal tax_cat (no match)
            "8.75%",  # Column J - Personal percent_taxable
            "Tag Level"  # Column K - Admin (matches filter)
        ]
        
        rows = [no_tax_type_row]
        records, error, processing_errors = row_mapper.process_sheet_rows(
            rows, sample_header_map, "Chicago Sales Tax Research", mock_config
        )
        
        assert error is None
        # Should have no records due to exclusion (no tax types found)
        assert len(records) == 0
    
    def test_mixed_rows_processing(self, row_mapper, sample_header_map, mock_config):
        """Test processing multiple rows with different admin filter values."""
        rows = [
            # Row 1: Matches admin filter
            [
                "", "1.1.1.0.0.0.0.0", "L1", "L2", "Taxable", "General Sales Tax", "8.75%",
                "Taxable", "General Sales Tax", "8.75%", "Tag Level"
            ],
            # Row 2: Doesn't match admin filter (should be skipped)
            [
                "", "1.1.2.0.0.0.0.0", "L1", "L2", "Taxable", "General Sales Tax", "8.75%",
                "Taxable", "General Sales Tax", "8.75%", "Other Value"
            ],
            # Row 3: Matches admin filter
            [
                "", "1.1.3.0.0.0.0.0", "L1", "L2", "Taxable", "General Sales Tax", "8.75%",
                "Taxable", "General Sales Tax", "8.75%", "Tag Level"
            ]
        ]
        
        records, error, processing_errors = row_mapper.process_sheet_rows(
            rows, sample_header_map, "Chicago Sales Tax Research", mock_config
        )
        
        assert error is None
        
        # Should process 2 rows (skip the middle one) across 3 geocodes
        # 2 rows × 3 geocodes × 1 tax_type each = 6 records
        assert len(records) == 6
        
        # Check that we have the expected item IDs
        item_ids = {record.item for record in records}
        assert "1.1.1.0.0.0.0.0" in item_ids
        assert "1.1.3.0.0.0.0.0" in item_ids  
        assert "1.1.2.0.0.0.0.0" not in item_ids  # Should be excluded
    
    def test_record_field_population(self, row_mapper, sample_header_map, sample_row_data, mock_config):
        """Test that all record fields are properly populated."""
        rows = [sample_row_data]
        records, error, processing_errors = row_mapper.process_sheet_rows(
            rows, sample_header_map, "Boulder Sales Tax Research", mock_config
        )
        
        assert error is None
        assert len(records) > 0
        
        # Check first record's fields
        record = records[0]
        assert record.geocode == "US08013A0025"  # Boulder geocode
        assert record.tax_auth_id == ""
        assert record.group == GroupType.DEFAULT.value  # "7777"
        assert record.item == "1.1.1.4.3.0.0.0"
        assert record.customer in [CustomerType.BUSINESS.value, CustomerType.PERSONAL.value]
        assert record.provider == ProviderType.DEFAULT.value  # "99"
        assert record.transaction == TransactionType.DEFAULT.value  # "01"
        assert record.taxable in [0, 1]  # Valid taxable values
        assert record.tax_type == "01"  # Boulder's tax_type
        assert record.tax_cat == "01"  # Mapped from "General Sales Tax"
        assert record.effective == "1999-01-01"
        assert record.per_taxable_type == PerTaxableType.DEFAULT.value  # "01"
        assert record.percent_taxable == "0.087500"  # 8.75% converted


if __name__ == "__main__":
    pytest.main([__file__])