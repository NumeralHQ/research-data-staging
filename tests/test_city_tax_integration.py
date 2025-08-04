"""Integration tests for city-level tax treatment processing."""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from src.models import Record, LookupTables, GroupType, CustomerType, ProviderType, TransactionType, TaxType, PerTaxableType
from src.orchestrator import ResearchDataOrchestrator


class TestCityTaxIntegration:
    """Test end-to-end integration of city-level tax treatment replication."""
    
    @pytest.fixture
    def mock_s3_client(self):
        """Mock S3 client for testing."""
        return Mock()
    
    @pytest.fixture
    def mock_lookup_tables(self, mock_s3_client):
        """Mock LookupTables with parent geocode construction."""
        lookup_tables = Mock(spec=LookupTables)
        
        # Mock parent geocode construction
        def construct_parent(geocode):
            return geocode[:4] + "00000000"
        
        lookup_tables._construct_parent_geocode = Mock(side_effect=construct_parent)
        
        # Mock product code mapper
        lookup_tables.product_code_mapper = Mock()
        lookup_tables.product_code_mapper.convert_research_id = Mock(return_value="001")
        lookup_tables.product_code_mapper.get_unmapped_ids = Mock(return_value=[])
        
        return lookup_tables
    
    @pytest.fixture
    def mock_orchestrator(self, mock_lookup_tables):
        """Create orchestrator with mocked dependencies."""
        with patch('src.orchestrator.boto3.client'), \
             patch('src.orchestrator.DriveClient'), \
             patch('src.orchestrator.SheetsClient'), \
             patch('src.orchestrator.RowMapper'):
            
            orchestrator = ResearchDataOrchestrator()
            orchestrator.lookup_tables = mock_lookup_tables
            orchestrator.s3_client = Mock()
            
            # Mock CSV generation methods
            orchestrator._create_csv_content = Mock(return_value="test,csv,content")
            orchestrator._upload_csv_to_s3 = AsyncMock(return_value="test-csv-key")
            orchestrator._create_product_item_csv_content = Mock(return_value="test,item,csv")
            orchestrator._upload_product_item_csv_to_s3 = AsyncMock(return_value="test-item-key")
            orchestrator._upload_static_files_to_s3 = AsyncMock(return_value=["static-key"])
            orchestrator._upload_errors_to_s3 = AsyncMock(return_value="errors-key")
            
            return orchestrator
    
    @pytest.fixture
    def sample_mixed_records(self):
        """Create sample records with both state and city data."""
        return [
            # Illinois state records
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "01", "01", "2024-01-01", "01", "1.000000"),
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "02", "01", "2024-01-01", "01", "1.000000"),
            Record("US1700000000", "", "7777", "001", "0B", "99", "01", 1, "01", "02", "2024-01-01", "01", "1.000000"),
            Record("US1700000000", "", "7777", "002", "99", "99", "01", 1, "01", "01", "2024-01-01", "01", "1.000000"),
            
            # Colorado state records  
            Record("US0800000000", "", "7777", "001", "99", "99", "01", 1, "03", "01", "2024-01-01", "01", "1.000000"),
            Record("US0800000000", "", "7777", "001", "0B", "99", "01", 1, "03", "02", "2024-01-01", "01", "1.000000"),
            
            # Chicago city records (parent: US1700000000)
            Record("US17031A0003", "", "7777", "001", "99", "99", "01", 1, "47", "01", "2024-01-01", "01", "1.000000"),
            Record("US17031A0003", "", "7777", "002", "99", "99", "01", 1, "47", "01", "2024-01-01", "01", "1.000000"),
            
            # Boulder city records (parent: US0800000000)
            Record("US08013A0025", "", "7777", "001", "99", "99", "01", 1, "04", "01", "2024-01-01", "01", "1.000000"),
        ]
    
    def test_end_to_end_replication_flow(self, mock_orchestrator, sample_mixed_records):
        """Test complete flow of state treatment replication to cities."""
        enhanced_records, error_messages = mock_orchestrator._replicate_state_tax_treatments_to_cities(sample_mixed_records)
        
        # Should have original records plus replicated treatments
        assert len(enhanced_records) > len(sample_mixed_records)
        
        # Count original vs enhanced
        original_count = len(sample_mixed_records)
        replicated_count = len(enhanced_records) - original_count
        
        # Should have added replicated records for city treatments
        assert replicated_count > 0
        
        # Verify city records got additional treatments
        chicago_records = [r for r in enhanced_records if r.geocode == "US17031A0003"]
        boulder_records = [r for r in enhanced_records if r.geocode == "US08013A0025"]
        
        # Chicago should have original city records + replicated state treatments
        # Original: 2 city records
        # Replicated: matching state records with different tax_type/tax_cat
        assert len(chicago_records) > 2
        
        # Boulder should have original city record + replicated state treatments  
        assert len(boulder_records) > 1
    
    def test_composite_key_filtering_integration(self, mock_orchestrator):
        """Test that composite key filtering works correctly in integration."""
        records = [
            # State records
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "01", "01", "2024-01-01", "01", "1.000000"),  # Different tax_cat ✓
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "02", "02", "2024-01-01", "01", "1.000000"),  # Different tax_type ✓
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "47", "01", "2024-01-01", "01", "1.000000"),  # Exact match ❌
            
            # City record
            Record("US17031A0003", "", "7777", "001", "99", "99", "01", 1, "47", "01", "2024-01-01", "01", "1.000000"),
        ]
        
        enhanced_records, error_messages = mock_orchestrator._replicate_state_tax_treatments_to_cities(records)
        
        # Should have original 4 records + 2 replicated (exclude exact match)
        assert len(enhanced_records) == 6
        
        # Check that exact composite key match was not replicated
        chicago_records = [r for r in enhanced_records if r.geocode == "US17031A0003"]
        
        # Should have original city record + 2 replicated (not the exact match)
        assert len(chicago_records) == 3
        
        # Verify exact match is not duplicated
        exact_matches = [r for r in chicago_records if r.tax_type == "47" and r.tax_cat == "01"]
        assert len(exact_matches) == 1  # Only the original city record
    
    def test_multiple_city_geocodes_processing(self, mock_orchestrator):
        """Test processing multiple city geocodes correctly."""
        records = [
            # State records for two different states
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "01", "01", "2024-01-01", "01", "1.000000"),
            Record("US0800000000", "", "7777", "001", "99", "99", "01", 1, "03", "01", "2024-01-01", "01", "1.000000"),
            
            # City records with different parent states
            Record("US17031A0003", "", "7777", "001", "99", "99", "01", 1, "47", "01", "2024-01-01", "01", "1.000000"),  # IL city
            Record("US08013A0025", "", "7777", "001", "99", "99", "01", 1, "04", "01", "2024-01-01", "01", "1.000000"),  # CO city
        ]
        
        enhanced_records, error_messages = mock_orchestrator._replicate_state_tax_treatments_to_cities(records)
        
        # Each city should get treatments from its parent state only
        chicago_records = [r for r in enhanced_records if r.geocode == "US17031A0003"]
        boulder_records = [r for r in enhanced_records if r.geocode == "US08013A0025"]
        
        # Chicago should get IL state treatment (tax_type=01)
        chicago_tax_types = {r.tax_type for r in chicago_records}
        assert "01" in chicago_tax_types  # From IL state
        assert "03" not in chicago_tax_types  # Should not get CO state treatments
        
        # Boulder should get CO state treatment (tax_type=03)  
        boulder_tax_types = {r.tax_type for r in boulder_records}
        assert "03" in boulder_tax_types  # From CO state
        assert "01" not in boulder_tax_types  # Should not get IL state treatments
    
    def test_no_matching_treatments_error_generation(self, mock_orchestrator):
        """Test error generation when cities have no matching state treatments."""
        records = [
            # State record with no matching city treatments
            Record("US1700000000", "", "7777", "999", "0B", "99", "01", 1, "01", "01", "2024-01-01", "01", "1.000000"),
            
            # City record with no matching state treatments
            Record("US17031A0003", "", "7777", "001", "99", "99", "01", 1, "47", "01", "2024-01-01", "01", "1.000000"),
        ]
        
        enhanced_records, error_messages = mock_orchestrator._replicate_state_tax_treatments_to_cities(records)
        
        # Should return original records (no replication possible)
        assert len(enhanced_records) == 2
        
        # Should generate error for city without matching treatments
        assert len(error_messages) == 1
        assert "US17031A0003" in error_messages[0]
        assert "no matching state-level tax treatments" in error_messages[0]
        assert "US1700000000" in error_messages[0]  # Parent geocode
    
    def test_record_count_scaling(self, mock_orchestrator):
        """Test that record counts scale correctly with replication."""
        records = [
            # 3 state treatments for same product
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "01", "01", "2024-01-01", "01", "1.000000"),
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "02", "01", "2024-01-01", "01", "1.000000"),
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "03", "01", "2024-01-01", "01", "1.000000"),
            
            # 1 city treatment for same product (different from all state treatments)
            Record("US17031A0003", "", "7777", "001", "99", "99", "01", 1, "47", "01", "2024-01-01", "01", "1.000000"),
        ]
        
        enhanced_records, error_messages = mock_orchestrator._replicate_state_tax_treatments_to_cities(records)
        
        # Should have: 4 original + 3 replicated = 7 total
        # (All 3 state treatments replicated to city since none match city's tax_type=47)
        assert len(enhanced_records) == 7
        
        # City should now have 4 treatments total (1 original + 3 replicated)
        city_records = [r for r in enhanced_records if r.geocode == "US17031A0003"]
        assert len(city_records) == 4
        
        # Verify tax types
        city_tax_types = {r.tax_type for r in city_records}
        assert city_tax_types == {"01", "02", "03", "47"}
    
    def test_integration_with_product_code_conversion(self, mock_orchestrator, sample_mixed_records):
        """Test that replicated records work correctly with product code conversion.""" 
        # Mock the filtering methods to verify they're called with enhanced records
        original_filter_method = mock_orchestrator._filter_and_convert_record_research_ids
        mock_orchestrator._filter_and_convert_record_research_ids = Mock(return_value=sample_mixed_records[:2])
        
        # Replicate treatments
        enhanced_records, error_messages = mock_orchestrator._replicate_state_tax_treatments_to_cities(sample_mixed_records)
        
        # Simulate calling the filtering with enhanced records
        filtered_records = mock_orchestrator._filter_and_convert_record_research_ids(enhanced_records)
        
        # Verify filtering was called with enhanced record set (original + replicated)
        assert len(enhanced_records) > len(sample_mixed_records)
        mock_orchestrator._filter_and_convert_record_research_ids.assert_called_once_with(enhanced_records)
    
    def test_error_aggregation_by_city(self, mock_orchestrator):
        """Test that errors are properly aggregated by city geocode."""
        records = [
            # State record that won't match any city records
            Record("US1700000000", "", "7777", "999", "99", "99", "01", 1, "01", "01", "2024-01-01", "01", "1.000000"),
            
            # Multiple city records without matching state treatments
            Record("US17031A0003", "", "7777", "001", "99", "99", "01", 1, "47", "01", "2024-01-01", "01", "1.000000"),
            Record("US17031A0003", "", "7777", "002", "99", "99", "01", 1, "47", "01", "2024-01-01", "01", "1.000000"),
            Record("US17031A0003", "", "7777", "003", "0B", "99", "01", 1, "47", "01", "2024-01-01", "01", "1.000000"),
        ]
        
        enhanced_records, error_messages = mock_orchestrator._replicate_state_tax_treatments_to_cities(records)
        
        # Should have one error message for the city (aggregated)
        assert len(error_messages) == 1
        
        error_msg = error_messages[0]
        assert "US17031A0003" in error_msg
        assert "3 records" in error_msg  # Aggregated count
        # Check items (order may vary due to set)
        assert ("001" in error_msg and "002" in error_msg and "003" in error_msg)  # Sample items
        # Check customers (order may vary due to set)
        assert ("customers=['99', '0B']" in error_msg or "customers=['0B', '99']" in error_msg)  # Unique customers


if __name__ == "__main__":
    pytest.main([__file__])