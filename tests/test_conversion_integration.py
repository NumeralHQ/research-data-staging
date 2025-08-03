"""Integration tests for the product code conversion system."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.models import Record, ProductItem, LookupTables
from src.orchestrator import ResearchDataOrchestrator
from src.product_code_mapper import ProductCodeMapper


class TestConversionIntegration:
    """Integration tests for the full conversion pipeline."""
    
    @pytest.fixture
    def mock_s3_client(self):
        """Mock S3 client for testing."""
        return Mock()
    
    @pytest.fixture
    def sample_mapping_csv(self):
        """Sample product code mapping CSV content."""
        return '''research_id,taxonomy_id,product_id,group,item,description
1.1.1.4.3,597,GENERAL_SAAS_NO_MS_SERVER,7777,22,Technology | Cloud Computing | SaaS
1.1.2.1.1,598,PROGRAMMING_IMPLEMENTATION,7777,5,Technology | Programming | Implementation
1.1.3.2.1,599,MAINTENANCE_REQUIRED,7777,123,Technology | Maintenance | Required'''
    
    @pytest.fixture
    async def lookup_tables_with_mapping(self, mock_s3_client, sample_mapping_csv):
        """Create LookupTables instance with mocked product code mapping."""
        lookup_tables = LookupTables("test-bucket")
        
        # Mock S3 response for product code mapping
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = sample_mapping_csv.encode('utf-8')
        
        with patch.object(lookup_tables.product_code_mapper.s3_client, 'get_object', return_value=mock_response):
            await lookup_tables.initialize_product_code_mapper()
        
        return lookup_tables
    
    @pytest.fixture
    def sample_records(self):
        """Create sample Record objects for testing."""
        return [
            Record(
                geocode="CA", tax_auth_id="", group="7777", item="1.1.1.4.3.0.0.0",
                customer="0B", provider="99", transaction="01", taxable=1,
                tax_type="01", tax_cat="05", effective="2024-01-01",
                per_taxable_type="01", percent_taxable="1.000000"
            ),
            Record(
                geocode="CA", tax_auth_id="", group="7777", item="1.1.2.1.1",
                customer="99", provider="99", transaction="01", taxable=1,
                tax_type="01", tax_cat="05", effective="2024-01-01",
                per_taxable_type="01", percent_taxable="1.000000"
            ),
            Record(
                geocode="CA", tax_auth_id="", group="7777", item="9.9.9.9.9",  # Unmapped
                customer="0B", provider="99", transaction="01", taxable=1,
                tax_type="01", tax_cat="05", effective="2024-01-01",
                per_taxable_type="01", percent_taxable="1.000000"
            )
        ]
    
    @pytest.fixture
    def sample_product_items(self):
        """Create sample ProductItem objects for testing."""
        return [
            ProductItem("1.1.1.4.3.0.0.0", "Technology | Cloud Computing | SaaS Service"),
            ProductItem("1.1.3.2.1", "Technology | Maintenance | Required Service"),
            ProductItem("8.8.8.8.8", "Unmapped Product Item")  # Unmapped
        ]
    
    @pytest.mark.asyncio
    async def test_record_filtering_and_conversion(self, lookup_tables_with_mapping):
        """Test filtering and conversion of Record objects."""
        lookup_tables = await lookup_tables_with_mapping
        
        # Create orchestrator with mocked dependencies
        orchestrator = ResearchDataOrchestrator()
        orchestrator.lookup_tables = lookup_tables
        
        # Create test records
        records = [
            Record("CA", "", "7777", "1.1.1.4.3.0.0.0", "0B", "99", "01", 1, "01", "05", "2024-01-01", "01", "1.000000"),
            Record("CA", "", "7777", "1.1.2.1.1", "99", "99", "01", 1, "01", "05", "2024-01-01", "01", "1.000000"),
            Record("CA", "", "7777", "9.9.9.9.9", "0B", "99", "01", 1, "01", "05", "2024-01-01", "01", "1.000000")  # Unmapped
        ]
        
        # Apply filtering and conversion
        filtered_records = orchestrator._filter_and_convert_record_research_ids(records)
        
        # Should have 2 records (third is unmapped)
        assert len(filtered_records) == 2
        
        # Check converted item codes
        assert filtered_records[0].item == "022"  # "22" -> "022"
        assert filtered_records[1].item == "005"  # "5" -> "005"
        
        # Verify other fields are unchanged
        assert filtered_records[0].geocode == "CA"
        assert filtered_records[0].customer == "0B"
        assert filtered_records[1].customer == "99"
        
        # Check unmapped tracking
        unmapped_ids = lookup_tables.product_code_mapper.get_unmapped_ids()
        assert "9.9.9.9.9" in unmapped_ids
    
    @pytest.mark.asyncio
    async def test_product_item_filtering_and_conversion(self, lookup_tables_with_mapping):
        """Test filtering and conversion of ProductItem objects."""
        lookup_tables = await lookup_tables_with_mapping
        
        # Create orchestrator with mocked dependencies
        orchestrator = ResearchDataOrchestrator()
        orchestrator.lookup_tables = lookup_tables
        
        # Create test product items
        product_items = [
            ProductItem("1.1.1.4.3.0.0.0", "Technology | Cloud Computing | SaaS Service"),
            ProductItem("1.1.3.2.1", "Technology | Maintenance | Required Service"),
            ProductItem("8.8.8.8.8", "Unmapped Product Item")  # Unmapped
        ]
        
        # Apply filtering and conversion
        filtered_items = orchestrator._filter_and_convert_product_item_research_ids(product_items)
        
        # Should have 2 items (third is unmapped)
        assert len(filtered_items) == 2
        
        # Check converted item codes
        assert filtered_items[0].item == "022"  # "22" -> "022"
        assert filtered_items[1].item == "123"  # "123" -> "123"
        
        # Verify descriptions are unchanged
        assert filtered_items[0].description == "Technology | Cloud Computing | SaaS Service"
        assert filtered_items[1].description == "Technology | Maintenance | Required Service"
        
        # Check unmapped tracking
        unmapped_ids = lookup_tables.product_code_mapper.get_unmapped_ids()
        assert "8.8.8.8.8" in unmapped_ids
    
    @pytest.mark.asyncio
    async def test_hierarchy_normalization_matching(self, lookup_tables_with_mapping):
        """Test that hierarchy normalization works correctly for matching."""
        lookup_tables = await lookup_tables_with_mapping
        orchestrator = ResearchDataOrchestrator()
        orchestrator.lookup_tables = lookup_tables
        
        # Test various forms of the same research ID
        test_variations = [
            "1.1.1.4.3",           # Exact match
            "1.1.1.4.3.0",         # One trailing zero
            "1.1.1.4.3.0.0",       # Two trailing zeros
            "1.1.1.4.3.0.0.0",     # Three trailing zeros
            "1.1.1.4.3.0.0.0.0"    # Four trailing zeros
        ]
        
        for research_id in test_variations:
            converted = lookup_tables.product_code_mapper.convert_research_id(research_id)
            assert converted == "022", f"Failed to convert {research_id} to 022, got {converted}"
    
    @pytest.mark.asyncio
    async def test_code_padding_validation(self, lookup_tables_with_mapping):
        """Test that item codes are properly padded to 3 characters."""
        lookup_tables = await lookup_tables_with_mapping
        
        # Test different code lengths from mapping
        test_cases = [
            ("1.1.2.1.1", "005"),    # "5" -> "005"
            ("1.1.1.4.3", "022"),    # "22" -> "022"
            ("1.1.3.2.1", "123")     # "123" -> "123"
        ]
        
        for research_id, expected_code in test_cases:
            converted = lookup_tables.product_code_mapper.convert_research_id(research_id)
            assert converted == expected_code
            assert len(converted) == 3, f"Code {converted} is not 3 characters"
    
    @pytest.mark.asyncio
    async def test_deduplication_with_converted_codes(self, lookup_tables_with_mapping):
        """Test that deduplication works correctly with converted codes."""
        lookup_tables = await lookup_tables_with_mapping
        orchestrator = ResearchDataOrchestrator()
        orchestrator.lookup_tables = lookup_tables
        
        # Create duplicate product items (same research ID, different descriptions)
        product_items = [
            ProductItem("1.1.1.4.3.0.0.0", "First description"),
            ProductItem("1.1.1.4.3", "Second description"),  # Same normalized ID
            ProductItem("1.1.2.1.1", "Unique description")
        ]
        
        # Apply filtering and conversion
        filtered_items = orchestrator._filter_and_convert_product_item_research_ids(product_items)
        
        # Both items with same research ID should convert to same code
        assert len(filtered_items) == 3  # All convert successfully
        assert filtered_items[0].item == "022"
        assert filtered_items[1].item == "022"  # Duplicate code
        assert filtered_items[2].item == "005"
        
        # Apply deduplication
        unique_items = orchestrator._deduplicate_product_items(filtered_items)
        
        # Should deduplicate by converted item code
        assert len(unique_items) == 2  # One duplicate removed
        item_codes = [item.item for item in unique_items]
        assert "022" in item_codes
        assert "005" in item_codes
        assert item_codes.count("022") == 1  # Only one instance of "022"
    
    @pytest.mark.asyncio
    async def test_error_tracking_integration(self, lookup_tables_with_mapping):
        """Test that unmapped research IDs are properly tracked in errors."""
        lookup_tables = await lookup_tables_with_mapping
        
        # Convert some IDs including unmapped ones
        mapped_id = "1.1.1.4.3.0.0.0"
        unmapped_ids = ["9.9.9.9.9", "8.8.8.8.8", "7.7.7.7.7"]
        
        # Convert mapped ID (should succeed)
        result = lookup_tables.product_code_mapper.convert_research_id(mapped_id)
        assert result == "022"
        
        # Convert unmapped IDs (should fail and track)
        for unmapped_id in unmapped_ids:
            result = lookup_tables.product_code_mapper.convert_research_id(unmapped_id)
            assert result is None
        
        # Check error tracking
        tracked_unmapped = lookup_tables.product_code_mapper.get_unmapped_ids()
        assert len(tracked_unmapped) == 3
        
        for unmapped_id in unmapped_ids:
            assert unmapped_id in tracked_unmapped
        
        # Mapped ID should not be in unmapped list
        assert mapped_id not in tracked_unmapped
    
    def test_empty_inputs_handling(self):
        """Test handling of empty or invalid inputs."""
        orchestrator = ResearchDataOrchestrator()
        
        # Mock empty lookup tables
        orchestrator.lookup_tables = Mock()
        orchestrator.lookup_tables.product_code_mapper.convert_research_id.return_value = None
        orchestrator.lookup_tables.product_code_mapper.get_unmapped_ids.return_value = []
        
        # Test empty record lists
        assert orchestrator._filter_and_convert_record_research_ids([]) == []
        assert orchestrator._filter_and_convert_product_item_research_ids([]) == []
        
        # Test with records that all get filtered out
        records = [
            Record("CA", "", "7777", "unmapped1", "0B", "99", "01", 1, "01", "05", "2024-01-01", "01", "1.000000"),
            Record("CA", "", "7777", "unmapped2", "99", "99", "01", 1, "01", "05", "2024-01-01", "01", "1.000000")
        ]
        
        filtered = orchestrator._filter_and_convert_record_research_ids(records)
        assert len(filtered) == 0
    
    @pytest.mark.asyncio 
    async def test_product_code_mapper_statistics(self, lookup_tables_with_mapping):
        """Test that mapping statistics are correctly calculated."""
        lookup_tables = await lookup_tables_with_mapping
        
        # Initially should have 3 mappings, 0 unmapped requests
        stats = lookup_tables.product_code_mapper.get_mapping_stats()
        assert stats["total_mappings"] == 3
        assert stats["unmapped_requests"] == 0
        
        # Make some conversion requests
        lookup_tables.product_code_mapper.convert_research_id("1.1.1.4.3")  # Mapped
        lookup_tables.product_code_mapper.convert_research_id("unmapped1")  # Unmapped
        lookup_tables.product_code_mapper.convert_research_id("unmapped2")  # Unmapped
        
        # Check updated stats
        updated_stats = lookup_tables.product_code_mapper.get_mapping_stats()
        assert updated_stats["total_mappings"] == 3  # Still 3 mappings
        assert updated_stats["unmapped_requests"] == 2  # 2 unmapped attempts