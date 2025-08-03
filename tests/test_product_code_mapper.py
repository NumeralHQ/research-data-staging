"""Tests for the ProductCodeMapper service."""

import io
import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.product_code_mapper import ProductCodeMapper


class TestProductCodeMapper:
    """Test cases for ProductCodeMapper functionality."""
    
    @pytest.fixture
    def mapper(self):
        """Create a ProductCodeMapper instance for testing."""
        return ProductCodeMapper("test-bucket")
    
    def test_normalize_research_id(self, mapper):
        """Test research ID normalization removes trailing .0 segments."""
        # Test cases with trailing zeros
        assert mapper._normalize_research_id("1.1.1.4.3.0.0.0") == "1.1.1.4.3"
        assert mapper._normalize_research_id("1.1.0.0.0.0.0.0") == "1.1"
        assert mapper._normalize_research_id("1.0.0.0.0.0.0.0") == "1"
        
        # Test cases without trailing zeros
        assert mapper._normalize_research_id("1.1.1.4.3.2.1.5") == "1.1.1.4.3.2.1.5"
        assert mapper._normalize_research_id("1.2.3") == "1.2.3"
        
        # Edge cases
        assert mapper._normalize_research_id("0.0.0.0") == "0.0.0.0"  # All zeros returns original
        assert mapper._normalize_research_id("") == ""
        assert mapper._normalize_research_id("   ") == ""
        assert mapper._normalize_research_id("1") == "1"
    
    def test_pad_item_code(self, mapper):
        """Test item code padding to 3 characters."""
        # Test padding
        assert mapper._pad_item_code("5") == "005"
        assert mapper._pad_item_code("22") == "022"
        assert mapper._pad_item_code("123") == "123"
        
        # Test with leading zeros already
        assert mapper._pad_item_code("007") == "007"
        assert mapper._pad_item_code("000") == "000"
        
        # Test longer codes (should not truncate)
        assert mapper._pad_item_code("1234") == "1234"
        
        # Edge cases
        assert mapper._pad_item_code("") == "000"
        assert mapper._pad_item_code("   ") == "000"
        assert mapper._pad_item_code("0") == "000"
    
    @pytest.mark.asyncio
    async def test_load_mapping_success(self, mapper):
        """Test successful loading of mapping data from S3."""
        # Mock CSV content
        csv_content = '''research_id,taxonomy_id,product_id,group,item,description
1.1.1.4.3,597,GENERAL_SAAS_NO_MS_SERVER,7777,22,Technology | Cloud Computing
1.1.1.4.3.0.0.0,598,GENERAL_SAAS_WITH_MS_SERVER,7777,5,Technology | Cloud Computing | Server
1.1.2.1.1,599,PROGRAMMING_IMPLEMENTATION,7777,123,Technology | Programming'''
        
        # Mock S3 response
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = csv_content.encode('utf-8')
        
        with patch.object(mapper.s3_client, 'get_object', return_value=mock_response):
            await mapper.load_mapping()
        
        # Verify mappings were created correctly
        # Note: "1.1.1.4.3" and "1.1.1.4.3.0.0.0" normalize to same key, so we get 2 mappings
        assert len(mapper.mapping) == 2
        assert mapper.mapping["1.1.1.4.3"] == "005"  # Last occurrence (5 -> 005) overwrites first (22 -> 022)
        assert mapper.mapping["1.1.2.1.1"] == "123"  # 123 -> 123
    
    @pytest.mark.asyncio
    async def test_load_mapping_s3_error(self, mapper):
        """Test handling of S3 errors during mapping load."""
        with patch.object(mapper.s3_client, 'get_object', side_effect=Exception("S3 Error")):
            await mapper.load_mapping()
        
        # Should have empty mapping but not crash
        assert len(mapper.mapping) == 0
    
    @pytest.mark.asyncio
    async def test_load_mapping_invalid_csv(self, mapper):
        """Test handling of invalid CSV content."""
        # Mock invalid CSV content
        csv_content = '''invalid,csv,data
incomplete'''
        
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = csv_content.encode('utf-8')
        
        with patch.object(mapper.s3_client, 'get_object', return_value=mock_response):
            await mapper.load_mapping()
        
        # Should handle gracefully with minimal valid data
        assert len(mapper.mapping) == 0  # No valid mappings
    
    def test_convert_research_id_mapped(self, mapper):
        """Test conversion of mapped research IDs."""
        # Set up mapping
        mapper.mapping = {
            "1.1.1.4.3": "022",
            "1.1.2.1.1": "123"
        }
        
        # Test successful conversions
        assert mapper.convert_research_id("1.1.1.4.3.0.0.0") == "022"  # Normalized to "1.1.1.4.3"
        assert mapper.convert_research_id("1.1.1.4.3") == "022"  # Exact match
        assert mapper.convert_research_id("1.1.2.1.1") == "123"
        
        # Verify no unmapped IDs tracked for successful conversions
        assert len(mapper.unmapped_ids) == 0
    
    def test_convert_research_id_unmapped(self, mapper):
        """Test handling of unmapped research IDs."""
        # Set up mapping
        mapper.mapping = {
            "1.1.1.4.3": "022"
        }
        
        # Test unmapped conversions
        assert mapper.convert_research_id("1.1.1.4.9") is None
        assert mapper.convert_research_id("2.2.2.2.2") is None
        assert mapper.convert_research_id("unknown.id") is None
        
        # Verify unmapped IDs are tracked
        assert len(mapper.unmapped_ids) == 3
        assert "1.1.1.4.9" in mapper.unmapped_ids
        assert "2.2.2.2.2" in mapper.unmapped_ids
        assert "unknown.id" in mapper.unmapped_ids
    
    def test_convert_research_id_edge_cases(self, mapper):
        """Test edge cases for research ID conversion."""
        mapper.mapping = {"1": "005"}
        
        # Empty/invalid inputs
        assert mapper.convert_research_id("") is None
        assert mapper.convert_research_id(None) is None
        
        # Verify empty string inputs tracked as unmapped (None is not tracked)
        unmapped = mapper.get_unmapped_ids()
        assert "" in unmapped
        assert None not in unmapped  # None should not be tracked
    
    def test_get_unmapped_ids(self, mapper):
        """Test retrieval of unmapped research IDs."""
        # Add some unmapped IDs
        mapper.unmapped_ids.add("1.1.1.4.9")
        mapper.unmapped_ids.add("2.2.2.2.2")
        mapper.unmapped_ids.add("unknown.id")
        
        unmapped_list = mapper.get_unmapped_ids()
        
        # Should be sorted and contain all unmapped IDs
        assert len(unmapped_list) == 3
        assert unmapped_list == sorted(["1.1.1.4.9", "2.2.2.2.2", "unknown.id"])
    
    def test_get_mapping_stats(self, mapper):
        """Test mapping statistics."""
        # Set up test data
        mapper.mapping = {
            "1.1.1.4.3": "022",
            "1.1.2.1.1": "123"
        }
        mapper.unmapped_ids.add("unknown.id")
        
        stats = mapper.get_mapping_stats()
        
        assert stats["total_mappings"] == 2
        assert stats["unmapped_requests"] == 1
    
    @pytest.mark.asyncio
    async def test_integration_load_and_convert(self, mapper):
        """Test integration of loading and converting."""
        # Mock CSV content with various formats
        csv_content = '''research_id,taxonomy_id,product_id,group,item,description
"1.1.1.4.3","597","GENERAL_SAAS","7777","22","Technology"
"1.1.2.1.1.0.0","598","PROGRAMMING","7777","5","Programming"
"1.1.3.2.1","599","MAINTENANCE","7777","123","Maintenance"'''
        
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = csv_content.encode('utf-8')
        
        with patch.object(mapper.s3_client, 'get_object', return_value=mock_response):
            await mapper.load_mapping()
        
        # Test conversions after loading
        assert mapper.convert_research_id("1.1.1.4.3.0.0.0") == "022"
        assert mapper.convert_research_id("1.1.2.1.1.0.0.0") == "005"  # "5" -> "005"
        assert mapper.convert_research_id("1.1.3.2.1") == "123"
        
        # Test unmapped
        assert mapper.convert_research_id("9.9.9.9.9") is None
        
        # Verify stats
        stats = mapper.get_mapping_stats()
        assert stats["total_mappings"] == 3
        assert stats["unmapped_requests"] == 1