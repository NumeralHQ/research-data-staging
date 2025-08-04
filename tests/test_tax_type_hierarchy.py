"""Tests for tax type hierarchy fallback functionality."""

import pytest
from unittest.mock import Mock, patch
from src.models import LookupTables


class TestTaxTypeHierarchy:
    """Test tax type hierarchy fallback logic for city geocodes."""
    
    @pytest.fixture
    def mock_s3_client(self):
        """Mock S3 client for testing."""
        mock_client = Mock()
        return mock_client
    
    @pytest.fixture
    def mock_geo_csv_content(self):
        """Mock geo_state.csv content."""
        return '''geocode,state,county,city,tax_district,jurisdiction
US1700000000,IL,,,,STATE
US0800000000,CO,,,,STATE
US08013A0025,CO,BOULDER,BOULDER,,CITY
US17031A0003,IL,COOK,CHICAGO,,CITY'''
    
    @pytest.fixture
    def mock_tax_type_csv_content(self):
        """Mock unique_tax_type.csv content with hierarchy scenarios."""
        return '''geocode,tax_cat,tax_type
US1700000000,01,01
US1700000000,01,02
US1700000000,01,47
US0800000000,01,01
US0800000000,01,02
US0800000000,01,03
US08013A0025,01,01
US08013A0025,01,04
US17031A0047,01,47'''
    
    @pytest.fixture  
    def mock_tax_cat_csv_content(self):
        """Mock tax_cat.csv content."""
        return '''tax_cat,tax_cat_desc
01,General Sales Tax
02,Special Tax'''
    
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
    
    def test_construct_parent_geocode(self, lookup_tables):
        """Test parent geocode construction logic."""
        # Test city geocodes -> parent state geocodes
        assert lookup_tables._construct_parent_geocode("US08013A0025") == "US0800000000"
        assert lookup_tables._construct_parent_geocode("US17031A0003") == "US1700000000"
        assert lookup_tables._construct_parent_geocode("US22033A0009") == "US2200000000"
        
        # Test state geocodes (should stay the same or be handled gracefully)
        assert lookup_tables._construct_parent_geocode("US0800000000") == "US0800000000"
        assert lookup_tables._construct_parent_geocode("US1700000000") == "US1700000000"
        
        # Test short geocodes
        assert lookup_tables._construct_parent_geocode("US08") == "US0800000000"
        assert lookup_tables._construct_parent_geocode("AB") == "AB0000000000"  # Should handle gracefully
    
    def test_direct_city_lookup_success(self, lookup_tables):
        """Test direct tax type lookup succeeds for city geocodes."""
        # US08013A0025 (Boulder, CO) has direct entries in tax type CSV
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US08013A0025", "01")
        assert tax_types == ["01", "04"]  # Should return city-specific tax types
        
        # Should NOT fall back to parent state even though parent has different tax types
        # (Parent US0800000000 has ["01", "02", "03"] but we should use ONLY city types)
    
    def test_parent_fallback_success(self, lookup_tables):
        """Test fallback to parent state geocode when city has no direct match."""
        # US17031A0003 (Chicago, IL) has NO direct entries, should fall back to IL state
        # Parent US1700000000 has ["01", "02", "47"]
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US17031A0003", "01")
        assert tax_types == ["01", "02", "47"]  # Should return parent state tax types
    
    def test_no_match_returns_none(self, lookup_tables):
        """Test that no match in both city and parent returns None."""
        # Use a geocode with no direct match and parent has no match for tax_cat
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US17031A0003", "99")
        assert tax_types is None
        
        # Use completely unknown geocode
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US99999Z9999", "01")
        assert tax_types is None
    
    def test_case_insensitive_matching(self, lookup_tables):
        """Test that tax type lookup is case insensitive."""
        # Test various case combinations
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("us08013a0025", "01")
        assert tax_types == ["01", "04"]
        
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US08013A0025", "01")
        assert tax_types == ["01", "04"]
        
        # Test tax_cat case insensitivity
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US08013A0025", "01")
        assert tax_types == ["01", "04"]
    
    def test_exclusive_or_logic(self, lookup_tables):
        """Test that results from city and parent are never combined (exclusive OR)."""
        # US08013A0025 (Boulder) has ["01", "04"]
        # Its parent US0800000000 (CO) has ["01", "02", "03"]  
        # Should return ONLY city results, NOT combined
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US08013A0025", "01")
        assert tax_types == ["01", "04"]
        assert "02" not in tax_types  # Should NOT include parent tax types
        assert "03" not in tax_types  # Should NOT include parent tax types
        
        # When city has no match, should return ONLY parent results
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US17031A0003", "01")
        assert tax_types == ["01", "02", "47"]  # ONLY parent results
    
    def test_tax_type_sorting(self, lookup_tables):
        """Test that tax types are returned in sorted order."""
        # Tax types should be sorted for consistent ordering
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US08013A0025", "01")
        assert tax_types == ["01", "04"]  # Should be sorted
        
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US17031A0003", "01") 
        assert tax_types == ["01", "02", "47"]  # Should be sorted
    
    def test_empty_geocode_and_tax_cat(self, lookup_tables):
        """Test handling of empty geocode and tax_cat values."""
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("", "01")
        assert tax_types is None
        
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US08013A0025", "")
        assert tax_types is None
        
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("", "")
        assert tax_types is None
    
    def test_whitespace_handling(self, lookup_tables):
        """Test that whitespace is properly stripped in lookups."""
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback(" US08013A0025 ", " 01 ")
        assert tax_types == ["01", "04"]
        
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("\tUS17031A0003\n", "\t01\n")
        assert tax_types == ["01", "02", "47"]
    
    def test_multiple_fallback_scenarios(self, lookup_tables):
        """Test various geocode scenarios for comprehensive coverage."""
        # Scenario 1: City with direct match
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US08013A0025", "01")
        assert tax_types == ["01", "04"]
        
        # Scenario 2: City without direct match, parent has match  
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US17031A0003", "01")
        assert tax_types == ["01", "02", "47"]
        
        # Scenario 3: Neither city nor parent has match
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US17031A0003", "99")
        assert tax_types is None
        
        # Scenario 4: State-level geocode (no parent fallback needed)
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US1700000000", "01")
        assert tax_types == ["01", "02", "47"]
    
    def test_parent_geocode_same_as_city(self, lookup_tables):
        """Test handling when constructed parent geocode is same as city geocode."""
        # This tests the edge case where parent_geocode == geocode
        # Should not attempt parent lookup when they're the same
        tax_types = lookup_tables.get_tax_types_with_hierarchy_fallback("US0800000000", "01")
        assert tax_types == ["01", "02", "03"]  # State-level lookup, no fallback needed
    

if __name__ == "__main__":
    pytest.main([__file__])