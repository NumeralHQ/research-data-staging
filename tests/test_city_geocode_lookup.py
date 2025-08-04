"""Tests for city geocode lookup functionality."""

import pytest
from unittest.mock import Mock, patch
from src.models import LookupTables


class TestCityGeocodeLookup:
    """Test city geocode resolution and lookup functionality."""
    
    @pytest.fixture
    def mock_s3_client(self):
        """Mock S3 client for testing."""
        mock_client = Mock()
        return mock_client
    
    @pytest.fixture
    def mock_csv_content(self):
        """Mock CSV content simulating geo_state.csv structure."""
        return '''geocode,state,county,city,tax_district,jurisdiction
US1700000000,IL,,,,STATE
US1800000000,IN,,,,STATE
US0800000000,CO,,,,STATE
US08013A0025,CO,BOULDER,BOULDER,,CITY
US08031A0002,CO,DENVER,DENVER,,CITY
US17031A0003,IL,COOK,CHICAGO,,CITY
US17031A0047,IL,COOK,CHICAGO,,CITY
US17043A0053,IL,DUPAGE,CHICAGO,,CITY
US22033A0009,LA,EAST BATON ROUGE,BATON ROUGE,,CITY
US22033A0020,LA,EAST BATON ROUGE,BATON ROUGE,,CITY'''
    
    @pytest.fixture
    def lookup_tables(self, mock_s3_client, mock_csv_content):
        """Create LookupTables instance with mocked S3 data."""
        with patch('src.models.boto3.client', return_value=mock_s3_client):
            # Mock the S3 response
            mock_response = {'Body': Mock()}
            mock_response['Body'].read.return_value = mock_csv_content.encode('utf-8')
            mock_s3_client.get_object.return_value = mock_response
            
            lookup_tables = LookupTables("test-bucket")
            # Trigger loading of geocode data
            _ = lookup_tables.geocode_lookup
            return lookup_tables
    
    def test_city_geocode_lookup_populated(self, lookup_tables):
        """Test that city geocode lookup is properly populated."""
        city_lookup = lookup_tables.city_geocode_lookup
        
        # Check that cities are present with correct case normalization
        assert "CHICAGO" in city_lookup
        assert "DENVER" in city_lookup  
        assert "BOULDER" in city_lookup
        assert "BATON ROUGE" in city_lookup
        
        # Check geocode counts
        assert len(city_lookup["CHICAGO"]) == 3  # 3 geocodes for Chicago
        assert len(city_lookup["DENVER"]) == 1   # 1 geocode for Denver
        assert len(city_lookup["BOULDER"]) == 1  # 1 geocode for Boulder
        assert len(city_lookup["BATON ROUGE"]) == 2  # 2 geocodes for Baton Rouge
    
    def test_city_geocode_values(self, lookup_tables):
        """Test that city geocodes contain correct values."""
        city_lookup = lookup_tables.city_geocode_lookup
        
        # Check Chicago geocodes
        chicago_geocodes = set(city_lookup["CHICAGO"])
        expected_chicago = {"US17031A0003", "US17031A0047", "US17043A0053"}
        assert chicago_geocodes == expected_chicago
        
        # Check Denver geocodes
        assert city_lookup["DENVER"] == ["US08031A0002"]
        
        # Check Boulder geocodes  
        assert city_lookup["BOULDER"] == ["US08013A0025"]
        
        # Check Baton Rouge geocodes
        baton_rouge_geocodes = set(city_lookup["BATON ROUGE"])
        expected_baton_rouge = {"US22033A0009", "US22033A0020"}
        assert baton_rouge_geocodes == expected_baton_rouge
    
    def test_state_geocode_lookup_unchanged(self, lookup_tables):
        """Test that state geocode lookup still works for state-level files."""
        state_lookup = lookup_tables.geocode_lookup
        
        # Check that only state-level geocodes are in state lookup
        assert "IL" in state_lookup
        assert "IN" in state_lookup
        assert "CO" in state_lookup
        
        # Check values
        assert state_lookup["IL"] == "US1700000000"
        assert state_lookup["IN"] == "US1800000000" 
        assert state_lookup["CO"] == "US0800000000"
    
    def test_get_geocodes_for_location_state_files(self, lookup_tables):
        """Test geocode resolution for state-level files."""
        # State files should return single geocode
        geocodes = lookup_tables.get_geocodes_for_location("Illinois Sales Tax Research")
        assert geocodes == ["US1700000000"]
        
        geocodes = lookup_tables.get_geocodes_for_location("Colorado Sales Tax Research")
        assert geocodes == ["US0800000000"]
        
        geocodes = lookup_tables.get_geocodes_for_location("Indiana Sales Tax Research")
        assert geocodes == ["US1800000000"]
    
    def test_get_geocodes_for_location_city_files(self, lookup_tables):
        """Test geocode resolution for city-level files."""
        # City files should return multiple geocodes
        geocodes = lookup_tables.get_geocodes_for_location("Chicago Sales Tax Research")
        expected_chicago = {"US17031A0003", "US17031A0047", "US17043A0053"}
        assert set(geocodes) == expected_chicago
        
        geocodes = lookup_tables.get_geocodes_for_location("Denver Sales Tax Research")
        assert geocodes == ["US08031A0002"]
        
        geocodes = lookup_tables.get_geocodes_for_location("Boulder Sales Tax Research")
        assert geocodes == ["US08013A0025"]
        
        geocodes = lookup_tables.get_geocodes_for_location("Baton Rouge Sales Tax Research")
        expected_baton_rouge = {"US22033A0009", "US22033A0020"}
        assert set(geocodes) == expected_baton_rouge
    
    def test_get_geocodes_for_location_unknown_location(self, lookup_tables):
        """Test geocode resolution for unknown locations."""
        # Unknown locations should return empty list
        geocodes = lookup_tables.get_geocodes_for_location("Unknown City Sales Tax Research")
        assert geocodes == []
        
        geocodes = lookup_tables.get_geocodes_for_location("Nonexistent State Research")
        assert geocodes == []
    
    def test_get_geocodes_for_location_state_priority(self, lookup_tables):
        """Test that state lookup has priority over city lookup."""
        # If a filename could match both state and city, state should win
        # This tests the fallback logic order: state first, then city
        
        # "Illinois" should match the state, not fall through to city lookup
        geocodes = lookup_tables.get_geocodes_for_location("Illinois Research File")
        assert geocodes == ["US1700000000"]  # State-level geocode
    
    def test_case_insensitive_city_matching(self, lookup_tables):
        """Test that city name matching is case insensitive."""
        # Test various case combinations
        geocodes = lookup_tables.get_geocodes_for_location("chicago sales tax research")
        expected_chicago = {"US17031A0003", "US17031A0047", "US17043A0053"}
        assert set(geocodes) == expected_chicago
        
        geocodes = lookup_tables.get_geocodes_for_location("DENVER Sales Tax Research")
        assert geocodes == ["US08031A0002"]
        
        geocodes = lookup_tables.get_geocodes_for_location("Boulder SALES TAX research")
        assert geocodes == ["US08013A0025"]
    
    def test_city_name_with_spaces(self, lookup_tables):
        """Test city names with spaces like 'BATON ROUGE'."""
        geocodes = lookup_tables.get_geocodes_for_location("Baton Rouge Sales Tax Research")
        expected_baton_rouge = {"US22033A0009", "US22033A0020"}
        assert set(geocodes) == expected_baton_rouge
        
        # Test case variations
        geocodes = lookup_tables.get_geocodes_for_location("BATON ROUGE research file")
        assert set(geocodes) == expected_baton_rouge
    
    def test_partial_city_name_matching(self, lookup_tables):
        """Test that partial city names in filenames are matched correctly."""
        # "Chicago" should be found in "Chicago Sales Tax Research"
        geocodes = lookup_tables.get_geocodes_for_location("Chicago Sales Tax Research 2024")
        expected_chicago = {"US17031A0003", "US17031A0047", "US17043A0053"}
        assert set(geocodes) == expected_chicago
        
        # Should also work with city name in middle of filename
        geocodes = lookup_tables.get_geocodes_for_location("Sales Tax Denver Research Q1")
        assert geocodes == ["US08031A0002"]
    
    def test_empty_filename(self, lookup_tables):
        """Test handling of empty or None filenames."""
        geocodes = lookup_tables.get_geocodes_for_location("")
        assert geocodes == []
        
        geocodes = lookup_tables.get_geocodes_for_location("   ")  # Whitespace only
        assert geocodes == []


if __name__ == "__main__":
    pytest.main([__file__])