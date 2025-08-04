"""Tests for state-level tax treatment replication functionality."""

import pytest
from unittest.mock import Mock, patch
from src.models import Record, LookupTables, GroupType, CustomerType, ProviderType, TransactionType, TaxType, PerTaxableType
from src.orchestrator import ResearchDataOrchestrator


class TestStateTreatmentReplication:
    """Test individual methods for state treatment replication to cities."""
    
    @pytest.fixture
    def mock_lookup_tables(self):
        """Mock LookupTables instance."""
        lookup_tables = Mock(spec=LookupTables)
        lookup_tables._construct_parent_geocode = Mock()
        # Mock parent geocode construction
        lookup_tables._construct_parent_geocode.side_effect = lambda geocode: geocode[:4] + "00000000"
        return lookup_tables
    
    @pytest.fixture
    def orchestrator(self, mock_lookup_tables):
        """Create orchestrator instance with mocked dependencies."""
        orchestrator = ResearchDataOrchestrator()
        orchestrator.lookup_tables = mock_lookup_tables
        return orchestrator
    
    @pytest.fixture
    def sample_records(self):
        """Create sample records for testing."""
        return [
            # State records
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "01", "01", "2024-01-01", "01", "1.000000"),
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "02", "01", "2024-01-01", "01", "1.000000"),
            Record("US1700000000", "", "7777", "001", "0B", "99", "01", 1, "01", "02", "2024-01-01", "01", "1.000000"),
            Record("US1700000000", "", "7777", "002", "99", "99", "01", 1, "01", "01", "2024-01-01", "01", "1.000000"),
            
            # City records
            Record("US17031A0003", "", "7777", "001", "99", "99", "01", 1, "47", "01", "2024-01-01", "01", "1.000000"),
            Record("US17031A0003", "", "7777", "002", "99", "99", "01", 1, "47", "01", "2024-01-01", "01", "1.000000"),
        ]
    
    def test_identify_city_records(self, orchestrator, sample_records):
        """Test identification of city vs state records."""
        city_records = orchestrator._identify_city_records(sample_records)
        
        # Should identify 2 city records (not ending with "00000000")
        assert len(city_records) == 2
        assert all(not record.geocode.endswith("00000000") for record in city_records)
        assert all(record.geocode == "US17031A0003" for record in city_records)
    
    def test_parent_geocode_derivation(self, orchestrator):
        """Test parent geocode construction."""
        # Test with city geocodes
        assert orchestrator.lookup_tables._construct_parent_geocode("US17031A0003") == "US1700000000"
        assert orchestrator.lookup_tables._construct_parent_geocode("US08013A0025") == "US0800000000"
        
        # Test with already-state geocodes
        assert orchestrator.lookup_tables._construct_parent_geocode("US1700000000") == "US1700000000"
    
    def test_find_matching_state_treatments_basic(self, orchestrator, sample_records):
        """Test finding matching state treatments with basic composite key exclusion."""
        # Get a city record to test against
        city_record = sample_records[4]  # US17031A0003, item=001, customer=99, tax_type=47, tax_cat=01
        
        matching_records = orchestrator._find_matching_state_treatments(city_record, sample_records)
        
        # Should find state records with same group/item/customer/provider but different tax_type or tax_cat
        # From sample_records, these should match:
        # - Record 0: same everything but tax_type=01 (different from 47) ✓
        # - Record 1: same core fields but tax_type=02 (different from 47) ✓
        # - Record 2: different customer (0B vs 99) ❌
        # - Record 3: different item (002 vs 001) ❌
        
        assert len(matching_records) == 2
        
        # Check that all matching records have the correct parent geocode
        assert all(record.geocode == "US1700000000" for record in matching_records)
        
        # Check that all matching records have same core fields
        assert all(record.group == city_record.group for record in matching_records)
        assert all(record.item == city_record.item for record in matching_records)
        assert all(record.customer == city_record.customer for record in matching_records)
        assert all(record.provider == city_record.provider for record in matching_records)
        
        # Check that none have exact composite key match
        for record in matching_records:
            assert not (record.tax_type == city_record.tax_type and record.tax_cat == city_record.tax_cat)
    
    def test_composite_key_exclusion_logic(self, orchestrator):
        """Test that exact composite key matches are excluded."""
        city_record = Record("US17031A0003", "", "7777", "001", "99", "99", "01", 1, "01", "02", "2024-01-01", "01", "1.000000")
        
        all_records = [
            # State records with different combinations
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "01", "01", "2024-01-01", "01", "1.000000"),  # Different tax_cat ✓
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "02", "02", "2024-01-01", "01", "1.000000"),  # Different tax_type ✓  
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "03", "01", "2024-01-01", "01", "1.000000"),  # Both different ✓
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "01", "02", "2024-01-01", "01", "1.000000"),  # Exact match ❌
        ]
        
        matching_records = orchestrator._find_matching_state_treatments(city_record, all_records)
        
        # Should include 3 records (exclude the exact composite key match)
        assert len(matching_records) == 3
        
        # Verify that the exact match is not included
        for record in matching_records:
            assert not (record.tax_type == "01" and record.tax_cat == "02")
    
    def test_create_city_treatment_record(self, orchestrator):
        """Test record cloning with geocode replacement."""
        state_record = Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "01", "01", "2024-01-01", "01", "1.000000")
        city_geocode = "US17031A0003"
        
        city_record = orchestrator._create_city_treatment_record(state_record, city_geocode)
        
        # Should have city geocode
        assert city_record.geocode == city_geocode
        
        # All other fields should be identical
        assert city_record.tax_auth_id == state_record.tax_auth_id
        assert city_record.group == state_record.group
        assert city_record.item == state_record.item
        assert city_record.customer == state_record.customer
        assert city_record.provider == state_record.provider
        assert city_record.transaction == state_record.transaction
        assert city_record.taxable == state_record.taxable
        assert city_record.tax_type == state_record.tax_type
        assert city_record.tax_cat == state_record.tax_cat
        assert city_record.effective == state_record.effective
        assert city_record.per_taxable_type == state_record.per_taxable_type
        assert city_record.percent_taxable == state_record.percent_taxable
    
    def test_group_missing_treatments_by_city(self, orchestrator):
        """Test grouping of missing treatments by city for error reporting."""
        missing_treatments = [
            ("US17031A0003", Record("US17031A0003", "", "7777", "001", "99", "99", "01", 1, "47", "01", "2024-01-01", "01", "1.000000")),
            ("US17031A0003", Record("US17031A0003", "", "7777", "002", "99", "99", "01", 1, "47", "01", "2024-01-01", "01", "1.000000")),
            ("US08013A0025", Record("US08013A0025", "", "7777", "001", "99", "99", "01", 1, "01", "01", "2024-01-01", "01", "1.000000")),
        ]
        
        grouped = orchestrator._group_missing_treatments_by_city(missing_treatments)
        
        # Should group by city geocode
        assert len(grouped) == 2
        assert "US17031A0003" in grouped
        assert "US08013A0025" in grouped
        assert len(grouped["US17031A0003"]) == 2
        assert len(grouped["US08013A0025"]) == 1
    
    def test_no_city_records_handling(self, orchestrator):
        """Test handling when no city records are present."""
        # All state records
        state_only_records = [
            Record("US1700000000", "", "7777", "001", "99", "99", "01", 1, "01", "01", "2024-01-01", "01", "1.000000"),
            Record("US0800000000", "", "7777", "002", "99", "99", "01", 1, "01", "01", "2024-01-01", "01", "1.000000"),
        ]
        
        enhanced_records, error_messages = orchestrator._replicate_state_tax_treatments_to_cities(state_only_records)
        
        # Should return original records unchanged
        assert enhanced_records == state_only_records
        assert len(error_messages) == 0
    
    def test_city_without_matching_state_treatments(self, orchestrator):
        """Test error handling for cities without matching state treatments."""
        records = [
            # State record with different item
            Record("US1700000000", "", "7777", "999", "99", "99", "01", 1, "01", "01", "2024-01-01", "01", "1.000000"),
            # City record with no matching state treatments
            Record("US17031A0003", "", "7777", "001", "99", "99", "01", 1, "47", "01", "2024-01-01", "01", "1.000000"),
        ]
        
        enhanced_records, error_messages = orchestrator._replicate_state_tax_treatments_to_cities(records)
        
        # Should have original records only (no replication)
        assert len(enhanced_records) == 2
        assert enhanced_records == records
        
        # Should have error message for the city without matches
        assert len(error_messages) == 1
        assert "US17031A0003" in error_messages[0]
        assert "no matching state-level tax treatments" in error_messages[0]
        assert "US1700000000" in error_messages[0]  # Parent geocode mentioned


if __name__ == "__main__":
    pytest.main([__file__])