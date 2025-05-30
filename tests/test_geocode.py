#!/usr/bin/env python3
"""
Test geocode lookup functionality with example filename.
"""

import os
import sys
import csv

# Add parent directory to Python path so we can import src modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Set required environment variables
os.environ['DRIVE_FOLDER_ID'] = '1VK3kgR-tS-nkTUSwq8_B-8JYl3zFkXFU'
os.environ['GOOGLE_SERVICE_ACCOUNT_SECRET'] = 'research-data-aggregation/google-service-account'
os.environ['S3_BUCKET'] = 'research-aggregation-test'

class LocalLookupTables:
    """Local version of LookupTables that reads from local files only."""
    
    def __init__(self):
        self._geocode_map = None
        self._state_name_to_code = None
    
    def get_geocode_map(self):
        """Get state code to geocode mapping from local file."""
        if self._geocode_map is None:
            try:
                with open('../mapping/geo_state.csv', 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    self._geocode_map = {row['state'].strip(): row['geocode'] for row in reader}
            except FileNotFoundError:
                # Try from current directory
                with open('mapping/geo_state.csv', 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    self._geocode_map = {row['state'].strip(): row['geocode'] for row in reader}
        return self._geocode_map
    
    def get_state_name_to_code_map(self):
        """Get state name to state code mapping."""
        if self._state_name_to_code is None:
            self._state_name_to_code = {
                "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
                "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE", "DISTRICT OF COLUMBIA": "DC",
                "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID",
                "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS",
                "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
                "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS",
                "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV",
                "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY",
                "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK",
                "OREGON": "OR", "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC",
                "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT",
                "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV",
                "WISCONSIN": "WI", "WYOMING": "WY"
            }
        return self._state_name_to_code
    
    def get_geocode_for_filename(self, filename):
        """Extract geocode from filename by finding state name."""
        filename_upper = filename.upper()
        state_map = self.get_state_name_to_code_map()
        geocode_map = self.get_geocode_map()
        
        # Sort states by length (longest first) to prioritize more specific matches
        # This ensures "WEST VIRGINIA" is checked before "VIRGINIA"
        sorted_states = sorted(state_map.items(), key=lambda x: len(x[0]), reverse=True)
        
        for state_name, state_code in sorted_states:
            if state_name in filename_upper:
                return geocode_map.get(state_code)
        
        return None

def test_geocode_lookup():
    """Test geocode lookup with example filenames."""
    print("Testing geocode lookup functionality...")
    print("=" * 60)
    
    # Create local lookup tables instance
    lookup_tables = LocalLookupTables()
    
    # Test cases with various filename formats
    test_filenames = [
        "Connecticut Sales Tax Research",
        "Connecticut Sales Tax Research.xlsx",
        "CONNECTICUT Sales Tax Research",
        "connecticut sales tax research",
        "Sales Tax Research - Connecticut",
        "California Tax Data",
        "New York Research File",
        "Texas Sales Tax Info",
        "District of Columbia Sales Tax Research",
        "DISTRICT OF COLUMBIA Tax Data",
        "Virginia Sales Tax Research",           # Test Virginia
        "West Virginia Sales Tax Research",      # Test West Virginia (should not match Virginia)
        "WEST VIRGINIA Tax Data",                # Test West Virginia uppercase
        "virginia tax info",                     # Test Virginia lowercase
        "Invalid State Name File",
        "Alabama Research Data",
        "FLORIDA tax information"
    ]
    
    print("Filename → State Found → Geocode")
    print("-" * 60)
    
    for filename in test_filenames:
        try:
            # Get the geocode for this filename
            geocode = lookup_tables.get_geocode_for_filename(filename)
            
            # Also show which state was detected (use same logic as geocode lookup)
            filename_upper = filename.upper()
            state_map = lookup_tables.get_state_name_to_code_map()
            detected_state = None
            
            # Sort states by length (longest first) to match the geocode lookup logic
            sorted_states = sorted(state_map.items(), key=lambda x: len(x[0]), reverse=True)
            
            for state_name, state_code in sorted_states:
                if state_name in filename_upper:
                    detected_state = f"{state_name} ({state_code})"
                    break
            
            if geocode:
                print(f"✅ '{filename}' → {detected_state} → {geocode}")
            else:
                print(f"❌ '{filename}' → No state detected → None")
                
        except Exception as e:
            print(f"❌ '{filename}' → ERROR: {e}")
    
    print("\n" + "=" * 60)
    print("📍 Geocode Lookup Process:")
    print("1. Extract state name from filename (case-insensitive)")
    print("2. Map state name to 2-letter code (e.g., CONNECTICUT → CT)")
    print("3. Look up geocode from geo_state.csv mapping")
    print("4. Return 12-character geocode for CSV output")
    
    # Show specific example
    print(f"\n🎯 Your Example:")
    example_filename = "Connecticut Sales Tax Research"
    geocode = lookup_tables.get_geocode_for_filename(example_filename)
    print(f"'{example_filename}' → CONNECTICUT (CT) → {geocode}")

if __name__ == "__main__":
    test_geocode_lookup() 