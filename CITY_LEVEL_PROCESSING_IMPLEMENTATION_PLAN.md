# 🏙️ City-Level Research File Processing Implementation Plan

## **📋 Overview**

This plan details the implementation of city-level research file processing capabilities to the existing state-level research data aggregation system. The enhancement will allow processing of city-level Google Sheets files (e.g., "Chicago Sales Tax Research") alongside existing state-level files, with intelligent geocode mapping and tax type hierarchy fallback.

## **🎯 Requirements Summary**

### **Current State**
- ✅ Processes state-level research files only
- ✅ Single geocode per location (state name → state code → geocode)
- ✅ Direct tax type lookup using geocode + tax_cat
- ✅ Each research record → 2 records (business + personal) × N tax types
- ✅ Results are summarized for unique tax treatment between personal (customer = '99') and business (customer = '0B') -- one of the values: taxability, percent_taxable, tax_type, or tax_cat must be different between customer types, otherwise the treatment is summarized to just customer '99' and the '0B' entry is removed for space efficienty

### **Target State**
- ✅ Process both state-level AND city-level research files
- ✅ Multiple geocodes per city location (city name → list of geocodes)
- ✅ Hierarchical tax type lookup (city geocode → fallback to parent state geocode)
- ✅ Results are summarized for each geocode using the existing unique tax treatment between personal (customer = '99') and business (customer = '0B') customer types
- ✅ Each research record → 2 records × M geocodes × N tax types per geocode


## **🔧 Technical Architecture**

### **1. Enhanced Geocode Resolution Flow**

```
Current: filename → extract_state_name → state_code → single_geocode
New:     filename → extract_location_name → try_state_lookup → if_fails_try_city_lookup → list_of_geocodes
```

**Implementation Location:** `src/models.py` - `LookupTables` class

### **2. City Geocode Mapping Structure**

The enhanced `geo_state.csv` structure already supports:
```csv
geocode,state,county,city,tax_district,jurisdiction
US08013A0025,CO,BOULDER,BOULDER,,CITY
US08031A0002,CO,DENVER,DENVER,,CITY
US17031A0003,IL,COOK,CHICAGO,,CITY
US17031A0047,IL,COOK,CHICAGO,,CITY
```

**Key Points:**
- Cities can have multiple geocode entries (e.g., Chicago has 3 geocodes)
- All lookups normalized to `.upper()` for consistency
- Parent state geocode derived from first 4 characters + "00000000"

### **3. Tax Type Hierarchy Logic (Exclusive OR)**

```
1. Try direct lookup: (city_geocode, tax_cat) → tax_types
   - If found, use ONLY these tax_types (stop here)
2. If no match: construct parent_geocode = city_geocode[:4] + "00000000"
3. Fallback lookup: (parent_geocode, tax_cat) → tax_types
   - If found, use ONLY these tax_types (stop here)
4. If still no match: Log error in errors.json and exclude this geocode from matrix_update.csv

NOTE: Never combine tax_types from both city and parent geocode - use exclusively one source
```

## **📝 Implementation Plan**

### **Phase 1: Enhanced Geocode Resolution**

#### **1.1 Update LookupTables Class** (`src/models.py`)

**Add new methods:**

```python
def _load_city_geocode_mapping(self) -> Dict[str, List[str]]:
    """Load city name → list of geocodes mapping from geo_state.csv."""
    # Parse geo_state.csv and create city → [geocodes] mapping
    # Normalize city names to uppercase
    pass

def get_geocodes_for_location(self, filename: str) -> List[str]:
    """
    Extract location geocodes from filename with state→city fallback.
    
    Returns:
        List of geocodes (single item for states, potentially multiple for cities)
    """
    # 1. Try existing state lookup logic
    # 2. If fails, try city lookup
    # 3. Return list of geocodes or empty list if no match
    pass

def _construct_parent_geocode(self, geocode: str) -> str:
    """Construct parent state geocode from city geocode."""
    # Extract first 4 characters + "00000000"
    pass

def get_tax_types_with_hierarchy_fallback(self, geocode: str, tax_cat: str) -> Optional[List[str]]:
    """Get tax types with parent geocode fallback for cities (exclusive OR logic)."""
    # 1. Try direct lookup with city geocode - if found, return ONLY these
    # 2. If no match, try parent state geocode - if found, return ONLY these  
    # 3. If still no match, return None (caller should exclude this geocode and log error)
    # NOTE: Never combine results from both city and parent geocode
    pass
```

**Update existing methods:**
- Modify `_load_geocode_csv()` to also build city mapping
- Keep existing `get_geocode_for_filename()` for backward compatibility
- Update `get_tax_types_for_geocode_and_tax_cat()` to use hierarchy fallback

#### **1.2 Data Structure Changes**

```python
class LookupTables:
    def __init__(self, s3_bucket: str):
        # Existing code...
        self._city_geocode_lookup: Optional[Dict[str, List[str]]] = None
        
    @property 
    def city_geocode_lookup(self) -> Dict[str, List[str]]:
        """Get city name → geocodes mapping."""
        if self._city_geocode_lookup is None:
            self._load_geocode_csv("mapping/geo_state.csv")  # This will populate both state and city lookups
        return self._city_geocode_lookup
```

### **Phase 2: Multi-Geocode Record Processing**

#### **2.1 Update RowMapper Class** (`src/mapper.py`)

**Modify existing methods:**

```python
def process_sheet_rows(self, rows, header_map, filename, config) -> Tuple[List[Record], Optional[str], List[ProcessingError]]:
    """Enhanced to handle multiple geocodes per location."""
    
    # NEW: Get list of geocodes instead of single geocode
    geocodes = self.lookup_tables.get_geocodes_for_location(filename)
    if not geocodes:
        error_msg = f"Could not determine geocode(s) for filename: {filename}"
        return [], error_msg, self.processing_errors
    
    # Process records for each geocode
    all_records = []
    for geocode in geocodes:
        # Existing row processing logic, but per geocode
        geocode_records = self._process_rows_for_geocode(rows, header_map, geocode, config, filename)
        all_records.extend(geocode_records)
    
    return all_records, None, self.processing_errors

def _process_rows_for_geocode(self, rows, header_map, geocode, config, filename) -> List[Record]:
    """Process all rows for a specific geocode (extracted from existing logic)."""
    # Move existing row processing logic here
    # This ensures each geocode gets its own set of records
    pass

def _expand_records_by_tax_types(self, records, geocode):
    """Update to use hierarchical tax type lookup."""
    # Change tax type lookup to use new hierarchy method
    tax_types = self.lookup_tables.get_tax_types_with_hierarchy_fallback(geocode, record.tax_cat)
    pass
```

#### **2.2 Record Generation Flow**

```
For each city-level file:
1. Extract city name from filename
2. Process all matching rows from the sheet (existing logic)
3. Generate business + personal records per row
4. Filter for unique tax treatment per row, remove business '0B' entries that are not unique (existing summarization logic)
5. Look up all applicable geocodes for that city
6. For each applicable geocode:
   a. Duplicate the summarized records for this geocode
   b. Expand by tax types using hierarchy fallback
   c. If no tax types found (hierarchy fallback returns None), exclude this geocode and log error
7. Combine all records from all valid geocodes

Example: "Chicago Sales Tax Research" with 10 unique tax treatment rows after summarization:
- Process 10 rows → generate and summarize to create base record templates
- Find 2 geocodes for Chicago
- For each geocode: duplicate 10 base records → expand by tax types
- Final result: 10 base records × 2 geocodes × N tax_types per geocode
```

### **Phase 3: Enhanced Error Handling & Logging**

#### **3.1 Improved File Classification**

```python
def _classify_research_file(self, filename: str) -> Dict[str, Any]:
    """Classify research file as state-level, city-level, or unknown."""
    geocodes = self.lookup_tables.get_geocodes_for_location(filename)
    
    if not geocodes:
        return {"type": "unknown", "location": None, "geocodes": []}
    
    # Determine if state or city based on geocode pattern
    if len(geocodes) == 1 and geocodes[0].endswith("00000000"):
        return {"type": "state", "location": extracted_name, "geocodes": geocodes}
    else:
        return {"type": "city", "location": extracted_name, "geocodes": geocodes}
```

#### **3.2 Enhanced Logging**

```python
# State-level file
logger.info(f"Processing STATE-level file '{filename}' → geocode: {geocodes[0]}")

# City-level file  
logger.info(f"Processing CITY-level file '{filename}' → {len(geocodes)} geocodes: {geocodes}")
logger.info(f"City '{city_name}' will generate {len(geocodes)} × base_records × tax_types records")

# Tax type fallback logging
logger.debug(f"No tax types for city geocode {geocode}, falling back to parent {parent_geocode}")
```

### **Phase 4: Testing & Validation**

#### **4.1 Unit Tests**

**New test files:**
- `tests/test_city_geocode_lookup.py` - Test city geocode resolution
- `tests/test_tax_type_hierarchy.py` - Test tax type fallback logic
- `tests/test_multi_geocode_processing.py` - Test record generation for cities

**Test scenarios:**
```python
def test_city_geocode_lookup():
    # Test: "Chicago Sales Tax Research" → ["US17031A0003", "US17031A0047"]
    # Test: "Denver Sales Tax Research" → ["US08031A0002"]  
    # Test: "Unknown City Research" → []

def test_tax_type_hierarchy():
    # Test: Direct city lookup succeeds
    # Test: City lookup fails → parent state lookup succeeds  
    # Test: Both fail → return None, exclude geocode from output

def test_parent_geocode_construction():
    # Test: "US08013A0025" → "US0800000000"
    # Test: "US17031A0003" → "US1700000000"
```

#### **4.2 Integration Tests**

```python
def test_city_file_processing():
    # Mock Google Sheets file: "Chicago Sales Tax Research"
    # Verify: Correct number of records generated
    # Verify: Records contain correct geocodes
    # Verify: Tax types properly resolved with fallback
```

#### **4.3 Performance Testing**

- Measure processing time impact (target: <10% increase)
- Test with mixed state/city file batches
- Verify memory usage with increased record counts

## **🚨 Risk Mitigation**

### **1. Backward Compatibility**
- ✅ Keep existing `get_geocode_for_filename()` method
- ✅ Existing state-level files continue to work unchanged
- ✅ All current tests should pass without modification

### **2. Performance Impact**
- ✅ City lookup only triggered on state lookup failure
- ✅ Efficient data structures for city → geocodes mapping
- ✅ Minimize additional S3 API calls (reuse geo_state.csv)

### **3. Data Quality**
- ✅ Comprehensive logging for geocode resolution decisions
- ✅ Track files that fail both state and city lookup
- ✅ Validate parent geocode construction logic

### **4. Error Handling**
- ✅ Graceful fallback for unknown cities
- ✅ Continue processing other files if one city file fails
- ✅ Clear error messages distinguishing state vs city lookup failures

## **📊 Expected Impact**

### **Processing Volume Changes**

**Before (State-only):**
- 51 files → ~11,730 records
- Each matching row → 2 records × N tax_types

**After (State + City):**
- 51+ files (including city files)
- State files: Same as before
- City files: Each matching row → 2 records × M geocodes × N tax_types per geocode

**Example City Impact:**
- Chicago file with 100 matching rows
- 2 geocodes for Chicago
- Average 3 tax types per geocode
- Result: 100 × 2 × 2 × 3 = 1,200 records

### **Processing Time**
- **Target**: <10% increase in total processing time
- **City lookup overhead**: Minimal (only when state lookup fails)
- **Record generation**: Linear increase based on geocode count

## **🛠️ Implementation Order**

### **Week 1: Core Infrastructure**
1. ✅ Enhance `LookupTables` class with city mapping
2. ✅ Implement parent geocode construction
3. ✅ Add hierarchical tax type lookup
4. ✅ Write comprehensive unit tests

### **Week 2: Integration**
1. ✅ Update `RowMapper` for multi-geocode processing
2. ✅ Enhance error handling and logging
3. ✅ Write integration tests
4. ✅ Performance testing with sample data

### **Week 3: Validation & Deployment**
1. ✅ End-to-end testing with real Google Sheets files
2. ✅ Validate backward compatibility
3. ✅ Update documentation
4. ✅ Deploy to production environment

## **✅ Requirements Clarifications (ANSWERED)**

1. **File Naming Patterns**: ✅ **CONFIRMED** - Research files are consistently named and reliable upstream processes ensure this remains a reliable lookup mechanism.

2. **City Name Normalization**: ✅ **CONFIRMED** - Assume exact matches with the geo_state.csv city column.

3. **Processing Priority**: ✅ **CONFIRMED** - No additional complexity needed with prioritization. City and state files can be processed using existing concurrent batching.

4. **Output File Naming**: ✅ **CONFIRMED** - Output files (matrix_update.csv, etc.) can maintain the same naming convention.

5. **Deduplication Across Geocodes**: ✅ **CONFIRMED** - Do NOT deduplicate across geocodes. If treatment is the same in two different geocodes, keep both records.

6. **Performance Threshold**: ✅ **CONFIRMED** - Target <10% increase in total processing time is acceptable.

7. **Geocode Validation**: ✅ **CONFIRMED** - No need to validate that all geocodes follow the expected 12-character format. This is ensured with upstream processing.

8. **Tax Type Priority**: ✅ **CONFIRMED** - Use ONLY the matched tax_types from city geocode OR parent geocode, never combine both. Hierarchy: try city geocode first, if no match then try parent geocode, but never use results from both simultaneously.

## **📚 Files to Modify**

### **Core Changes:**
1. **`src/models.py`** - Major changes to LookupTables class
2. **`src/mapper.py`** - Significant changes to RowMapper class
3. **`src/worker.py`** - Minor logging enhancements

### **New Test Files:**
1. **`tests/test_city_geocode_lookup.py`**
2. **`tests/test_tax_type_hierarchy.py`** 
3. **`tests/test_multi_geocode_processing.py`**

### **Documentation Updates:**
1. **`README.md`** - Add city-level processing documentation
2. **Update execution flow** - Document new geocode resolution logic

---

**🎯 This implementation will enable the system to seamlessly process both state-level and city-level research files, expanding the system's capability while maintaining full backward compatibility and performance.**