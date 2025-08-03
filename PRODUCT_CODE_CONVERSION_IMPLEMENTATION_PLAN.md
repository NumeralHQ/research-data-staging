# Product Code Conversion Implementation Plan

## Overview
Convert research IDs (e.g., "1.1.0.0.0.0.0") to 3-character VARCHAR codes in output CSV files using a lookup table, while preserving all existing hierarchy and processing logic.

## Problem Statement
- **Current**: Direct use of research IDs from Google Sheets in `matrix_update.csv` and `product_item_update.csv` 
- **Required**: Convert to 3-character codes using `product_code_mapping.csv` lookup
- **Challenge**: Research IDs are extensively used for hierarchy processing and business logic
- **Hierarchy Matching**: Research files have 8+ levels (e.g., "1.1.1.4.3.0.0.0") but mapping file has truncated trailing .0s
- **Row Exclusion**: Unmapped research_ids must be completely excluded from output files
- **Error Handling**: Track excluded research_ids in `errors.json`

## Current Data Flow Analysis

### Key Components:
1. **Google Sheets** → research_ids extracted
2. **mapper.py** → Creates `Record` objects with `item=research_id` (lines 281, 296, 313, 330)
3. **worker.py** → Creates `ProductItem` objects with `item=research_id` (line 239)
4. **worker.py** → Uses research_ids for hierarchy (`_parse_hierarchical_id`, `_build_hierarchical_description`)
5. **orchestrator.py** → Generates final CSV output (lines 287, 301)

### Critical Dependencies on Research IDs:
- **Hierarchical Processing**: `_parse_hierarchical_id()` parses "1.1.1.4.3.0.0.0" structure
- **Description Building**: `_build_hierarchical_description()` builds parent-child relationships
- **Lookup Operations**: Used as keys throughout processing
- **Business Logic**: Tax type expansion, geocode mapping, validation

## Recommended Implementation Strategy

### **Approach: Late Filtering + Conversion (Recommended)**
Filter out unmapped research_ids and convert remaining ones to 3-character codes **just before CSV output generation**, preserving all existing logic.

**Advantages:**
- ✅ Preserves all existing hierarchy and business logic
- ✅ Minimal code changes required
- ✅ Lowest risk of breaking existing functionality
- ✅ Clean separation of concerns
- ✅ Complete exclusion of unmapped records ensures data integrity

**Implementation Point:** In `ResearchDataOrchestrator` before `_create_csv_content()` and `_create_product_item_csv_content()`

## Detailed Implementation Plan

### 1. Create Product Code Mapping Service

**File:** `src/product_code_mapper.py`

**Responsibilities:**
- Load `product_code_mapping.csv` from S3
- Normalize research_ids for matching (remove trailing .0s)
- Provide research_id → 3-character code conversion with padding
- Track unmapped research_ids for error reporting

```python
class ProductCodeMapper:
    def __init__(self, s3_bucket: str):
        self.mapping: Dict[str, str] = {}  # normalized_research_id -> item_code
        self.unmapped_ids: Set[str] = set()
        
    def _normalize_research_id(self, research_id: str) -> str:
        """Normalize research_id by removing trailing .0 segments for matching."""
        # "1.1.1.4.3.0.0.0" -> "1.1.1.4.3"
        
    def _pad_item_code(self, item_code: str) -> str:
        """Pad item code to exactly 3 characters with leading zeros."""
        # "22" -> "022", "5" -> "005", "123" -> "123"
        
    async def load_mapping(self) -> None:
        # Load from S3: research-aggregation/mapping/product_code_mapping.csv
        # Apply normalization to research_ids during loading
        
    def convert_research_id(self, research_id: str) -> Optional[str]:
        """Convert research_id to 3-character padded code or None if unmapped."""
        # Normalize input, lookup, pad result, track unmapped
        
    def get_unmapped_ids(self) -> List[str]:
        # Return list of original unmapped research_ids (not normalized)
```

### 2. Integrate with LookupTables

**File:** `src/models.py` - Update `LookupTables` class

Add product code mapper initialization:
```python
class LookupTables:
    def __init__(self, s3_bucket: str):
        # ... existing code ...
        self.product_code_mapper = ProductCodeMapper(s3_bucket)
        
    async def _load_all_data(self):
        # ... existing loads ...
        await self.product_code_mapper.load_mapping()
```

### 3. Implement Conversion in Orchestrator

**File:** `src/orchestrator.py`

**Before Matrix CSV Generation (line ~287):**
```python
# Step 5: Filter unmapped research_ids and convert to product codes
csv_key = None
if all_records:
    logger.info(f"Filtering and converting research_ids to product codes for {len(all_records)} records")
    filtered_records = self._filter_and_convert_record_research_ids(all_records)
    
    excluded_count = len(all_records) - len(filtered_records)
    if excluded_count > 0:
        logger.warning(f"Excluded {excluded_count} records with unmapped research_ids")
    
    logger.info(f"Generating matrix CSV with {len(filtered_records)} mapped records")
    csv_content = self._create_csv_content(filtered_records)
    csv_key = await self._upload_csv_to_s3(csv_content, output_folder)
```

**Before Product Item CSV Generation (line ~301):**
```python
# Step 6: Filter unmapped research_ids and convert product items
product_item_key = None
if all_product_items:
    logger.info(f"Filtering and converting research_ids to product codes for {len(all_product_items)} product items")
    filtered_product_items = self._filter_and_convert_product_item_research_ids(all_product_items)
    
    excluded_count = len(all_product_items) - len(filtered_product_items)
    if excluded_count > 0:
        logger.warning(f"Excluded {excluded_count} product items with unmapped research_ids")
    
    unique_product_items = self._deduplicate_product_items(filtered_product_items)
    # ... rest of existing logic
```

**New Helper Methods:**
```python
def _filter_and_convert_record_research_ids(self, records: List[Record]) -> List[Record]:
    """Filter out unmapped research_ids and convert remaining to 3-character codes."""
    filtered_records = []
    for record in records:
        converted_code = self.lookup_tables.product_code_mapper.convert_research_id(record.item)
        if converted_code:  # Only include records with mapped research_ids
            # Create new record with converted item code
            converted_record = Record(
                geocode=record.geocode,
                tax_auth_id=record.tax_auth_id,
                group=record.group,
                item=converted_code,  # ← 3-CHARACTER PADDED CODE HERE
                customer=record.customer,
                provider=record.provider,
                transaction=record.transaction,
                taxable=record.taxable,
                tax_type=record.tax_type,
                tax_cat=record.tax_cat,
                effective=record.effective,
                per_taxable_type=record.per_taxable_type,
                percent_taxable=record.percent_taxable
            )
            filtered_records.append(converted_record)
        # Unmapped IDs are automatically tracked in ProductCodeMapper
    return filtered_records

def _filter_and_convert_product_item_research_ids(self, product_items: List[ProductItem]) -> List[ProductItem]:
    """Filter out unmapped research_ids and convert remaining to 3-character codes."""
    filtered_items = []
    for item in product_items:
        converted_code = self.lookup_tables.product_code_mapper.convert_research_id(item.item)
        if converted_code:  # Only include items with mapped research_ids
            # Create new ProductItem with converted item code
            converted_item = ProductItem(converted_code, item.description)
            filtered_items.append(converted_item)
        # Unmapped IDs are automatically tracked in ProductCodeMapper
    return filtered_items
```

### 4. Enhanced Error Tracking

**File:** `src/orchestrator.py` - Update error logging (line ~324)

```python
# Step 7: Upload error log with unmapped/excluded research_ids
all_errors = []

# ... existing error collection ...

# Add unmapped research_id errors (records excluded from output)
unmapped_ids = self.lookup_tables.product_code_mapper.get_unmapped_ids()
if unmapped_ids:
    all_errors.append({
        "error_type": "ExcludedUnmappedResearchIds",
        "count": len(unmapped_ids),
        "research_ids": sorted(list(unmapped_ids)),
        "message": f"{len(unmapped_ids)} research_ids could not be mapped to product codes and were excluded from output files",
        "impact": "These records do not appear in matrix_update.csv or product_item_update.csv"
    })

error_key = await self._upload_errors_to_s3(all_errors, output_folder)
```

### 5. File Changes Summary

| File | Change Type | Description |
|------|-------------|-------------|
| `src/product_code_mapper.py` | **NEW** | Mapping service for research_id → 3-char conversion |
| `src/models.py` | **MODIFY** | Add ProductCodeMapper to LookupTables |
| `src/orchestrator.py` | **MODIFY** | Add conversion methods and integrate before CSV output |
| `mapping/product_code_mapping.csv` | **EXISTS** | Lookup table (already present) |

### 6. Testing Strategy

**New Tests Required:**
- `test_product_code_mapper.py` - Test mapping service
- `test_conversion_integration.py` - Test end-to-end conversion
- **Update existing tests** - Account for converted output format

**Validation Points:**
- Verify all existing hierarchy logic still works with research_ids during processing
- Confirm CSV output contains exactly 3-character padded codes (e.g., "005", "022", "123")
- Test hierarchy normalization handles trailing .0s correctly ("1.1.1.4.3.0.0.0" matches "1.1.1.4.3")
- Test error handling for unmapped research_ids (complete exclusion from output)
- Validate excluded records are properly tracked in errors.json
- Confirm no data corruption during filtering and conversion

## Implementation Sequence

1. **Phase 1**: Create `ProductCodeMapper` service with tests
2. **Phase 2**: Integrate mapper into `LookupTables` 
3. **Phase 3**: Add conversion methods to orchestrator
4. **Phase 4**: Update error tracking for unmapped IDs
5. **Phase 5**: Update and validate existing tests
6. **Phase 6**: Deploy and verify with test dataset

## Risk Mitigation

**Low Risk Approach:**
- All existing business logic remains unchanged during processing
- Research IDs preserved throughout processing pipeline until final output
- Filtering and conversion only at final output stage
- Comprehensive error tracking for excluded unmapped IDs
- Hierarchy normalization handles truncated .0s safely
- Complete data integrity through exclusion rather than partial conversion

## Implementation Requirements (Based on User Feedback)

1. **Hierarchy Normalization**: Research IDs in files have 8+ levels but mapping file has truncated trailing .0s - implement normalization for matching

2. **Row Exclusion**: Unmapped research_ids must be completely excluded from output files (not just tracked as errors)

3. **Code Padding**: Ensure all item codes are exactly 3 characters with leading zero padding (e.g., "5" → "005")

4. **No Feature Flag**: This will be standard behavior for all future runs

5. **Performance Acceptable**: <5% performance impact is acceptable but optimize for efficiency

## Success Criteria

- ✅ `matrix_update.csv` and `product_item_update.csv` contain exactly 3-character padded codes instead of research_ids
- ✅ All existing hierarchy and business logic continues to work unchanged during processing
- ✅ Research ID normalization correctly handles trailing .0s for matching ("1.1.1.4.3.0.0.0" matches "1.1.1.4.3")
- ✅ Unmapped research_ids are completely excluded from output files (no partial data)
- ✅ Excluded research_ids are tracked in `errors.json` with complete list and impact description
- ✅ All item codes are padded to exactly 3 characters ("5" → "005", "22" → "022", "123" → "123")
- ✅ No data corruption during filtering and conversion process
- ✅ Performance impact is minimal (< 5% increase in processing time)
- ✅ All existing tests pass with minimal updates