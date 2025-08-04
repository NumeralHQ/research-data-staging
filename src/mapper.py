"""Row to CSV record conversion logic."""

import logging
import re
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Any, Optional, Tuple

from .models import Record, CustomerType, LookupTables, TaxableValue, GroupType, ProviderType, TransactionType, TaxType, PerTaxableType, ProcessingError

logger = logging.getLogger(__name__)


class RowMapper:
    """Converts Google Sheets rows to CSV records."""
    
    def __init__(self, lookup_tables: LookupTables):
        self.lookup_tables = lookup_tables
        
        # Taxable value mappings
        self.taxable_mappings = {
            'NOT TAXABLE': TaxableValue.NOT_TAXABLE.value,
            'NONTAXABLE': TaxableValue.NOT_TAXABLE.value,
            'EXEMPT': TaxableValue.NOT_TAXABLE.value,
            'TAXABLE': TaxableValue.TAXABLE.value
        }
        
        # Values that indicate uncertainty - these will be skipped to avoid incorrect tax calculations
        self.uncertain_taxable_values = {
            'DRILL DOWN',
            'TO RESEARCH'
        }
        
        # Collect errors during processing
        self.processing_errors = []
        
        # Track missing tax type issues for summarization (per file)
        # Structure: {filename: {(geocode, tax_cat): [list of record items]}}
        self.missing_tax_type_issues = {}
    
    def _clear_errors(self):
        """Clear errors at the start of processing a new file."""
        self.processing_errors = []
        self.missing_tax_type_issues = {}  # Also clear tax type issues
    
    def _add_error(self, filename: str, error_message: str, error_type: str = "DataQualityError"):
        """Add an error to the collection."""
        error = ProcessingError(filename, error_message, error_type)
        self.processing_errors.append(error)
        logger.error(f"Data quality error in {filename}: {error_message}")
    
    def _track_missing_tax_type_issue(self, filename: str, geocode: str, tax_cat: str, record_item: str):
        """Track a missing tax type issue for later summarization."""
        if filename not in self.missing_tax_type_issues:
            self.missing_tax_type_issues[filename] = {}
        
        key = (geocode, tax_cat)
        if key not in self.missing_tax_type_issues[filename]:
            self.missing_tax_type_issues[filename][key] = []
        
        self.missing_tax_type_issues[filename][key].append(record_item)
    
    def _create_missing_tax_type_summary_errors(self):
        """Create summarized errors for missing tax type issues."""
        for filename, issues_by_key in self.missing_tax_type_issues.items():
            for (geocode, tax_cat), affected_items in issues_by_key.items():
                count = len(affected_items)
                
                # Create a sample of affected items (max 5 for readability)
                sample_items = affected_items[:5]
                sample_text = ", ".join(sample_items)
                if count > 5:
                    sample_text += f" ... and {count - 5} more"
                
                error_message = (f"No tax types found for geocode='{geocode}', tax_cat='{tax_cat}' - "
                               f"excluded {count} records from output. "
                               f"Affected items: {sample_text}")
                
                self._add_error(filename, error_message, "MissingTaxTypeError")
    
    def _get_cell_value(self, row: List[Any], index: Optional[int]) -> str:
        """Safely get cell value from row by index."""
        if index is None or index >= len(row):
            return ""
        
        value = row[index]
        return str(value).strip() if value is not None else ""
    
    def _parse_taxable_value(self, taxable_text: str, filename: str = "unknown_file") -> Optional[int]:
        """Parse taxable value from text."""
        if not taxable_text:
            return None
        
        taxable_upper = taxable_text.upper().strip()
        
        # Check if it's a known mappable value
        mapped_value = self.taxable_mappings.get(taxable_upper)
        if mapped_value is not None:
            return mapped_value
        
        # Check if it's an uncertain value that should be skipped for tax safety
        if taxable_upper in self.uncertain_taxable_values:
            logger.debug(f"Skipping taxable value '{taxable_text}' - uncertain status, avoiding potential tax calculation errors")
            return None
        
        # Truly unknown value - this is a data quality error that should be tracked
        error_message = f"Unknown taxable value '{taxable_text}' (normalized: '{taxable_upper}'). Known values: {list(self.taxable_mappings.keys())}. Uncertain values (skipped): {list(self.uncertain_taxable_values)}"
        self._add_error(filename, error_message, "UnknownTaxableValueError")
        return None
    
    def _parse_percent_taxable(self, percent_text: str) -> Optional[Decimal]:
        """Parse percent taxable value, removing % symbol if present."""
        if not percent_text:
            return None
        
        try:
            # Check if the value contains a % symbol
            if '%' in percent_text:
                # Remove % symbol and any whitespace, then divide by 100
                clean_text = percent_text.strip().replace('%', '')
                percent_value = Decimal(clean_text) / 100
            else:
                # No % symbol, treat as already in decimal form
                percent_value = Decimal(percent_text.strip())
            
            return percent_value
            
        except (InvalidOperation, ValueError) as e:
            logger.warning(f"Could not parse percent value '{percent_text}': {e}")
            return None
    
    def _get_tax_cat_code(self, tax_cat_desc: str) -> str:
        """Get tax category code from description, with fallback."""
        if not tax_cat_desc:
            return "00"  # Default fallback
        
        code = self.lookup_tables.get_tax_cat_code(tax_cat_desc)
        if code is None:
            logger.warning(f"No tax category code found for '{tax_cat_desc}', using default '00'")
            return "00"
        
        return code
    
    def _expand_records_by_tax_types(self, records: List[Optional[Record]], geocode: str, filename: str = "unknown_file") -> List[Record]:
        """Expand list of records by creating one copy per tax_type for each record's geocode+tax_cat combination."""
        if not records:
            return []
        
        expanded_records = []
        for record in records:
            if record is None:
                continue
                
            # Get tax types specific to this record's geocode+tax_cat combination using hierarchy fallback
            tax_types = self.lookup_tables.get_tax_types_with_hierarchy_fallback(record.geocode, record.tax_cat)
            
            # If no tax types found (None returned), exclude this record and collect for summary
            if tax_types is None:
                # Track this issue for summarization
                self._track_missing_tax_type_issue(filename, record.geocode, record.tax_cat, record.item)
                # Log as warning for immediate CloudWatch visibility
                logger.warning(f"No tax types found for geocode='{record.geocode}', tax_cat='{record.tax_cat}' - excluding record {record.item} from output")
                continue
                
            # Create one copy of this record for each applicable tax_type
            for tax_type in tax_types:
                # Create a new record with the same data but different tax_type
                expanded_record = Record(
                    geocode=record.geocode,
                    tax_auth_id=record.tax_auth_id,
                    group=record.group,
                    item=record.item,
                    customer=record.customer,
                    provider=record.provider,
                    transaction=record.transaction,
                    taxable=record.taxable,
                    tax_type=tax_type,  # This is the only field that changes
                    tax_cat=record.tax_cat,
                    effective=record.effective,
                    per_taxable_type=record.per_taxable_type,
                    percent_taxable=record.percent_taxable
                )
                expanded_records.append(expanded_record)
            
            logger.debug(f"Expanded record {record.item} (geocode={record.geocode}, customer={record.customer}, tax_cat={record.tax_cat}) into {len(tax_types)} records using tax types: {tax_types}")
        
        total_templates = len([r for r in records if r is not None])
        logger.debug(f"Expanded {total_templates} record templates into {len(expanded_records)} final records")
        return expanded_records
    
    def convert_row_to_records(
        self, 
        row: List[Any], 
        header_map: Dict[str, int], 
        geocode: str,  # Pass geocode as parameter instead of looking it up
        config,
        filename: str = "unknown_file"
    ) -> Tuple[Optional[Record], Optional[Record]]:
        """
        Convert a single Google Sheets row to Business and Personal CSV records.
        
        Args:
            row: List of cell values from the spreadsheet row
            header_map: Mapping of column names to indices
            geocode: Pre-determined geocode for this sheet
            config: Configuration object
            
        Returns:
            Tuple of (business_record, personal_record), either can be None if invalid
        """
        try:
            # Extract common values
            current_id = self._get_cell_value(row, header_map.get('current_id'))
            if not current_id:
                logger.warning("No Current ID found in row, skipping")
                return None, None
            
            # Extract business values
            business_use = self._get_cell_value(row, header_map.get('business_use'))
            business_tax_cat_desc = self._get_cell_value(row, header_map.get('business_tax_cat'))
            business_percent_tax = self._get_cell_value(row, header_map.get('business_percent_tax'))
            
            # Extract personal values
            personal_use = self._get_cell_value(row, header_map.get('personal_use'))
            personal_tax_cat_desc = self._get_cell_value(row, header_map.get('personal_tax_cat'))
            personal_percent_tax = self._get_cell_value(row, header_map.get('personal_percent_tax'))
            
            # Parse and validate business values
            business_taxable = None
            business_percent = None
            business_tax_cat_code = None
            business_valid = False
            
            if business_use:  # Only parse if business use has a value
                try:
                    business_taxable = self._parse_taxable_value(business_use, filename)
                    business_percent = self._parse_percent_taxable(business_percent_tax)
                    business_tax_cat_code = self._get_tax_cat_code(business_tax_cat_desc)
                    
                    if business_taxable is not None and business_percent is not None:
                        business_valid = True
                    else:
                        # Log why business record would be skipped
                        reasons = []
                        if business_taxable is None:
                            if business_use.upper().strip() in self.uncertain_taxable_values:
                                reasons.append(f"uncertain taxable status '{business_use}' (skipped for tax safety)")
                            else:
                                reasons.append(f"unparseable taxable value '{business_use}'")
                        if business_percent is None:
                            reasons.append(f"unparseable percent value '{business_percent_tax}'")
                        
                        logger.debug(f"Skipping business record for {current_id}: {' and '.join(reasons)}")
                except Exception as e:
                    logger.error(f"Error parsing business values for {current_id}: {e}")
            
            # Parse and validate personal values
            personal_taxable = None
            personal_percent = None
            personal_tax_cat_code = None
            personal_valid = False
            
            if personal_use:  # Only parse if personal use has a value
                try:
                    personal_taxable = self._parse_taxable_value(personal_use, filename)
                    personal_percent = self._parse_percent_taxable(personal_percent_tax)
                    personal_tax_cat_code = self._get_tax_cat_code(personal_tax_cat_desc)
                    
                    if personal_taxable is not None and personal_percent is not None:
                        personal_valid = True
                    else:
                        # Log why personal record would be skipped
                        reasons = []
                        if personal_taxable is None:
                            if personal_use.upper().strip() in self.uncertain_taxable_values:
                                reasons.append(f"uncertain taxable status '{personal_use}' (skipped for tax safety)")
                            else:
                                reasons.append(f"unparseable taxable value '{personal_use}'")
                        if personal_percent is None:
                            reasons.append(f"unparseable percent value '{personal_percent_tax}'")
                        
                        logger.debug(f"Skipping personal record for {current_id}: {' and '.join(reasons)}")
                except Exception as e:
                    logger.error(f"Error parsing personal values for {current_id}: {e}")
            
            # Determine which records to create based on validity and deduplication logic
            business_record = None
            personal_record = None
            
            if business_valid and personal_valid:
                # Both are valid - check if they have identical tax treatment
                # Compare all three critical tax treatment components: taxable, tax_cat, and percent_taxable
                treatment_identical = (
                    business_taxable == personal_taxable and 
                    business_tax_cat_code == personal_tax_cat_code and
                    business_percent == personal_percent
                )
                
                if treatment_identical:
                    # Create only personal (99) record to avoid duplication
                    logger.debug(f"Tax treatment identical for {current_id} - creating only general (99) customer record")
                    personal_record = Record(
                        geocode=geocode,
                        tax_auth_id="",
                        group=GroupType.DEFAULT.value,
                        item=current_id,
                        customer=CustomerType.PERSONAL.value,  # "99"
                        provider=ProviderType.DEFAULT.value,
                        transaction=TransactionType.DEFAULT.value,
                        taxable=personal_taxable,
                        tax_type=TaxType.DEFAULT.value,
                        tax_cat=personal_tax_cat_code,
                        effective=config.effective_date,
                        per_taxable_type=PerTaxableType.DEFAULT.value,
                        percent_taxable=f"{personal_percent:.6f}"
                    )
                else:
                    # Create both records - different tax treatment
                    logger.debug(f"Different tax treatment for {current_id} - creating both 0B and 99 customer records")
                    business_record = Record(
                        geocode=geocode,
                        tax_auth_id="",
                        group=GroupType.DEFAULT.value,
                        item=current_id,
                        customer=CustomerType.BUSINESS.value,  # "BB"
                        provider=ProviderType.DEFAULT.value,
                        transaction=TransactionType.DEFAULT.value,
                        taxable=business_taxable,
                        tax_type=TaxType.DEFAULT.value,
                        tax_cat=business_tax_cat_code,
                        effective=config.effective_date,
                        per_taxable_type=PerTaxableType.DEFAULT.value,
                        percent_taxable=f"{business_percent:.6f}"
                    )
                    personal_record = Record(
                        geocode=geocode,
                        tax_auth_id="",
                        group=GroupType.DEFAULT.value,
                        item=current_id,
                        customer=CustomerType.PERSONAL.value,  # "99"
                        provider=ProviderType.DEFAULT.value,
                        transaction=TransactionType.DEFAULT.value,
                        taxable=personal_taxable,
                        tax_type=TaxType.DEFAULT.value,
                        tax_cat=personal_tax_cat_code,
                        effective=config.effective_date,
                        per_taxable_type=PerTaxableType.DEFAULT.value,
                        percent_taxable=f"{personal_percent:.6f}"
                    )
            elif business_valid:
                # Only business is valid - create business record only
                business_record = Record(
                    geocode=geocode,
                    tax_auth_id="",
                    group=GroupType.DEFAULT.value,
                    item=current_id,
                    customer=CustomerType.BUSINESS.value,  # "BB"
                    provider=ProviderType.DEFAULT.value,
                    transaction=TransactionType.DEFAULT.value,
                    taxable=business_taxable,
                    tax_type=TaxType.DEFAULT.value,
                    tax_cat=business_tax_cat_code,
                    effective=config.effective_date,
                    per_taxable_type=PerTaxableType.DEFAULT.value,
                    percent_taxable=f"{business_percent:.6f}"
                )
            elif personal_valid:
                # Only personal is valid - create personal record only
                personal_record = Record(
                    geocode=geocode,
                    tax_auth_id="",
                    group=GroupType.DEFAULT.value,
                    item=current_id,
                    customer=CustomerType.PERSONAL.value,  # "99"
                    provider=ProviderType.DEFAULT.value,
                    transaction=TransactionType.DEFAULT.value,
                    taxable=personal_taxable,
                    tax_type=TaxType.DEFAULT.value,
                    tax_cat=personal_tax_cat_code,
                    effective=config.effective_date,
                    per_taxable_type=PerTaxableType.DEFAULT.value,
                    percent_taxable=f"{personal_percent:.6f}"
                )
            
            return business_record, personal_record
            
        except Exception as e:
            logger.error(f"Error converting row to records: {e}")
            return None, None
    
    def _process_rows_for_geocode(
        self, 
        rows: List[List[Any]], 
        header_map: Dict[str, int], 
        geocode: str,
        config,
        filename: str
    ) -> List[Record]:
        """Process all rows for a specific geocode (extracted from existing logic)."""
        records = []
        admin_index = header_map.get('admin')
        
        if admin_index is None:
            logger.error(f"Admin column not found in header map for {filename}")
            return records
        
        for row_idx, row in enumerate(rows, start=1):
            try:
                # Check if this row has the admin match value
                admin_value = self._get_cell_value(row, admin_index)
                
                if admin_value.upper() != config.admin_filter_value.upper():
                    continue  # Skip rows that don't match
                
                # Convert row to records (pass geocode as parameter)
                business_record, personal_record = self.convert_row_to_records(
                    row, header_map, geocode, config, filename
                )
                
                # Expand records by tax_types for this geocode
                expanded_records = self._expand_records_by_tax_types(
                    [business_record, personal_record], geocode, filename
                )
                
                # Add all expanded records
                records.extend(expanded_records)
                
                if expanded_records:
                    logger.debug(f"Processed row {row_idx} from {filename} (geocode: {geocode}) - created {len(expanded_records)} records")
                
            except Exception as e:
                logger.error(f"Error processing row {row_idx} in {filename} for geocode {geocode}: {e}")
                continue
        
        logger.debug(f"Processed {len(records)} records from {filename} for geocode {geocode}")
        return records
    
    def process_sheet_rows(
        self, 
        rows: List[List[Any]], 
        header_map: Dict[str, int], 
        filename: str,
        config
    ) -> Tuple[List[Record], Optional[str], List[ProcessingError]]:
        """
        Process all rows from a sheet and return valid records.
        
        Args:
            rows: List of rows from the spreadsheet
            header_map: Mapping of column names to indices
            filename: Name of the source file
            config: Configuration object
            
        Returns:
            Tuple of (List of valid Record objects, error_message if geocode lookup failed, List of processing errors)
        """
        # Clear any existing errors for this file
        self._clear_errors()
        
        # NEW: Get list of geocodes instead of single geocode
        geocodes = self.lookup_tables.get_geocodes_for_location(filename)
        if not geocodes:
            error_msg = f"Could not determine geocode(s) for filename: {filename}"
            logger.error(error_msg)
            return [], error_msg, self.processing_errors
        
        # Log file classification
        if len(geocodes) == 1 and geocodes[0].endswith("00000000"):
            logger.info(f"Processing STATE-level file '{filename}' → geocode: {geocodes[0]}")
        else:
            logger.info(f"Processing CITY-level file '{filename}' → {len(geocodes)} geocodes: {geocodes}")
        
        # Process records for each geocode
        all_records = []
        for geocode in geocodes:
            # Process rows for this specific geocode
            geocode_records = self._process_rows_for_geocode(rows, header_map, geocode, config, filename)
            all_records.extend(geocode_records)
        
        logger.info(f"Processed {len(all_records)} total records from {filename} across {len(geocodes)} geocode(s)")
        
        # Create summary errors for missing tax type issues before returning
        self._create_missing_tax_type_summary_errors()
        
        return all_records, None, self.processing_errors 