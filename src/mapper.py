"""Row to CSV record conversion logic."""

import logging
import re
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Any, Optional, Tuple

from .models import Record, CustomerType, LookupTables, TaxableValue

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
            'TAXABLE': TaxableValue.TAXABLE.value,
            'DRILL DOWN': TaxableValue.DRILL_DOWN.value
        }
    
    def _get_cell_value(self, row: List[Any], index: Optional[int]) -> str:
        """Safely get cell value from row by index."""
        if index is None or index >= len(row):
            return ""
        
        value = row[index]
        return str(value).strip() if value is not None else ""
    
    def _parse_taxable_value(self, taxable_text: str) -> Optional[int]:
        """Parse taxable value from text."""
        if not taxable_text:
            return None
        
        taxable_upper = taxable_text.upper().strip()
        return self.taxable_mappings.get(taxable_upper)
    
    def _parse_percent_taxable(self, percent_text: str) -> Optional[Decimal]:
        """Parse percent taxable value, removing % and converting to decimal."""
        if not percent_text:
            return None
        
        try:
            # Remove % symbol and any whitespace
            clean_text = percent_text.strip().replace('%', '')
            
            # Convert to decimal and divide by 100
            percent_value = Decimal(clean_text)
            return percent_value / 100
            
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
    
    def convert_row_to_records(
        self, 
        row: List[Any], 
        header_map: Dict[str, int], 
        filename: str,
        config
    ) -> Tuple[Optional[Record], Optional[Record]]:
        """
        Convert a single Google Sheets row to Business and Personal CSV records.
        
        Args:
            row: List of cell values from the spreadsheet row
            header_map: Mapping of column names to indices
            filename: Name of the source file (for geocode lookup)
            config: Configuration object
            
        Returns:
            Tuple of (business_record, personal_record), either can be None if invalid
        """
        try:
            # Get geocode from filename
            geocode = self.lookup_tables.get_geocode_for_filename(filename)
            if not geocode:
                logger.error(f"Could not determine geocode for filename: {filename}")
                return None, None
            
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
            
            # Create business record
            business_record = None
            if business_use:  # Only create if business use has a value
                try:
                    business_record = Record(
                        geocode=geocode,
                        tax_auth_id=None,
                        item=current_id,
                        customer=CustomerType.BUSINESS.value,
                        taxable=self._parse_taxable_value(business_use),
                        tax_cat=self._get_tax_cat_code(business_tax_cat_desc),
                        percent_taxable=self._parse_percent_taxable(business_percent_tax)
                    )
                except Exception as e:
                    logger.error(f"Error creating business record for {current_id}: {e}")
            
            # Create personal record
            personal_record = None
            if personal_use:  # Only create if personal use has a value
                try:
                    personal_record = Record(
                        geocode=geocode,
                        tax_auth_id=None,
                        item=current_id,
                        customer=CustomerType.PERSONAL.value,
                        taxable=self._parse_taxable_value(personal_use),
                        tax_cat=self._get_tax_cat_code(personal_tax_cat_desc),
                        percent_taxable=self._parse_percent_taxable(personal_percent_tax)
                    )
                except Exception as e:
                    logger.error(f"Error creating personal record for {current_id}: {e}")
            
            return business_record, personal_record
            
        except Exception as e:
            logger.error(f"Error converting row to records: {e}")
            return None, None
    
    def process_sheet_rows(
        self, 
        rows: List[List[Any]], 
        header_map: Dict[str, int], 
        filename: str,
        config
    ) -> List[Record]:
        """
        Process all rows from a sheet and return valid records.
        
        Args:
            rows: List of rows from the spreadsheet
            header_map: Mapping of column names to indices
            filename: Name of the source file
            config: Configuration object
            
        Returns:
            List of valid Record objects
        """
        records = []
        admin_index = header_map.get('admin')
        
        if admin_index is None:
            logger.error(f"Admin column not found in header map for {filename}")
            return records
        
        for row_idx, row in enumerate(rows, start=1):
            try:
                # Check if this row has the admin match value
                admin_value = self._get_cell_value(row, admin_index)
                
                if admin_value.upper() != config.admin_match_value.upper():
                    continue  # Skip rows that don't match
                
                # Convert row to records
                business_record, personal_record = self.convert_row_to_records(
                    row, header_map, filename, config
                )
                
                # Add valid records
                if business_record:
                    records.append(business_record)
                
                if personal_record:
                    records.append(personal_record)
                
                if business_record or personal_record:
                    logger.debug(f"Processed row {row_idx} from {filename}")
                
            except Exception as e:
                logger.error(f"Error processing row {row_idx} in {filename}: {e}")
                continue
        
        logger.info(f"Processed {len(records)} records from {filename}")
        return records 