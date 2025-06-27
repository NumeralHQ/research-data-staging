"""Worker module for processing individual Google Sheets files."""

import asyncio
import logging
import time
from typing import List, Dict, Any, Optional
import uuid

from .config import Config
from .config import config
from .sheets_client import SheetsClient
from .mapper import RowMapper
from .models import Record, ProductItem, ProcessingError, LookupTables

logger = logging.getLogger(__name__)


class SheetWorker:
    """Worker for processing individual Google Sheets files."""
    
    def __init__(self, row_mapper: RowMapper):
        # Create a dedicated SheetsClient for this worker instance
        self.sheets_client = SheetsClient()
        self.row_mapper = row_mapper
        
        # Create a unique worker ID for logging
        self.worker_id = str(uuid.uuid4())[:8]
    
    def _parse_hierarchical_id(self, item_id: str) -> List[str]:
        """
        Parse hierarchical ID and generate parent IDs.
        
        Args:
            item_id: ID like "1.1.1.4.3.0.0.0"
            
        Returns:
            List of parent IDs in order: ["1.0.0.0.0.0.0.0", "1.1.0.0.0.0.0.0", ...]
        """
        try:
            # Split the ID into parts
            parts = item_id.split('.')
            
            # Ensure we have 8 parts (pad with zeros if needed)
            while len(parts) < 8:
                parts.append('0')
            
            # Only use first 8 parts if there are more
            parts = parts[:8]
            
            parent_ids = []
            
            # Generate parent IDs by keeping first N parts and zeroing the rest
            for level in range(1, len(parts)):
                # Skip if this level is already zero (no parent at this level)
                if parts[level] == '0':
                    continue
                    
                # Create parent ID: keep first 'level' parts, zero the rest
                parent_parts = parts[:level] + ['0'] * (8 - level)
                parent_id = '.'.join(parent_parts)
                
                # Only add if it's not the same as the original ID
                if parent_id != item_id:
                    parent_ids.append(parent_id)
            
            return parent_ids
            
        except Exception as e:
            logger.warning(f"Error parsing hierarchical ID '{item_id}': {e}")
            return []
    
    def _build_description_lookup(self, sheet_data: List[List[Any]], header_mapping: Dict[str, int]) -> Dict[str, str]:
        """
        Build lookup dictionary mapping item_id to description for all rows in the sheet.
        
        Args:
            sheet_data: All rows from the sheet
            header_mapping: Column index mapping
            
        Returns:
            Dictionary mapping item_id to concatenated description from columns C:J
        """
        lookup_dict = {}
        current_id_col_idx = header_mapping.get('current_id')
        
        if current_id_col_idx is None:
            logger.warning("Current ID column not found, cannot build description lookup")
            return lookup_dict
        
        for row_idx, row_data in enumerate(sheet_data):
            try:
                # Extract Current ID
                item_id = ""
                if len(row_data) > current_id_col_idx and row_data[current_id_col_idx]:
                    item_id = str(row_data[current_id_col_idx]).strip()
                
                if not item_id:
                    continue  # Skip rows with empty Current ID
                
                # Extract description from columns C:J (indices 2-9)
                description_parts = []
                for col_idx in range(2, 10):  # Columns C through J (indices 2-9)
                    if len(row_data) > col_idx and row_data[col_idx]:
                        part = str(row_data[col_idx]).strip()
                        if part:
                            description_parts.append(part)
                
                # Direct concatenation with no separators
                description = "".join(description_parts).strip()
                
                # Store in lookup (even if empty - we'll handle that in hierarchical building)
                lookup_dict[item_id] = description
                    
            except Exception as e:
                logger.warning(f"Error building description lookup for row {row_idx}: {e}")
                continue
        
        logger.debug(f"Built description lookup with {len(lookup_dict)} entries")
        return lookup_dict
    
    def _build_hierarchical_description(self, item_id: str, lookup_dict: Dict[str, str]) -> str:
        """
        Build hierarchical description by combining parent descriptions with item's own description.
        
        Args:
            item_id: The item ID to build description for
            lookup_dict: Dictionary mapping item_id to description
            
        Returns:
            Hierarchical description like "Parent1 | Parent2 | Own Description"
        """
        try:
            # Get parent IDs in hierarchical order
            parent_ids = self._parse_hierarchical_id(item_id)
            
            # Build description parts
            description_parts = []
            
            # Add parent descriptions
            for parent_id in parent_ids:
                parent_desc = lookup_dict.get(parent_id, "").strip()
                if parent_desc:
                    description_parts.append(parent_desc)
                else:
                    # Missing or empty parent description - add space as requested
                    description_parts.append(" ")
            
            # Add own description
            own_desc = lookup_dict.get(item_id, "").strip()
            if own_desc:
                description_parts.append(own_desc)
            else:
                # Empty own description - add space as requested
                description_parts.append(" ")
            
            # Join with " | " separator
            hierarchical_description = " | ".join(description_parts)
            
            logger.debug(f"Built hierarchical description for {item_id}: '{hierarchical_description}'")
            return hierarchical_description
            
        except Exception as e:
            logger.warning(f"Error building hierarchical description for '{item_id}': {e}")
            # Fallback to original description
            return lookup_dict.get(item_id, " ")
    
    def _extract_product_items_from_rows(self, sheet_data: List[List[Any]], header_mapping: Dict[str, int], file_name: str) -> List[ProductItem]:
        """Extract product items from sheet rows that match the admin filter with hierarchical descriptions."""
        product_items = []
        
        # Get column indices
        admin_col_idx = header_mapping.get('admin')
        current_id_col_idx = header_mapping.get('current_id')
        business_use_col_idx = header_mapping.get('business_use')
        personal_use_col_idx = header_mapping.get('personal_use')
        
        if admin_col_idx is None:
            logger.warning(f"{file_name}: Admin column not found in headers")
            return product_items
            
        if current_id_col_idx is None:
            logger.warning(f"{file_name}: Current ID column not found in headers")
            return product_items
        
        # Define uncertain taxable values (same as in mapper.py)
        uncertain_taxable_values = {'DRILL DOWN', 'TO RESEARCH'}
        
        logger.info(f"{file_name}: Extracting product items from rows (Admin col: {admin_col_idx}, Current ID col: {current_id_col_idx})")
        
        # Step 1: Build description lookup for all rows in the sheet (for hierarchical descriptions)
        logger.debug(f"{file_name}: Building description lookup for hierarchical descriptions")
        description_lookup = self._build_description_lookup(sheet_data, header_mapping)
        
        # Step 2: Extract product items with hierarchical descriptions
        for row_idx, row_data in enumerate(sheet_data):
            try:
                # Check admin filter first
                admin_value = ""
                if len(row_data) > admin_col_idx and row_data[admin_col_idx]:
                    admin_value = str(row_data[admin_col_idx]).strip()
                
                if admin_value != config.admin_filter_value:
                    continue  # Skip rows that don't match the admin filter
                
                # Extract Current ID (Column B, index 1, but using header mapping)
                item_id = ""
                if len(row_data) > current_id_col_idx and row_data[current_id_col_idx]:
                    item_id = str(row_data[current_id_col_idx]).strip()
                
                if not item_id:
                    continue  # Skip rows with empty Current ID
                
                # Check taxable status for both business and personal use
                # Skip rows with uncertain taxable values to maintain consistency with matrix records
                should_skip = False
                
                if business_use_col_idx is not None and len(row_data) > business_use_col_idx and row_data[business_use_col_idx]:
                    business_use = str(row_data[business_use_col_idx]).strip().upper()
                    if business_use in uncertain_taxable_values:
                        logger.debug(f"{file_name}: Skipping product item for {item_id} - uncertain business taxable status '{row_data[business_use_col_idx]}' (skipped for tax safety)")
                        should_skip = True
                
                if personal_use_col_idx is not None and len(row_data) > personal_use_col_idx and row_data[personal_use_col_idx]:
                    personal_use = str(row_data[personal_use_col_idx]).strip().upper()
                    if personal_use in uncertain_taxable_values:
                        logger.debug(f"{file_name}: Skipping product item for {item_id} - uncertain personal taxable status '{row_data[personal_use_col_idx]}' (skipped for tax safety)")
                        should_skip = True
                
                if should_skip:
                    continue  # Skip this row entirely due to uncertain taxable status
                
                # Build hierarchical description for this item
                hierarchical_description = self._build_hierarchical_description(item_id, description_lookup)
                
                if not hierarchical_description or hierarchical_description.strip() == "":
                    continue  # Skip rows with completely empty hierarchical description
                
                # Create ProductItem with hierarchical description
                product_item = ProductItem(item_id, hierarchical_description)
                if product_item.is_valid():
                    product_items.append(product_item)
                    
            except Exception as e:
                logger.warning(f"{file_name}: Error processing row {row_idx + config.header_row + 1} for product items: {e}")
                continue
        
        logger.info(f"{file_name}: Extracted {len(product_items)} product items with hierarchical descriptions")
        return product_items
    
    async def process_sheet(self, file_info: Dict[str, Any], header_mapping: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
        """
        Process a single Google Sheets file.
        
        Args:
            file_info: File metadata from Drive API
            header_mapping: Optional pre-computed header mapping to reuse
            
        Returns:
            Dictionary with processing results including both matrix records and product items
        """
        file_id = file_info['id']
        file_name = file_info['name']
        
        start_time = time.time()
        logger.info(f"🚀 Worker[{self.worker_id}] Starting: {file_name} ({file_id})")
        
        try:
            # Get header mapping (use provided one or fetch new)
            if header_mapping is None:
                header_mapping = await self.sheets_client.get_header_mapping(
                    file_id, 
                    config.sheet_name, 
                    config.header_row
                )
            
            if not header_mapping:
                error_msg = f"No headers found in {config.sheet_name} tab at row {config.header_row}"
                logger.error(f"{file_name}: {error_msg}")
                return {
                    'file_id': file_id,
                    'file_name': file_name,
                    'success': False,
                    'error': error_msg,
                    'records': [],
                    'product_items': [],
                    'rows_processed': 0,
                    'processing_errors': []
                }
            
            # Get sheet data starting after header row
            data_start_row = config.header_row + 1
            sheet_data = await self.sheets_client.get_sheet_data(
                file_id,
                config.sheet_name,
                data_start_row
            )
            
            if not sheet_data:
                logger.warning(f"{file_name}: No data rows found")
                return {
                    'file_id': file_id,
                    'file_name': file_name,
                    'success': True,
                    'records': [],
                    'product_items': [],
                    'rows_processed': 0,
                    'processing_errors': []
                }
            
            # Process rows for matrix records using the existing mapper
            records, geocode_error, processing_errors = self.row_mapper.process_sheet_rows(
                sheet_data,
                header_mapping,
                file_name,
                config
            )
            
            # If there was a geocode error, mark the sheet as failed
            if geocode_error:
                return {
                    'file_id': file_id,
                    'file_name': file_name,
                    'success': False,
                    'error': geocode_error,
                    'records': [],
                    'product_items': [],
                    'rows_processed': 0,
                    'processing_errors': processing_errors  # Include any processing errors collected before the geocode error
                }
            
            # Extract product items from the same sheet data
            product_items = self._extract_product_items_from_rows(sheet_data, header_mapping, file_name)
            
            # Count rows that matched the admin filter
            admin_col_idx = header_mapping.get('admin')
            rows_processed = 0
            if admin_col_idx is not None:
                for row_data in sheet_data:
                    if len(row_data) > admin_col_idx:
                        admin_value = str(row_data[admin_col_idx]).strip() if row_data[admin_col_idx] else ""
                        if admin_value == config.admin_filter_value:
                            rows_processed += 1

            logger.info(f"{file_name}: Processed {rows_processed} rows, generated {len(records)} matrix records, {len(product_items)} product items")
            
            elapsed_time = time.time() - start_time
            logger.info(f"✅ Worker[{self.worker_id}] Completed: {file_name} in {elapsed_time:.2f}s")
            
            return {
                'file_id': file_id,
                'file_name': file_name,
                'success': True,
                'records': records,
                'product_items': product_items,
                'rows_processed': rows_processed,
                'processing_errors': processing_errors
            }
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            error_msg = f"Failed to process sheet: {str(e)}"
            logger.error(f"❌ Worker[{self.worker_id}] Failed: {file_name} after {elapsed_time:.2f}s - {error_msg}")
            return {
                'file_id': file_id,
                'file_name': file_name,
                'success': False,
                'error': error_msg,
                'records': [],
                'product_items': [],
                'rows_processed': 0,
                'processing_errors': []
            }


async def process_sheets_concurrently(
    file_list: List[Dict[str, Any]], 
    row_mapper: RowMapper,
    max_concurrency: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Process multiple sheets concurrently with header mapping optimization.
    
    Args:
        file_list: List of file metadata from Drive API
        row_mapper: Row mapping logic
        max_concurrency: Maximum concurrent operations (defaults to config value)
        
    Returns:
        List of processing results including both matrix records and product items
    """
    if not file_list:
        logger.info("No files to process")
        return []
    
    max_concurrency = max_concurrency or config.max_concurrent_requests
    semaphore = asyncio.Semaphore(max_concurrency)
    
    logger.info(f"Processing {len(file_list)} sheets with max concurrency {max_concurrency}")
    
    # Optimization: Get header mapping from first file and reuse for all
    # Create a temporary SheetsClient just for header mapping
    temp_sheets_client = SheetsClient()
    header_mapping = None
    if file_list:
        first_file = file_list[0]
        try:
            logger.info(f"Getting header mapping from first file: {first_file['name']}")
            header_mapping = await temp_sheets_client.get_header_mapping(
                first_file['id'],
                config.sheet_name,
                config.header_row
            )
            logger.info(f"Header mapping obtained: {list(header_mapping.keys()) if header_mapping else 'None'}")
        except Exception as e:
            logger.warning(f"Failed to get header mapping from first file: {e}")
            header_mapping = None
    
    async def process_with_semaphore(file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single file with semaphore control."""
        logger.info(f"🔄 Acquiring semaphore for: {file_info['name']}")
        async with semaphore:
            logger.info(f"🎯 Processing (semaphore acquired): {file_info['name']}")
            worker = SheetWorker(row_mapper)
            result = await worker.process_sheet(file_info, header_mapping)
            logger.info(f"🔓 Releasing semaphore for: {file_info['name']}")
            return result
    
    # Process all files concurrently
    logger.info(f"🚀 Creating {len(file_list)} concurrent tasks...")
    tasks = [process_with_semaphore(file_info) for file_info in file_list]
    
    logger.info(f"⏳ Waiting for all {len(tasks)} tasks to complete...")
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Handle any exceptions that occurred
    processed_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            file_info = file_list[i]
            logger.error(f"Exception processing {file_info['name']}: {result}")
            processed_results.append({
                'file_id': file_info['id'],
                'file_name': file_info['name'],
                'success': False,
                'error': f"Exception: {str(result)}",
                'records': [],
                'product_items': [],
                'rows_processed': 0,
                'processing_errors': []
            })
        else:
            processed_results.append(result)
    
    # Log summary
    successful = sum(1 for r in processed_results if r['success'])
    total_records = sum(len(r['records']) for r in processed_results)
    total_product_items = sum(len(r.get('product_items', [])) for r in processed_results)
    total_rows = sum(r['rows_processed'] for r in processed_results)
    
    logger.info(f"Processing complete: {successful}/{len(file_list)} files successful, "
                f"{total_rows} rows processed, {total_records} matrix records generated, "
                f"{total_product_items} product items extracted")
    
    return processed_results 