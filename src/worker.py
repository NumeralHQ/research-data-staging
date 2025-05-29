"""Worker module for processing individual Google Sheets files."""

import asyncio
import logging
from typing import List, Dict, Any, Optional

from .config import Config
from .config import config
from .sheets_client import SheetsClient
from .mapper import RowMapper
from .models import Record, ProcessingError, LookupTables

logger = logging.getLogger(__name__)


class SheetWorker:
    """Worker for processing individual Google Sheets files."""
    
    def __init__(self, sheets_client: SheetsClient, row_mapper: RowMapper):
        self.sheets_client = sheets_client
        self.row_mapper = row_mapper
    
    async def process_sheet(self, file_info: Dict[str, Any], header_mapping: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
        """
        Process a single Google Sheets file.
        
        Args:
            file_info: File metadata from Drive API
            header_mapping: Optional pre-computed header mapping to reuse
            
        Returns:
            Dictionary with processing results
        """
        file_id = file_info['id']
        file_name = file_info['name']
        
        logger.info(f"Processing sheet: {file_name} ({file_id})")
        
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
                    'rows_processed': 0
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
                    'rows_processed': 0
                }
            
            # Check for Admin column
            admin_col_idx = header_mapping.get('admin')
            if admin_col_idx is None:
                error_msg = f"Admin column not found in headers"
                logger.error(f"{file_name}: {error_msg}")
                return {
                    'file_id': file_id,
                    'file_name': file_name,
                    'success': False,
                    'error': error_msg,
                    'records': [],
                    'rows_processed': 0
                }
            
            # Process rows
            records = []
            rows_processed = 0
            
            for row_idx, row_data in enumerate(sheet_data):
                try:
                    # Check if row has enough columns
                    if len(row_data) <= admin_col_idx:
                        continue
                    
                    # Check Admin column filter
                    admin_value = str(row_data[admin_col_idx]).strip() if row_data[admin_col_idx] else ""
                    if admin_value != config.admin_filter_value:
                        continue
                    
                    # Map row to records
                    business_record, personal_record = self.row_mapper.convert_row_to_records(
                        row_data, 
                        header_mapping, 
                        file_name,
                        config
                    )
                    
                    # Add valid records to the list
                    if business_record:
                        records.append(business_record)
                    if personal_record:
                        records.append(personal_record)
                    rows_processed += 1
                    
                except Exception as e:
                    logger.warning(f"{file_name} row {data_start_row + row_idx}: Error processing row: {e}")
                    continue
            
            logger.info(f"{file_name}: Processed {rows_processed} rows, generated {len(records)} records")
            
            return {
                'file_id': file_id,
                'file_name': file_name,
                'success': True,
                'records': records,
                'rows_processed': rows_processed
            }
            
        except Exception as e:
            error_msg = f"Failed to process sheet: {str(e)}"
            logger.error(f"{file_name}: {error_msg}")
            return {
                'file_id': file_id,
                'file_name': file_name,
                'success': False,
                'error': error_msg,
                'records': [],
                'rows_processed': 0
            }


async def process_sheets_concurrently(
    file_list: List[Dict[str, Any]], 
    sheets_client: SheetsClient, 
    row_mapper: RowMapper,
    max_concurrency: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Process multiple sheets concurrently with header mapping optimization.
    
    Args:
        file_list: List of file metadata from Drive API
        sheets_client: Sheets API client
        row_mapper: Row mapping logic
        max_concurrency: Maximum concurrent operations (defaults to config value)
        
    Returns:
        List of processing results
    """
    if not file_list:
        logger.info("No files to process")
        return []
    
    max_concurrency = max_concurrency or config.max_concurrent_requests
    semaphore = asyncio.Semaphore(max_concurrency)
    
    logger.info(f"Processing {len(file_list)} sheets with max concurrency {max_concurrency}")
    
    # Optimization: Get header mapping from first file and reuse for all
    header_mapping = None
    if file_list:
        first_file = file_list[0]
        try:
            logger.info(f"Getting header mapping from first file: {first_file['name']}")
            header_mapping = await sheets_client.get_header_mapping(
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
        async with semaphore:
            worker = SheetWorker(sheets_client, row_mapper)
            return await worker.process_sheet(file_info, header_mapping)
    
    # Process all files concurrently
    tasks = [process_with_semaphore(file_info) for file_info in file_list]
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
                'rows_processed': 0
            })
        else:
            processed_results.append(result)
    
    # Log summary
    successful = sum(1 for r in processed_results if r['success'])
    total_records = sum(len(r['records']) for r in processed_results)
    total_rows = sum(r['rows_processed'] for r in processed_results)
    
    logger.info(f"Processing complete: {successful}/{len(file_list)} files successful, "
                f"{total_rows} rows processed, {total_records} records generated")
    
    return processed_results 