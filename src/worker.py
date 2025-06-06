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
from .models import Record, ProcessingError, LookupTables

logger = logging.getLogger(__name__)


class SheetWorker:
    """Worker for processing individual Google Sheets files."""
    
    def __init__(self, row_mapper: RowMapper):
        # Create a dedicated SheetsClient for this worker instance
        self.sheets_client = SheetsClient()
        self.row_mapper = row_mapper
        
        # Create a unique worker ID for logging
        self.worker_id = str(uuid.uuid4())[:8]
    
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
            
            # Process rows using the mapper's sheet-level processing
            records, geocode_error = self.row_mapper.process_sheet_rows(
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
                    'rows_processed': 0
                }
            
            # Count rows that matched the admin filter
            admin_col_idx = header_mapping.get('admin')
            rows_processed = 0
            if admin_col_idx is not None:
                for row_data in sheet_data:
                    if len(row_data) > admin_col_idx:
                        admin_value = str(row_data[admin_col_idx]).strip() if row_data[admin_col_idx] else ""
                        if admin_value == config.admin_filter_value:
                            rows_processed += 1

            logger.info(f"{file_name}: Processed {rows_processed} rows, generated {len(records)} records")
            
            elapsed_time = time.time() - start_time
            logger.info(f"✅ Worker[{self.worker_id}] Completed: {file_name} in {elapsed_time:.2f}s")
            
            return {
                'file_id': file_id,
                'file_name': file_name,
                'success': True,
                'records': records,
                'rows_processed': rows_processed
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
                'rows_processed': 0
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
        List of processing results
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