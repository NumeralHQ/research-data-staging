"""Main orchestrator for the research data aggregation service."""

import asyncio
import csv
import io
import json
import logging
import os
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
from pathlib import Path

import boto3
import pytz

from .config import config
from .drive_client import DriveClient
from .sheets_client import SheetsClient
from .worker import process_sheets_concurrently
from .mapper import RowMapper
from .models import Record, ProductItem, LookupTables

logger = logging.getLogger(__name__)


class ResearchDataOrchestrator:
    """Main orchestrator for processing Google Sheets and generating CSV output."""
    
    def __init__(self):
        self.lookup_tables = LookupTables(config.s3_bucket)
        self.drive_client = DriveClient()
        self.sheets_client = SheetsClient()
        self.row_mapper = RowMapper(self.lookup_tables)
        self.s3_client = boto3.client('s3')
        
        # Pacific timezone for timestamps
        self.pacific_tz = pytz.timezone('America/Los_Angeles')
    
    def _generate_output_folder(self) -> str:
        """Generate a timestamped output folder using Pacific Time."""
        now = datetime.now(self.pacific_tz)
        timestamp = now.strftime("%Y%m%d-%H%M")
        return f"output-{timestamp}"
    
    def _create_csv_content(self, records: List[Record]) -> str:
        """Create CSV content from matrix records."""
        output = io.StringIO()
        
        # Write header row manually since we're using custom quoting
        headers = Record.csv_headers()
        output.write(','.join(headers) + '\n')
        
        # Write data rows manually
        for record in records:
            csv_row = record.to_csv_row()
            output.write(','.join(csv_row) + '\n')
        
        content = output.getvalue()
        output.close()
        
        logger.info(f"Generated matrix CSV with {len(records)} records")
        return content
    
    def _deduplicate_product_items(self, product_items: List[ProductItem]) -> List[ProductItem]:
        """Remove duplicate product items, keeping first occurrence of each item ID."""
        seen_items: Set[str] = set()
        unique_items: List[ProductItem] = []
        
        for item in product_items:
            if item.item not in seen_items:
                seen_items.add(item.item)
                unique_items.append(item)
        
        logger.info(f"Deduplicated product items: {len(product_items)} -> {len(unique_items)} unique items")
        return unique_items
    
    def _create_product_item_csv_content(self, product_items: List[ProductItem]) -> str:
        """Create CSV content from product items."""
        output = io.StringIO()
        
        # Write header row
        headers = ProductItem.csv_headers()
        output.write(','.join(headers) + '\n')
        
        # Write data rows
        for item in product_items:
            csv_row = item.to_csv_row()
            output.write(','.join(csv_row) + '\n')
        
        content = output.getvalue()
        output.close()
        
        logger.info(f"Generated product item CSV with {len(product_items)} items")
        return content

    async def _upload_csv_to_s3(self, csv_content: str, output_folder: str) -> str:
        """Upload matrix CSV content to S3 and return the key."""
        key = f"{output_folder}/matrix_update.csv"
        
        try:
            self.s3_client.put_object(
                Bucket=config.s3_bucket,
                Key=key,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )
            
            logger.info(f"Successfully uploaded matrix CSV to s3://{config.s3_bucket}/{key}")
            return key
            
        except Exception as e:
            logger.error(f"Error uploading matrix CSV to S3: {e}")
            raise

    async def _upload_product_item_csv_to_s3(self, csv_content: str, output_folder: str) -> str:
        """Upload product item CSV content to S3 and return the key."""
        key = f"{output_folder}/product_item_update.csv"
        
        try:
            self.s3_client.put_object(
                Bucket=config.s3_bucket,
                Key=key,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )
            
            logger.info(f"Successfully uploaded product item CSV to s3://{config.s3_bucket}/{key}")
            return key
            
        except Exception as e:
            logger.error(f"Error uploading product item CSV to S3: {e}")
            raise

    async def _upload_static_files_to_s3(self, output_folder: str) -> List[str]:
        """Upload static data files from src/data directory to S3 output folder."""
        static_file_keys = []
        
        # Get the path to the data directory
        # In Lambda, this will be relative to the lambda function's working directory
        data_dir = Path(__file__).parent / "data"
        
        if not data_dir.exists():
            logger.warning(f"Static data directory not found: {data_dir}")
            return static_file_keys
        
        # Find all CSV files in the data directory
        csv_files = list(data_dir.glob("*.csv"))
        
        if not csv_files:
            logger.warning(f"No CSV files found in data directory: {data_dir}")
            return static_file_keys
        
        logger.info(f"Found {len(csv_files)} static CSV files to upload")
        
        for file_path in csv_files:
            try:
                # Read file content
                with open(file_path, 'r', encoding='utf-8') as f:
                    file_content = f.read()
                
                # Create S3 key using the original filename
                key = f"{output_folder}/{file_path.name}"
                
                # Upload to S3
                self.s3_client.put_object(
                    Bucket=config.s3_bucket,
                    Key=key,
                    Body=file_content.encode('utf-8'),
                    ContentType='text/csv'
                )
                
                static_file_keys.append(key)
                logger.info(f"Successfully uploaded static file to s3://{config.s3_bucket}/{key}")
                
            except Exception as e:
                logger.error(f"Error uploading static file {file_path.name}: {e}")
                # Continue with other files - don't let one failure stop the process
                continue
        
        return static_file_keys
    
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
    
    async def _upload_errors_to_s3(self, errors: List[Dict[str, Any]], output_folder: str) -> Optional[str]:
        """Upload error information to S3 if there are any errors."""
        if not errors:
            return None
        
        key = f"{output_folder}/errors.json"
        
        try:
            # Convert errors to JSON
            error_data = {
                "timestamp": datetime.now(self.pacific_tz).isoformat(),
                "total_errors": len(errors),
                "errors": errors
            }
            
            json_content = json.dumps(error_data, indent=2)
            
            self.s3_client.put_object(
                Bucket=config.s3_bucket,
                Key=key,
                Body=json_content.encode('utf-8'),
                ContentType='application/json'
            )
            
            logger.info(f"Successfully uploaded error log to s3://{config.s3_bucket}/{key}")
            return key
            
        except Exception as e:
            logger.error(f"Error uploading error log to S3: {e}")
            return None
    
    def _log_processing_errors(self, failed_results: List[Dict[str, Any]]):
        """Log processing errors for CloudWatch alerting."""
        for result in failed_results:
            if not result['success']:
                logger.error(f"Error: Processing {result['file_name']}: {result.get('error', 'Unknown error')}")
    
    async def process_all_sheets(self) -> Dict[str, Any]:
        """
        Main processing function that orchestrates the entire workflow.
        
        Returns:
            Dictionary with processing results and statistics
        """
        logger.info("Starting research data aggregation process")
        
        # Generate output folder for this run
        output_folder = self._generate_output_folder()
        logger.info(f"Output folder for this run: {output_folder}")
        
        try:
            # Step 0: Initialize product code mapper
            logger.info("Initializing product code mapper")
            await self.lookup_tables.initialize_product_code_mapper()
            
            # Step 1: List all spreadsheets in the folder
            logger.info(f"Listing spreadsheets in folder: {config.drive_folder_id}")
            files = await self.drive_client.list_files_in_folder(config.drive_folder_id)
            
            if not files:
                logger.warning("No spreadsheet files found in the specified folder")
                return {
                    "success": True,
                    "output_folder": output_folder,
                    "files_processed": 0,
                    "records_generated": 0,
                    "product_items_generated": 0,
                    "errors": 0,
                    "csv_key": None,
                    "product_item_key": None,
                    "static_file_keys": [],
                    "error_key": None
                }
            
            # Step 2: Process all files concurrently
            logger.info(f"Processing {len(files)} files with max concurrency {config.max_concurrent_requests}")
            results = await process_sheets_concurrently(
                files, 
                self.row_mapper,
                config.max_concurrent_requests
            )
            
            # Step 3: Collect records, product items, and errors
            all_records = []
            all_product_items = []
            failed_results = []
            all_processing_errors = []
            
            for result in results:
                if result['success']:
                    all_records.extend(result['records'])
                    all_product_items.extend(result.get('product_items', []))
                    # Collect processing errors from successful files
                    processing_errors = result.get('processing_errors', [])
                    all_processing_errors.extend(processing_errors)
                else:
                    failed_results.append(result)
                    # Also collect processing errors from failed files if they have any
                    processing_errors = result.get('processing_errors', [])
                    all_processing_errors.extend(processing_errors)
            
            # Step 4: Log errors for CloudWatch alerting
            if failed_results:
                self._log_processing_errors(failed_results)
            
            # Step 5: Filter unmapped research_ids and generate matrix CSV
            csv_key = None
            if all_records:
                logger.info(f"Filtering and converting research_ids to product codes for {len(all_records)} records")
                filtered_records = self._filter_and_convert_record_research_ids(all_records)
                
                excluded_count = len(all_records) - len(filtered_records)
                if excluded_count > 0:
                    logger.warning(f"Excluded {excluded_count} records with unmapped research_ids")
                
                if filtered_records:
                    logger.info(f"Generating matrix CSV with {len(filtered_records)} mapped records")
                    csv_content = self._create_csv_content(filtered_records)
                    csv_key = await self._upload_csv_to_s3(csv_content, output_folder)
                else:
                    logger.warning("No records with mapped research_ids, skipping matrix CSV upload")
            else:
                logger.warning("No matrix records generated, skipping matrix CSV upload")
            
            # Step 6: Filter unmapped research_ids and process product items CSV
            product_item_key = None
            if all_product_items:
                logger.info(f"Filtering and converting research_ids to product codes for {len(all_product_items)} product items")
                filtered_product_items = self._filter_and_convert_product_item_research_ids(all_product_items)
                
                excluded_count = len(all_product_items) - len(filtered_product_items)
                if excluded_count > 0:
                    logger.warning(f"Excluded {excluded_count} product items with unmapped research_ids")
                
                if filtered_product_items:
                    # Deduplicate product items by item ID (now using converted codes)
                    unique_product_items = self._deduplicate_product_items(filtered_product_items)
                    
                    if unique_product_items:
                        logger.info(f"Generating product item CSV with {len(unique_product_items)} unique mapped items")
                        product_item_csv_content = self._create_product_item_csv_content(unique_product_items)
                        product_item_key = await self._upload_product_item_csv_to_s3(product_item_csv_content, output_folder)
                    else:
                        logger.warning("No unique product items found after deduplication, skipping product item CSV upload")
                else:
                    logger.warning("No product items with mapped research_ids, skipping product item CSV upload")
            else:
                logger.warning("No product items generated, skipping product item CSV upload")
            
            # Step 7: Upload error log if there are any errors
            # Combine file-level errors and processing errors  
            all_errors = []
            
            # Add file-level processing errors
            for failed_result in failed_results:
                all_errors.append({
                    "error_type": "FileProcessingError",
                    "file_name": failed_result['file_name'],
                    "error_message": failed_result.get('error', 'Unknown error')
                })
            
            # Add data quality errors
            for processing_error in all_processing_errors:
                all_errors.append(processing_error.to_dict())
            
            # Add unmapped research_id errors (records excluded from output)
            unmapped_ids = self.lookup_tables.product_code_mapper.get_unmapped_ids()
            if unmapped_ids:
                all_errors.append({
                    "error_type": "ExcludedUnmappedResearchIds",
                    "count": len(unmapped_ids),
                    "research_ids": unmapped_ids,  # Already sorted in get_unmapped_ids()
                    "message": f"{len(unmapped_ids)} research_ids could not be mapped to product codes and were excluded from output files",
                    "impact": "These records do not appear in matrix_update.csv or product_item_update.csv"
                })
            
            error_key = await self._upload_errors_to_s3(all_errors, output_folder)
            
            # Step 8: Upload static data files
            logger.info("Uploading static data files")
            static_file_keys = await self._upload_static_files_to_s3(output_folder)
            
            # Step 9: Return results with filtering statistics
            # Calculate filtered counts for reporting
            filtered_records_count = len(self._filter_and_convert_record_research_ids(all_records)) if all_records else 0
            filtered_product_items = self._filter_and_convert_product_item_research_ids(all_product_items) if all_product_items else []
            unique_filtered_items = len(self._deduplicate_product_items(filtered_product_items))
            
            result = {
                "success": True,
                "output_folder": output_folder,
                "files_processed": len(files),
                "records_generated": len(all_records),
                "records_with_mapped_codes": filtered_records_count,
                "records_excluded": len(all_records) - filtered_records_count if all_records else 0,
                "product_items_generated": len(all_product_items),
                "product_items_with_mapped_codes": len(filtered_product_items),
                "product_items_excluded": len(all_product_items) - len(filtered_product_items) if all_product_items else 0,
                "unique_product_items": unique_filtered_items,
                "unmapped_research_ids": len(self.lookup_tables.product_code_mapper.get_unmapped_ids()),
                "errors": len(failed_results) + len(all_processing_errors),
                "csv_key": csv_key,
                "product_item_key": product_item_key,
                "static_file_keys": static_file_keys,
                "error_key": error_key
            }
            
            logger.info(f"Processing complete: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Fatal error in processing: {e}")
            return {
                "success": False,
                "output_folder": output_folder,
                "error": str(e),
                "files_processed": 0,
                "records_generated": 0,
                "product_items_generated": 0,
                "unique_product_items": 0,
                "errors": 0,
                "csv_key": None,
                "product_item_key": None,
                "static_file_keys": [],
                "error_key": None
            }


async def main():
    """Main entry point for the orchestrator."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    orchestrator = ResearchDataOrchestrator()
    result = await orchestrator.process_all_sheets()
    
    return result 