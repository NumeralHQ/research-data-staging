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
        key = f"{output_folder}/matrix_append.csv"
        
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
        key = f"{output_folder}/product_item_append.csv"
        
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
            
            for result in results:
                if result['success']:
                    all_records.extend(result['records'])
                    all_product_items.extend(result.get('product_items', []))
                else:
                    failed_results.append(result)
            
            # Step 4: Log errors for CloudWatch alerting
            if failed_results:
                self._log_processing_errors(failed_results)
            
            # Step 5: Generate and upload matrix CSV
            csv_key = None
            if all_records:
                logger.info(f"Generating matrix CSV with {len(all_records)} records")
                csv_content = self._create_csv_content(all_records)
                csv_key = await self._upload_csv_to_s3(csv_content, output_folder)
            else:
                logger.warning("No matrix records generated, skipping matrix CSV upload")
            
            # Step 6: Process and upload product items CSV
            product_item_key = None
            if all_product_items:
                logger.info(f"Processing {len(all_product_items)} product items")
                # Deduplicate product items by item ID
                unique_product_items = self._deduplicate_product_items(all_product_items)
                
                if unique_product_items:
                    logger.info(f"Generating product item CSV with {len(unique_product_items)} unique items")
                    product_item_csv_content = self._create_product_item_csv_content(unique_product_items)
                    product_item_key = await self._upload_product_item_csv_to_s3(product_item_csv_content, output_folder)
                else:
                    logger.warning("No unique product items found, skipping product item CSV upload")
            else:
                logger.warning("No product items generated, skipping product item CSV upload")
            
            # Step 7: Upload error log if there are errors
            error_key = await self._upload_errors_to_s3(failed_results, output_folder)
            
            # Step 8: Upload static data files
            logger.info("Uploading static data files")
            static_file_keys = await self._upload_static_files_to_s3(output_folder)
            
            # Step 9: Return results
            result = {
                "success": True,
                "output_folder": output_folder,
                "files_processed": len(files),
                "records_generated": len(all_records),
                "product_items_generated": len(all_product_items),
                "unique_product_items": len(self._deduplicate_product_items(all_product_items)) if all_product_items else 0,
                "errors": len(failed_results),
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