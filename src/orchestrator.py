"""Main orchestrator for the research data aggregation service."""

import asyncio
import csv
import io
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import boto3
import pytz

from .config import config
from .drive_client import DriveClient
from .sheets_client import SheetsClient
from .worker import process_sheets_concurrently
from .mapper import RowMapper
from .models import Record, LookupTables

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
        """Create CSV content from records."""
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header row
        writer.writerow(Record.csv_headers())
        
        # Write data rows
        for record in records:
            writer.writerow(record.to_csv_row())
        
        content = output.getvalue()
        output.close()
        
        logger.info(f"Generated CSV with {len(records)} records")
        return content
    
    async def _upload_csv_to_s3(self, csv_content: str, output_folder: str) -> str:
        """Upload CSV content to S3 and return the key."""
        key = f"{output_folder}/results.csv"
        
        try:
            self.s3_client.put_object(
                Bucket=config.s3_bucket,
                Key=key,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )
            
            logger.info(f"Successfully uploaded CSV to s3://{config.s3_bucket}/{key}")
            return key
            
        except Exception as e:
            logger.error(f"Error uploading CSV to S3: {e}")
            raise
    
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
                    "errors": 0,
                    "csv_key": None,
                    "error_key": None
                }
            
            # Step 2: Process all files concurrently
            logger.info(f"Processing {len(files)} files with max concurrency {config.max_concurrent_requests}")
            results = await process_sheets_concurrently(
                files, 
                self.row_mapper,
                config.max_concurrent_requests
            )
            
            # Step 3: Collect records and errors
            all_records = []
            failed_results = []
            
            for result in results:
                if result['success']:
                    all_records.extend(result['records'])
                else:
                    failed_results.append(result)
            
            # Step 4: Log errors for CloudWatch alerting
            if failed_results:
                self._log_processing_errors(failed_results)
            
            # Step 5: Generate and upload CSV
            csv_key = None
            if all_records:
                logger.info(f"Generating CSV with {len(all_records)} records")
                csv_content = self._create_csv_content(all_records)
                csv_key = await self._upload_csv_to_s3(csv_content, output_folder)
            else:
                logger.warning("No records generated, skipping CSV upload")
            
            # Step 6: Upload error log if there are errors
            error_key = await self._upload_errors_to_s3(failed_results, output_folder)
            
            # Step 7: Return results
            result = {
                "success": True,
                "output_folder": output_folder,
                "files_processed": len(files),
                "records_generated": len(all_records),
                "errors": len(failed_results),
                "csv_key": csv_key,
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
                "errors": 0,
                "csv_key": None,
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