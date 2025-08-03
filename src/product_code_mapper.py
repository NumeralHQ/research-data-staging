"""Product code mapping service for converting research_ids to 3-character codes."""

import csv
import io
import logging
from typing import Dict, List, Optional, Set

import boto3

logger = logging.getLogger(__name__)


class ProductCodeMapper:
    """Service for mapping research_ids to 3-character product codes."""
    
    def __init__(self, s3_bucket: str):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3')
        
        # Mapping from normalized research_id to item code
        self.mapping: Dict[str, str] = {}
        
        # Track unmapped research_ids for error reporting
        self.unmapped_ids: Set[str] = set()
    
    def _normalize_research_id(self, research_id: str) -> str:
        """
        Normalize research_id by removing trailing .0 segments for matching.
        
        Examples:
            "1.1.1.4.3.0.0.0" -> "1.1.1.4.3"
            "1.1.0.0.0.0.0.0" -> "1.1"
            "1.0.0.0.0.0.0.0" -> "1"
        
        Args:
            research_id: Original research ID from Google Sheets
            
        Returns:
            Normalized research ID for mapping lookup
        """
        if not research_id:
            return ""
        
        # Split into parts
        parts = research_id.strip().split('.')
        
        # Remove trailing .0 segments
        while parts and parts[-1] == '0':
            parts.pop()
        
        # Return normalized ID, or original if all were zeros
        if not parts:
            return research_id.strip()
        
        return '.'.join(parts)
    
    def _pad_item_code(self, item_code: str) -> str:
        """
        Pad item code to exactly 3 characters with leading zeros.
        
        Examples:
            "5" -> "005"
            "22" -> "022" 
            "123" -> "123"
            "1234" -> "1234" (no truncation, but log warning)
        
        Args:
            item_code: Raw item code from mapping file
            
        Returns:
            3-character padded code
        """
        if not item_code:
            return "000"
        
        code = item_code.strip()
        
        if len(code) > 3:
            logger.warning(f"Item code '{code}' is longer than 3 characters - not truncating")
            return code
        
        # Pad with leading zeros to exactly 3 characters
        return code.zfill(3)
    
    async def load_mapping(self) -> None:
        """Load product code mapping from S3."""
        try:
            logger.info("Loading product code mapping from S3")
            
            # Load CSV from S3
            s3_key = "mapping/product_code_mapping.csv"
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            csv_content = response['Body'].read().decode('utf-8')
            
            # Parse CSV content
            csv_reader = csv.reader(io.StringIO(csv_content))
            
            # Skip header row
            header = next(csv_reader, None)
            if not header:
                logger.error("Product code mapping CSV is empty")
                return
            
            # Expected columns: research_id, taxonomy_id, product_id, group, item, description
            # We need research_id (col 0) -> item (col 4)
            mapping_count = 0
            
            for row_num, row in enumerate(csv_reader, start=2):  # Start at 2 since we skipped header
                try:
                    if len(row) < 5:  # Need at least 5 columns (0-4)
                        logger.warning(f"Row {row_num}: Insufficient columns ({len(row)}), skipping")
                        continue
                    
                    research_id = row[0].strip().strip('"')
                    item_code = row[4].strip().strip('"')
                    
                    if not research_id or not item_code:
                        logger.warning(f"Row {row_num}: Empty research_id or item_code, skipping")
                        continue
                    
                    # Normalize research_id for lookup
                    normalized_id = self._normalize_research_id(research_id)
                    
                    # Pad item code to 3 characters
                    padded_code = self._pad_item_code(item_code)
                    
                    # Store in mapping
                    self.mapping[normalized_id] = padded_code
                    mapping_count += 1
                    
                    logger.debug(f"Mapped '{research_id}' -> '{normalized_id}' -> '{padded_code}'")
                    
                except Exception as e:
                    logger.warning(f"Row {row_num}: Error processing row {row}: {e}")
                    continue
            
            logger.info(f"Successfully loaded {mapping_count} product code mappings from {s3_key}")
            logger.info(f"Sample mappings: {dict(list(self.mapping.items())[:3])}")
            
        except Exception as e:
            logger.error(f"Error loading product code mapping: {e}")
            # Continue with empty mapping - don't fail the entire process
            self.mapping = {}
    
    def convert_research_id(self, research_id: str) -> Optional[str]:
        """
        Convert research_id to 3-character padded code.
        
        Args:
            research_id: Original research ID from Google Sheets
            
        Returns:
            3-character padded code if mapped, None if unmapped
        """
        if not research_id:
            # Track empty/None inputs as unmapped for error reporting
            if research_id is not None:  # Only track empty strings, not None
                self.unmapped_ids.add(research_id)
            return None
        
        # Normalize for lookup
        normalized_id = self._normalize_research_id(research_id)
        
        # Look up in mapping
        item_code = self.mapping.get(normalized_id)
        
        if item_code:
            logger.debug(f"Converted '{research_id}' -> '{normalized_id}' -> '{item_code}'")
            return item_code
        else:
            # Track unmapped ID (use original, not normalized)
            self.unmapped_ids.add(research_id)
            logger.debug(f"No mapping found for '{research_id}' (normalized: '{normalized_id}')")
            return None
    
    def get_unmapped_ids(self) -> List[str]:
        """
        Get list of original unmapped research_ids for error reporting.
        
        Returns:
            Sorted list of research_ids that could not be mapped
        """
        return sorted(list(self.unmapped_ids))
    
    def get_mapping_stats(self) -> Dict[str, int]:
        """
        Get statistics about the mapping.
        
        Returns:
            Dictionary with mapping statistics
        """
        return {
            "total_mappings": len(self.mapping),
            "unmapped_requests": len(self.unmapped_ids)
        }