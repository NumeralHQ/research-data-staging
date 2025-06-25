"""Simplified data models for Research Data Aggregation Service (Lambda compatible)."""

import csv
import io
import logging
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Union, Any, Set

import boto3

logger = logging.getLogger(__name__)


class TaxableStatus(Enum):
    """Enumeration for taxable status values."""
    NOT_TAXABLE = 0      # "Not Taxable", "Nontaxable", "Exempt"
    TAXABLE = 1          # "Taxable"
    DRILL_DOWN = -1      # "Drill Down"


class CustomerType(Enum):
    """Enumeration for customer types."""
    BUSINESS = "BB"
    PERSONAL = "99"


class ProviderType(str, Enum):
    """Provider type codes."""
    DEFAULT = "99"


class TransactionType(str, Enum):
    """Transaction type codes."""
    DEFAULT = "01"


class TaxType(str, Enum):
    """Tax type codes."""
    DEFAULT = "01"


class PerTaxableType(str, Enum):
    """Per taxable type codes."""
    DEFAULT = "01"


class GroupType(str, Enum):
    """Group type codes."""
    DEFAULT = "ZZZZ"


class TaxableValue(int, Enum):
    """Taxable value mappings."""
    NOT_TAXABLE = 0
    TAXABLE = 1
    DRILL_DOWN = -1


class Record:
    """Represents a single CSV record for matrix_append.csv output."""
    
    def __init__(self, geocode: str, tax_auth_id: str, group: str, item: str, 
                 customer: str, provider: str, transaction: str, taxable: int,
                 tax_type: str, tax_cat: str, effective: str, per_taxable_type: str,
                 percent_taxable: str):
        self.geocode = geocode
        self.tax_auth_id = tax_auth_id
        self.group = group
        self.item = item
        self.customer = customer
        self.provider = provider
        self.transaction = transaction
        self.taxable = taxable
        self.tax_type = tax_type
        self.tax_cat = tax_cat
        self.effective = effective
        self.per_taxable_type = per_taxable_type
        self.percent_taxable = percent_taxable

    @staticmethod
    def csv_headers() -> List[str]:
        """Return CSV headers in the exact order required."""
        return [
            '"geocode"', '"tax_auth_id"', '"group"', '"item"', '"customer"',
            '"provider"', '"transaction"', '"taxable"', '"tax_type"', '"tax_cat"',
            '"effective"', '"per_taxable_type"', '"percent_taxable"'
        ]

    def to_csv_row(self) -> List[str]:
        """Convert record to CSV row with proper quoting."""
        return [
            f'"{self.geocode}"',
            f'"{self.tax_auth_id}"',
            f'"{self.group}"',
            f'"{self.item}"',
            f'"{self.customer}"',
            f'"{self.provider}"',
            f'"{self.transaction}"',
            f'"{self.taxable}"',
            f'"{self.tax_type}"',
            f'"{self.tax_cat}"',
            f'"{self.effective}"',
            f'"{self.per_taxable_type}"',
            f'"{self.percent_taxable}"'
        ]

    def __repr__(self):
        return f"Record(item={self.item}, customer={self.customer}, geocode={self.geocode})"


class ProductItem:
    """Represents a product item for the product_item_append.csv output."""
    
    def __init__(self, item_id: str, description: str):
        self.group = "ZZZZ"  # Always ZZZZ as specified
        self.item = item_id.strip() if item_id else ""
        self.description = description.strip() if description else ""
    
    @staticmethod
    def csv_headers() -> List[str]:
        """Return CSV headers for product item output."""
        return ['"group"', '"item"', '"description"']
    
    def to_csv_row(self) -> List[str]:
        """Convert product item to CSV row with proper quoting."""
        # Escape any internal quotes by doubling them
        escaped_description = self.description.replace('"', '""')
        return [
            f'"{self.group}"',
            f'"{self.item}"',
            f'"{escaped_description}"'
        ]
    
    def is_valid(self) -> bool:
        """Check if this product item has valid data."""
        return bool(self.item and self.description)
    
    def __repr__(self):
        return f"ProductItem(item={self.item}, description={self.description[:30]}...)"
    
    def __hash__(self):
        """Make ProductItem hashable for deduplication."""
        return hash(self.item)
    
    def __eq__(self, other):
        """Compare ProductItems by item ID for deduplication."""
        if isinstance(other, ProductItem):
            return self.item == other.item
        return False


class ProcessingError:
    """Represents an error that occurred during processing."""
    
    def __init__(self, file_name: str, error_message: str, error_type: str = "ProcessingError"):
        self.file_name = file_name
        self.error_message = error_message
        self.error_type = error_type

    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for JSON serialization."""
        return {
            "file_name": self.file_name,
            "error_message": self.error_message,
            "error_type": self.error_type
        }

    def __repr__(self):
        return f"ProcessingError(file={self.file_name}, error={self.error_message})"


class LookupTables:
    """Manages lookup tables for geocode and tax category mappings."""
    
    def __init__(self, s3_bucket: str):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3')
        
        # Initialize lookup dictionaries
        self._geocode_lookup: Optional[Dict[str, str]] = None
        self._tax_cat_lookup: Optional[Dict[str, str]] = None
        self._state_name_to_code: Optional[Dict[str, str]] = None
        
        # Taxable status lookup (hardcoded as per requirements)
        self.taxable_lookup = {
            "Not Taxable": TaxableStatus.NOT_TAXABLE.value,
            "Nontaxable": TaxableStatus.NOT_TAXABLE.value,
            "Exempt": TaxableStatus.NOT_TAXABLE.value,
            "Taxable": TaxableStatus.TAXABLE.value,
            "Drill Down": TaxableStatus.DRILL_DOWN.value
        }
    
    def _load_csv_from_s3(self, s3_key: str) -> Dict[str, str]:
        """Load a CSV lookup table from S3."""
        try:
            logger.info(f"Loading lookup table from s3://{self.s3_bucket}/{s3_key}")
            
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            csv_content = response['Body'].read().decode('utf-8')
            
            # Parse CSV content
            lookup_dict = {}
            csv_reader = csv.reader(io.StringIO(csv_content))
            
            # Skip header row
            next(csv_reader, None)
            
            for row in csv_reader:
                if len(row) >= 2:
                    # Key is first column, value is second column
                    key = row[0].strip().strip('"')
                    value = row[1].strip().strip('"')
                    if key and value:
                        lookup_dict[key] = value
            
            logger.info(f"Loaded {len(lookup_dict)} entries from {s3_key}")
            return lookup_dict
            
        except Exception as e:
            logger.error(f"Error loading lookup table from {s3_key}: {e}")
            return {}
    
    @property
    def geocode_lookup(self) -> Dict[str, str]:
        """Get geocode lookup table, loading if necessary."""
        if self._geocode_lookup is None:
            self._geocode_lookup = self._load_csv_from_s3("mapping/geo_state.csv")
        return self._geocode_lookup
    
    @property
    def tax_cat_lookup(self) -> Dict[str, str]:
        """Get tax category lookup table, loading if necessary."""
        if self._tax_cat_lookup is None:
            self._tax_cat_lookup = self._load_csv_from_s3("mapping/tax_cat.csv")
        return self._tax_cat_lookup
    
    def get_state_name_to_code_map(self) -> Dict[str, str]:
        """Get state name to state code mapping for filename parsing."""
        if self._state_name_to_code is None:
            self._state_name_to_code = {
                "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
                "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE", "DISTRICT OF COLUMBIA": "DC",
                "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID",
                "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS",
                "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
                "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS",
                "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV",
                "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY",
                "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK",
                "OREGON": "OR", "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC",
                "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT",
                "VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV",
                "WISCONSIN": "WI", "WYOMING": "WY"
            }
        return self._state_name_to_code
    
    def get_geocode_for_filename(self, filename: str) -> Optional[str]:
        """Extract geocode from filename by finding state name."""
        filename_upper = filename.upper()
        state_map = self.get_state_name_to_code_map()
        geocode_map = self.geocode_lookup
        
        # Sort states by length (longest first) to prioritize more specific matches
        # This ensures "WEST VIRGINIA" is checked before "VIRGINIA"
        sorted_states = sorted(state_map.items(), key=lambda x: len(x[0]), reverse=True)
        
        for state_name, state_code in sorted_states:
            if state_name in filename_upper:
                return geocode_map.get(state_code)
        
        return None
    
    def get_geocode_for_state(self, state_name: str) -> Optional[str]:
        """Get geocode for a state name."""
        return self.geocode_lookup.get(state_name)
    
    def get_tax_cat_code(self, tax_cat_text: str) -> str:
        """Get 2-character tax category code from text."""
        return self.tax_cat_lookup.get(tax_cat_text, tax_cat_text)
    
    def get_taxable_status(self, taxable_text: str) -> int:
        """Get numeric taxable status from text."""
        return self.taxable_lookup.get(taxable_text, TaxableStatus.TAXABLE.value)  # Default to taxable 