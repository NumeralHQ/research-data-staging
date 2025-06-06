"""Simplified data models for Research Data Aggregation Service (Lambda compatible)."""

import csv
import io
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Union

import boto3


class CustomerType(str, Enum):
    """Customer type codes."""
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
    """CSV record model with exact field order and validation."""
    
    def __init__(self, geocode: str, tax_auth_id: Optional[str] = None, 
                 group: str = GroupType.DEFAULT.value, item: str = "",
                 customer: str = "", provider: str = ProviderType.DEFAULT.value,
                 transaction: str = TransactionType.DEFAULT.value, 
                 taxable: Optional[int] = None, tax_type: str = TaxType.DEFAULT.value,
                 tax_cat: str = "", effective: Optional[str] = None,
                 per_taxable_type: str = PerTaxableType.DEFAULT.value,
                 percent_taxable: Optional[Decimal] = None):
        
        # Import config here to avoid circular imports
        from .config import config
        
        # Basic validation
        if len(geocode) != 12:
            raise ValueError(f"geocode must be 12 characters, got {len(geocode)}")
        if len(customer) != 2:
            raise ValueError(f"customer must be 2 characters, got {len(customer)}")
        if len(provider) != 2:
            raise ValueError(f"provider must be 2 characters, got {len(provider)}")
        if len(transaction) != 2:
            raise ValueError(f"transaction must be 2 characters, got {len(transaction)}")
        if len(tax_type) != 2:
            raise ValueError(f"tax_type must be 2 characters, got {len(tax_type)}")
        if len(tax_cat) != 2:
            raise ValueError(f"tax_cat must be 2 characters, got {len(tax_cat)}")
        if len(per_taxable_type) != 2:
            raise ValueError(f"per_taxable_type must be 2 characters, got {len(per_taxable_type)}")
        if len(group) < 4:
            raise ValueError(f"group must be at least 4 characters, got {len(group)}")
        
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
        # Use effective date from config if not provided
        self.effective = effective or config.effective_date
        self.per_taxable_type = per_taxable_type
        self.percent_taxable = round(percent_taxable, 6) if percent_taxable is not None else None
    
    def to_csv_row(self) -> List[str]:
        """Convert record to CSV row with proper formatting - all values wrapped in quotes."""
        def quote_and_escape(value):
            """Quote a value and escape any internal quotes."""
            if value is None:
                return '""'
            str_value = str(value)
            # Escape any existing quotes by doubling them
            escaped_value = str_value.replace('"', '""')
            return f'"{escaped_value}"'
        
        return [
            quote_and_escape(self.geocode),
            quote_and_escape(self.tax_auth_id),
            quote_and_escape(self.group),
            quote_and_escape(self.item),
            quote_and_escape(self.customer),
            quote_and_escape(self.provider),
            quote_and_escape(self.transaction),
            quote_and_escape(self.taxable),
            quote_and_escape(self.tax_type),
            quote_and_escape(self.tax_cat),
            quote_and_escape(self.effective),
            quote_and_escape(self.per_taxable_type),
            quote_and_escape(self.percent_taxable)
        ]
    
    @classmethod
    def csv_headers(cls) -> List[str]:
        """Return CSV headers in the correct order - all wrapped in quotes."""
        return [
            '"geocode"',
            '"tax_auth_id"', 
            '"group"',
            '"item"',
            '"customer"',
            '"provider"',
            '"transaction"',
            '"taxable"',
            '"tax_type"',
            '"tax_cat"',
            '"effective"',
            '"per_taxable_type"',
            '"percent_taxable"'
        ]


class LookupTables:
    """Manages lookup tables for geocodes and tax categories."""
    
    def __init__(self, bucket_name: str = "research-aggregation"):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
        self._geocode_map: Optional[Dict[str, str]] = None
        self._tax_cat_map: Optional[Dict[str, str]] = None
        self._state_name_to_code: Optional[Dict[str, str]] = None
    
    def _load_csv_from_s3_or_local(self, s3_key: str, local_path: str) -> List[Dict[str, str]]:
        """Load CSV from S3 if available, otherwise from local file."""
        try:
            # Try S3 first
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
        except self.s3_client.exceptions.NoSuchKey:
            # Fall back to local file
            with open(local_path, 'r', encoding='utf-8') as f:
                content = f.read()
        
        # Parse CSV content
        reader = csv.DictReader(io.StringIO(content))
        return list(reader)
    
    def get_geocode_map(self) -> Dict[str, str]:
        """Get state code to geocode mapping."""
        if self._geocode_map is None:
            rows = self._load_csv_from_s3_or_local(
                "mapping/geo_state.csv", 
                "mapping/geo_state.csv"
            )
            self._geocode_map = {row['state'].strip(): row['geocode'] for row in rows}
        return self._geocode_map
    
    def get_tax_cat_map(self) -> Dict[str, str]:
        """Get tax category description to code mapping."""
        if self._tax_cat_map is None:
            rows = self._load_csv_from_s3_or_local(
                "mapping/tax_cat.csv",
                "mapping/tax_cat.csv"
            )
            self._tax_cat_map = {row['tax_cat_desc'].strip().upper(): row['tax_cat'] for row in rows}
        return self._tax_cat_map
    
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
        geocode_map = self.get_geocode_map()
        
        # Sort states by length (longest first) to prioritize more specific matches
        # This ensures "WEST VIRGINIA" is checked before "VIRGINIA"
        sorted_states = sorted(state_map.items(), key=lambda x: len(x[0]), reverse=True)
        
        for state_name, state_code in sorted_states:
            if state_name in filename_upper:
                return geocode_map.get(state_code)
        
        return None
    
    def get_tax_cat_code(self, tax_cat_desc: str) -> Optional[str]:
        """Get tax category code from description."""
        if not tax_cat_desc:
            return None
        
        tax_cat_map = self.get_tax_cat_map()
        return tax_cat_map.get(tax_cat_desc.strip().upper())


class ProcessingError:
    """Model for tracking processing errors."""
    
    def __init__(self, filename: str, error_type: str, error_message: str, timestamp: str):
        self.filename = filename
        self.error_type = error_type
        self.error_message = error_message
        self.timestamp = timestamp 