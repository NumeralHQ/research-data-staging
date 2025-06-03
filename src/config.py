"""Simplified configuration management for Research Data Aggregation Service (Lambda compatible)."""

import json
import logging
import os
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class Config:
    """Configuration settings loaded from environment variables and AWS Secrets Manager."""
    
    def __init__(self):
        # Google Drive settings
        self.drive_folder_id = os.environ.get('DRIVE_FOLDER_ID', '')
        
        # Google Service Account settings
        self.google_service_account_secret = os.environ.get('GOOGLE_SERVICE_ACCOUNT_SECRET', '')
        
        # AWS settings - use AWS_DEFAULT_REGION, fallback to us-west-2
        self.aws_region = os.environ.get('AWS_DEFAULT_REGION', 'us-west-2')
        self.s3_bucket = os.environ.get('S3_BUCKET', '')
        self.secrets_manager_secret_name = os.environ.get('SECRETS_MANAGER_SECRET_NAME')
        
        # Processing settings
        self.sheet_name = os.environ.get('SHEET_NAME', 'Research')
        self.header_row = int(os.environ.get('HEADER_ROW', '4'))
        self.admin_filter_value = os.environ.get('ADMIN_FILTER_VALUE', 'Tag Level')
        self.max_concurrent_requests = int(os.environ.get('MAX_CONCURRENT_REQUESTS', '5'))
        self.rate_limit_delay = float(os.environ.get('RATE_LIMIT_DELAY', '0.1'))
        
        # CSV output settings
        self.effective_date = os.environ.get('EFFECTIVE_DATE', '1999-01-01')
        
        # Column name mappings
        self.admin_column = os.environ.get('ADMIN_COLUMN', 'Admin')
        self.col_current_id = os.environ.get('COL_CURRENT_ID', 'Current ID')
        self.col_business_use = os.environ.get('COL_BUSINESS_USE', 'Business Use')
        self.col_personal_use = os.environ.get('COL_PERSONAL_USE', 'Personal Use')
        self.col_personal_tax_cat = os.environ.get('COL_PERSONAL_TAX_CAT', 'Personal tax_cat')
        self.col_personal_percent_tax = os.environ.get('COL_PERSONAL_PERCENT_TAX', 'Personal percent_taxable')
        self.col_business_tax_cat = os.environ.get('COL_BUSINESS_TAX_CAT', 'Business tax_cat')
        self.col_business_percent_tax = os.environ.get('COL_BUSINESS_PERCENT_TAX', 'Business percent_taxable')
        
        # Lookup table settings
        self.geo_state_key = os.environ.get('GEO_STATE_KEY', 'mapping/geo_state.csv')
        self.tax_cat_key = os.environ.get('TAX_CAT_KEY', 'mapping/tax_cat.csv')

    def load_from_secrets_manager(self) -> dict:
        """Load additional configuration from AWS Secrets Manager if configured."""
        if not self.secrets_manager_secret_name:
            return {}
            
        try:
            session = boto3.Session()
            client = session.client('secretsmanager', region_name=self.aws_region)
            
            response = client.get_secret_value(SecretId=self.secrets_manager_secret_name)
            return json.loads(response['SecretString'])
            
        except ClientError as e:
            logger.warning(f"Warning: Could not load secrets from AWS Secrets Manager: {e}")
            return {}
        except json.JSONDecodeError as e:
            logger.warning(f"Warning: Invalid JSON in secrets manager: {e}")
            return {}

    def get_google_service_account_info(self) -> dict:
        """Get Google service account credentials from AWS Secrets Manager."""
        if not self.google_service_account_secret:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_SECRET environment variable not set")
            
        try:
            session = boto3.Session()
            client = session.client('secretsmanager', region_name=self.aws_region)
            
            response = client.get_secret_value(SecretId=self.google_service_account_secret)
            service_account_info = json.loads(response['SecretString'])
            
            logger.info("Successfully retrieved Google service account credentials from Secrets Manager")
            return service_account_info
            
        except ClientError as e:
            logger.error(f"Error retrieving Google service account from Secrets Manager: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in Google service account secret: {e}")
            raise

    def setup_google_credentials(self):
        """Set up Google credentials using service account key from Secrets Manager."""
        # Get service account info from Secrets Manager
        service_account_info = self.get_google_service_account_info()
        
        # Write service account info to a temporary file
        cred_file_path = '/tmp/google_service_account.json'
        
        logger.info(f"Writing Google service account credentials to {cred_file_path}")
        
        with open(cred_file_path, 'w') as f:
            json.dump(service_account_info, f, indent=2)
        
        # Set the environment variable that google.auth.default() looks for
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = cred_file_path
        
        logger.info(f"Google service account credentials configured at {cred_file_path}")
        logger.info(f"Service account email: {service_account_info.get('client_email', 'unknown')}")
        logger.info(f"Project ID: {service_account_info.get('project_id', 'unknown')}")


# Global config instance
config = Config() 