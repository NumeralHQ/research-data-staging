"""Simplified configuration management for Research Data Aggregation Service (Lambda compatible)."""

import json
import os
from typing import Optional

import boto3
from botocore.exceptions import ClientError


class Config:
    """Configuration settings loaded from environment variables and AWS Secrets Manager."""
    
    def __init__(self):
        # Google Drive settings
        self.drive_folder_id = os.environ.get('DRIVE_FOLDER_ID', '')
        
        # Workload Identity Federation settings
        self.wif_audience = os.environ.get('WIF_AUDIENCE', '')
        self.wif_service_account = os.environ.get('WIF_SERVICE_ACCOUNT', '')
        
        # AWS settings
        self.aws_region = os.environ.get('AWS_REGION', 'us-west-2')
        self.s3_bucket = os.environ.get('S3_BUCKET', '')
        self.secrets_manager_secret_name = os.environ.get('SECRETS_MANAGER_SECRET_NAME')
        
        # Processing settings
        self.sheet_name = os.environ.get('SHEET_NAME', 'Research')
        self.header_row = int(os.environ.get('HEADER_ROW', '4'))
        self.admin_filter_value = os.environ.get('ADMIN_FILTER_VALUE', 'Tag Level')
        self.max_concurrent_requests = int(os.environ.get('MAX_CONCURRENT_REQUESTS', '5'))
        self.rate_limit_delay = float(os.environ.get('RATE_LIMIT_DELAY', '0.1'))
        
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
            print(f"Warning: Could not load secrets from AWS Secrets Manager: {e}")
            return {}
        except json.JSONDecodeError as e:
            print(f"Warning: Invalid JSON in secrets manager: {e}")
            return {}

    @property
    def google_credentials_info(self) -> dict:
        """Get Google credentials configuration for Workload Identity Federation."""
        # Load any additional config from Secrets Manager
        secrets = self.load_from_secrets_manager()
        
        # Build the credential configuration for Workload Identity Federation
        credential_config = {
            "type": "external_account",
            "audience": self.wif_audience,
            "subject_token_type": "urn:ietf:params:aws:token-type:aws4_request",
            "token_url": "https://sts.googleapis.com/v1/token",
            "service_account_impersonation_url": f"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{self.wif_service_account}:generateAccessToken",
            "credential_source": {
                "environment_id": "aws1",
                "regional_cred_verification_url": f"https://sts.{self.aws_region}.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15"
            }
        }
        
        # Override with any values from Secrets Manager
        credential_config.update(secrets.get("google_credentials", {}))
        
        return credential_config


# Global config instance
config = Config() 