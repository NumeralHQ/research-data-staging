"""Deployment configuration for research-data-aggregation Lambda function."""

# Lambda function configuration
LAMBDA_CONFIG = {
    "function_name": "research-data-aggregation",
    "function_arn": "arn:aws:lambda:us-west-2:056694064025:function:research-data-aggregation",
    "runtime": "python3.13",
    "timeout": 900,  # 15 minutes
    "memory_size": 1024,
    "description": "Research Data Aggregation Service - processes Google Sheets and generates CSV outputs",
    "handler": "src.lambda_handler.lambda_handler"
}

# AWS configuration
AWS_CONFIG = {
    "region": "us-west-2",
    "account_id": "056694064025"
}

# Environment variables that should be preserved during deployment
# These are typically configured in AWS and should not be overwritten
PRESERVE_ENV_VARS = [
    "DRIVE_FOLDER_ID",
    "GOOGLE_SERVICE_ACCOUNT_SECRET", 
    "S3_BUCKET",
    "MAX_CONCURRENT_REQUESTS",
    "RATE_LIMIT_DELAY",
    "SHEET_NAME",
    "HEADER_ROW",
    "ADMIN_COLUMN",
    "ADMIN_FILTER_VALUE",
    "COL_CURRENT_ID",
    "COL_BUSINESS_USE",
    "COL_PERSONAL_USE",
    "COL_PERSONAL_TAX_CAT",
    "COL_PERSONAL_PERCENT_TAX",
    "COL_BUSINESS_TAX_CAT",
    "COL_BUSINESS_PERCENT_TAX",
    "EFFECTIVE_DATE",
    "GEO_STATE_KEY",
    "TAX_CAT_KEY"
]

# Expected deployment package contents
EXPECTED_PACKAGE_CONTENTS = [
    "src/",
    "mapping/",
    # Dependencies will be in root
]

# Deployment validation checks
VALIDATION_CHECKS = {
    "min_package_size_mb": 10,    # Minimum expected package size
    "max_package_size_mb": 250,   # Maximum allowed package size for Lambda
    "required_files": [
        "src/lambda_handler.py",
        "src/orchestrator.py",
        "src/models.py",
        "src/config.py",
        "mapping/geo_state.csv",
        "mapping/tax_cat.csv"
    ]
}

# Default test payload for Lambda invocation
DEFAULT_TEST_PAYLOAD = {
    "source": "terminal_invoke",
    "test": True,
    "timestamp": None  # Will be set at runtime
}

# Build configuration
BUILD_CONFIG = {
    "build_dir": "lambda-package",
    "output_zip": "research-data-aggregation.zip",
    "temp_dirs": ["build", "__pycache__"],
    "exclude_patterns": [
        "*.pyc",
        "__pycache__",
        ".pytest_cache",
        "test_*",
        ".git*",
        "*.md",
        "example deployment files"
    ]
} 