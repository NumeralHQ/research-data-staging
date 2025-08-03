# Research Data Aggregation Service

## 1. Purpose
Combine several state-specific Google Sheets into **one canonical CSV** and push it to an S3 bucket (`research-aggregation`).  The service runs inside an AWS Lambda, retrieves every sheet in a designated Google Drive folder, converts rows on the *Research* tab into the target tax schema, merges the output, and uploads the final file.

---

## 2. High-level Architecture
- **Trigger** – two options can coexist
  • **Scheduled**: EventBridge cron (weekly Sundays 2am PT / 9am UTC)
  • **On-demand**: manual invoke via AWS Console / CLI for ad-hoc runs
- **AWS Lambda** – Python 3.13 runtime.  Orchestrates asynchronous jobs for each Google Sheet with **true concurrent processing**.
- **Google Workspace APIs** – `drive`, `sheets` v4 via **Workload Identity Federation** (no service account keys required).
- **Amazon S3** – destination bucket `research-aggregation` with organized folder structure:
  • `output-YYYYMMDD-HHMM/` – timestamped folders for each run (Pacific Time)
    - `matrix_append.csv` – final aggregated matrix data
    - `product_item_append.csv` – generated product item data with deduplication
    - `product_group_append.csv` – static product group data
    - `errors.json` – processing errors (if any)
  • `mapping/*` – lookup tables (`geo_state.csv`, `tax_cat.csv`)
- **AWS IAM / KMS** – least-privilege roles, S3 encryption, Workload Identity Federation for secure Google API access.

```text
            +--------------+           +------------------+
(trigger) -> | EventBridge  |  --->    |  Lambda Function |
             +--------------+          /------------------\
                                        | 1. List Sheets |
                                        | 2. Spawn N jobs|  ← TRUE CONCURRENCY
                                        | 3. Merge rows  |    (Thread Pool)
                                        | 4. Upload CSV  |
                                        \----------------/
                                               |
                                               v
                                         research-aggregation
```

---

## 3. Repository Layout (✅ IMPLEMENTED)
```text
research-data-staging/
├── src/                   # Application source code
│   ├── __init__.py
│   ├── config.py          # env vars + WIF credentials (simplified, no Pydantic)
│   ├── drive_client.py    # Google Drive helper with optimized rate limiting + thread pool
│   ├── sheets_client.py   # Google Sheets helper with retry logic + header caching + thread pool
│   ├── models.py          # Simplified data models / lookups / enums (no Pydantic)
│   ├── mapper.py          # row → CSV record conversion with percentage parsing
│   ├── product_code_mapper.py  # research_id → 3-char code conversion service
│   ├── worker.py          # async processing for one sheet with concurrency control
│   ├── orchestrator.py    # fan-out/fan-in logic with Pacific Time + static file upload
│   ├── lambda_handler.py  # AWS entry-point with Powertools
│   └── data/              # Static data files
│       └── product_group_append.csv  # Static product group data
├── mapping/               # Lookup tables
│   ├── geo_state.csv      # State → geocode (✅ implemented)
│   ├── tax_cat.csv        # tax_cat text → 2-char code (✅ implemented)
│   └── product_code_mapping.csv  # research_id → 3-char item code (✅ implemented)
├── tests/                 # Unit tests
│   ├── __init__.py
│   ├── test_models.py     # Model validation and CSV output tests
│   ├── test_imports.py    # Import validation and basic functionality
│   ├── test_geocode.py    # Geocode lookup functionality tests
│   ├── test_concurrent_fix.py  # Thread pool concurrency validation tests
│   ├── test_product_code_mapper.py  # Product code mapping unit tests
│   └── test_conversion_integration.py  # Integration tests for code conversion
├── infrastructure/        # AWS infrastructure
│   └── template.yaml      # Complete AWS SAM template with optimized rate limiting
├── lambda-package/        # Deployment package (auto-generated)
│   ├── src/               # Source code copy
│   ├── mapping/           # Lookup tables copy
│   └── [dependencies]/    # All Python dependencies
├── build.py               # Enhanced build script (full/source-only/interactive modes)
├── create_zip.py          # ZIP creation utility
├── test_config.env        # Environment variables for local testing
├── README.md
├── TODO.md
└── requirements.txt       # Single requirements file
```

---

## 4. Setup & Installation

### Prerequisites
- Python 3.13+
- AWS CLI configured
- Google Cloud account

### 1. Clone and Setup Environment
```bash
git clone <repository>
cd research-data-staging
python -m venv .venv && source .venv/bin/activate  # Linux/Mac
# OR
python -m venv .venv && .venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

### 2. Google Cloud Setup (✅ COMPLETED)
**Using Google Service Account with keys stored in AWS Secrets Manager**

- **Project**: Google Cloud project with Drive and Sheets APIs enabled
- **Service Account**: Created with appropriate permissions for Drive and Sheets access
- **Service Account Key**: JSON key file stored securely in AWS Secrets Manager
- **Drive Folder Shared**: ✅ with service account email address

**Security**: Service account keys are stored in AWS Secrets Manager and loaded at runtime, never stored in code or environment variables.

### 3. AWS Deployment (✅ COMPLETED)

**Infrastructure Created:**
- **S3 Bucket**: `research-aggregation` (us-west-2)
- **IAM Role**: `research-data-aggregation-role`
- **Lambda Function**: `research-data-aggregation` (Python 3.13, 1024MB, 15min timeout)

**Environment Variables Configured:**
```bash
DRIVE_FOLDER_ID=1VK3kgR-tS-nkTUSwq8_B-8JYl3zFkXFU
GOOGLE_SERVICE_ACCOUNT_SECRET=research-data-aggregation/google-service-account
S3_BUCKET=research-aggregation
MAX_CONCURRENT_REQUESTS=5
RATE_LIMIT_DELAY=0.05  # Optimized for performance
# ... (all other configuration variables)
```

**Deployment Package**: 33.4MB ZIP with 5,563 files including simplified dependencies (no Pydantic).

---

## 5. Runtime Configuration
| Variable | Value | Purpose |
|----------|-------|---------|
| `DRIVE_FOLDER_ID`           | `1VK3kgR-tS-nkTUSwq8_B-8JYl3zFkXFU` | Source folder |
| `GOOGLE_SERVICE_ACCOUNT_SECRET` | `research-data-aggregation/google-service-account` | Secret name for Google service account |
| `S3_BUCKET`                 | `research-aggregation` | S3 bucket name |
| `EFFECTIVE_DATE`            | `1999-01-01` | Default effective date for CSV records (YYYY-MM-DD format) |
| `MAX_CONCURRENT_REQUESTS`   | `5` | **True concurrent processing degree** |
| `RATE_LIMIT_DELAY`          | `0.05` | **Optimized 20ms global rate limiting** |
| `SHEET_NAME`                | `Research` | Sheet tab to scan |
| `HEADER_ROW`                | `4` | 1-based row idx containing headers |
| `ADMIN_COLUMN`              | `Admin` | Column header with admin tags |
| `ADMIN_FILTER_VALUE`        | `Tag Level` | Filter value for processing |
| `COL_CURRENT_ID`            | `Current ID` | Column mappings... |
| `COL_BUSINESS_USE`          | `Business Use` |
| `COL_PERSONAL_USE`          | `Personal Use` |
| `COL_PERSONAL_TAX_CAT`      | `Personal tax_cat` |
| `COL_PERSONAL_PERCENT_TAX`  | `Personal percent_taxable` |
| `COL_BUSINESS_TAX_CAT`      | `Business tax_cat` |
| `COL_BUSINESS_PERCENT_TAX`  | `Business percent_taxable` |

The generated CSV will always emit columns in **this exact order**:
`geocode, tax_auth_id, group, item, customer, provider, transaction, taxable, tax_type, tax_cat, effective, per_taxable_type, percent_taxable`

**CSV Output Format**: All values are wrapped in quotes with proper escaping for special characters.

---

## 6. Execution Flow (✅ OPTIMIZED WITH TRUE CONCURRENCY)
1. **orchestrator.lambda_handler** is invoked.
2. **Product Code Mapper Initialization**: Load `product_code_mapping.csv` from S3 for research_id conversion.
3. List all files inside `DRIVE_FOLDER_ID` (mimeType = spreadsheet).
4. **Header mapping optimization**: Read header row from the first sheet once and reuse for all subsequent sheets.
5. **True concurrent processing**: Launch `MAX_CONCURRENT_REQUESTS` async tasks with:
   - **Thread Pool Executor**: Google API calls run in separate threads for true parallelism
   - **Global Rate Limiting**: Class-level 50ms intervals shared across all clients
   - **Semaphore Control**: Maintains concurrency limits while enabling parallel execution
6. **worker.py** (for each sheet, using the shared header map):
   a. Pull sheet data via Sheets API (optimized to fetch only data rows).
   b. Filter rows where `ADMIN_COLUMN` value == `ADMIN_FILTER_VALUE`.
   c. For each match create **two** `Record` objects (Business, Personal) using `mapper.py`.
7. `orchestrator` gathers all `Record`s and applies **Product Code Conversion**:
   - Converts research_ids to 3-character item codes using the mapping
   - Excludes records with unmapped research_ids from output
   - Tracks unmapped research_ids for error reporting
8. Streams converted records into a `csv.writer` **in the fixed column order**.
9. **Product Item Extraction**: Extract product items from rows with Admin="Tag Level", using Current ID (Column B) and item descriptions (Columns C:J with direct concatenation).
10. **Product Item Code Conversion**: Convert product item research_ids to 3-character codes and exclude unmapped items.
11. **Deduplication**: Remove duplicate product items by converted Item ID, keeping first occurrence.
12. Create timestamped output folder `output-YYYYMMDD-HHMM` and upload both `matrix_update.csv` and `product_item_update.csv` (timestamp in America/Los_Angeles).
13. Upload static data files from `src/data/` directory (e.g., `product_group_update.csv`) to the same output folder.
14. If any sheets were skipped due to errors, dump their details to `errors.json` in the same folder; also `logger.error("Error: Processing {file}")` for CloudWatch alarm.

---

## 7. Data Mapping Rules (✅ IMPLEMENTED)
See `mapper.py` for canonical reference.  Key features:

### **Matrix CSV Output (matrix_append.csv)**

**CSV Output Formatting**: All values in the output CSV are wrapped in quotes for consistency
- All field values: `"US1800000000"`, `"BB"`, `"1.000000"`, etc.
- Empty values: `""` (quoted empty strings)
- Headers: `"geocode"`, `"tax_auth_id"`, etc.
- Proper escaping: Internal quotes are escaped by doubling (`"` becomes `""`)

**Effective Date Configuration**: New environment variable controls the effective date field
- Environment Variable: `EFFECTIVE_DATE` (format: "YYYY-MM-DD")
- Default Value: `"1999-01-01"` if not set
- Example: Set `EFFECTIVE_DATE=2024-01-01` for all records to use that date

**Percentage Parsing**: Handles Google Sheets percentage values correctly
- Input: `'100%'` → Output: `'1.000000'` in CSV (divides by 100 when % symbol present)
- Input: `'8.75%'` → Output: `'0.087500'` in CSV (divides by 100 when % symbol present)  
- Input: `'1.0'` → Output: `'1.000000'` in CSV (preserves decimal values when no % symbol)
- Automatically detects and handles both percentage strings and decimal values

**Product Code Conversion**: Converts hierarchical research_ids to 3-character item codes
- **Mapping File**: `mapping/product_code_mapping.csv` with columns: research_id, taxonomy_id, product_id, group, item, description
- **Normalization**: Removes trailing `.0` segments (e.g., "1.1.1.4.3.0.0.0" → "1.1.1.4.3" for matching)
- **Code Padding**: Pads item codes to exactly 3 characters with leading zeros (e.g., "5" → "005", "22" → "022")
- **Error Handling**: Unmapped research_ids are excluded from output files and tracked in `errors.json`

**Geocode Lookup**: Extracts state name from filename, maps to 12-digit geocode
- `taxable` → mapping table `{Not Taxable|Nontaxable|Exempt:0, Taxable:1, "Drill Down":-1}`.

**Business vs Personal Records:**
- Each "Tag Level" row generates two CSV records:
- Business: `customer="BB"`, uses "Business Use", "Business tax_cat", "Business percent_taxable" columns
- Personal: `customer="99"`, uses "Personal Use", "Personal tax_cat", "Personal percent_taxable" columns

### **Product Item CSV Output (product_item_append.csv)**

**✅ NEW FEATURE**: Generates unique product item data from Google Sheets with zero additional API calls.

**Data Extraction Rules:**
- **Row Filter**: Only process rows where Admin column (K) = "Tag Level" (same as matrix processing)
- **Item ID**: Extract from Current ID column (Column B)
- **Description**: Direct concatenation of columns C:J (L1, L2, L3, L4, L5, L6, L7, L8 headers) with no separators
- **Group**: Always set to "7777" for all product items

**Processing Logic:**
```csv
"group","item","description"
"7777","ITEM001","Product Name From Columns C-J"
"7777","ITEM002","Another Product Description"
```

**Deduplication**: Remove duplicate items by Item ID, keeping first occurrence
- Example: If "ITEM001" appears in 3 different sheets, only first occurrence is kept
- Performance: O(1) deduplication using Python sets

**Skip Conditions**: Rows are skipped if:
- Admin column ≠ "Tag Level"
- Current ID (Column B) is empty or blank
- All description columns (C:J) are empty or blank

---

## 8. Performance & Concurrency Optimizations (✅ ENHANCED)

### **True Concurrent Processing**
- **Thread Pool Executor**: Google API calls run in separate threads, enabling true parallelism
- **Async/Await Integration**: Maintains async patterns while leveraging thread-based concurrency
- **Semaphore Control**: Limits concurrent operations while allowing parallel execution

### **Rate Limiting Optimizations**
- **Global Rate Limiting**: Class-level coordination prevents per-instance delays
- **Optimized Intervals**: Reduced from 100ms to 50ms between requests
- **Shared State**: All client instances coordinate through class-level locks

### **Performance Improvements**
- **Before Optimization**: Sequential processing (~157 seconds for 51 sheets)
- **After Optimization**: True concurrent processing (~20-30 seconds for 51 sheets)
- **Speed Improvement**: **5x faster overall processing**
- **Individual Sheet Processing**: Reduced from 3-4 seconds to 1-2 seconds per sheet

### **Memory & Cost Optimizations**
- **Single Lambda** → avoids step-function overhead; fan-out inside process keeps warm memory-footprint bounded.
- **Header mapping optimization**: Column indices determined from first sheet and reused, reducing API calls by ~98%.
- **Memory efficient**: Stream records to `csv.writer` → O(1) memory usage.
- **Simplified dependencies**: 33.4MB deployment package without Pydantic (vs 45MB with Pydantic).
- **Expected volume**: 50-250 sheets → single Lambda invocation completes in <15 minutes.

### **Concurrency Architecture**
```python
# Thread Pool for True Concurrency
executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="sheets_api")
result = await loop.run_in_executor(executor, api_call_function)

# Global Rate Limiting
async with cls._rate_limit_lock:
    # 20ms global interval between all API calls
    await asyncio.sleep(min_interval - time_since_last)
```

---

## 9. Local Development & Testing (✅ COMPREHENSIVE)

**Set up local testing environment:**
```bash
# Copy test environment variables (optional)
cp test_config.env .env  # or source test_config.env
```

**Run comprehensive test suite:**
```bash
# Run all tests with pytest
pytest tests/ -v

# Or run individual test files
python tests/test_imports.py      # Import validation and basic functionality
python tests/test_geocode.py      # Geocode lookup functionality  
python tests/test_csv_formatting.py  # CSV formatting with quotes and effective date
pytest tests/test_models.py -v    # Model validation and CSV output
pytest tests/test_concurrent_fix.py -v  # Thread pool concurrency validation
```

**Test specific functionality:**
```bash
# Test imports and basic functionality
python tests/test_imports.py

# Test geocode lookup with example filenames
python tests/test_geocode.py

# Test true concurrency with thread pool
pytest tests/test_concurrent_fix.py -v

# Test product code mapping functionality
pytest tests/test_product_code_mapper.py -v
pytest tests/test_conversion_integration.py -v

# Build deployment package (interactive mode)
python build.py

# Quick source-only build for development
python build.py --src

# Full build for first-time or dependency changes
python build.py --full
```

**Available Test Files:**
- **`test_imports.py`**: Validates all module imports and basic functionality without external dependencies
- **`test_models.py`**: Tests Record model creation, validation, and CSV output formatting
- **`test_geocode.py`**: Tests geocode lookup from filenames using local mapping files
- **`test_concurrent_fix.py`**: Validates thread pool executor enables true concurrent processing
- **`test_product_code_mapper.py`**: **NEW** - Unit tests for research_id to 3-character code conversion
- **`test_conversion_integration.py`**: **NEW** - Integration tests for end-to-end product code conversion
- **`test_config.env`**: Environment variables for local testing (can be sourced or copied to `.env`)

**Test concurrency performance:**
```python
# The test demonstrates 4x speed improvement with true concurrency
# Sequential: 3.6s for 6 files
# Concurrent: 0.9s for 6 files (with overlapping API calls detected)
pytest tests/test_concurrent_fix.py::test_thread_pool_enables_true_concurrency -v -s
```

---

## 10. Monitoring & Observability (✅ IMPLEMENTED)

**AWS Powertools Integration:**
- Structured JSON logging with correlation IDs
- X-Ray distributed tracing for performance analysis
- Custom CloudWatch metrics: `FilesProcessed`, `RecordsGenerated`, `ProcessingErrors`

**CloudWatch Alarms:**
- Processing errors trigger alerts via SNS
- Metric filter on log pattern: `"Error: Processing"`
- Lambda duration and error rate monitoring

**Performance Monitoring:**
- **Concurrency Metrics**: Track overlapping API calls and thread pool utilization
- **Processing Time**: Monitor individual sheet processing time improvements
- **Rate Limiting**: Track global rate limiting effectiveness

**Error Handling:**
- Individual file errors don't fail entire job
- Error details saved to `errors/errors-YYYYMMDD-HHMM.json`
- CloudWatch logs contain full error context with stack traces

---

## 11. Security Features (✅ ENHANCED)
- **Google Service Account Keys** stored securely in AWS Secrets Manager (never in code or environment variables)
- **S3 bucket** with encryption at rest and public access blocked
- **IAM roles** with least-privilege policies
- **TLS 1.2** enforced for all Google API calls
- **Input validation** and data sanitization throughout
- **Thread-safe operations** with proper async locks and synchronization

---

## 12. Deployment Commands (✅ ENHANCED)

**Enhanced Build & Deploy Script with Multiple Modes:**

```bash
# Interactive mode (enhanced menu with deployment options)
python build.py

# Build options
python build.py --full             # Full build only
python build.py --src              # Source-only build

# Deployment options
python build.py --deploy           # Build + Deploy to AWS Lambda
python build.py --deploy-only      # Deploy existing ZIP to AWS Lambda
python build.py --full-deploy      # Full build + Deploy

# Lambda operations
python build.py --invoke           # Run Lambda function remotely
python build.py --invoke --payload '{"test": true}'  # With custom payload
python build.py --info             # Show Lambda function information
python build.py --test-aws         # Test AWS credentials

# Show help and usage options
python build.py --help
```

**Enhanced Build & Deploy Modes:**

### **Build-Only Modes:**
1. **🔄 Full Build** (`--full`):
   - Cleans lambda-package directory completely
   - Downloads and installs all dependencies (~60+ seconds)
   - Copies source code and mapping files
   - Creates deployment ZIP file
   - Use for: First-time setup, dependency changes

2. **⚡ Source-Only Build** (`--src`):
   - Keeps existing dependencies (much faster ~10-15 seconds)
   - Only updates source code and mapping files
   - Creates deployment ZIP file
   - Use for: Code changes during development

### **Deployment Modes:**
3. **📦 Build + Deploy** (`--deploy`):
   - Full build process + automatic deployment to AWS Lambda
   - Handles MFA authentication if required
   - Preserves environment variables and function configuration
   - Use for: Complete build and deployment in one step

4. **🚀 Deploy Only** (`--deploy-only`):
   - Deploys existing ZIP file without rebuilding
   - Fast deployment for already built packages
   - Use for: Quick deployment of current build

### **Lambda Operations:**
5. **▶️ Invoke Function** (`--invoke`):
   - Runs the Lambda function remotely for testing
   - Shows execution results and logs
   - Supports custom payloads
   - Use for: Testing deployed function

6. **📊 Function Info** (`--info`):
   - Shows current Lambda function configuration
   - Displays memory, timeout, runtime, code size
   - Shows environment variables (non-sensitive)
   - Use for: Checking current deployment status

### **🚀 Interactive Mode** (default):
   - Enhanced menu with all build and deployment options
   - MFA-enabled AWS operations
   - Guided deployment process
   - Use for: Full-featured interactive experience

**Recommended Development Workflows:**

### **🔧 Development Cycle:**
```bash
# First time setup
python build.py --full

# During development (code changes)
python build.py --src

# Test AWS credentials
python build.py --test-aws

# Build and deploy
python build.py --deploy

# Test the deployed function
python build.py --invoke

# Check function status
python build.py --info
```

### **🚀 Quick Deploy Workflow:**
```bash
# One-command build and deploy
python build.py --deploy

# Quick redeploy after changes
python build.py --src
python build.py --deploy-only

# Test the deployment
python build.py --invoke
```

### **🔄 Continuous Development:**
```bash
# Interactive mode for guided experience
python build.py
> Choose: "3. Build + Deploy to AWS Lambda"
> MFA authentication (handled automatically)
> Build and deploy
> Test with: "5. Run Lambda function"
```

**Manual invoke:**
```bash
aws lambda invoke --function-name research-data-aggregation response.json
cat response.json
```

**View logs:**
```bash
aws logs tail /aws/lambda/research-data-aggregation --follow
```

**Update function code:**
1. **AWS Console**: Lambda → Functions → research-data-aggregation → Code source → Upload from .zip file
2. **AWS CLI**: `aws lambda update-function-code --function-name research-data-aggregation --zip-file fileb://research-data-aggregation.zip`

---

## 13. Current Status (✅ PRODUCTION READY WITH CSV FORMATTING)

### ✅ **Completed Implementation**
- **Google Cloud**: Service account with keys stored in AWS Secrets Manager
- **AWS Infrastructure**: Lambda function, S3 bucket, IAM roles deployed
- **Code**: All modules implemented and tested with optimized concurrency
- **Dependencies**: Optimized deployment package (33.4MB, 5,561 files)
- **Concurrency**: Thread pool executor for true parallel processing
- **Performance**: 5x speed improvement with optimized rate limiting
- **CSV Formatting**: All values wrapped in quotes with proper escaping
- **Product Code Conversion**: research_id to 3-character code mapping with error handling
- **Testing**: Comprehensive test suite including product code conversion validation
- **Security**: Service account keys securely stored in AWS Secrets Manager
- **Monitoring**: CloudWatch integration with structured logging

### 🧪 **Testing Status**
- ✅ **Import validation**: All modules load correctly (`test_imports.py`)
- ✅ **Basic functionality**: Core logic working without external dependencies (`test_imports.py`)
- ✅ **Model validation**: Record creation, validation, and CSV output (`test_models.py`)
- ✅ **Geocode lookup**: State name extraction from filenames (`test_geocode.py`)
- ✅ **CSV formatting**: Quoted values and effective date configuration (`test_csv_formatting.py`)
- ✅ **Concurrency validation**: Thread pool enables true parallel processing (`test_concurrent_fix.py`)
- ✅ **Performance testing**: 4x speed improvement demonstrated in tests
- ✅ **Percentage parsing**: Handles `'100%'` → `'1.000000'` conversion
- ✅ **Customer field mapping**: Business="BB", Personal="99" validated
- ✅ **CSV output format**: All values properly quoted with escaping
- ✅ **Product code mapping**: research_id normalization, 3-character padding, error handling (`test_product_code_mapper.py`)
- ✅ **Conversion integration**: End-to-end filtering and code conversion (`test_conversion_integration.py`)
- ✅ **Production deployment**: Successfully processing 51 files, 11,730 records

### 📊 **Production Performance**
The service is currently running successfully in production:
1. Processes 51 Google Sheets files from Drive folder **concurrently**
2. Completes processing in **20-30 seconds** (vs 157 seconds sequential)
3. **Product Code Conversion**: Converts hierarchical research_ids to 3-character item codes using mapping file
4. Generates 11,730 CSV records (2 per "Tag Level" row: Business + Personal) with converted item codes
5. **Product Item Extraction**: Extracts unique product items from the same rows using Current ID + item descriptions (Columns C:J)
6. Uploads matrix CSV to S3: `output-YYYYMMDD-HHMM/matrix_update.csv`
7. Uploads product item CSV to S3: `output-YYYYMMDD-HHMM/product_item_update.csv` (deduplicated by converted item ID)
8. Uploads static data files to S3: `output-YYYYMMDD-HHMM/product_group_update.csv`
9. All values properly quoted: `"US1800000000"`, `"7777"`, `"005"`, etc.
10. Effective date configurable via `EFFECTIVE_DATE` environment variable

### 🚀 **Performance Benchmarks**
- **Individual Sheet Processing**: 1-2 seconds (down from 3-4 seconds)
- **Overall Processing Time**: 25-30 seconds for 51 sheets (down from 157 seconds)
- **Concurrency Factor**: 5x parallel processing with thread pool executor
- **API Efficiency**: 98% reduction in header mapping API calls
- **Rate Limiting**: Optimized 20ms global intervals (down from 100ms per-instance)
- **Product Code Conversion**: Efficient in-memory mapping with normalization and filtering
- **CSV Generation**: Fixed escaping issues for reliable output formatting

---

## 14. Troubleshooting

**Common Issues:**
- **Import errors**: Ensure deployment package includes all dependencies
- **Permission errors**: Verify Google Drive folder is shared with service account
- **Rate limiting**: Adjust `MAX_CONCURRENT_REQUESTS` if hitting API limits
- **Memory issues**: Increase Lambda memory allocation if processing large datasets
- **Concurrency issues**: Check thread pool executor logs for synchronization problems

**Performance Issues:**
- **Slow processing**: Verify `RATE_LIMIT_DELAY` is set to `0.05` for optimal performance
- **Sequential execution**: Check CloudWatch logs for thread pool creation and overlapping API calls
- **Memory pressure**: Monitor Lambda memory usage with concurrent processing

**CSV Formatting Issues:**
- **Quote escaping**: Internal quotes are automatically escaped by doubling (`"` becomes `""`)
- **Empty values**: Properly formatted as quoted empty strings (`""`)
- **Effective date**: Set `EFFECTIVE_DATE` environment variable for custom dates

**Product Code Conversion Issues:**
- **Missing mapping file**: Ensure `mapping/product_code_mapping.csv` exists in S3 bucket
- **Unmapped research_ids**: Check `errors.json` output file for `ExcludedUnmappedResearchIds` list
- **Code padding**: Item codes are automatically padded to 3 characters with leading zeros
- **Hierarchy normalization**: Trailing `.0` segments are removed for matching (e.g., "1.1.0.0" matches "1.1")

**Logs Location:**
- CloudWatch Log Group: `/aws/lambda/research-data-aggregation`
- Output files: `s3://research-aggregation/output-YYYYMMDD-HHMM/`
- Performance metrics: CloudWatch custom metrics namespace `ResearchDataAggregation`

---

© 2025 Numeral - **Production Ready** 🚀