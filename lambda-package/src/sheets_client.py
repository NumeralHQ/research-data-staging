"""Google Sheets API client with rate limiting and error handling."""

import asyncio
import logging
import time
import concurrent.futures
from typing import List, Dict, Any, Optional

from google.auth.transport.requests import Request
import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .config import config

logger = logging.getLogger(__name__)


class SheetsClient:
    """Google Sheets API client with rate limiting and retry logic."""
    
    # Class-level thread pool executor for all instances
    _executor = None
    _executor_lock = asyncio.Lock()
    
    # Class-level rate limiting for all instances (shared across all clients)
    _last_request_time = 0
    _rate_limit_lock = asyncio.Lock()
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.service = None
        # Reduced minimum interval for better concurrency
        self._min_request_interval = 0.05  # 50ms instead of 50ms
        self._rate_limit_delay = config.rate_limit_delay
        self._header_mapping_cache = {}  # Cache header mappings by spreadsheet_id
        
        # Create a unique instance ID for logging
        import uuid
        self._instance_id = str(uuid.uuid4())[:8]
    
    @classmethod
    async def _get_executor(cls):
        """Get or create the shared thread pool executor."""
        if cls._executor is None:
            async with cls._executor_lock:
                if cls._executor is None:
                    # Create a thread pool with enough threads for concurrent operations
                    max_workers = min(32, (config.max_concurrent_requests or 5) * 2)
                    cls._executor = concurrent.futures.ThreadPoolExecutor(
                        max_workers=max_workers,
                        thread_name_prefix="sheets_api"
                    )
                    logger.info(f"Created thread pool executor with {max_workers} workers")
        return cls._executor
    
    def _get_credentials(self):
        """Get Google credentials using service account key."""
        # Use google.auth.default() which will automatically detect and use
        # the service account key from GOOGLE_APPLICATION_CREDENTIALS
        credentials, _ = google.auth.default(
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        
        # Refresh credentials if needed
        if not credentials.valid:
            credentials.refresh(Request())
            
        return credentials
    
    def _initialize_service(self):
        """Initialize the Google Sheets service."""
        if self.service is None:
            credentials = self._get_credentials()
            self.service = build('sheets', 'v4', credentials=credentials)
    
    @classmethod
    async def _global_rate_limit(cls):
        """Implement class-level rate limiting shared across all instances."""
        async with cls._rate_limit_lock:
            current_time = time.time()
            time_since_last = current_time - cls._last_request_time
            
            # Use a smaller interval for better concurrency
            min_interval = 0.05  # 50ms between requests globally
            
            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                logger.debug(f"Global rate limiting, sleeping {sleep_time:.3f}s")
                await asyncio.sleep(sleep_time)
            
            cls._last_request_time = time.time()
    
    def _execute_request_sync(self, request_func, *args, **kwargs):
        """Execute a synchronous request in a thread."""
        try:
            # This runs in a thread, so we can use synchronous operations
            result = request_func(*args, **kwargs).execute()
            return result
        except Exception as e:
            # Re-raise the exception to be handled by the async wrapper
            raise e
    
    async def _execute_with_retry(self, request_func, *args, **kwargs):
        """Execute a request with exponential backoff retry using thread pool."""
        
        for attempt in range(self.max_retries + 1):
            try:
                # Apply global rate limiting only once per request
                await self._global_rate_limit()
                
                # Execute the request in a thread pool for concurrent operations
                executor = await self._get_executor()
                import functools
                callable_func = functools.partial(self._execute_request_sync, request_func, *args, **kwargs)
                
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(executor, callable_func)
                
                return result
                
            except HttpError as e:
                if e.resp.status in [429, 500, 502, 503, 504]:
                    if attempt < self.max_retries:
                        delay = self.base_delay * (2 ** attempt)
                        logger.warning(
                            f"SheetsClient[{self._instance_id}]: Request failed with {e.resp.status}, retrying in {delay}s "
                            f"(attempt {attempt + 1}/{self.max_retries + 1})"
                        )
                        await asyncio.sleep(delay)
                        continue
                
                logger.error(f"SheetsClient[{self._instance_id}]: Sheets API request failed: {e}")
                raise
            
            except Exception as e:
                logger.error(f"SheetsClient[{self._instance_id}]: Unexpected error in Sheets API request: {e}")
                raise
        
        raise RuntimeError(f"Request failed after {self.max_retries + 1} attempts")
    
    async def get_sheet_values(
        self, 
        spreadsheet_id: str, 
        range_name: str,
        value_render_option: str = 'UNFORMATTED_VALUE'
    ) -> List[List[Any]]:
        """
        Get values from a specific range in a spreadsheet.
        
        Args:
            spreadsheet_id: The spreadsheet ID
            range_name: A1 notation range (e.g., 'Sheet1!A1:Z100')
            value_render_option: How values should be rendered
            
        Returns:
            List of rows, where each row is a list of cell values
        """
        self._initialize_service()
        
        try:
            result = await self._execute_with_retry(
                self.service.spreadsheets().values().get,
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueRenderOption=value_render_option
            )
            
            values = result.get('values', [])
            logger.info(f"Retrieved {len(values)} rows from {range_name}")
            return values
            
        except Exception as e:
            logger.error(f"Error getting values from {spreadsheet_id}, range {range_name}: {e}")
            raise
    
    async def get_header_row(
        self, 
        spreadsheet_id: str, 
        tab_name: str, 
        header_row_idx: int
    ) -> List[str]:
        """
        Get the header row from a specific tab.
        
        Args:
            spreadsheet_id: The spreadsheet ID
            tab_name: Name of the tab/sheet
            header_row_idx: 1-based row index for headers
            
        Returns:
            List of header values
        """
        range_name = f"{tab_name}!{header_row_idx}:{header_row_idx}"
        
        try:
            values = await self.get_sheet_values(spreadsheet_id, range_name)
            
            if not values:
                raise ValueError(f"No header row found at row {header_row_idx} in {tab_name}")
            
            headers = [str(cell).strip() for cell in values[0]]
            logger.info(f"Retrieved {len(headers)} headers from {tab_name}")
            return headers
            
        except Exception as e:
            logger.error(f"Error getting header row from {spreadsheet_id}, tab {tab_name}: {e}")
            raise
    
    async def get_data_rows(
        self, 
        spreadsheet_id: str, 
        tab_name: str, 
        start_row: int,
        end_row: Optional[int] = None
    ) -> List[List[Any]]:
        """
        Get data rows from a specific tab, excluding headers.
        
        Args:
            spreadsheet_id: The spreadsheet ID
            tab_name: Name of the tab/sheet
            start_row: 1-based row index to start from (typically header_row_idx + 1)
            end_row: 1-based row index to end at (optional, gets all if not specified)
            
        Returns:
            List of data rows
        """
        if end_row:
            range_name = f"{tab_name}!{start_row}:{end_row}"
        else:
            range_name = f"{tab_name}!{start_row}:1000000"  # Large range to get all data
        
        try:
            values = await self.get_sheet_values(spreadsheet_id, range_name)
            logger.info(f"Retrieved {len(values)} data rows from {tab_name}")
            return values
            
        except Exception as e:
            logger.error(f"Error getting data rows from {spreadsheet_id}, tab {tab_name}: {e}")
            raise
    
    async def get_sheet_metadata(self, spreadsheet_id: str) -> Dict[str, Any]:
        """
        Get metadata about the spreadsheet including sheet names.
        
        Args:
            spreadsheet_id: The spreadsheet ID
            
        Returns:
            Spreadsheet metadata
        """
        self._initialize_service()
        
        try:
            result = await self._execute_with_retry(
                self.service.spreadsheets().get,
                spreadsheetId=spreadsheet_id,
                fields='sheets.properties'
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting metadata for spreadsheet {spreadsheet_id}: {e}")
            raise
    
    def build_header_index(self, headers: List[str], config) -> Dict[str, int]:
        """
        Build a mapping from column names to indices based on configuration.
        
        Args:
            headers: List of header values from the spreadsheet
            config: Configuration object with column name mappings
            
        Returns:
            Dictionary mapping column names to 0-based indices
        """
        header_map = {}
        
        # Map each configured column name to its index
        column_mappings = {
            'admin': config.admin_header,
            'current_id': config.col_current_id,
            'business_use': config.col_business_use,
            'personal_use': config.col_personal_use,
            'personal_tax_cat': config.col_personal_tax_cat,
            'personal_percent_tax': config.col_personal_percent_tax,
            'business_tax_cat': config.col_business_tax_cat,
            'business_percent_tax': config.col_business_percent_tax
        }
        
        for key, column_name in column_mappings.items():
            try:
                index = headers.index(column_name)
                header_map[key] = index
                logger.debug(f"Mapped column '{column_name}' to index {index}")
            except ValueError:
                logger.warning(f"Column '{column_name}' not found in headers")
                header_map[key] = None
        
        logger.info(f"Built header index with {len([v for v in header_map.values() if v is not None])} mapped columns")
        return header_map
    
    async def get_header_mapping(self, spreadsheet_id: str, sheet_name: str, header_row: int, max_retries: int = 3) -> Dict[str, int]:
        """
        Get column header mapping for a spreadsheet (with caching).
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet tab
            header_row: 1-based row number containing headers
            max_retries: Maximum number of retry attempts
            
        Returns:
            Dictionary mapping expected column keys to column indices (0-based)
        """
        cache_key = f"{spreadsheet_id}:{sheet_name}:{header_row}"
        
        # Return cached mapping if available
        if cache_key in self._header_mapping_cache:
            logger.debug(f"SheetsClient[{self._instance_id}]: Using cached header mapping for {cache_key}")
            return self._header_mapping_cache[cache_key]
        
        self._initialize_service()
        
        for attempt in range(max_retries + 1):
            try:
                # Get header row using the thread pool
                range_name = f"{sheet_name}!{header_row}:{header_row}"
                result = await self._execute_with_retry(
                    self.service.spreadsheets().values().get,
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueRenderOption='UNFORMATTED_VALUE'
                )
                
                values = result.get('values', [])
                if not values or not values[0]:
                    logger.warning(f"SheetsClient[{self._instance_id}]: No header data found in {range_name}")
                    return {}
                
                # Create mapping from actual column names to indices
                headers = values[0]
                name_to_index = {str(header).strip(): idx for idx, header in enumerate(headers) if header}
                
                logger.info(f"SheetsClient[{self._instance_id}]: Found headers: {list(name_to_index.keys())}")
                
                # Map expected keys to column indices based on config
                mapping = {}
                column_mappings = {
                    'admin': config.admin_column,
                    'current_id': config.col_current_id,
                    'business_use': config.col_business_use,
                    'personal_use': config.col_personal_use,
                    'personal_tax_cat': config.col_personal_tax_cat,
                    'personal_percent_tax': config.col_personal_percent_tax,
                    'business_tax_cat': config.col_business_tax_cat,
                    'business_percent_tax': config.col_business_percent_tax
                }
                
                for key, column_name in column_mappings.items():
                    if column_name in name_to_index:
                        mapping[key] = name_to_index[column_name]
                        logger.info(f"SheetsClient[{self._instance_id}]: Mapped '{key}' -> '{column_name}' (index {name_to_index[column_name]})")
                    else:
                        logger.warning(f"SheetsClient[{self._instance_id}]: Column '{column_name}' not found in headers for key '{key}'")
                        mapping[key] = None
                
                # Cache the mapping
                self._header_mapping_cache[cache_key] = mapping
                logger.info(f"SheetsClient[{self._instance_id}]: Created header mapping for {cache_key}: {mapping}")
                
                return mapping
                
            except HttpError as e:
                if e.resp.status == 429:  # Rate limit exceeded
                    if attempt < max_retries:
                        logger.warning(f"SheetsClient[{self._instance_id}]: Rate limit exceeded (429), using exponential backoff for retry {attempt + 1}")
                        await self._exponential_backoff_sleep(attempt)
                        continue
                    else:
                        logger.error(f"SheetsClient[{self._instance_id}]: Rate limit exceeded after {max_retries} retries")
                        raise
                elif e.resp.status == 404:
                    logger.error(f"SheetsClient[{self._instance_id}]: Spreadsheet {spreadsheet_id} or sheet {sheet_name} not found")
                    raise
                else:
                    logger.error(f"SheetsClient[{self._instance_id}]: Sheets API error getting headers: {e}")
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * self._rate_limit_delay
                        logger.warning(f"SheetsClient[{self._instance_id}]: Retrying in {wait_time}s (attempt {attempt + 1})")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise
            except Exception as e:
                logger.error(f"SheetsClient[{self._instance_id}]: Unexpected error getting headers: {e}")
                if attempt < max_retries:
                    wait_time = (2 ** attempt) * self._rate_limit_delay
                    logger.warning(f"SheetsClient[{self._instance_id}]: Retrying in {wait_time}s (attempt {attempt + 1})")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise
        
        return {}
    
    async def get_sheet_data(self, spreadsheet_id: str, sheet_name: str, start_row: int, max_retries: int = 3) -> List[List[Any]]:
        """
        Get all data from a sheet starting from the specified row.
        
        Args:
            spreadsheet_id: Google Sheets spreadsheet ID
            sheet_name: Name of the sheet tab
            start_row: 1-based row number to start reading from
            max_retries: Maximum number of retry attempts
            
        Returns:
            List of rows, where each row is a list of cell values
        """
        self._initialize_service()
        
        for attempt in range(max_retries + 1):
            try:
                # Get all data from start_row onwards using thread pool
                range_name = f"{sheet_name}!{start_row}:1000000"  # Large range to get all data
                result = await self._execute_with_retry(
                    self.service.spreadsheets().values().get,
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueRenderOption='UNFORMATTED_VALUE'
                )
                
                values = result.get('values', [])
                logger.info(f"SheetsClient[{self._instance_id}]: Retrieved {len(values)} rows from {sheet_name} starting at row {start_row}")
                
                return values
                
            except HttpError as e:
                if e.resp.status == 429:  # Rate limit exceeded
                    if attempt < max_retries:
                        logger.warning(f"SheetsClient[{self._instance_id}]: Rate limit exceeded (429), using exponential backoff for retry {attempt + 1}")
                        await self._exponential_backoff_sleep(attempt)
                        continue
                    else:
                        logger.error(f"SheetsClient[{self._instance_id}]: Rate limit exceeded after {max_retries} retries")
                        raise
                elif e.resp.status == 404:
                    logger.error(f"SheetsClient[{self._instance_id}]: Spreadsheet {spreadsheet_id} or sheet {sheet_name} not found")
                    raise
                else:
                    logger.error(f"SheetsClient[{self._instance_id}]: Sheets API error getting data: {e}")
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * self._rate_limit_delay
                        logger.warning(f"SheetsClient[{self._instance_id}]: Retrying in {wait_time}s (attempt {attempt + 1})")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise
            except Exception as e:
                logger.error(f"SheetsClient[{self._instance_id}]: Unexpected error getting sheet data: {e}")
                if attempt < max_retries:
                    wait_time = (2 ** attempt) * self._rate_limit_delay
                    logger.warning(f"SheetsClient[{self._instance_id}]: Retrying in {wait_time}s (attempt {attempt + 1})")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise
        
        return []
    
    def clear_header_cache(self):
        """Clear the header mapping cache."""
        self._header_mapping_cache.clear()
        logger.info("Header mapping cache cleared")
    
    async def _exponential_backoff_sleep(self, attempt: int, max_backoff: float = 32.0):
        """
        Implement Google's recommended exponential backoff with jitter.
        
        Based on: https://developers.google.com/workspace/sheets/api/limits#resolve_time-based_quota_errors
        """
        import random
        
        # Calculate base delay: (2^n) + random_number_milliseconds
        base_delay = (2 ** attempt)
        random_jitter = random.uniform(0, 1.0)  # Random number up to 1 second
        
        # Apply maximum backoff limit
        wait_time = min(base_delay + random_jitter, max_backoff)
        
        logger.info(f"SheetsClient[{self._instance_id}]: Exponential backoff - waiting {wait_time:.2f}s (attempt {attempt + 1})")
        await asyncio.sleep(wait_time)
        
        return wait_time 