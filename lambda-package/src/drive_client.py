"""Google Drive API client with rate limiting and error handling."""

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


class DriveClient:
    """Google Drive API client with rate limiting and retry logic."""
    
    # Class-level thread pool executor for all instances
    _executor = None
    _executor_lock = asyncio.Lock()
    
    # Class-level rate limiting for all instances (shared across all clients)
    _last_request_time = 0
    _rate_limit_lock = asyncio.Lock()
    
    def __init__(self):
        self.service = None
        # Reduced rate limit delay for better performance
        self._rate_limit_delay = max(0.05, config.rate_limit_delay)  # Minimum 50ms, but respect config
        
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
                        thread_name_prefix="drive_api"
                    )
                    logger.info(f"Created Drive API thread pool executor with {max_workers} workers")
        return cls._executor
    
    def _get_credentials(self):
        """Get Google credentials using service account key."""
        logger.info("Getting Google credentials using service account")
        
        try:
            # Use google.auth.default() which will automatically detect and use
            # the service account key from GOOGLE_APPLICATION_CREDENTIALS
            credentials, project = google.auth.default(
                scopes=['https://www.googleapis.com/auth/drive.readonly']
            )
            
            logger.info(f"Successfully obtained credentials for project: {project}")
            
            # Refresh credentials if needed
            if not credentials.valid:
                logger.info("Refreshing credentials...")
                credentials.refresh(Request())
                
            return credentials
            
        except Exception as e:
            logger.error(f"Error getting Google credentials: {e}")
            raise
    
    def _initialize_service(self):
        """Initialize the Google Drive service."""
        if self.service is None:
            credentials = self._get_credentials()
            self.service = build('drive', 'v3', credentials=credentials)
    
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
        executor = await self._get_executor()
        
        for attempt in range(3 + 1):  # max_retries = 3
            try:
                # Apply global rate limiting only once per request
                await self._global_rate_limit()
                
                # Execute the request in a thread pool
                # We need to create a callable that captures the arguments
                import functools
                callable_func = functools.partial(self._execute_request_sync, request_func, *args, **kwargs)
                
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(executor, callable_func)
                return result
                
            except HttpError as e:
                if e.resp.status in [429, 500, 502, 503, 504]:
                    if attempt < 3:  # max_retries
                        delay = 1.0 * (2 ** attempt)  # base_delay = 1.0
                        logger.warning(
                            f"DriveClient[{self._instance_id}]: Request failed with {e.resp.status}, retrying in {delay}s "
                            f"(attempt {attempt + 1}/{3 + 1})"
                        )
                        await asyncio.sleep(delay)
                        continue
                
                logger.error(f"DriveClient[{self._instance_id}]: Drive API request failed: {e}")
                raise
            
            except Exception as e:
                logger.error(f"DriveClient[{self._instance_id}]: Unexpected error in Drive API request: {e}")
                raise
        
        raise RuntimeError(f"Request failed after {3 + 1} attempts")
    
    async def list_files_in_folder(self, folder_id: str, max_retries: int = 3) -> List[dict]:
        """
        List all Google Sheets files in the specified folder.
        
        Args:
            folder_id: Google Drive folder ID
            max_retries: Maximum number of retry attempts
            
        Returns:
            List of file metadata dictionaries
        """
        self._initialize_service()
        
        try:
            # First, let's try to get the folder itself to verify access
            logger.info(f"DriveClient[{self._instance_id}]: Verifying access to folder: {folder_id}")
            try:
                folder_info = await self._execute_with_retry(
                    self.service.files().get,
                    fileId=folder_id,
                    fields="id, name, mimeType, parents, capabilities",
                    supportsAllDrives=True
                )
                logger.info(f"DriveClient[{self._instance_id}]: Folder access verified: {folder_info.get('name')} (MIME: {folder_info.get('mimeType')})")
                logger.info(f"DriveClient[{self._instance_id}]: Folder parents: {folder_info.get('parents', [])}")
                logger.info(f"DriveClient[{self._instance_id}]: Folder capabilities: {folder_info.get('capabilities', {})}")
            except HttpError as folder_error:
                logger.error(f"DriveClient[{self._instance_id}]: Cannot access folder {folder_id}: {folder_error}")
                
                # Try to search for any files the service account can access
                logger.info("DriveClient[{self._instance_id}]: Testing basic Drive API access...")
                try:
                    test_results = await self._execute_with_retry(
                        self.service.files().list,
                        q="trashed=false",
                        fields="nextPageToken, files(id, name, mimeType, parents)",
                        pageSize=5,
                        corpora="user",
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True
                    )
                    
                    test_files = test_results.get('files', [])
                    logger.info(f"DriveClient[{self._instance_id}]: Service account can access {len(test_files)} files total")
                    
                    if test_files:
                        for file in test_files:
                            logger.info(f"DriveClient[{self._instance_id}]: Accessible file: {file.get('name')} (Parents: {file.get('parents', [])})")
                    else:
                        logger.warning("DriveClient[{self._instance_id}]: Service account cannot access any files - check authentication")
                        
                except Exception as test_error:
                    logger.error(f"DriveClient[{self._instance_id}]: Basic Drive API test failed: {test_error}")
                
                raise folder_error
            
            # Query for Google Sheets files in the folder using proper pagination
            # Following the official documentation format
            all_files = []
            page_token = None
            
            while True:
                # Use the exact query format from the documentation
                query = f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
                logger.info(f"DriveClient[{self._instance_id}]: Drive API query: {query}")
                
                # Use parameters exactly as shown in the documentation
                request_params = {
                    'q': query,
                    'fields': "nextPageToken, files(id, name, mimeType, parents, modifiedTime, size)",
                    'pageSize': 1000,  # Maximum allowed
                    'corpora': 'user',  # Search user's files (default)
                    'includeItemsFromAllDrives': True,  # Include shared drives
                    'supportsAllDrives': True  # Support shared drives
                }
                
                if page_token:
                    request_params['pageToken'] = page_token
                
                logger.info(f"DriveClient[{self._instance_id}]: Request parameters: {request_params}")
                
                results = await self._execute_with_retry(
                    self.service.files().list,
                    **request_params
                )
                
                files = results.get('files', [])
                all_files.extend(files)
                
                logger.info(f"DriveClient[{self._instance_id}]: Page returned {len(files)} Google Sheets files")
                
                # Log details about what we found on this page
                for file in files:
                    logger.info(f"DriveClient[{self._instance_id}]: Found Google Sheet: {file.get('name')} (ID: {file.get('id')})")
                
                # Check for next page
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
                
                logger.info(f"DriveClient[{self._instance_id}]: Fetching next page with token: {page_token[:20]}...")
            
            logger.info(f"DriveClient[{self._instance_id}]: Total Google Sheets files found: {len(all_files)}")
            
            # If no Google Sheets found, try broader searches for debugging
            if not all_files:
                logger.warning("DriveClient[{self._instance_id}]: No Google Sheets files found - running diagnostic queries...")
                
                # Try to find ANY files in the folder
                logger.info("DriveClient[{self._instance_id}]: Searching for any files in folder...")
                broad_query = f"'{folder_id}' in parents and trashed = false"
                
                broad_results = await self._execute_with_retry(
                    self.service.files().list,
                    q=broad_query,
                    fields="nextPageToken, files(id, name, mimeType, parents)",
                    pageSize=100,
                    corpora='user',
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True
                )
                
                broad_files = broad_results.get('files', [])
                logger.info(f"DriveClient[{self._instance_id}]: Found {len(broad_files)} files of any type in folder")
                
                if broad_files:
                    for file in broad_files:
                        logger.info(f"DriveClient[{self._instance_id}]: File in folder: {file.get('name')} (MIME: {file.get('mimeType')})")
                else:
                    logger.warning("DriveClient[{self._instance_id}]: No files of any type found in folder")
                    
                    # Try searching all accessible Google Sheets
                    logger.info("DriveClient[{self._instance_id}]: Searching for any accessible Google Sheets...")
                    sheets_query = "mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
                    
                    sheets_results = await self._execute_with_retry(
                        self.service.files().list,
                        q=sheets_query,
                        fields="nextPageToken, files(id, name, mimeType, parents)",
                        pageSize=10,
                        corpora='user',
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True
                    )
                    
                    accessible_sheets = sheets_results.get('files', [])
                    logger.info(f"DriveClient[{self._instance_id}]: Service account can access {len(accessible_sheets)} Google Sheets total")
                    
                    if accessible_sheets:
                        for sheet in accessible_sheets[:5]:  # Show first 5
                            parents = sheet.get('parents', [])
                            logger.info(f"DriveClient[{self._instance_id}]: Accessible Google Sheet: {sheet.get('name')} (Parents: {parents})")
            
            return all_files
            
        except Exception as e:
            logger.error(f"DriveClient[{self._instance_id}]: Error listing files: {e}")
            raise
    
    async def get_file_metadata(self, file_id: str, max_retries: int = 3) -> Optional[dict]:
        """
        Get metadata for a specific file.
        
        Args:
            file_id: Google Drive file ID
            max_retries: Maximum number of retry attempts
            
        Returns:
            File metadata dictionary or None if not found
        """
        self._initialize_service()
        
        try:
            result = await self._execute_with_retry(
                self.service.files().get,
                fileId=file_id,
                fields="id,name,modifiedTime,size,parents"
            )
            
            return result
            
        except HttpError as e:
            if e.resp.status == 404:
                logger.warning(f"DriveClient[{self._instance_id}]: File {file_id} not found")
                return None
            else:
                logger.error(f"DriveClient[{self._instance_id}]: Drive API error getting file metadata: {e}")
                raise
        except Exception as e:
            logger.error(f"DriveClient[{self._instance_id}]: Unexpected error getting file metadata: {e}")
            raise 