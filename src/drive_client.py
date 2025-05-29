"""Google Drive API client with rate limiting and error handling."""

import asyncio
import logging
import time
from typing import List, Dict, Any, Optional

from google.auth.transport.requests import Request
import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .config import config

logger = logging.getLogger(__name__)


class DriveClient:
    """Google Drive API client with rate limiting and retry logic."""
    
    def __init__(self):
        self.service = None
        self._last_request_time = 0
        self._rate_limit_delay = config.rate_limit_delay
    
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
    
    async def _rate_limit(self):
        """Apply rate limiting between requests."""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        
        if time_since_last < self._rate_limit_delay:
            sleep_time = self._rate_limit_delay - time_since_last
            await asyncio.sleep(sleep_time)
        
        self._last_request_time = time.time()
    
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
        
        for attempt in range(max_retries + 1):
            try:
                await self._rate_limit()
                
                # First, let's try to get the folder itself to verify access
                logger.info(f"Verifying access to folder: {folder_id}")
                try:
                    folder_info = self.service.files().get(
                        fileId=folder_id,
                        fields="id, name, mimeType, parents, capabilities",
                        supportsAllDrives=True
                    ).execute()
                    logger.info(f"Folder access verified: {folder_info.get('name')} (MIME: {folder_info.get('mimeType')})")
                    logger.info(f"Folder parents: {folder_info.get('parents', [])}")
                    logger.info(f"Folder capabilities: {folder_info.get('capabilities', {})}")
                except HttpError as folder_error:
                    logger.error(f"Cannot access folder {folder_id}: {folder_error}")
                    
                    # Try to search for any files the service account can access
                    logger.info("Testing basic Drive API access...")
                    try:
                        test_results = self.service.files().list(
                            q="trashed=false",
                            fields="nextPageToken, files(id, name, mimeType, parents)",
                            pageSize=5,
                            corpora="user",
                            includeItemsFromAllDrives=True,
                            supportsAllDrives=True
                        ).execute()
                        
                        test_files = test_results.get('files', [])
                        logger.info(f"Service account can access {len(test_files)} files total")
                        
                        if test_files:
                            for file in test_files:
                                logger.info(f"Accessible file: {file.get('name')} (Parents: {file.get('parents', [])})")
                        else:
                            logger.warning("Service account cannot access any files - check authentication")
                            
                    except Exception as test_error:
                        logger.error(f"Basic Drive API test failed: {test_error}")
                    
                    raise folder_error
                
                # Query for Google Sheets files in the folder using proper pagination
                # Following the official documentation format
                all_files = []
                page_token = None
                
                while True:
                    # Use the exact query format from the documentation
                    query = f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
                    logger.info(f"Drive API query: {query}")
                    
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
                    
                    logger.info(f"Request parameters: {request_params}")
                    
                    results = self.service.files().list(**request_params).execute()
                    
                    files = results.get('files', [])
                    all_files.extend(files)
                    
                    logger.info(f"Page returned {len(files)} Google Sheets files")
                    
                    # Log details about what we found on this page
                    for file in files:
                        logger.info(f"Found Google Sheet: {file.get('name')} (ID: {file.get('id')})")
                    
                    # Check for next page
                    page_token = results.get('nextPageToken')
                    if not page_token:
                        break
                    
                    logger.info(f"Fetching next page with token: {page_token[:20]}...")
                
                logger.info(f"Total Google Sheets files found: {len(all_files)}")
                
                # If no Google Sheets found, try broader searches for debugging
                if not all_files:
                    logger.warning("No Google Sheets files found - running diagnostic queries...")
                    
                    # Try to find ANY files in the folder
                    logger.info("Searching for any files in folder...")
                    broad_query = f"'{folder_id}' in parents and trashed = false"
                    
                    broad_results = self.service.files().list(
                        q=broad_query,
                        fields="nextPageToken, files(id, name, mimeType, parents)",
                        pageSize=100,
                        corpora='user',
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True
                    ).execute()
                    
                    broad_files = broad_results.get('files', [])
                    logger.info(f"Found {len(broad_files)} files of any type in folder")
                    
                    if broad_files:
                        for file in broad_files:
                            logger.info(f"File in folder: {file.get('name')} (MIME: {file.get('mimeType')})")
                    else:
                        logger.warning("No files of any type found in folder")
                        
                        # Try searching all accessible Google Sheets
                        logger.info("Searching for any accessible Google Sheets...")
                        sheets_query = "mimeType = 'application/vnd.google-apps.spreadsheet' and trashed = false"
                        
                        sheets_results = self.service.files().list(
                            q=sheets_query,
                            fields="nextPageToken, files(id, name, mimeType, parents)",
                            pageSize=10,
                            corpora='user',
                            includeItemsFromAllDrives=True,
                            supportsAllDrives=True
                        ).execute()
                        
                        accessible_sheets = sheets_results.get('files', [])
                        logger.info(f"Service account can access {len(accessible_sheets)} Google Sheets total")
                        
                        if accessible_sheets:
                            for sheet in accessible_sheets[:5]:  # Show first 5
                                parents = sheet.get('parents', [])
                                logger.info(f"Accessible Google Sheet: {sheet.get('name')} (Parents: {parents})")
                
                return all_files
                
            except HttpError as e:
                if e.resp.status == 429:  # Rate limit exceeded
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * self._rate_limit_delay
                        logger.warning(f"Rate limit exceeded, waiting {wait_time}s before retry {attempt + 1}")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Rate limit exceeded after {max_retries} retries")
                        raise
                elif e.resp.status == 403:  # Forbidden
                    logger.error(f"Access denied to folder {folder_id}. Check permissions.")
                    logger.error(f"Error details: {e}")
                    raise
                elif e.resp.status == 404:  # Not found
                    logger.error(f"Folder {folder_id} not found")
                    logger.error(f"This means the service account cannot see the folder at all")
                    logger.error(f"Please verify the folder is shared with: research-data-service@possible-origin-456416-f4.iam.gserviceaccount.com")
                    raise
                else:
                    logger.error(f"Drive API error: {e}")
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * self._rate_limit_delay
                        logger.warning(f"Retrying in {wait_time}s (attempt {attempt + 1})")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise
            except Exception as e:
                logger.error(f"Unexpected error listing files: {e}")
                if attempt < max_retries:
                    wait_time = (2 ** attempt) * self._rate_limit_delay
                    logger.warning(f"Retrying in {wait_time}s (attempt {attempt + 1})")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise
        
        return []  # Should not reach here
    
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
        
        for attempt in range(max_retries + 1):
            try:
                await self._rate_limit()
                
                result = self.service.files().get(
                    fileId=file_id,
                    fields="id,name,modifiedTime,size,parents"
                ).execute()
                
                return result
                
            except HttpError as e:
                if e.resp.status == 404:
                    logger.warning(f"File {file_id} not found")
                    return None
                elif e.resp.status == 429:  # Rate limit
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * self._rate_limit_delay
                        logger.warning(f"Rate limit exceeded, waiting {wait_time}s before retry {attempt + 1}")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise
                else:
                    logger.error(f"Drive API error getting file metadata: {e}")
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * self._rate_limit_delay
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise
            except Exception as e:
                logger.error(f"Unexpected error getting file metadata: {e}")
                if attempt < max_retries:
                    wait_time = (2 ** attempt) * self._rate_limit_delay
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    raise
        
        return None 