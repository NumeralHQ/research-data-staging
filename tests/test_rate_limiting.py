"""Test rate limiting and 429 error handling."""

import asyncio
import time
import logging
from unittest.mock import Mock, AsyncMock, patch
import pytest
from googleapiclient.errors import HttpError

from src.sheets_client import SheetsClient
from src.drive_client import DriveClient

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_mock_429_error():
    """Create a mock 429 HttpError."""
    mock_response = Mock()
    mock_response.status = 429
    mock_response.reason = "Too Many Requests"
    
    error = HttpError(mock_response, b'{"error": {"code": 429, "message": "Rate limit exceeded"}}')
    return error


@pytest.mark.asyncio
async def test_sheets_client_429_handling():
    """Test that SheetsClient handles 429 errors with exponential backoff."""
    
    client = SheetsClient(max_retries=3)
    
    # Mock the service and credentials
    with patch.object(client, '_get_credentials'), \
         patch('googleapiclient.discovery.build') as mock_build:
        
        mock_service = Mock()
        mock_build.return_value = mock_service
        
        # Mock the spreadsheets().values().get() chain
        mock_request = Mock()
        mock_service.spreadsheets.return_value.values.return_value.get.return_value = mock_request
        
        # First two calls raise 429, third succeeds
        mock_request.execute.side_effect = [
            create_mock_429_error(),  # First attempt: 429
            create_mock_429_error(),  # Second attempt: 429
            {'values': [['Header1', 'Header2', 'Header3']]}  # Third attempt: success
        ]
        
        # Track timing
        start_time = time.time()
        
        # This should succeed after 2 retries
        result = await client.get_header_mapping('test_sheet_id', 'Sheet1', 1)
        
        total_time = time.time() - start_time
        
        # Verify the result
        assert result is not None
        assert 'admin' in result  # Should have mapped some headers
        
        # Verify exponential backoff timing
        # First retry: ~1s, Second retry: ~2s = ~3s total minimum
        assert total_time >= 3.0, f"Expected at least 3s for exponential backoff, got {total_time:.2f}s"
        assert total_time < 10.0, f"Backoff took too long: {total_time:.2f}s"
        
        # Verify execute was called 3 times (initial + 2 retries)
        assert mock_request.execute.call_count == 3
        
        logger.info(f"✅ 429 handling test passed - total time: {total_time:.2f}s")


@pytest.mark.asyncio
async def test_sheets_client_429_exhaustion():
    """Test that SheetsClient eventually gives up after max retries."""
    
    client = SheetsClient(max_retries=2)  # Only 2 retries
    
    # Mock the service and credentials
    with patch.object(client, '_get_credentials'), \
         patch('googleapiclient.discovery.build') as mock_build:
        
        mock_service = Mock()
        mock_build.return_value = mock_service
        
        # Mock the spreadsheets().values().get() chain
        mock_request = Mock()
        mock_service.spreadsheets.return_value.values.return_value.get.return_value = mock_request
        
        # All calls raise 429
        mock_request.execute.side_effect = create_mock_429_error()
        
        # This should eventually raise the 429 error
        with pytest.raises(HttpError) as exc_info:
            await client.get_header_mapping('test_sheet_id', 'Sheet1', 1)
        
        # Verify it's a 429 error
        assert exc_info.value.resp.status == 429
        
        # Verify execute was called 3 times (initial + 2 retries)
        assert mock_request.execute.call_count == 3
        
        logger.info("✅ 429 exhaustion test passed - correctly gave up after max retries")


@pytest.mark.asyncio
async def test_drive_client_429_handling():
    """Test that DriveClient handles 429 errors with exponential backoff."""
    
    client = DriveClient()
    
    # Mock the service and credentials
    with patch.object(client, '_get_credentials'), \
         patch('googleapiclient.discovery.build') as mock_build:
        
        mock_service = Mock()
        mock_build.return_value = mock_service
        
        # Mock the files().list() chain
        mock_request = Mock()
        mock_service.files.return_value.list.return_value = mock_request
        
        # First call raises 429, second succeeds
        mock_request.execute.side_effect = [
            create_mock_429_error(),  # First attempt: 429
            {'files': [{'id': 'test_file', 'name': 'Test File'}]}  # Second attempt: success
        ]
        
        # Mock the files().get() call for folder verification
        mock_get_request = Mock()
        mock_service.files.return_value.get.return_value = mock_get_request
        mock_get_request.execute.return_value = {
            'id': 'test_folder',
            'name': 'Test Folder',
            'mimeType': 'application/vnd.google-apps.folder'
        }
        
        # Track timing
        start_time = time.time()
        
        # This should succeed after 1 retry
        result = await client.list_files_in_folder('test_folder_id')
        
        total_time = time.time() - start_time
        
        # Verify the result
        assert len(result) == 1
        assert result[0]['name'] == 'Test File'
        
        # Verify exponential backoff timing (at least 1 second for first retry)
        assert total_time >= 1.0, f"Expected at least 1s for exponential backoff, got {total_time:.2f}s"
        
        logger.info(f"✅ DriveClient 429 handling test passed - total time: {total_time:.2f}s")


@pytest.mark.asyncio
async def test_exponential_backoff_timing():
    """Test that exponential backoff follows Google's recommended algorithm."""
    
    client = SheetsClient()
    
    # Test the exponential backoff method directly
    timings = []
    
    for attempt in range(4):
        start = time.time()
        await client._exponential_backoff_sleep(attempt, max_backoff=32.0)
        elapsed = time.time() - start
        timings.append(elapsed)
        logger.info(f"Attempt {attempt}: waited {elapsed:.2f}s")
    
    # Verify exponential growth with jitter
    # Expected: ~1s, ~2s, ~4s, ~8s (plus random jitter up to 1s)
    assert 1.0 <= timings[0] <= 3.0, f"First backoff should be 1-3s, got {timings[0]:.2f}s"
    assert 2.0 <= timings[1] <= 4.0, f"Second backoff should be 2-4s, got {timings[1]:.2f}s"
    assert 4.0 <= timings[2] <= 6.0, f"Third backoff should be 4-6s, got {timings[2]:.2f}s"
    assert 8.0 <= timings[3] <= 10.0, f"Fourth backoff should be 8-10s, got {timings[3]:.2f}s"
    
    logger.info("✅ Exponential backoff timing test passed")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"]) 