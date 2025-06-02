"""Test async concurrency behavior."""

import asyncio
import time
import logging
from unittest.mock import Mock, AsyncMock
import pytest

from src.worker import process_sheets_concurrently
from src.sheets_client import SheetsClient
from src.mapper import RowMapper
from src.models import LookupTables

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_concurrent_processing_timing():
    """Test that concurrent processing actually runs in parallel."""
    
    # Create mock file list
    file_list = [
        {'id': f'file_{i}', 'name': f'Test File {i}'} 
        for i in range(5)
    ]
    
    # Create mock clients
    mock_sheets_client = Mock(spec=SheetsClient)
    mock_row_mapper = Mock(spec=RowMapper)
    
    # Mock the header mapping call (called once at the start)
    mock_sheets_client.get_header_mapping = AsyncMock(return_value={
        'admin': 0,
        'current_id': 1,
        'business_use': 2,
        'personal_use': 3
    })
    
    # Track timing for each "sheet processing" call
    processing_times = []
    
    async def mock_process_sheet(self, file_info, header_mapping=None):
        """Mock sheet processing that takes 0.5 seconds."""
        start_time = time.time()
        file_name = file_info['name']
        logger.info(f"Mock processing started: {file_name}")
        
        # Simulate processing time
        await asyncio.sleep(0.5)
        
        end_time = time.time()
        processing_times.append((file_name, start_time, end_time))
        logger.info(f"Mock processing completed: {file_name}")
        
        return {
            'file_id': file_info['id'],
            'file_name': file_name,
            'success': True,
            'records': [],
            'rows_processed': 0
        }
    
    # Patch the SheetWorker to use our mock
    from src.worker import SheetWorker
    original_process_sheet = SheetWorker.process_sheet
    SheetWorker.process_sheet = mock_process_sheet
    
    try:
        # Test with max_concurrency = 3
        start_time = time.time()
        results = await process_sheets_concurrently(
            file_list, 
            mock_row_mapper,
            max_concurrency=3
        )
        total_time = time.time() - start_time
        
        # Verify results
        assert len(results) == 5
        assert all(r['success'] for r in results)
        
        # Verify timing - with concurrency=3 and 5 files taking 0.5s each:
        # - First 3 files should start immediately
        # - Next 2 files should start after first batch completes
        # - Total time should be around 1.0s (2 batches of 0.5s each)
        # - NOT 2.5s (which would be sequential)
        
        logger.info(f"Total processing time: {total_time:.2f}s")
        logger.info("Processing timeline:")
        for file_name, start, end in processing_times:
            relative_start = start - start_time
            relative_end = end - start_time
            logger.info(f"  {file_name}: {relative_start:.2f}s - {relative_end:.2f}s")
        
        # Assert that total time is significantly less than sequential time
        sequential_time = 5 * 0.5  # 2.5 seconds if run sequentially
        assert total_time < sequential_time * 0.8, f"Processing took {total_time:.2f}s, expected < {sequential_time * 0.8:.2f}s (80% of sequential)"
        
        # Assert that we have overlapping execution (concurrent behavior)
        # Check if any files were processing at the same time
        overlaps = 0
        for i, (name1, start1, end1) in enumerate(processing_times):
            for j, (name2, start2, end2) in enumerate(processing_times):
                if i != j and start1 < end2 and start2 < end1:
                    overlaps += 1
        
        assert overlaps > 0, "No overlapping execution detected - processing appears to be sequential"
        logger.info(f"Detected {overlaps} overlapping execution periods - concurrency is working!")
        
    finally:
        # Restore original method
        SheetWorker.process_sheet = original_process_sheet


if __name__ == "__main__":
    asyncio.run(test_concurrent_processing_timing()) 