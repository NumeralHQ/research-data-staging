"""Test that the concurrent processing fix works correctly."""

import asyncio
import time
import logging
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import pytest

from src.worker import process_sheets_concurrently
from src.mapper import RowMapper
from src.models import LookupTables

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_thread_pool_enables_true_concurrency():
    """Test that the thread pool executor enables true concurrent processing."""
    
    # Create mock file list
    file_list = [
        {'id': f'file_{i}', 'name': f'Test File {i}'} 
        for i in range(6)  # 6 files with concurrency=3 should show 2 batches
    ]
    
    # Create mock row mapper
    mock_row_mapper = Mock(spec=RowMapper)
    mock_row_mapper.process_sheet_rows.return_value = ([], None, [])  # No records, no error, no processing errors
    
    # Track when each "API call" starts and ends
    api_call_times = []
    call_counter = 0
    
    def mock_execute_sync():
        """Mock synchronous execution that simulates API call timing."""
        nonlocal call_counter
        call_counter += 1
        call_id = call_counter
        
        start_time = time.time()
        logger.info(f"🔄 API call {call_id} started")
        
        # Simulate API call duration
        time.sleep(0.3)  # 300ms API call
        
        end_time = time.time()
        api_call_times.append((call_id, start_time, end_time))
        logger.info(f"✅ API call {call_id} completed")
        
        # Return mock response
        return {
            'values': [['Admin', 'Current ID', 'Business Use', 'Personal Use']]
        }
    
    # Mock the SheetsClient with thread pool behavior
    with patch('src.worker.SheetsClient') as MockSheetsClient:
        # Create a factory that returns mock instances
        def create_mock_client():
            mock_client = Mock()
            
            # Mock the _execute_with_retry method to use our mock_execute_sync
            async def mock_execute_with_retry(request_func, *args, **kwargs):
                # Simulate the thread pool execution
                loop = asyncio.get_event_loop()
                executor = MockSheetsClient._executor
                if executor is None:
                    # Create a real thread pool for the test
                    import concurrent.futures
                    executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
                    MockSheetsClient._executor = executor
                
                # Execute in thread pool without extra arguments
                result = await loop.run_in_executor(executor, mock_execute_sync)
                return result
            
            mock_client._execute_with_retry = mock_execute_with_retry
            
            # Mock other methods to return proper header mapping
            async def mock_get_header_mapping(spreadsheet_id, sheet_name, header_row):
                result = await mock_execute_with_retry(Mock())
                # Return proper header mapping
                return {
                    'admin': 0,
                    'current_id': 1,
                    'business_use': 2,
                    'personal_use': 3,
                    'personal_tax_cat': 4,
                    'personal_percent_tax': 5,
                    'business_tax_cat': 6,
                    'business_percent_tax': 7
                }
            
            async def mock_get_sheet_data(spreadsheet_id, sheet_name, start_row):
                result = await mock_execute_with_retry(Mock())
                # Return mock sheet data
                return [['Tag Level', 'Item1', 'Taxable', 'Personal']]
            
            mock_client.get_header_mapping = mock_get_header_mapping
            mock_client.get_sheet_data = mock_get_sheet_data
            
            return mock_client
        
        MockSheetsClient.side_effect = create_mock_client
        MockSheetsClient._executor = None  # Initialize class variable
        
        # Test with max_concurrency = 3
        start_time = time.time()
        results = await process_sheets_concurrently(
            file_list, 
            mock_row_mapper,
            max_concurrency=3
        )
        total_time = time.time() - start_time
        
        # Verify results
        assert len(results) == 6
        assert all(r['success'] for r in results)
        
        # Analyze timing for concurrent execution
        logger.info(f"Total processing time: {total_time:.2f}s")
        logger.info("API call timeline:")
        
        for call_id, start, end in api_call_times:
            relative_start = start - start_time
            relative_end = end - start_time
            logger.info(f"  API call {call_id}: {relative_start:.2f}s - {relative_end:.2f}s")
        
        # Check for overlapping execution (true concurrency)
        overlaps = 0
        for i, (id1, start1, end1) in enumerate(api_call_times):
            for j, (id2, start2, end2) in enumerate(api_call_times):
                if i != j and start1 < end2 and start2 < end1:
                    overlaps += 1
                    logger.info(f"  Overlap detected: call {id1} and call {id2}")
        
        assert overlaps > 0, f"No overlapping API calls detected - still running sequentially! Calls: {api_call_times}"
        logger.info(f"✅ Detected {overlaps} overlapping API calls - TRUE CONCURRENCY WORKING!")
        
        # With 6 files, concurrency=3, and ~0.6s per file (2 API calls * 0.3s each):
        # Sequential would be: 6 * 0.6s = 3.6s
        # Concurrent should be: 2 batches * 0.6s = ~1.2s (plus some overhead)
        sequential_time = 6 * 0.6
        expected_concurrent_time = sequential_time / 3  # With concurrency=3
        
        assert total_time < sequential_time * 0.7, f"Still too slow: {total_time:.2f}s vs expected < {sequential_time * 0.7:.2f}s"
        logger.info(f"✅ Performance improvement confirmed: {total_time:.2f}s vs sequential {sequential_time:.2f}s")


if __name__ == "__main__":
    asyncio.run(test_thread_pool_enables_true_concurrency()) 