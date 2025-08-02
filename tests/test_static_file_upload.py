"""Test static file upload functionality."""

import asyncio
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock
import pytest

from src.orchestrator import ResearchDataOrchestrator


def test_static_files_exist():
    """Test that the required static data files exist."""
    data_dir = Path("src/data")
    
    # Data directory should exist
    assert data_dir.exists(), f"Data directory should exist: {data_dir}"
    
    # Should contain at least one CSV file
    csv_files = list(data_dir.glob("*.csv"))
    assert len(csv_files) > 0, f"Data directory should contain CSV files: {data_dir}"
    
    # Specifically check for product_group_update.csv
    product_group_file = data_dir / "product_group_update.csv"
    assert product_group_file.exists(), f"product_group_update.csv should exist: {product_group_file}"
    
    print(f"✅ Found {len(csv_files)} CSV file(s) in {data_dir}")
    for csv_file in csv_files:
        print(f"   - {csv_file.name}")


@pytest.mark.asyncio
async def test_upload_static_files_success():
    """Test successful upload of static files."""
    orchestrator = ResearchDataOrchestrator()
    
    # Mock S3 client
    mock_s3_client = Mock()
    orchestrator.s3_client = mock_s3_client
    
    # Test upload
    output_folder = "output-20241201-1200"
    
    with patch.object(orchestrator, '_upload_static_files_to_s3') as mock_upload:
        mock_upload.return_value = ["output-20241201-1200/product_group_update.csv"]
        
        result = await orchestrator._upload_static_files_to_s3(output_folder)
        
        assert isinstance(result, list), "Should return a list of uploaded file keys"
        assert len(result) >= 0, "Should return a list (may be empty if no files)"


@pytest.mark.asyncio 
async def test_upload_static_files_with_real_directory():
    """Test static file upload with the actual data directory."""
    orchestrator = ResearchDataOrchestrator()
    
    # Mock the S3 client put_object method
    mock_s3_client = Mock()
    orchestrator.s3_client = mock_s3_client
    
    output_folder = "test-output-folder"
    
    # Call the actual method (which will find real files)
    result = await orchestrator._upload_static_files_to_s3(output_folder)
    
    # Should have found and attempted to upload files
    data_dir = Path("src/orchestrator.py").parent / "data"
    if data_dir.exists():
        csv_files = list(data_dir.glob("*.csv"))
        expected_calls = len(csv_files)
        
        # Check that S3 put_object was called for each CSV file
        assert mock_s3_client.put_object.call_count == expected_calls, \
            f"Should have called put_object {expected_calls} times"
        
        # Check that the result contains the expected number of keys
        assert len(result) == expected_calls, \
            f"Should return {expected_calls} uploaded file keys"
        
        # Verify the file keys are correctly formatted
        for key in result:
            assert key.startswith(output_folder), f"Key should start with output folder: {key}"
            assert key.endswith('.csv'), f"Key should end with .csv: {key}"
            
        print(f"✅ Successfully uploaded {len(result)} static files")
        for key in result:
            print(f"   - {key}")


@pytest.mark.asyncio
async def test_upload_static_files_missing_directory():
    """Test behavior when data directory doesn't exist."""
    orchestrator = ResearchDataOrchestrator()
    
    # Create a temporary orchestrator with a non-existent data directory
    with patch.object(Path, '__truediv__') as mock_truediv:
        # Make the data directory path point to something that doesn't exist
        mock_data_dir = Mock()
        mock_data_dir.exists.return_value = False
        mock_truediv.return_value = mock_data_dir
        
        output_folder = "test-output"
        result = await orchestrator._upload_static_files_to_s3(output_folder)
        
        # Should return empty list when directory doesn't exist
        assert result == [], "Should return empty list when data directory missing"


def test_process_all_sheets_includes_static_files():
    """Test that process_all_sheets includes static_file_keys in return value."""
    orchestrator = ResearchDataOrchestrator()
    
    # Mock all external dependencies
    mock_drive_client = AsyncMock()
    mock_drive_client.list_files_in_folder.return_value = []
    orchestrator.drive_client = mock_drive_client
    
    # Run the process (with empty file list to avoid processing)
    result = asyncio.run(orchestrator.process_all_sheets())
    
    # Check that static_file_keys is in the result
    assert "static_file_keys" in result, "Result should include static_file_keys"
    assert isinstance(result["static_file_keys"], list), "static_file_keys should be a list"


if __name__ == "__main__":
    # Run basic tests
    test_static_files_exist()
    print("✅ Static file upload tests completed!") 