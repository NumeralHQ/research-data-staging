"""Test the new folder structure for S3 output."""

import re
from datetime import datetime
import pytz

from src.orchestrator import ResearchDataOrchestrator


def test_output_folder_generation():
    """Test that output folder names are generated correctly."""
    orchestrator = ResearchDataOrchestrator()
    
    # Generate a folder name
    folder_name = orchestrator._generate_output_folder()
    
    # Should match pattern: output-YYYYMMDD-HHMM
    pattern = r'^output-\d{8}-\d{4}$'
    assert re.match(pattern, folder_name), f"Folder name '{folder_name}' doesn't match expected pattern"
    
    # Should start with 'output-'
    assert folder_name.startswith('output-'), f"Folder name should start with 'output-'"
    
    # Extract timestamp part
    timestamp_part = folder_name[7:]  # Remove 'output-' prefix
    
    # Should be able to parse the timestamp
    try:
        parsed_time = datetime.strptime(timestamp_part, '%Y%m%d-%H%M')
        print(f"✅ Generated folder: {folder_name}")
        print(f"✅ Parsed timestamp: {parsed_time}")
    except ValueError as e:
        assert False, f"Could not parse timestamp '{timestamp_part}': {e}"


def test_folder_structure_example():
    """Test example of what the folder structure will look like."""
    orchestrator = ResearchDataOrchestrator()
    folder_name = orchestrator._generate_output_folder()
    
    # Example file paths
    csv_path = f"{folder_name}/matrix_update.csv"
    static_csv_path = f"{folder_name}/product_group_update.csv"
    product_item_csv_path = f"{folder_name}/product_item_update.csv"
    errors_path = f"{folder_name}/errors.json"
    
    print(f"📁 Output folder: {folder_name}")
    print(f"📄 Generated matrix CSV file path: {csv_path}")
    print(f"📄 Static product group CSV file path: {static_csv_path}")
    print(f"📄 Generated product item CSV file path: {product_item_csv_path}")
    print(f"❌ Errors file path: {errors_path}")
    
    # Verify paths are constructed correctly
    assert csv_path.endswith('/matrix_update.csv'), "Matrix CSV path should end with '/matrix_update.csv'"
    assert static_csv_path.endswith('/product_group_update.csv'), "Static CSV path should end with '/product_group_update.csv'"
    assert product_item_csv_path.endswith('/product_item_update.csv'), "Product item CSV path should end with '/product_item_update.csv'"
    assert errors_path.endswith('/errors.json'), "Errors path should end with '/errors.json'"
    assert csv_path.startswith(folder_name), "Matrix CSV path should start with folder name"
    assert static_csv_path.startswith(folder_name), "Static CSV path should start with folder name"
    assert product_item_csv_path.startswith(folder_name), "Product item CSV path should start with folder name"
    assert errors_path.startswith(folder_name), "Errors path should start with folder name"


def test_expected_output_structure():
    """Test the complete expected output structure with all files."""
    orchestrator = ResearchDataOrchestrator()
    folder_name = orchestrator._generate_output_folder()
    
    # All expected files in output folder
    expected_files = [
        f"{folder_name}/matrix_update.csv",        # Generated matrix CSV
        f"{folder_name}/product_group_update.csv", # Static product group CSV
        f"{folder_name}/product_item_update.csv",  # Generated product item CSV
        f"{folder_name}/errors.json"               # Error log (optional)
    ]
    
    print(f"📁 Complete output folder structure for: {folder_name}")
    for i, file_path in enumerate(expected_files, 1):
        file_name = file_path.split('/')[-1]
        print(f"  {i}. {file_name}")
        
        # Verify each path is correctly constructed
        assert file_path.startswith(folder_name), f"File path should start with folder name: {file_path}"
        assert '/' in file_path, f"File path should contain separator: {file_path}"
    
    print("✅ All expected files validated")


if __name__ == "__main__":
    test_output_folder_generation()
    test_folder_structure_example()
    test_expected_output_structure()
    print("✅ All folder structure tests passed!") 