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
    csv_path = f"{folder_name}/results.csv"
    errors_path = f"{folder_name}/errors.json"
    
    print(f"📁 Output folder: {folder_name}")
    print(f"📄 CSV file path: {csv_path}")
    print(f"❌ Errors file path: {errors_path}")
    
    # Verify paths are constructed correctly
    assert csv_path.endswith('/results.csv'), "CSV path should end with '/results.csv'"
    assert errors_path.endswith('/errors.json'), "Errors path should end with '/errors.json'"
    assert csv_path.startswith(folder_name), "CSV path should start with folder name"
    assert errors_path.startswith(folder_name), "Errors path should start with folder name"


if __name__ == "__main__":
    test_output_folder_generation()
    test_folder_structure_example()
    print("✅ All folder structure tests passed!") 