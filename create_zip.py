#!/usr/bin/env python3
"""
Create ZIP file for Lambda deployment.

This utility script creates a deployment package for AWS Lambda by:
1. Taking all files from the lambda-package directory (which contains both dependencies and source code)
2. Creating a compressed ZIP file with proper structure
3. Reporting file count and size for deployment validation

The lambda-package directory contains:
- src/ - Our application source code
- mapping/ - Lookup tables (geo_state.csv, tax_cat.csv)
- All Python dependencies installed via pip install -t

Usage: python create_zip.py

The script will create 'research-data-aggregation.zip' ready for Lambda upload.
"""

import os
import zipfile
from pathlib import Path
from datetime import datetime

def create_deployment_zip():
    """Create a deployment ZIP file with all dependencies and source code."""
    
    # Use fixed filename that will overwrite previous versions
    zip_filename = "research-data-aggregation.zip"
    
    print(f"Creating ZIP file: {zip_filename}")
    
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add all files from lambda-package directory
        package_dir = 'lambda-package'
        file_count = 0
        
        for root, dirs, files in os.walk(package_dir):
            for file in files:
                file_path = os.path.join(root, file)
                # Calculate the archive path (remove 'lambda-package/' prefix)
                archive_path = os.path.relpath(file_path, package_dir)
                zipf.write(file_path, archive_path)
                file_count += 1
                
                if file_count % 100 == 0:
                    print(f"Added {file_count} files...")
    
    # Get file size
    file_size = os.path.getsize(zip_filename)
    file_size_mb = file_size / (1024 * 1024)
    
    print("✅ ZIP file created successfully!")
    print(f"📁 File: {zip_filename}")
    print(f"📊 Size: {file_size_mb:.1f} MB")
    print(f"📄 Files: {file_count}")
    
    if file_size_mb > 50:
        print("⚠️  Warning: ZIP file is larger than 50MB. You may need to upload via S3.")
    else:
        print("✅ ZIP file size is acceptable for direct Lambda upload.")
    
    return zip_filename

if __name__ == "__main__":
    create_deployment_zip() 