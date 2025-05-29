#!/usr/bin/env python3
"""
Build script for Research Data Aggregation Lambda function.

This script handles the complete build process:
1. Installs dependencies to lambda-package/
2. Copies source code to lambda-package/src/
3. Copies mapping files to lambda-package/mapping/
4. Creates deployment ZIP file

Usage: python build.py
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

def run_command(cmd, description):
    """Run a command and handle errors."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed")
        return result
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed:")
        print(f"Command: {cmd}")
        print(f"Error: {e.stderr}")
        sys.exit(1)

def clean_build_dir():
    """Clean the lambda-package directory."""
    package_dir = Path("lambda-package")
    if package_dir.exists():
        print("🧹 Cleaning existing lambda-package directory...")
        shutil.rmtree(package_dir)
    package_dir.mkdir(exist_ok=True)
    print("✅ Build directory ready")

def install_dependencies():
    """Install Python dependencies."""
    run_command(
        "pip install -r requirements.txt -t lambda-package/ --no-deps",
        "Installing dependencies"
    )

def copy_source_code():
    """Copy source code to package directory."""
    print("📁 Copying source code...")
    src_dir = Path("src")
    dest_dir = Path("lambda-package/src")
    
    if src_dir.exists():
        shutil.copytree(src_dir, dest_dir)
        print("✅ Source code copied")
    else:
        print("❌ Source directory not found")
        sys.exit(1)

def copy_mapping_files():
    """Copy mapping files to package directory."""
    print("📁 Copying mapping files...")
    mapping_dir = Path("mapping")
    dest_dir = Path("lambda-package/mapping")
    
    if mapping_dir.exists():
        shutil.copytree(mapping_dir, dest_dir)
        print("✅ Mapping files copied")
    else:
        print("⚠️  Mapping directory not found - skipping")

def create_deployment_zip():
    """Create the deployment ZIP file."""
    print("📦 Creating deployment ZIP...")
    from create_zip import create_deployment_zip
    zip_file = create_deployment_zip()
    return zip_file

def main():
    """Main build process."""
    print("🚀 Starting build process for Research Data Aggregation Lambda")
    print("=" * 60)
    
    # Step 1: Clean build directory
    clean_build_dir()
    
    # Step 2: Install dependencies
    install_dependencies()
    
    # Step 3: Copy source code
    copy_source_code()
    
    # Step 4: Copy mapping files
    copy_mapping_files()
    
    # Step 5: Create ZIP file
    zip_file = create_deployment_zip()
    
    print("=" * 60)
    print("🎉 Build completed successfully!")
    print(f"📦 Deployment package: {zip_file}")
    print("\nNext steps:")
    print("1. Deploy infrastructure: sam deploy --template-file infrastructure/template.yaml")
    print("2. Upload mapping files to S3 bucket")
    print("3. Test the Lambda function")

if __name__ == "__main__":
    main() 