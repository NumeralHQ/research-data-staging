#!/usr/bin/env python3
"""
Build script for Research Data Aggregation Lambda function.

This script handles the complete build process:
1. Installs dependencies to lambda-package/
2. Copies source code to lambda-package/src/
3. Copies mapping files to lambda-package/mapping/
4. Creates deployment ZIP file

Usage: 
  python build.py           # Interactive mode
  python build.py --full    # Force full build
  python build.py --src     # Source code only
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
        "pip install -r requirements.txt -t lambda-package/",
        "Installing dependencies"
    )

def copy_source_code():
    """Copy source code to package directory."""
    print("📁 Copying source code...")
    src_dir = Path("src")
    dest_dir = Path("lambda-package/src")
    
    # Remove existing source code if it exists
    if dest_dir.exists():
        print("🧹 Removing existing source code...")
        shutil.rmtree(dest_dir)
    
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
    
    # Remove existing mapping files if they exist
    if dest_dir.exists():
        print("🧹 Removing existing mapping files...")
        shutil.rmtree(dest_dir)
    
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

def get_build_mode():
    """Get build mode from user input or command line args."""
    if len(sys.argv) > 1:
        if "--full" in sys.argv:
            return "full"
        elif "--src" in sys.argv:
            return "src"
        elif "--help" in sys.argv or "-h" in sys.argv:
            print(__doc__)
            sys.exit(0)
    
    # Interactive mode
    print("🚀 Research Data Aggregation Lambda Build Tool")
    print("=" * 60)
    print("Choose build mode:")
    print("1. 🔄 Full build (clean + dependencies + source + ZIP)")
    print("2. ⚡ Source only (update source code + ZIP)")
    print("3. ❌ Cancel")
    print()
    
    while True:
        choice = input("Enter your choice (1/2/3): ").strip()
        if choice == "1":
            return "full"
        elif choice == "2":
            return "src"
        elif choice == "3":
            print("Build cancelled.")
            sys.exit(0)
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

def check_dependencies_exist():
    """Check if dependencies are already installed."""
    package_dir = Path("lambda-package")
    if not package_dir.exists():
        return False
    
    # Check for some key dependency directories
    key_deps = ["google", "aws_lambda_powertools", "typing_extensions.py"]
    for dep in key_deps:
        if not (package_dir / dep).exists():
            return False
    
    return True

def main():
    """Main build process."""
    build_mode = get_build_mode()
    
    print(f"🚀 Starting {'full' if build_mode == 'full' else 'source-only'} build process")
    print("=" * 60)
    
    if build_mode == "full":
        # Full build: clean everything and rebuild
        clean_build_dir()
        install_dependencies()
        copy_source_code()
        copy_mapping_files()
        
    elif build_mode == "src":
        # Source-only build: check dependencies exist, then update source
        if not check_dependencies_exist():
            print("❌ Dependencies not found in lambda-package/")
            print("   Run a full build first or use --full flag")
            sys.exit(1)
        
        print("✅ Dependencies found, updating source code only...")
        copy_source_code()
        copy_mapping_files()
    
    # Always create ZIP file
    zip_file = create_deployment_zip()
    
    print("=" * 60)
    print("🎉 Build completed successfully!")
    print(f"📦 Deployment package: {zip_file}")
    
    if build_mode == "src":
        print("⚡ Source-only build completed in seconds!")
    
    print("\nNext steps:")
    print("1. Upload to Lambda: AWS Console → Functions → research-data-aggregation → Code source → Upload from .zip file")
    print("2. Or use AWS CLI: aws lambda update-function-code --function-name research-data-aggregation --zip-file fileb://research-data-aggregation.zip")
    print("3. Test the Lambda function")

if __name__ == "__main__":
    main() 