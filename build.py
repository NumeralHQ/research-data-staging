#!/usr/bin/env python3
"""
Enhanced Build & Deploy script for Research Data Aggregation Lambda function.

This script handles the complete build and deployment process:
1. Installs dependencies to lambda-package/
2. Copies source code to lambda-package/src/
3. Copies mapping files to lambda-package/mapping/
4. Creates deployment ZIP file
5. Deploys to AWS Lambda (NEW)
6. Invokes Lambda function for testing (NEW)

Usage: 
  python build.py                    # Interactive mode (enhanced menu)
  python build.py --full             # Force full build
  python build.py --src              # Source code only
  python build.py --deploy           # Build + Deploy to AWS
  python build.py --src-deploy       # Source build + Deploy (fast for code changes)
  python build.py --deploy-only      # Deploy existing ZIP
  python build.py --full-deploy      # Full build + Deploy
  python build.py --invoke           # Invoke Lambda function
  python build.py --info             # Show Lambda info
  python build.py --help             # Show help
"""

import os
import shutil
import subprocess
import sys
import argparse
import json
from pathlib import Path
from datetime import datetime

# Import AWS utilities and configuration
try:
    from aws_utils import AWSManager
    from deployment_config import LAMBDA_CONFIG, AWS_CONFIG
    LAMBDA_FUNCTION_NAME = LAMBDA_CONFIG["function_name"]
    LAMBDA_FUNCTION_ARN = LAMBDA_CONFIG["function_arn"]
except ImportError:
    # Graceful fallback if AWS utilities aren't available
    AWSManager = None
    LAMBDA_FUNCTION_NAME = "research-data-aggregation"
    LAMBDA_FUNCTION_ARN = "arn:aws:lambda:us-west-2:056694064025:function:research-data-aggregation"

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Enhanced Build & Deploy tool for Research Data Aggregation Lambda function",
        epilog="Examples:\n"
               "  python build.py                 # Interactive mode\n"
               "  python build.py --deploy        # Build and deploy\n"
               "  python build.py --src-deploy    # Source build + deploy (fast)\n"
               "  python build.py --invoke        # Run Lambda function\n"
               "  python build.py --info          # Show function info",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Build options
    parser.add_argument("--full", action="store_true", 
                       help="Force full build (clean + dependencies)")
    parser.add_argument("--src", action="store_true",
                       help="Source code only build")
    
    # Deploy options  
    parser.add_argument("--deploy", action="store_true",
                       help="Build and deploy to AWS Lambda")
    parser.add_argument("--deploy-only", action="store_true",
                       help="Deploy existing ZIP file to AWS Lambda")
    parser.add_argument("--src-deploy", action="store_true",
                       help="Source-only build and deploy to AWS Lambda (faster for code changes)")
    parser.add_argument("--full-deploy", action="store_true", 
                       help="Full build and deploy to AWS Lambda")
    
    # Lambda operations
    parser.add_argument("--invoke", action="store_true",
                       help="Invoke Lambda function for testing")
    parser.add_argument("--payload", type=str,
                       help="Custom JSON payload for Lambda invocation")
    parser.add_argument("--info", action="store_true",
                       help="Show current Lambda function information")
    
    # Utility options
    parser.add_argument("--test-aws", action="store_true",
                       help="Test AWS credentials and access")
    
    return parser.parse_args()

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

def validate_data_files():
    """Validate that required data files exist."""
    print("🔍 Validating data files...")
    data_dir = Path("src/data")
    
    if not data_dir.exists():
        print(f"❌ Data directory not found: {data_dir}")
        sys.exit(1)
    
    # Check for CSV files in data directory
    csv_files = list(data_dir.glob("*.csv"))
    if not csv_files:
        print(f"⚠️  No CSV files found in {data_dir}")
        print("   The static file upload feature will not work without data files")
    else:
        print(f"✅ Found {len(csv_files)} CSV file(s) in data directory:")
        for csv_file in csv_files:
            print(f"   - {csv_file.name}")

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
        
        # Validate that data directory was copied
        data_dest = dest_dir / "data"
        if data_dest.exists():
            csv_files = list(data_dest.glob("*.csv"))
            print(f"✅ Data directory copied with {len(csv_files)} CSV file(s)")
        else:
            print("⚠️  Data directory not found in copied source")
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

def find_deployment_zip():
    """Find the most recent deployment ZIP file."""
    zip_files = list(Path(".").glob("research-data-aggregation*.zip"))
    if not zip_files:
        return None
    
    # Return the most recently created ZIP file as a Path object
    return max(zip_files, key=lambda p: p.stat().st_mtime)

def get_build_mode(args):
    """Determine build mode from arguments or user input."""
    if args.full or args.full_deploy:
        return "full"
    elif args.src:
        return "src"
    elif args.src_deploy:
        return "src+deploy"
    elif args.deploy:
        return "full"  # Default to full build for deploy
    else:
        return get_build_mode_interactive()

def get_build_mode_interactive():
    """Get build mode from user input (original function)."""
    # Interactive mode - show enhanced menu
    print("\n🚀 Research Data Aggregation Build & Deploy Tool")
    print("=" * 60)
    print("Choose your action:")
    print("1. 🔄 Full build (clean + dependencies + source + ZIP)")
    print("2. ⚡ Source-only build (update source code + ZIP)")
    print("3. ⚡ Source build + Deploy (fast for code changes)")
    print("4. 📦 Full build + Deploy to AWS Lambda")
    print("5. 🚀 Deploy existing ZIP to AWS Lambda")
    print("6. ▶️  Run Lambda function (invoke remotely)")
    print("7. 📊 Show current Lambda function info")
    print("8. 🔧 Test AWS credentials")
    print("9. ❌ Exit")
    
    try:
        choice = input("\nEnter your choice (1-9): ").strip()
        
        if choice == "1":
            return "full"
        elif choice == "2":
            return "src"
        elif choice == "3":
            return "src+deploy"
        elif choice == "4":
            return "build+deploy"
        elif choice == "5":
            return "deploy-only"
        elif choice == "6":
            return "invoke"
        elif choice == "7":
            return "info"
        elif choice == "8":
            return "test"
        elif choice == "9":
            print("👋 Goodbye!")
            sys.exit(0)
        else:
            print("❌ Invalid choice. Please enter 1-9.")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n🛑 Build cancelled by user.")
        sys.exit(0)

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

def perform_build(build_mode):
    """Perform the build process."""
    print(f"🚀 Starting {'full' if build_mode == 'full' else 'source-only'} build process")
    print("=" * 60)
    
    # Always validate data files before building
    validate_data_files()
    
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
            return False
        
        print("✅ Dependencies found, updating source code only...")
        copy_source_code()
        copy_mapping_files()
    
    # Always create ZIP file
    zip_file = create_deployment_zip()
    
    # Show build summary - ensure zip_file is a Path object
    zip_file_path = Path(zip_file) if isinstance(zip_file, str) else zip_file
    file_size = zip_file_path.stat().st_size / (1024 * 1024)  # MB
    print("=" * 60)
    print("🎉 Build completed successfully!")
    print(f"📦 Deployment package: {zip_file_path}")
    print(f"📏 Package size: {file_size:.1f} MB")
    
    if build_mode == "src":
        print("⚡ Source-only build completed in seconds!")
    
    return True

def deploy_to_aws():
    """Deploy the built package to AWS Lambda."""
    try:
        import aws_utils
        
        # Find the deployment ZIP
        zip_file = find_deployment_zip()
        if not zip_file:
            print("❌ No deployment ZIP found. Please build first.")
            return False
        
        # Ensure zip_file is a Path object
        zip_file_path = Path(zip_file) if isinstance(zip_file, str) else zip_file
        print(f"🚀 Deploying {zip_file_path.name} to AWS Lambda...")
        success = aws_utils.deploy_lambda(zip_file_path)
        
        if success:
            print("✅ Deployment successful!")
            show_deployment_summary()
        else:
            print("❌ Deployment failed!")
        
        return success
        
    except ImportError:
        print("❌ AWS utilities not available. Please ensure aws_utils.py is present.")
        return False

def deploy_existing_zip():
    """Deploy an existing ZIP file to AWS Lambda."""
    zip_path = Path("research-data-aggregation.zip")
    
    if not zip_path.exists():
        print(f"❌ ZIP file not found: {zip_path}")
        print("🔧 Run a build first to create the ZIP file")
        return False
    
    print(f"📦 Found existing package: {zip_path}")
    print(f"🚀 Deploying {zip_path} to AWS Lambda...")
    
    if AWSManager is None:
        print("❌ AWS utilities not available. Please ensure aws_utils.py is present.")
        return False
    
    aws_manager = AWSManager()
    
    # Try to ensure AWS access
    if not aws_manager.ensure_aws_access():
        print("\n💡 Alternative options if Lambda permissions are limited:")
        print("1. Contact your AWS administrator to review IAM policies")
        print("2. Use AWS CLI directly: aws lambda update-function-code \\")
        print(f"   --function-name {LAMBDA_FUNCTION_NAME} \\")
        print(f"   --zip-file fileb://research-data-aggregation.zip")
        print("3. Use the AWS Console to upload the ZIP file manually")
        return False
    
    # Deploy the ZIP file
    success = aws_manager.deploy_lambda_function(zip_path)
    if success:
        print("\n🎉 Deployment completed successfully!")
        print(f"📍 Function: {LAMBDA_FUNCTION_ARN}")
        
        # Try to show function info (may fail with limited permissions)
        print("\n📊 Function Status:")
        info = aws_manager.get_lambda_info()
        if info:
            print(f"   📦 Code Size: {info['code_size']:,} bytes")
            print(f"   ⏱️  Last Modified: {info['last_modified']}")
            print(f"   🔧 Runtime: {info['runtime']}")
            print(f"   💾 Memory: {info['memory_size']} MB")
            print(f"   ⏳ Timeout: {info['timeout']} seconds")
        else:
            print("   ℹ️  Function info unavailable (limited permissions)")
    else:
        print("\n💡 If deployment failed due to permissions:")
        print("1. Verify your IAM user has 'lambda:UpdateFunctionCode' permission")
        print("2. Check if MFA conditions in your policies are correct")
        print("3. Ensure no explicit DENY statements block Lambda access")
        print("4. Try deploying via AWS CLI or Console as an alternative")
    
    return success

def invoke_lambda():
    """Invoke the Lambda function remotely."""
    print("🚀 Invoking Lambda function remotely...")
    
    if AWSManager is None:
        print("❌ AWS utilities not available. Please ensure aws_utils.py is present.")
        return False
    
    aws_manager = AWSManager()
    
    # Try to ensure AWS access
    if not aws_manager.ensure_aws_access():
        print("\n💡 Alternative options if Lambda permissions are limited:")
        print("1. Use AWS CLI directly: aws lambda invoke \\")
        print(f"   --function-name {LAMBDA_FUNCTION_NAME} \\")
        print("   --payload '{}' response.json")
        print("2. Use the AWS Console to test the function manually")
        return False
    
    # Invoke the function
    success, result = aws_manager.invoke_lambda_function()
    
    if success:
        print("\n🎉 Lambda function executed successfully!")
        print(f"📊 Execution Details:")
        print(f"   ⏱️  Duration: {result.get('execution_duration', 'Unknown')}")
        print(f"   📤 Status Code: {result.get('status_code', 'Unknown')}")
        
        # Show payload if available
        if result.get('payload'):
            print(f"   📋 Response:")
            payload = result['payload']
            if isinstance(payload, dict):
                for key, value in payload.items():
                    print(f"      {key}: {value}")
            else:
                print(f"      {payload}")
        
        # Show logs if available (usually base64 encoded)
        if result.get('log_result'):
            print(f"   📝 Logs available (base64 encoded)")
            
    else:
        print(f"\n❌ Lambda function execution failed!")
        if result.get('error'):
            print(f"   Error: {result['error']}")
        
        print("\n💡 If invocation failed due to permissions:")
        print("1. Verify your IAM user has 'lambda:InvokeFunction' permission")
        print("2. Try invoking via AWS CLI or Console as an alternative")
    
    return success

def show_lambda_info():
    """Show current Lambda function information."""
    print("📊 Getting Lambda function information...")
    
    if AWSManager is None:
        print("❌ AWS utilities not available. Please ensure aws_utils.py is present.")
        return False
    
    aws_manager = AWSManager()
    
    # Try to ensure AWS access
    if not aws_manager.ensure_aws_access():
        print("\n💡 Alternative options if Lambda permissions are limited:")
        print("1. Use AWS CLI: aws lambda get-function \\")
        print(f"   --function-name {LAMBDA_FUNCTION_NAME}")
        print("2. Check the AWS Console Lambda section")
        return False
    
    # Get function info
    info = aws_manager.get_lambda_info()
    
    if info:
        print("\n📋 Lambda Function Information:")
        print("="*50)
        print(f"🏷️  Function Name: {info['function_name']}")
        print(f"📝 Description: {info['description']}")
        print(f"🔧 Runtime: {info['runtime']}")
        print(f"💾 Memory Size: {info['memory_size']} MB")
        print(f"⏳ Timeout: {info['timeout']} seconds")
        print(f"📦 Code Size: {info['code_size']:,} bytes")
        print(f"🔖 Version: {info['version']}")
        print(f"⏱️  Last Modified: {info['last_modified']}")
        print(f"🔐 Code SHA256: {info['code_sha256'][:16]}...")
        print(f"📊 State: {info['state']}")
        
        if info['environment']:
            print(f"🌍 Environment Variables:")
            for key, value in info['environment'].items():
                # Don't show sensitive values
                if any(secret in key.upper() for secret in ['SECRET', 'KEY', 'PASSWORD', 'TOKEN']):
                    print(f"      {key}: ***")
                else:
                    print(f"      {key}: {value}")
        else:
            print(f"🌍 Environment Variables: None")
            
        print("="*50)
        print(f"📍 Function ARN: {LAMBDA_FUNCTION_ARN}")
        return True
    else:
        print("\n❌ Could not retrieve function information")
        print("💡 This might be due to insufficient permissions (lambda:GetFunction)")
        print("   The function may still exist and be deployable/invokable.")
        return False

def test_aws_credentials():
    """Test AWS credentials and permissions."""
    print("🔧 Testing AWS credentials and permissions...")
    
    if AWSManager is None:
        print("❌ AWS utilities not available. Please ensure aws_utils.py is present.")
        return False
    
    aws_manager = AWSManager()
    
    # Test basic AWS access
    if aws_manager.check_aws_credentials():
        print("✅ Basic AWS credentials are working")
        
        # Test Lambda-specific access
        lambda_works = aws_manager.test_lambda_access()
        
        if lambda_works:
            print("✅ Lambda permissions are working")
            print(f"📍 Function: {LAMBDA_FUNCTION_ARN}")
            print("\n🎉 All systems ready for deployment!")
            return True
        else:
            print("\n📋 Summary:")
            print("✅ AWS credentials: Working")
            print("❌ Lambda permissions: Issues detected")
            print("\n🔧 What you can try:")
            print("1. Review IAM policies for Lambda permissions")
            print("2. Check MFA conditions in policies")
            print("3. Contact AWS administrator for policy review")
            print("4. Use AWS CLI/Console as alternative methods")
            return False
    else:
        print("❌ Basic AWS credentials are not working")
        print("🔧 Check your AWS CLI configuration:")
        print("   aws configure list")
        print("   aws sts get-caller-identity")
        return False

def show_deployment_summary():
    """Show post-deployment summary."""
    print("\n" + "=" * 60)
    print("🎉 DEPLOYMENT SUCCESSFUL!")
    print("=" * 60)
    print("✅ research-data-aggregation function updated")
    print("✅ Environment variables preserved")
    print("✅ Function configuration maintained")
    
    print(f"\n🔧 Function Details:")
    print(f"   Name: research-data-aggregation")
    print(f"   Region: us-west-2") 
    print(f"   Runtime: Python 3.13")
    print(f"   Memory: 1024 MB")
    print(f"   Timeout: 15 minutes")
    
    print(f"\n📋 Next Steps:")
    print(f"   1. Test with: python build.py --invoke")
    print(f"   2. Monitor CloudWatch logs")
    print(f"   3. Verify S3 output generation")

def main():
    """Enhanced main build and deployment process."""
    args = parse_arguments()
    
    # Handle direct action arguments
    if args.info:
        show_lambda_info()
        return
        
    if args.test_aws:
        test_aws_credentials()
        return
        
    if args.invoke:
        invoke_lambda()
        return
        
    if args.deploy_only:
        deploy_existing_zip()
        return
    
    # Determine build mode
    if any([args.full, args.src, args.deploy, args.src_deploy, args.full_deploy]):
        build_mode = get_build_mode(args)
    else:
        # Interactive mode
        build_mode = get_build_mode_interactive()
    
    # Handle interactive choices that aren't build modes
    if build_mode == "deploy-only":
        deploy_existing_zip()
        return
    elif build_mode == "invoke":
        invoke_lambda()
        return
    elif build_mode == "info":
        show_lambda_info()
        return
    elif build_mode == "test":
        test_aws_credentials()
        return
    elif build_mode == "src+deploy":
        # Source build then deploy (fast for code changes)
        print("🔄 Step 1: Source-only build...")
        if perform_build("src"):
            print("🔄 Step 2: Deploying to AWS...")
            deploy_existing_zip()
        return
    elif build_mode == "build+deploy":
        # First build, then deploy
        print("🔄 Step 1: Full build...")
        if perform_build("full"):
            print("🔄 Step 2: Deploying to AWS...")
            deploy_existing_zip()
        return
    
    # Perform build process for regular build modes
    if build_mode in ["full", "src"]:
        build_success = perform_build(build_mode)
        if not build_success:
            sys.exit(1)
    elif build_mode == "deploy":
        # Build then deploy (from command line args)
        build_success = perform_build("full")
        if build_success:
            deploy_to_aws()
        else:
            print("❌ Build failed, skipping deployment")
            sys.exit(1)
    else:
        print(f"❌ Unknown build mode: {build_mode}")
        sys.exit(1)
    
    # Show completion message
    if build_mode in ["full", "src"]:
        print("\nNext steps:")
        print("1. Deploy: python build.py --deploy-only")
        print("2. Test: python build.py --invoke")
        print("3. Info: python build.py --info")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Build cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1) 