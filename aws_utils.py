#!/usr/bin/env python3
"""
AWS utilities for Research Data Aggregation deployment.
Handles MFA authentication, credentials, and Lambda operations.
Adapted from existing aws_utils.py pattern.
"""

import os
import boto3
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

from deployment_config import LAMBDA_CONFIG, AWS_CONFIG

# Configuration for research-data-aggregation
LAMBDA_FUNCTION_NAME = "research-data-aggregation"
LAMBDA_FUNCTION_ARN = "arn:aws:lambda:us-west-2:056694064025:function:research-data-aggregation"
DEFAULT_MFA_DEVICE_ARN = "arn:aws:iam::056694064025:mfa/Numeral-Device"
AWS_REGION = "us-west-2"

# Session storage for temporary MFA ARN updates
_session_mfa_arn = None


class AWSManager:
    """Manages AWS credentials and Lambda deployment for research-data-aggregation."""
    
    def __init__(self):
        self.session_mfa_arn = None
        
    def check_aws_credentials(self) -> bool:
        """
        Check if AWS credentials are configured and working.
        Returns True if credentials work, False if MFA re-auth needed.
        """
        try:
            # Try a simple AWS call
            sts_client = boto3.client('sts', region_name=AWS_REGION)
            response = sts_client.get_caller_identity()
            print(f"✅ AWS credentials working for user: {response.get('Arn', 'Unknown')}")
            return True
        except Exception as e:
            error_str = str(e)
            if "InvalidClientTokenId" in error_str or "credentials" in error_str.lower() or "token" in error_str.lower() or "expired" in error_str.lower():
                print("⚠️  AWS credentials expired or not configured")
                return False
            else:
                print(f"❌ AWS error: {error_str}")
                return False

    def get_mfa_devices(self) -> Optional[str]:
        """
        Get MFA devices for the current user.
        Returns the MFA serial number or None if not found.
        """
        try:
            iam_client = boto3.client('iam', region_name=AWS_REGION)
            # Get current user
            user_response = iam_client.get_user()
            username = user_response['User']['UserName']
            
            # Get MFA devices
            mfa_response = iam_client.list_mfa_devices(UserName=username)
            devices = mfa_response.get('MFADevices', [])
            
            if devices:
                return devices[0]['SerialNumber']
            else:
                print("❌ No MFA device found for user")
                return None
        except Exception as e:
            print(f"❌ Error getting MFA devices: {e}")
            return None

    def prompt_mfa_reauth(self) -> bool:
        """
        Prompt user for MFA re-authentication using the proven working approach.
        Sets up temporary credentials properly for Lambda access.
        """
        print("\n" + "="*60)
        print("🔐 MFA RE-AUTHENTICATION REQUIRED")
        print("="*60)
        print("Your AWS session has expired or MFA is required.")
        print("Please follow these steps to re-authenticate:")
        
        print(f"\n📱 MFA Device: {DEFAULT_MFA_DEVICE_ARN}")
        print("\n1. Open your MFA app (Google Authenticator, Authy, etc.)")
        print("2. Get the current 6-digit code")
        print("3. Enter it below")
        
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                print(f"\nEnter MFA code (6 digits) - Attempt {attempt + 1}/{max_attempts}:", end=" ")
                mfa_code = input().strip()
                
                if not re.match(r'^\d{6}$', mfa_code):
                    print("❌ Please enter exactly 6 digits")
                    continue
                
                print("🔄 Getting temporary credentials...")
                
                # Clear any existing session token but keep base credentials
                if 'AWS_SESSION_TOKEN' in os.environ:
                    del os.environ['AWS_SESSION_TOKEN']
                
                # Force fresh boto3 session
                boto3.DEFAULT_SESSION = None
                
                # Get session token using base credentials
                sts_client = boto3.client('sts', region_name=AWS_REGION)
                response = sts_client.get_session_token(
                    SerialNumber=DEFAULT_MFA_DEVICE_ARN,
                    TokenCode=mfa_code,
                    DurationSeconds=3600  # 1 hour
                )
                
                credentials = response['Credentials']
                
                # Set ALL temporary credentials for this session (critical difference!)
                os.environ['AWS_ACCESS_KEY_ID'] = credentials['AccessKeyId']
                os.environ['AWS_SECRET_ACCESS_KEY'] = credentials['SecretAccessKey']
                os.environ['AWS_SESSION_TOKEN'] = credentials['SessionToken']
                
                # Clear boto3 cache to use new credentials
                boto3.DEFAULT_SESSION = None
                
                print("✅ MFA authentication successful!")
                print(f"🕐 Session valid until: {credentials['Expiration']}")
                
                # Verify the new credentials work for Lambda operations specifically
                if self.test_lambda_access():
                    print("✅ Lambda operations verified - MFA authentication complete")
                    return True
                else:
                    print("❌ MFA session credentials don't work for Lambda operations.")
                    # Clear the invalid session credentials
                    for key in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN']:
                        if key in os.environ:
                            del os.environ[key]
                    boto3.DEFAULT_SESSION = None
                    continue
                    
            except Exception as e:
                error_str = str(e)
                if "InvalidClientTokenId" in error_str:
                    print(f"❌ Error during MFA authentication: {error_str}")
                    print("   This may indicate your base AWS credentials are invalid.")
                    continue
                else:
                    print(f"❌ Error during MFA authentication: {error_str}")
                    continue
        
        print("❌ Deployment failed!")
        return False

    def test_lambda_access(self) -> bool:
        """Test if we can access Lambda functions using the proven working approach."""
        try:
            # Force a fresh boto3 session to ensure it uses current credentials
            boto3.DEFAULT_SESSION = None
            lambda_client = boto3.client('lambda', region_name=AWS_REGION)
            
            # Use list_functions instead of get_function - this has different permission requirements
            # and matches the working approach from the example files
            lambda_client.list_functions(MaxItems=1)
            return True
        except Exception as e:
            error_str = str(e)
            if "AccessDeniedException" in error_str:
                if "explicit deny in an identity-based policy" in error_str:
                    print(f"⚠️  Lambda access denied due to IAM policy restrictions.")
                    print(f"   This appears to be a permissions issue, not an MFA issue.")
                    print(f"   Your IAM user may need additional Lambda permissions or")
                    print(f"   the MFA condition in your policy may need adjustment.")
                    return False
                elif "MultiFactorAuthentication" in error_str or "MFA" in error_str:
                    print(f"⚠️  Lambda access requires MFA authentication")
                    return False
                else:
                    print(f"⚠️  Lambda access denied: {error_str}")
                    return False
            else:
                print(f"⚠️  Lambda access test failed: {e}")
                return False

    def ensure_aws_access(self) -> bool:
        """
        Ensure we have working AWS access with MFA if needed.
        Uses the proven working approach with session credential caching.
        Returns True if access is working, False otherwise.
        """
        print("🔄 Checking AWS credentials...")
        
        # First, check if we already have valid temporary credentials (from parent process or cache)
        if ('AWS_ACCESS_KEY_ID' in os.environ and 
            'AWS_SECRET_ACCESS_KEY' in os.environ and 
            'AWS_SESSION_TOKEN' in os.environ):
            
            # Force fresh boto3 session to use current environment credentials
            boto3.DEFAULT_SESSION = None
            
            # Test if existing temporary credentials work for Lambda operations
            if self.test_lambda_access():
                print("✅ AWS access confirmed - Using existing session credentials")
                return True
        
        # Check if current credentials work without MFA first
        if self.check_aws_credentials():
            # Test Lambda-specific access
            if self.test_lambda_access():
                print("✅ AWS access confirmed - Lambda operations available")
                return True
            else:
                # Basic AWS works but Lambda doesn't - likely need MFA
                print("⚠️  Basic AWS access works, but Lambda access requires MFA")
                return self.prompt_mfa_reauth()
        
        # Basic credentials don't work
        print("⚠️  AWS credentials expired or not configured")
        return self.prompt_mfa_reauth()

    def get_lambda_info(self) -> Optional[Dict[str, Any]]:
        """Get current Lambda function information."""
        try:
            lambda_client = boto3.client('lambda', region_name=AWS_REGION)
            response = lambda_client.get_function(FunctionName=LAMBDA_FUNCTION_NAME)
            
            function_config = response['Configuration']
            code_info = response.get('Code', {})
            
            return {
                'function_name': function_config['FunctionName'],
                'runtime': function_config['Runtime'],
                'memory_size': function_config['MemorySize'],
                'timeout': function_config['Timeout'],
                'last_modified': function_config['LastModified'],
                'code_size': function_config['CodeSize'],
                'description': function_config.get('Description', 'No description'),
                'environment': function_config.get('Environment', {}).get('Variables', {}),
                'code_sha256': function_config['CodeSha256'],
                'version': function_config['Version'],
                'state': function_config.get('State', 'Unknown'),
                'repository_type': code_info.get('RepositoryType', 'Unknown')
            }
        except Exception as e:
            error_str = str(e)
            if "AccessDeniedException" in error_str:
                print(f"❌ Cannot get function info: Insufficient permissions")
                print(f"   Error: {error_str}")
                print(f"\n🔧 Required permission: lambda:GetFunction")
                print(f"📍 Function: {LAMBDA_FUNCTION_NAME}")
                print(f"\n💡 Note: You may still be able to deploy and invoke the function")
                print(f"   even without GetFunction permission.")
                return None
            else:
                print(f"❌ Error getting Lambda function info: {e}")
                return None

    def deploy_lambda_function(self, zip_file_path: Path) -> bool:
        """Deploy the research-data-aggregation Lambda function."""
        try:
            lambda_client = boto3.client('lambda', region_name=AWS_REGION)
            
            # Read the ZIP file
            with open(zip_file_path, 'rb') as f:
                zip_content = f.read()
            
            print(f"📦 Uploading {zip_file_path.name} ({len(zip_content) / (1024*1024):.1f} MB)...")
            
            # Try deployment without checking function info first
            # This allows deployment even if GetFunction permission is missing
            response = lambda_client.update_function_code(
                FunctionName=LAMBDA_FUNCTION_NAME,
                ZipFile=zip_content
            )
            
            print("🔄 Waiting for function update to complete...")
            
            # Wait for the update to complete
            waiter = lambda_client.get_waiter('function_updated')
            waiter.wait(
                FunctionName=LAMBDA_FUNCTION_NAME,
                WaiterConfig={'Delay': 2, 'MaxAttempts': 30}
            )
            
            print("✅ Function code updated successfully!")
            print(f"   New CodeSha256: {response['CodeSha256']}")
            print(f"   Last Modified: {response['LastModified']}")
            
            return True
            
        except Exception as e:
            error_str = str(e)
            if "AccessDeniedException" in error_str:
                print(f"❌ Deployment failed: Insufficient permissions")
                print(f"   Error: {error_str}")
                print(f"\n🔧 Required permission: lambda:UpdateFunctionCode")
                print(f"📍 Function: {LAMBDA_FUNCTION_NAME}")
            else:
                print(f"❌ Error deploying Lambda function: {e}")
            return False

    def invoke_lambda_function(self, payload: Optional[Dict[str, Any]] = None, invocation_type: str = 'RequestResponse') -> Tuple[bool, Dict[str, Any]]:
        """
        Invoke the research-data-aggregation Lambda function.
        
        Args:
            payload: Optional payload to send to the function
            invocation_type: 'RequestResponse' (synchronous) or 'Event' (asynchronous)
            
        Returns:
            Tuple of (success: bool, result: dict)
        """
        try:
            # Ensure fresh boto3 session to prevent credential caching issues
            boto3.DEFAULT_SESSION = None
            
            # Create Lambda client with explicit retry configuration to prevent multiple invocations
            from botocore.config import Config
            lambda_config = Config(
                retries={
                    'max_attempts': 1,  # NO RETRIES - single attempt only
                    'mode': 'standard'
                },
                read_timeout=6000,  # 10 minutes - longer than Lambda execution
                connect_timeout=60  # 1 minute connection timeout
            )
            
            lambda_client = boto3.client('lambda', region_name=AWS_REGION, config=lambda_config)
            
            # Prepare payload
            if payload is None:
                payload = {"source": "terminal_invoke", "test": True}
            
            payload_json = json.dumps(payload)
            print(f"🚀 Invoking {LAMBDA_FUNCTION_NAME}...")
            print(f"📝 Payload: {payload_json}")
            
            # Record start time
            start_time = time.time()
            
            # Invoke the function
            response = lambda_client.invoke(
                FunctionName=LAMBDA_FUNCTION_NAME,
                InvocationType=invocation_type,
                Payload=payload_json
            )
            
            # Calculate duration
            duration = time.time() - start_time
            
            # Parse response
            result = {
                'status_code': response['StatusCode'],
                'execution_duration': f"{duration:.2f}s",
                'log_result': response.get('LogResult'),
                'payload': None,
                'error': None
            }
            
            # Read response payload
            if 'Payload' in response:
                payload_data = response['Payload'].read()
                if payload_data:
                    try:
                        result['payload'] = json.loads(payload_data.decode('utf-8'))
                    except json.JSONDecodeError:
                        result['payload'] = payload_data.decode('utf-8')
            
            # Check for function errors
            if 'FunctionError' in response:
                result['error'] = response['FunctionError']
                print(f"❌ Function execution error: {response['FunctionError']}")
                return False, result
            
            if response['StatusCode'] == 200:
                print(f"✅ Function executed successfully!")
                print(f"⏱️  Duration: {duration:.2f}s")
                return True, result
            else:
                print(f"⚠️  Function returned status code: {response['StatusCode']}")
                return False, result
                
        except Exception as e:
            error_str = str(e)
            if "AccessDeniedException" in error_str:
                print(f"❌ Invocation failed: Insufficient permissions")
                print(f"   Error: {error_str}")
                print(f"\n🔧 Required permission: lambda:InvokeFunction")
                print(f"📍 Function: {LAMBDA_FUNCTION_NAME}")
                return False, {'error': 'Insufficient permissions for lambda:InvokeFunction'}
            else:
                print(f"❌ Error invoking Lambda function: {e}")
                return False, {'error': str(e)}

    def show_lambda_info_formatted(self, info: Dict[str, Any]):
        """Display Lambda function information in a formatted way."""
        print("\n" + "="*60)
        print(f"📊 {info['function_name']} - Function Information")
        print("="*60)
        
        print(f"🔧 Configuration:")
        print(f"   Runtime: {info['runtime']}")
        print(f"   Memory: {info['memory_size']} MB")
        print(f"   Timeout: {info['timeout']} seconds")
        print(f"   State: {info['state']}")
        print(f"   Version: {info['version']}")
        
        print(f"\n📦 Code Information:")
        print(f"   Size: {info['code_size'] / (1024*1024):.1f} MB")
        print(f"   SHA256: {info['code_sha256'][:16]}...")
        print(f"   Last Modified: {info['last_modified']}")
        
        if info.get('description'):
            print(f"\n📝 Description:")
            print(f"   {info['description']}")
        
        env_vars = info.get('environment', {})
        if env_vars:
            print(f"\n🌍 Environment Variables ({len(env_vars)}):")
            # Show only non-sensitive environment variables
            safe_vars = ['DRIVE_FOLDER_ID', 'S3_BUCKET', 'MAX_CONCURRENT_REQUESTS', 'RATE_LIMIT_DELAY']
            for var in safe_vars:
                if var in env_vars:
                    print(f"   {var}: {env_vars[var]}")
            if len(env_vars) > len(safe_vars):
                print(f"   ... and {len(env_vars) - len(safe_vars)} more variables")


# Module-level convenience functions
def ensure_aws_access() -> bool:
    """Convenience function for ensuring AWS access."""
    manager = AWSManager()
    return manager.ensure_aws_access()

def deploy_lambda(zip_file_path: Path) -> bool:
    """Convenience function for deploying Lambda."""
    manager = AWSManager()
    if manager.ensure_aws_access():
        return manager.deploy_lambda_function(zip_file_path)
    return False

def invoke_lambda(payload: Optional[Dict[str, Any]] = None) -> Tuple[bool, Dict[str, Any]]:
    """Convenience function for invoking Lambda."""
    manager = AWSManager()
    if manager.ensure_aws_access():
        return manager.invoke_lambda_function(payload)
    return False, {'error': 'AWS access failed'}

def show_lambda_info() -> bool:
    """Convenience function for showing Lambda info."""
    manager = AWSManager()
    if manager.ensure_aws_access():
        info = manager.get_lambda_info()
        if info:
            manager.show_lambda_info_formatted(info)
            return True
    return False 