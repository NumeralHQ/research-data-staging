import sys
import json

def lambda_handler(event, context):
    """Minimal test handler to verify deployment."""
    sys.stderr.write("TEST HANDLER: This is a test handler being executed\n")
    sys.stderr.flush()
    
    print("TEST HANDLER: Print statement from test handler")
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Test handler executed successfully",
            "handler": "test_handler.lambda_handler"
        })
    } 