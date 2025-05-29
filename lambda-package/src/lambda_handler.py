"""AWS Lambda handler for the research data aggregation service."""

import asyncio
import json
import logging
import os
import sys
from typing import Dict, Any

from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.metrics import MetricUnit

from .orchestrator import ResearchDataOrchestrator
from .config import config

# Initialize AWS Powertools
logger = Logger()
tracer = Tracer()
metrics = Metrics(namespace="ResearchDataAggregation")

# Add a module-level log to see if this module is being loaded
sys.stderr.write("DEBUG: lambda_handler.py module is being imported\n")
sys.stderr.flush()


@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_REST)
@tracer.capture_lambda_handler
@metrics.log_metrics
def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    AWS Lambda handler for processing Google Sheets and generating CSV.
    
    Args:
        event: Lambda event data
        context: Lambda context object
        
    Returns:
        Response dictionary with processing results
    """
    sys.stderr.write("DEBUG: lambda_handler function called\n")
    sys.stderr.flush()
    print("LAMBDA HANDLER CALLED - lambda_handler function started")
    logger.info("LAMBDA HANDLER CALLED - Research data aggregation Lambda started", extra={"event": event})
    
    try:
        # Set up Google credentials for Workload Identity Federation
        sys.stderr.write("DEBUG: About to setup Google credentials\n")
        sys.stderr.flush()
        logger.info("Setting up Google credentials for Workload Identity Federation")
        try:
            sys.stderr.write("DEBUG: Calling config.setup_google_credentials()\n")
            sys.stderr.flush()
            config.setup_google_credentials()
            sys.stderr.write("DEBUG: config.setup_google_credentials() completed\n")
            sys.stderr.flush()
            logger.info("Google credentials setup completed")
        except Exception as cred_error:
            sys.stderr.write(f"DEBUG: Error in credential setup: {cred_error}\n")
            sys.stderr.flush()
            logger.error(f"Error setting up Google credentials: {cred_error}")
            raise
        
        # Set up logging level from environment
        log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
        logging.getLogger().setLevel(getattr(logging, log_level))
        
        # Add custom metrics
        metrics.add_metric(name="LambdaInvocations", unit=MetricUnit.Count, value=1)
        
        # Run the async orchestrator
        result = asyncio.run(run_orchestrator())
        
        # Add metrics based on results
        if result.get("success"):
            metrics.add_metric(name="SuccessfulRuns", unit=MetricUnit.Count, value=1)
            metrics.add_metric(
                name="FilesProcessed", 
                unit=MetricUnit.Count, 
                value=result.get("files_processed", 0)
            )
            metrics.add_metric(
                name="RecordsGenerated", 
                unit=MetricUnit.Count, 
                value=result.get("records_generated", 0)
            )
            metrics.add_metric(
                name="ProcessingErrors", 
                unit=MetricUnit.Count, 
                value=result.get("errors", 0)
            )
        else:
            metrics.add_metric(name="FailedRuns", unit=MetricUnit.Count, value=1)
        
        # Prepare response
        response = {
            "statusCode": 200 if result.get("success") else 500,
            "body": json.dumps(result, default=str),
            "headers": {
                "Content-Type": "application/json"
            }
        }
        
        logger.info("Lambda execution completed", extra={"result": result})
        return response
        
    except Exception as e:
        logger.exception("Fatal error in Lambda handler")
        metrics.add_metric(name="FatalErrors", unit=MetricUnit.Count, value=1)
        
        error_response = {
            "statusCode": 500,
            "body": json.dumps({
                "success": False,
                "error": f"Fatal error: {str(e)}",
                "files_processed": 0,
                "records_generated": 0,
                "errors": 0,
                "csv_key": None,
                "error_key": None
            }),
            "headers": {
                "Content-Type": "application/json"
            }
        }
        
        return error_response


@tracer.capture_method
async def run_orchestrator() -> Dict[str, Any]:
    """
    Run the orchestrator asynchronously.
    
    Returns:
        Processing results dictionary
    """
    logger.info("Starting orchestrator")
    
    orchestrator = ResearchDataOrchestrator()
    result = await orchestrator.process_all_sheets()
    
    logger.info("Orchestrator completed", extra={"result": result})
    return result


# For local testing
if __name__ == "__main__":
    import sys
    
    # Mock event and context for local testing
    test_event = {}
    
    class MockContext:
        def __init__(self):
            self.function_name = "research-data-aggregation"
            self.function_version = "$LATEST"
            self.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:research-data-aggregation"
            self.memory_limit_in_mb = "1024"
            self.remaining_time_in_millis = lambda: 300000
            self.aws_request_id = "test-request-id"
    
    # Set required environment variables for testing
    os.environ.setdefault('DRIVE_FOLDER_ID', '1VK3kgR-tS-nkTUSwq8_B-8JYl3zFkXFU')
    os.environ.setdefault('TARGET_BUCKET', 'research-aggregation')
    os.environ.setdefault('LOG_LEVEL', 'INFO')
    
    # Run the handler
    result = lambda_handler(test_event, MockContext())
    print(json.dumps(result, indent=2)) 