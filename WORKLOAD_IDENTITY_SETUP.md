# Workload Identity Federation Setup Guide

This guide walks you through setting up Workload Identity Federation to allow your AWS Lambda function to authenticate with Google APIs without storing service account keys.

## Overview

Workload Identity Federation allows AWS Lambda to:
- Use its built-in AWS credentials to authenticate with Google APIs
- Eliminate the need for service account keys
- Provide more secure, automatic credential rotation
- Simplify credential management

## Prerequisites

- AWS CLI configured with appropriate permissions
- Google Cloud CLI (`gcloud`) installed and configured
- A Google Cloud project (minimal setup required)

## Step 1: Minimal Google Cloud Setup

### 1.1 Create or Select a Google Cloud Project

```bash
# List existing projects
gcloud projects list

# Or create a new one (optional)
gcloud projects create research-data-project-123
gcloud config set project research-data-project-123
```

### 1.2 Enable Required APIs

```bash
# Enable the APIs we need
gcloud services enable iamcredentials.googleapis.com
gcloud services enable drive.googleapis.com
gcloud services enable sheets.googleapis.com
```

### 1.3 Get Project Information

```bash
# Get project number (needed for audience)
export PROJECT_NUMBER=$(gcloud projects describe $(gcloud config get-value project) --format="value(projectNumber)")
export PROJECT_ID=$(gcloud config get-value project)

echo "Project ID: $PROJECT_ID"
echo "Project Number: $PROJECT_NUMBER"
```

## Step 2: Create Workload Identity Pool and Provider

### 2.1 Create Workload Identity Pool

```bash
export POOL_ID="aws-lambda-pool"

gcloud iam workload-identity-pools create $POOL_ID \
    --location="global" \
    --description="Pool for AWS Lambda access to Google APIs" \
    --display-name="AWS Lambda Pool"
```

### 2.2 Create AWS Provider

```bash
export PROVIDER_ID="aws-lambda-provider"
export AWS_ACCOUNT_ID="YOUR_AWS_ACCOUNT_ID"  # Replace with your AWS account ID

gcloud iam workload-identity-pools providers create-aws $PROVIDER_ID \
    --location="global" \
    --workload-identity-pool=$POOL_ID \
    --account-id=$AWS_ACCOUNT_ID \
    --attribute-mapping="google.subject=assertion.arn,attribute.aws_role=assertion.arn.contains('assumed-role') ? assertion.arn.extract('{account_arn}assumed-role/') + 'assumed-role/' + assertion.arn.extract('assumed-role/{role_name}/') : assertion.arn"
```

### 2.3 Create Service Account

```bash
export SERVICE_ACCOUNT_NAME="research-data-service"

gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
    --display-name="Research Data Aggregation Service Account" \
    --description="Service account for AWS Lambda to access Google APIs"
```

### 2.4 Grant Permissions to Service Account

```bash
# Grant necessary Google API permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/drive.readonly"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/sheets.readonly"
```

## Step 3: Configure AWS Lambda Role Access

### 3.1 Get Your Lambda Role ARN

When you deploy the AWS infrastructure, note the Lambda execution role ARN. It will look like:
```
arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/research-data-aggregation-LambdaExecutionRole-XXXXX
```

### 3.2 Allow Lambda Role to Impersonate Service Account

```bash
export LAMBDA_ROLE_NAME="research-data-aggregation-LambdaExecutionRole-XXXXX"  # Replace with actual role name

gcloud iam service-accounts add-iam-policy-binding \
    $SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com \
    --role="roles/iam.workloadIdentityUser" \
    --member="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL_ID/attribute.aws_role/arn:aws:sts::$AWS_ACCOUNT_ID:assumed-role/$LAMBDA_ROLE_NAME"
```

## Step 4: Get Configuration Values

### 4.1 Generate Audience String

```bash
export WIF_AUDIENCE="//iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL_ID/providers/$PROVIDER_ID"
echo "WIF_AUDIENCE: $WIF_AUDIENCE"
```

### 4.2 Get Service Account Email

```bash
export WIF_SERVICE_ACCOUNT="$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
echo "WIF_SERVICE_ACCOUNT: $WIF_SERVICE_ACCOUNT"
```

## Step 5: Share Google Drive Folder

### 5.1 Share with Service Account

1. Go to your Google Drive folder: `https://drive.google.com/drive/folders/1VK3kgR-tS-nkTUSwq8_B-8JYl3zFkXFU`
2. Click "Share"
3. Add the service account email: `$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com`
4. Set permission to "Viewer"
5. Click "Send"

## Step 6: Deploy AWS Infrastructure

### 6.1 Deploy with SAM

```bash
cd infrastructure

sam deploy \
    --template-file template.yaml \
    --stack-name research-data-aggregation \
    --parameter-overrides \
        WifAudience="$WIF_AUDIENCE" \
        WifServiceAccount="$WIF_SERVICE_ACCOUNT" \
        DriveFolder="1VK3kgR-tS-nkTUSwq8_B-8JYl3zFkXFU" \
        S3BucketName="research-aggregation-$(date +%s)" \
    --capabilities CAPABILITY_IAM \
    --resolve-s3
```

### 6.2 Update Lambda Role Binding

After deployment, get the actual Lambda role name and update the IAM binding:

```bash
# Get the actual role name from CloudFormation
export ACTUAL_LAMBDA_ROLE=$(aws cloudformation describe-stacks \
    --stack-name research-data-aggregation \
    --query 'Stacks[0].Outputs[?OutputKey==`LambdaExecutionRole`].OutputValue' \
    --output text | cut -d'/' -f2)

# Update the IAM binding with the correct role name
gcloud iam service-accounts add-iam-policy-binding \
    $SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com \
    --role="roles/iam.workloadIdentityUser" \
    --member="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL_ID/attribute.aws_role/arn:aws:sts::$AWS_ACCOUNT_ID:assumed-role/$ACTUAL_LAMBDA_ROLE"
```

## Step 7: Upload Mapping Files

### 7.1 Upload to S3

```bash
# Get the S3 bucket name
export S3_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name research-data-aggregation \
    --query 'Stacks[0].Outputs[?OutputKey==`S3Bucket`].OutputValue' \
    --output text)

# Upload mapping files
aws s3 cp mapping/geo_state.csv s3://$S3_BUCKET/mapping/geo_state.csv
aws s3 cp mapping/tax_cat.csv s3://$S3_BUCKET/mapping/tax_cat.csv
```

## Step 8: Test the Setup

### 8.1 Invoke Lambda Function

```bash
aws lambda invoke \
    --function-name research-data-aggregation \
    --payload '{}' \
    response.json

cat response.json
```

### 8.2 Check CloudWatch Logs

```bash
aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/research-data-aggregation"

# Get recent logs
aws logs filter-log-events \
    --log-group-name "/aws/lambda/research-data-aggregation" \
    --start-time $(date -d '10 minutes ago' +%s)000
```

## Troubleshooting

### Common Issues

1. **Permission Denied**: Ensure the Google Drive folder is shared with the service account
2. **Invalid Audience**: Double-check the WIF_AUDIENCE format
3. **Role Not Found**: Verify the Lambda role name in the IAM binding
4. **API Not Enabled**: Ensure all required Google APIs are enabled

### Verification Commands

```bash
# Verify workload identity pool
gcloud iam workload-identity-pools describe $POOL_ID --location=global

# Verify provider
gcloud iam workload-identity-pools providers describe $PROVIDER_ID \
    --location=global --workload-identity-pool=$POOL_ID

# Verify service account
gcloud iam service-accounts describe $SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com

# Check IAM bindings
gcloud iam service-accounts get-iam-policy $SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com
```

## Security Considerations

1. **Least Privilege**: The service account only has read access to Drive and Sheets
2. **No Keys**: No service account keys are created or stored
3. **Automatic Rotation**: AWS credentials are automatically rotated
4. **Audit Trail**: All access is logged in both AWS CloudTrail and Google Cloud Audit Logs

## Cost Considerations

- **Google Cloud**: Minimal cost for API calls only
- **AWS**: Standard Lambda, S3, and CloudWatch costs
- **No Additional Fees**: Workload Identity Federation is free

## Summary

You now have:
- ✅ Workload Identity Federation configured
- ✅ AWS Lambda with secure Google API access
- ✅ No service account keys to manage
- ✅ Automatic credential rotation
- ✅ Comprehensive monitoring and logging

The Lambda function will now authenticate to Google APIs using its AWS identity through Workload Identity Federation! 