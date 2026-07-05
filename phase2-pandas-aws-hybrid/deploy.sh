#!/bin/bash

echo "Deploying GX Lambda Container..."

# Get AWS account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="us-west-2"
REPO_NAME="validate-nutri-gx"

echo "Account ID: $ACCOUNT_ID"
echo "Region: $REGION"

# Create ECR repository if it doesn't exist
echo "Creating ECR repository..."
aws ecr create-repository --repository-name $REPO_NAME --region $REGION 2>/dev/null || echo "Repository already exists"

# Login to ECR
echo "Logging into ECR..."
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

# Build container
echo "Building Docker image..."
docker build -t $REPO_NAME .

# Tag for ECR
echo "Tagging image..."
docker tag $REPO_NAME:latest $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME:latest

# Push to ECR
echo "Pushing to ECR..."
docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME:latest

echo "Deployment complete!"
echo "Image pushed: $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME:latest"
