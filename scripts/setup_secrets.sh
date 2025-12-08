#!/bin/bash

################################################################################
# GCP Secret Manager Setup Script
#
# This script initializes all secrets needed for the Dental Leads ETL pipeline.
# Run this once to set up your production secrets.
#
# Usage:
#   bash setup_secrets.sh
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - Secret Manager API enabled
#   - Proper IAM permissions
################################################################################

set -e  # Exit on error

echo "🔐 Dental Leads ETL - Secret Manager Setup"
echo "==========================================="
echo ""

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "❌ Error: gcloud CLI not found"
    echo "   Install from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Get GCP project ID
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)

if [ -z "$PROJECT_ID" ]; then
    echo "❌ Error: No GCP project set"
    echo "   Run: gcloud config set project YOUR_PROJECT_ID"
    exit 1
fi

echo "✅ GCP Project: $PROJECT_ID"
echo ""

# Enable Secret Manager API
echo "📦 Enabling Secret Manager API..."
gcloud services enable secretmanager.googleapis.com --project="$PROJECT_ID"
echo "✅ Secret Manager API enabled"
echo ""

################################################################################
# Create Secrets
################################################################################

echo "🔑 Creating secrets..."
echo ""

# Function to create or update secret
create_secret() {
    local SECRET_NAME=$1
    local PROMPT=$2
    local IS_FILE=$3
    
    echo "---"
    echo "Secret: $SECRET_NAME"
    echo "$PROMPT"
    
    # Check if secret already exists
    if gcloud secrets describe "$SECRET_NAME" --project="$PROJECT_ID" &>/dev/null; then
        read -p "Secret already exists. Update it? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "⏭️  Skipped"
            return
        fi
    fi
    
    if [ "$IS_FILE" = "true" ]; then
        read -p "Enter file path: " FILE_PATH
        
        if [ ! -f "$FILE_PATH" ]; then
            echo "❌ File not found: $FILE_PATH"
            return
        fi
        
        # Create or update secret from file
        if gcloud secrets describe "$SECRET_NAME" --project="$PROJECT_ID" &>/dev/null; then
            gcloud secrets versions add "$SECRET_NAME" \
                --data-file="$FILE_PATH" \
                --project="$PROJECT_ID"
        else
            gcloud secrets create "$SECRET_NAME" \
                --data-file="$FILE_PATH" \
                --replication-policy="automatic" \
                --project="$PROJECT_ID"
        fi
    else
        read -sp "Enter value: " SECRET_VALUE
        echo
        
        if [ -z "$SECRET_VALUE" ]; then
            echo "⚠️  Empty value, skipping"
            return
        fi
        
        # Create or update secret from value
        if gcloud secrets describe "$SECRET_NAME" --project="$PROJECT_ID" &>/dev/null; then
            echo -n "$SECRET_VALUE" | gcloud secrets versions add "$SECRET_NAME" \
                --data-file=- \
                --project="$PROJECT_ID"
        else
            echo -n "$SECRET_VALUE" | gcloud secrets create "$SECRET_NAME" \
                --data-file=- \
                --replication-policy="automatic" \
                --project="$PROJECT_ID"
        fi
    fi
    
    echo "✅ Secret '$SECRET_NAME' created/updated"
    echo ""
}

################################################################################
# Snowflake Secrets
################################################################################

echo "=== Snowflake Credentials ==="
echo ""

create_secret "snowflake-account" \
    "Snowflake account identifier (e.g., xy12345.us-east-1)" \
    false

create_secret "snowflake-user" \
    "Snowflake username" \
    false

create_secret "snowflake-private-key" \
    "Snowflake RSA private key (path to .p8 file)" \
    true

create_secret "snowflake-warehouse" \
    "Snowflake warehouse name (default: DL_WH)" \
    false

create_secret "snowflake-database" \
    "Snowflake database name (default: DENTAL_LEADS)" \
    false

create_secret "snowflake-schema" \
    "Snowflake schema name (default: RAW)" \
    false

################################################################################
# GCP Secrets
################################################################################

echo "=== GCP Configuration ==="
echo ""

create_secret "gcp-project-id" \
    "GCP project ID (default: $PROJECT_ID)" \
    false

create_secret "gcs-bucket-name" \
    "GCS bucket name (default: dl-ingestion-lake)" \
    false

################################################################################
# Validation API Secrets
################################################################################

echo "=== Validation APIs ==="
echo ""

create_secret "addy-api-key" \
    "Addy API key for address validation" \
    false

create_secret "twilio-account-sid" \
    "Twilio Account SID for phone validation" \
    false

create_secret "twilio-auth-token" \
    "Twilio Auth Token" \
    false

################################################################################
# Enrichment API Secrets (Optional)
################################################################################

echo "=== Enrichment APIs (Optional) ==="
echo ""

read -p "Set up enrichment API keys now? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    create_secret "wiza-api-key" \
        "Wiza API key for email enrichment" \
        false
    
    create_secret "apollo-api-key" \
        "Apollo API key for contact enrichment" \
        false
    
    create_secret "clay-api-key" \
        "Clay API key for waterfall enrichment" \
        false
fi

################################################################################
# Set IAM Permissions
################################################################################

echo ""
echo "🔒 Setting IAM permissions..."
echo ""

# Get default compute service account
COMPUTE_SA=$(gcloud iam service-accounts list \
    --filter="email:*-compute@developer.gserviceaccount.com" \
    --format="value(email)" \
    --project="$PROJECT_ID" \
    | head -n 1)

if [ -n "$COMPUTE_SA" ]; then
    echo "Service account: $COMPUTE_SA"
    
    # Grant secretAccessor role to all secrets
    for SECRET in snowflake-account snowflake-user snowflake-private-key \
                  snowflake-warehouse snowflake-database snowflake-schema \
                  gcp-project-id gcs-bucket-name \
                  addy-api-key twilio-account-sid twilio-auth-token; do
        
        if gcloud secrets describe "$SECRET" --project="$PROJECT_ID" &>/dev/null; then
            gcloud secrets add-iam-policy-binding "$SECRET" \
                --member="serviceAccount:$COMPUTE_SA" \
                --role="roles/secretmanager.secretAccessor" \
                --project="$PROJECT_ID" \
                &>/dev/null || true
        fi
    done
    
    echo "✅ IAM permissions set"
else
    echo "⚠️  Could not find compute service account"
    echo "   You may need to set permissions manually"
fi

################################################################################
# Summary
################################################################################

echo ""
echo "==========================================="
echo "✅ Secret Manager Setup Complete!"
echo "==========================================="
echo ""
echo "📋 Next steps:"
echo ""
echo "1. Set GCP_PROJECT_ID environment variable:"
echo "   export GCP_PROJECT_ID='$PROJECT_ID'"
echo ""
echo "2. Test secrets access:"
echo "   gcloud secrets versions access latest --secret='snowflake-account'"
echo ""
echo "3. Run your ETL pipeline:"
echo "   cd ~/dev/dental-leads-etl"
echo "   python etl/npi_ingestion.py"
echo ""
echo "🔐 All secrets are stored in GCP Secret Manager"
echo "   View in console: https://console.cloud.google.com/security/secret-manager?project=$PROJECT_ID"
echo ""
