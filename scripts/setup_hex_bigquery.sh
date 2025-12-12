#!/bin/bash
# ============================================================================
# Setup BigQuery Service Account for Hex
# ============================================================================
# This script creates a dedicated service account for Hex with read-only
# access to BigQuery data.
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - Appropriate IAM permissions to create service accounts
#
# Usage:
#   ./scripts/setup_hex_bigquery.sh
# ============================================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SERVICE_ACCOUNT_NAME="hex-bigquery"
SERVICE_ACCOUNT_DISPLAY_NAME="Hex BigQuery Service Account"
KEY_OUTPUT_FILE="config/hex-bigquery-key.json"

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  Hex BigQuery Service Account Setup${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Get current project
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
if [ -z "$PROJECT_ID" ]; then
    echo -e "${RED}❌ Error: No GCP project set. Run: gcloud config set project YOUR_PROJECT_ID${NC}"
    exit 1
fi

echo -e "\n${YELLOW}📋 Configuration:${NC}"
echo "   Project ID: $PROJECT_ID"
echo "   Service Account: $SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
echo "   Output Key File: $KEY_OUTPUT_FILE"

# Confirm
read -p $'\n'"Continue with setup? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Setup cancelled."
    exit 0
fi

# Create service account
echo -e "\n${YELLOW}1. Creating service account...${NC}"
if gcloud iam service-accounts describe "${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" &>/dev/null; then
    echo -e "   ${GREEN}✓ Service account already exists${NC}"
else
    gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
        --description="Service account for Hex BigQuery read access" \
        --display-name="$SERVICE_ACCOUNT_DISPLAY_NAME"
    echo -e "   ${GREEN}✓ Service account created${NC}"
fi

# Grant BigQuery Data Viewer role
echo -e "\n${YELLOW}2. Granting BigQuery Data Viewer role...${NC}"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataViewer" \
    --quiet
echo -e "   ${GREEN}✓ Data Viewer role granted${NC}"

# Grant BigQuery Job User role
echo -e "\n${YELLOW}3. Granting BigQuery Job User role...${NC}"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser" \
    --quiet
echo -e "   ${GREEN}✓ Job User role granted${NC}"

# Generate key file
echo -e "\n${YELLOW}4. Generating service account key...${NC}"
if [ -f "$KEY_OUTPUT_FILE" ]; then
    echo -e "   ${YELLOW}⚠ Key file already exists. Creating new key...${NC}"
fi

gcloud iam service-accounts keys create "$KEY_OUTPUT_FILE" \
    --iam-account="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
echo -e "   ${GREEN}✓ Key saved to: $KEY_OUTPUT_FILE${NC}"

# Add to .gitignore if not already there
if ! grep -q "hex-bigquery-key.json" .gitignore 2>/dev/null; then
    echo -e "\n${YELLOW}5. Adding key to .gitignore...${NC}"
    echo "config/hex-bigquery-key.json" >> .gitignore
    echo -e "   ${GREEN}✓ Added to .gitignore${NC}"
fi

# Summary
echo -e "\n${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  ✅ Setup Complete!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "\n${YELLOW}Next Steps:${NC}"
echo "   1. Go to Hex → Settings → Data connections → Add connection"
echo "   2. Select 'BigQuery'"
echo "   3. Upload the key file: $KEY_OUTPUT_FILE"
echo "   4. Set Project ID: $PROJECT_ID"
echo "   5. Set Default Dataset: dental_leads (optional)"
echo ""
echo -e "${RED}⚠ SECURITY: Never commit the key file to version control!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"


