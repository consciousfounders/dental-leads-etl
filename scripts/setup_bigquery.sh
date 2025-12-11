#!/bin/bash
# Setup BigQuery medallion architecture
# Usage: ./scripts/setup_bigquery.sh

set -e

PROJECT_ID="silicon-will-480022-f8"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="$SCRIPT_DIR/../sql/bigquery"

echo "🚀 Setting up BigQuery medallion architecture..."
echo ""

# Step 1: Create schemas (datasets)
echo "📁 Step 1: Creating datasets..."
bq query --use_legacy_sql=false < "$SQL_DIR/01_create_schemas.sql"
echo "✅ Datasets created"
echo ""

# Step 2: Create CLEAN dental_providers table
echo "🧹 Step 2: Creating dental_providers table..."
bq query --use_legacy_sql=false < "$SQL_DIR/02_clean_dental_providers.sql"
echo "✅ dental_providers table created"
echo ""

# Step 3: Create analytics views
echo "📊 Step 3: Creating analytics views..."
bq query --use_legacy_sql=false < "$SQL_DIR/03_clean_views.sql"
echo "✅ Views created"
echo ""

# Verify
echo "📋 Verification:"
echo ""
echo "Tables in dental_leads_clean:"
bq ls "$PROJECT_ID:dental_leads_clean"
echo ""

echo "Views in dental_leads_segmented:"
bq ls "$PROJECT_ID:dental_leads_segmented"
echo ""

# Row counts
echo "📈 Row counts:"
bq query --use_legacy_sql=false --format=pretty "
SELECT 'dental_providers' as table_name, COUNT(*) as rows 
FROM \`$PROJECT_ID.dental_leads_clean.dental_providers\`
"

echo ""
echo "✅ BigQuery setup complete!"
echo ""
echo "📊 Access your data:"
echo "   - Console: https://console.cloud.google.com/bigquery?project=$PROJECT_ID"
echo "   - Looker Studio: Connect to BigQuery dataset 'dental_leads_clean'"

