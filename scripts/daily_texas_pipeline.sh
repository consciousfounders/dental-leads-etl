#!/bin/bash
# =============================================================================
# Daily Texas License Pipeline
# =============================================================================
# Run daily to:
# 1. Fetch latest Texas license data (creates dated snapshot)
# 2. Generate events for recently licensed professionals
# 3. Trigger actions (GHL, webhook, Slack)
#
# Usage:
#   ./scripts/daily_texas_pipeline.sh                    # Full run
#   ./scripts/daily_texas_pipeline.sh --dry-run          # Test mode
#   ./scripts/daily_texas_pipeline.sh --fetch-only       # Just fetch data
#
# Environment variables (optional):
#   GHL_API_KEY         - GoHighLevel API key
#   GHL_LOCATION_ID     - GoHighLevel location ID
#   TRIGGER_WEBHOOK_URL - Generic webhook URL
#   SLACK_WEBHOOK_URL   - Slack webhook URL
# =============================================================================

set -e

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Parse arguments
DRY_RUN=""
FETCH_ONLY=""
DAYS=7  # Look back 7 days for recent licenses

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        --fetch-only)
            FETCH_ONLY="true"
            shift
            ;;
        --days)
            DAYS="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

cd "$PROJECT_DIR"

echo "============================================================"
echo "Daily Texas License Pipeline"
echo "Date: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"
echo ""

# Step 1: Fetch latest data
echo "[1/3] Fetching Texas license data..."
python3 ingestion/licenses/connectors/fetch_texas.py \
    --output-dir data/licenses/texas

if [[ "$FETCH_ONLY" == "true" ]]; then
    echo ""
    echo "Fetch only mode - stopping here."
    exit 0
fi

# Step 2: Generate events for recent licenses
echo ""
echo "[2/3] Finding recent licenses (last $DAYS days)..."
DATE_STR=$(date '+%Y-%m-%d')
python3 events/find_recent_licenses.py \
    --state TX \
    --days "$DAYS" \
    --types dentist \
    --output "events/tx_events_${DATE_STR}.json"

# Step 3: Run triggers
echo ""
echo "[3/3] Running triggers..."
if [[ -n "$DRY_RUN" ]]; then
    echo "(DRY RUN MODE - no actual API calls)"
fi

python3 triggers/new_licensee_trigger.py \
    --events "events/tx_events_${DATE_STR}.json" \
    $DRY_RUN

echo ""
echo "============================================================"
echo "Pipeline complete!"
echo "============================================================"
