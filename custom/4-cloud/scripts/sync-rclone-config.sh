#!/bin/bash
# Sync rclone configuration from .env file
# Usage: ./scripts/sync-rclone-config.sh

set -e

# Find the repository root (where .env is located)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"

# Check if .env exists
if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

# Source the .env file and extract R2 variables
export $(grep -v '^#' "$ENV_FILE" | grep 'R2_' | xargs)

# Validate required variables
if [ -z "$R2_ACCESS_KEY_ID" ] || [ -z "$R2_SECRET_ACCESS_KEY" ] || [ -z "$R2_ENDPOINT" ]; then
    echo "Error: Missing required R2 credentials in .env file"
    echo "Required: R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT"
    exit 1
fi

# Create rclone config directory if it doesn't exist
mkdir -p ~/.config/rclone

# Write rclone configuration
cat > ~/.config/rclone/rclone.conf <<EOF
[grid3-tiles-rclone]
type = s3
provider = Cloudflare
env_auth = false
access_key_id = $R2_ACCESS_KEY_ID
secret_access_key = $R2_SECRET_ACCESS_KEY
region = ${R2_REGION:-auto}
endpoint = $R2_ENDPOINT
acl = ${R2_ACL:-private}
EOF

echo "rclone config updated from .env file"
echo "  Remote: grid3-tiles-rclone"
echo "  Bucket: ${R2_BUCKET_NAME:-grid3-tiles}"
echo "  Endpoint: $R2_ENDPOINT"

# Test the connection
echo ""
echo "Testing connection..."
if rclone lsd grid3-tiles-rclone: >/dev/null 2>&1; then
    echo "Connection successful!"
else
    echo "Connection failed - check credentials"
    exit 1
fi
