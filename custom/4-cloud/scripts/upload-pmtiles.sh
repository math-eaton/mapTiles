#!/bin/bash

# Upload PMTiles to Cloudflare R2
# Usage: ./upload-pmtiles.sh [bucket-name]

set -e

# Configuration
BUCKET_NAME="${1:-grid3-tiles}"
PMTILES_DIR="../../data/3-pmtiles"

echo "🚀 Uploading PMTiles to R2 bucket: $BUCKET_NAME"
echo "📁 Source directory: $PMTILES_DIR"
echo ""

# Check if wrangler is installed
if ! command -v wrangler &> /dev/null; then
    echo "❌ Error: wrangler is not installed"
    echo "Install it with: npm install -g wrangler"
    exit 1
fi

# Check if PMTiles directory exists
if [ ! -d "$PMTILES_DIR" ]; then
    echo "❌ Error: PMTiles directory not found: $PMTILES_DIR"
    exit 1
fi

# Count files
file_count=$(find "$PMTILES_DIR" -name "*.pmtiles" | wc -l)
if [ "$file_count" -eq 0 ]; then
    echo "❌ Error: No .pmtiles files found in $PMTILES_DIR"
    exit 1
fi

echo "Found $file_count PMTiles files to upload"
echo ""

# Upload each PMTiles file
upload_count=0
for file in "$PMTILES_DIR"/*.pmtiles; do
    if [ -f "$file" ]; then
        filename=$(basename "$file")
        filesize=$(du -h "$file" | cut -f1)
        
        echo "📤 Uploading: $filename ($filesize)"
        
        if wrangler r2 object put "$BUCKET_NAME/$filename" --file "$file"; then
            echo "   ✅ Success"
            ((upload_count++))
        else
            echo "   ❌ Failed"
        fi
        echo ""
    fi
done

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✨ Upload complete: $upload_count/$file_count files uploaded"
echo ""
echo "To verify uploads:"
echo "  wrangler r2 object list $BUCKET_NAME"
echo ""
echo "To test a tile:"
echo "  curl https://your-worker.workers.dev/base.json"
