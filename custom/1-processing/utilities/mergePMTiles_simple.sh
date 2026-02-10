#!/bin/bash
#
# Simple PMTiles merge script for GRID3 tiles
# Merges all individual layer PMTiles from a directory into a single archive
#

# Configuration
INPUT_DIR="/mnt/d/mheaton/grid3_tiles/data/3-pmtiles"
OUTPUT_FILE="${INPUT_DIR}/grid3.pmtiles"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=================================="
echo "GRID3 PMTiles Merge"
echo "=================================="
echo ""
echo "Input directory: ${INPUT_DIR}"
echo "Output file: ${OUTPUT_FILE}"
echo ""

# Check if input directory exists
if [ ! -d "${INPUT_DIR}" ]; then
    echo -e "${RED}Error: Input directory does not exist: ${INPUT_DIR}${NC}"
    exit 1
fi

# Count PMTiles files
PMTILES_COUNT=$(find "${INPUT_DIR}" -maxdepth 1 -name "*.pmtiles" -type f | wc -l)
echo "Found ${PMTILES_COUNT} PMTiles files to merge"
echo ""

if [ ${PMTILES_COUNT} -eq 0 ]; then
    echo -e "${YELLOW}Warning: No PMTiles files found in directory${NC}"
    exit 1
fi

# List files that will be merged (excluding the output file)
echo "Files to merge:"
find "${INPUT_DIR}" -maxdepth 1 -name "*.pmtiles" -type f ! -name "grid3.pmtiles" -exec basename {} \; | sort
echo ""

# Run the Python merge script with analysis
# Pass through any command-line arguments (like --overzoom)
python3 "${SCRIPT_DIR}/mergePMTiles.py" \
    -i "${INPUT_DIR}" \
    -o "${OUTPUT_FILE}" \
    -n "GRID3 Merged Tiles" \
    -A "©2026 GRID3" \
    -N "GRID3 DRC" \
    --force \
    -v \
    "$@"

# Check exit status
if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓ Merge completed successfully!${NC}"
    echo ""
    
    # Display output file info
    if [ -f "${OUTPUT_FILE}" ]; then
        FILE_SIZE=$(du -h "${OUTPUT_FILE}" | cut -f1)
        echo "Output: ${OUTPUT_FILE}"
        echo "Size: ${FILE_SIZE}"
        
        # Show metadata if pmtiles CLI is available
        if command -v pmtiles &> /dev/null; then
            echo ""
            echo "Metadata:"
            pmtiles show "${OUTPUT_FILE}"
        fi
    fi
else
    echo ""
    echo -e "${RED}✗ Merge failed${NC}"
    exit 1
fi
