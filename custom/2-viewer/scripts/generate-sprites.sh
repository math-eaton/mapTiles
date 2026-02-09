#!/bin/bash
# Simple sprite generator using ImageMagick (if available) or creates placeholder metadata

SPRITES_DIR="$(dirname "$0")/../public/sprites"
cd "$SPRITES_DIR" || exit 1

echo "Generating sprite metadata..."

# Create sprite metadata JSON for 1x (larger 64x64 tiles)
cat > sprite.json << 'EOF'
{
  "speckles_green": {
    "width": 64,
    "height": 64,
    "x": 0,
    "y": 0,
    "pixelRatio": 1
  },
  "speckles_blue": {
    "width": 64,
    "height": 64,
    "x": 64,
    "y": 0,
    "pixelRatio": 1
  },
  "speckles_yellow": {
    "width": 64,
    "height": 64,
    "x": 128,
    "y": 0,
    "pixelRatio": 1
  },
    "speckles_grey": {
    "width": 64,
    "height": 64,
    "x": 128,
    "y": 0,
    "pixelRatio": 1
  }
}
EOF

# Create sprite metadata JSON for 2x (larger 128x128 tiles)
cat > sprite@2x.json << 'EOF'
{
  "speckles_green": {
    "width": 64,
    "height": 64,
    "x": 0,
    "y": 0,
    "pixelRatio": 2
  },
  "speckles_blue": {
    "width": 64,
    "height": 64,
    "x": 128,
    "y": 0,
    "pixelRatio": 2
  },
  "speckles_yellow": {
    "width": 64,
    "height": 64,
    "x": 256,
    "y": 0,
    "pixelRatio": 2
  }
}
EOF

echo "✓ Created sprite.json"
echo "✓ Created sprite@2x.json"

# Check if ImageMagick/rsvg-convert is available
if command -v rsvg-convert &> /dev/null && command -v convert &> /dev/null; then
    echo ""
    echo "Generating PNG sprite sheets ..."
    
    # Generate 1x sprites (64x64 for better visibility)
    rsvg-convert -w 64 -h 64 speckles_green.svg -o speckles_green_1x.png
    rsvg-convert -w 64 -h 64 speckles_blue.svg -o speckles_blue_1x.png
    rsvg-convert -w 64 -h 64 speckles_yellow.svg -o speckles_yellow_1x.png
    convert +append speckles_green_1x.png speckles_blue_1x.png speckles_yellow_1x.png sprite.png
    rm speckles_green_1x.png speckles_blue_1x.png speckles_yellow_1x.png
    echo "✓ Created sprite.png (64x64 patterns)"
    
    # Generate 2x sprites (128x128 for retina displays)
    rsvg-convert -w 128 -h 128 speckles_green.svg -o speckles_green_2x.png
    rsvg-convert -w 128 -h 128 speckles_blue.svg -o speckles_blue_2x.png
    rsvg-convert -w 128 -h 128 speckles_yellow.svg -o speckles_yellow_2x.png
    convert +append speckles_green_2x.png speckles_blue_2x.png speckles_yellow_2x.png sprite@2x.png
    rm speckles_green_2x.png speckles_blue_2x.png speckles_yellow_2x.png
    echo "✓ Created sprite@2x.png (128x128 patterns)"
    
    echo ""
    echo "✓ Sprite generation complete!"
else
    echo ""
    echo "Note: ImageMagick or rsvg-convert not found."
    echo "To generate PNG sprites, install with:"
    echo "  brew install librsvg imagemagick  (macOS)"
    echo ""
    echo "For now, creating placeholder PNGs..."
    
    # Create minimal placeholder PNGs with larger dimensions
    convert -size 192x64 xc:transparent sprite.png 2>/dev/null || echo "  (PNG creation skipped - install ImageMagick)"
    convert -size 384x128 xc:transparent sprite@2x.png 2>/dev/null || echo "  (PNG creation skipped - install ImageMagick)"
fi

echo ""
echo "Sprite files ready in: $SPRITES_DIR"
