# Map Sprites

This directory contains sprite sheets for MapLibre GL map styles.

## Wetland Textures

USGS-style marsh textures for land cover visualization:

- **wetland.svg** - Vertical grass-like strokes for general wetlands
- **swamp.svg** - Denser curved vegetation strokes for swamps
- **marsh.svg** - Dense grass with horizontal water indicators for marshes

## Files

- `sprite.png` / `sprite.json` - 1x resolution sprite sheet and metadata
- `sprite@2x.png` / `sprite@2x.json` - 2x resolution sprite sheet and metadata (for retina displays)

## Regenerating Sprites

To regenerate the sprite sheets after modifying SVG files:

```bash
./scripts/generate-sprites.sh
```

**Requirements:**
- `librsvg` (for rsvg-convert)
- `imagemagick` (for magick convert)

Install on macOS:
```bash
brew install librsvg imagemagick
```

## Usage in MapLibre Style

The sprites are referenced in the style JSON:

```json
{
  "sprite": "sprites/sprite",
  ...
}
```

Pattern layers use the sprite names:

```json
{
  "id": "land-cover-marsh-pattern",
  "type": "fill",
  "paint": {
    "fill-pattern": "marsh",
    "fill-opacity": 0.5
  }
}
```

## Customization

To modify patterns:

1. Edit the SVG files in this directory
2. Run `./scripts/generate-sprites.sh` to regenerate PNG sheets
3. Refresh your map viewer to see changes

Pattern dimensions are 32x32 pixels at 1x resolution.
