#!/usr/bin/env python3
"""
Generate MapLibre sprite sheets from SVG files
Creates both 1x and 2x resolution sprites with JSON metadata
"""

import json
import os
from pathlib import Path
from PIL import Image
import cairosvg
from io import BytesIO

SPRITES_DIR = Path(__file__).parent.parent / 'public' / 'sprites'
OUTPUT_DIR = SPRITES_DIR

# Sprite configurations
CONFIGS = [
    {'scale': 1, 'suffix': ''},
    {'scale': 2, 'suffix': '@2x'}
]

def svg_to_png(svg_path, width, height, scale):
    """Convert SVG to PNG at specified scale"""
    png_data = cairosvg.svg2png(
        url=str(svg_path),
        output_width=width * scale,
        output_height=height * scale
    )
    return Image.open(BytesIO(png_data))

def generate_sprites():
    """Generate sprite sheets from SVG files"""
    # Find all SVG files
    svg_files = [
        {'name': f.stem, 'path': f}
        for f in SPRITES_DIR.glob('*.svg')
    ]
    
    if not svg_files:
        print('No SVG files found in sprites directory')
        return
    
    print(f"Found {len(svg_files)} SVG files: {', '.join(f['name'] for f in svg_files)}")
    
    for config in CONFIGS:
        scale = config['scale']
        suffix = config['suffix']
        
        # Sprite dimensions
        sprite_width = 64
        sprite_height = 64
        total_width = sprite_width * scale * len(svg_files)
        total_height = sprite_height * scale
        
        # Create canvas
        canvas = Image.new('RGBA', (total_width, total_height), (0, 0, 0, 0))
        metadata = {}
        x_offset = 0
        
        # Process each SVG
        for svg_file in svg_files:
            print(f"Processing {svg_file['name']} at {scale}x...")
            
            # Convert SVG to PNG
            sprite_img = svg_to_png(
                svg_file['path'],
                sprite_width,
                sprite_height,
                scale
            )
            
            # Paste onto canvas
            canvas.paste(sprite_img, (x_offset, 0))
            
            # Add metadata
            metadata[svg_file['name']] = {
                'width': sprite_width,
                'height': sprite_height,
                'x': x_offset // scale,
                'y': 0,
                'pixelRatio': scale
            }
            
            x_offset += sprite_width * scale
        
        # Save PNG
        png_path = OUTPUT_DIR / f'sprite{suffix}.png'
        canvas.save(png_path, 'PNG')
        print(f"✓ Created {png_path}")
        
        # Save JSON metadata
        json_path = OUTPUT_DIR / f'sprite{suffix}.json'
        with open(json_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        print(f"✓ Created {json_path}")
    
    print('\n✓ Sprite generation complete!')

if __name__ == '__main__':
    try:
        generate_sprites()
    except Exception as e:
        print(f'Error generating sprites: {e}')
        import traceback
        traceback.print_exc()
        exit(1)
