#!/usr/bin/env python3
"""
tippDecode.py - Decode PMTiles/MBTiles back to GeoJSON using tippecanoe-decode

This module provides utilities to convert vector PMTiles files back to GeoJSON
format using the tippecanoe-decode utility. Supports both full file decoding
and individual tile extraction.

Usage:
    python tippDecode.py --input="tiles/water.pmtiles" --output="decoded/water.geojson"
    python tippDecode.py --input="tiles/" --output="decoded/" --batch
    
    # From another script:
    from tippDecode import decode_pmtiles_to_geojson
    decode_pmtiles_to_geojson("tiles/water.pmtiles", "decoded/water.geojson")
"""

import os
import subprocess
import json
import argparse
import sys
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# Set up project paths - aligned with existing structure
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSING_DIR = PROJECT_ROOT / "processing"
DATA_DIR = PROCESSING_DIR / "data"
TILE_DIR = DATA_DIR / "tiles"
PUBLIC_TILES_DIR = PROJECT_ROOT / "public" / "tiles"
OUTPUT_DIR = DATA_DIR / "decoded"

def check_tippecanoe_decode():
    """Check if tippecanoe-decode is available in the system"""
    try:
        result = subprocess.run(['tippecanoe-decode', '--help'], 
                              capture_output=True, text=True, check=False)
        return result.returncode == 0
    except FileNotFoundError:
        return False

def decode_pmtiles_to_geojson(input_path, output_path=None, **options):
    """Decode a single PMTiles/MBTiles file to GeoJSON
    
    Args:
        input_path (str|Path): Path to input PMTiles/MBTiles file
        output_path (str|Path): Path for output GeoJSON file (optional)
        **options: Additional options for tippecanoe-decode
    
    Returns:
        dict: Result with success status and details
    """
    input_path = Path(input_path)
    
    if not input_path.exists():
        return {"success": False, "message": f"Input file not found: {input_path}"}
    
    if not input_path.suffix.lower() in ['.pmtiles', '.mbtiles']:
        return {"success": False, "message": f"Unsupported file type: {input_path.suffix}"}
    
    # Generate output path if not provided
    if output_path is None:
        output_path = OUTPUT_DIR / f"{input_path.stem}_decoded.geojson"
    else:
        output_path = Path(output_path)
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Build tippecanoe-decode command
    cmd = ['tippecanoe-decode']
    
    # Add options
    if options.get('projection'):
        cmd.extend(['-s', options['projection']])
    
    if options.get('max_zoom') is not None:
        cmd.extend(['-z', str(options['max_zoom'])])
    
    if options.get('min_zoom') is not None:
        cmd.extend(['-Z', str(options['min_zoom'])])
    
    if options.get('layers'):
        if isinstance(options['layers'], list):
            for layer in options['layers']:
                cmd.extend(['-l', layer])
        else:
            cmd.extend(['-l', options['layers']])
    
    if options.get('tag_layer_and_zoom', False):
        cmd.append('-c')
    
    if options.get('stats_only', False):
        cmd.append('-S')
    
    if options.get('force', False):
        cmd.append('-f')
    
    if options.get('integer_coords', False):
        cmd.append('-I')
    
    if options.get('fraction_coords', False):
        cmd.append('-F')
    
    # Add input file
    cmd.append(str(input_path))
    
    try:
        # Execute tippecanoe-decode
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Write output to file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(result.stdout)
        
        return {
            "success": True,
            "message": f"Successfully decoded to {output_path.name}",
            "input_file": input_path,
            "output_file": output_path,
            "command": ' '.join(cmd)
        }
        
    except subprocess.CalledProcessError as e:
        return {
            "success": False,
            "message": f"tippecanoe-decode error: {e.stderr if e.stderr else str(e)}",
            "command": ' '.join(cmd)
        }
    except Exception as e:
        return {"success": False, "message": f"Error: {str(e)}"}

def decode_individual_tile(input_path, zoom, x, y, output_path=None, **options):
    """Decode an individual tile from PMTiles/MBTiles to GeoJSON
    
    Args:
        input_path (str|Path): Path to input PMTiles/MBTiles file
        zoom (int): Zoom level
        x (int): Tile X coordinate
        y (int): Tile Y coordinate
        output_path (str|Path): Path for output GeoJSON file (optional)
        **options: Additional options for tippecanoe-decode
    
    Returns:
        dict: Result with success status and details
    """
    input_path = Path(input_path)
    
    if not input_path.exists():
        return {"success": False, "message": f"Input file not found: {input_path}"}
    
    # Generate output path if not provided
    if output_path is None:
        output_path = OUTPUT_DIR / f"{input_path.stem}_tile_{zoom}_{x}_{y}.geojson"
    else:
        output_path = Path(output_path)
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Build tippecanoe-decode command for individual tile
    cmd = ['tippecanoe-decode']
    
    # Add options (similar to full file decode)
    if options.get('projection'):
        cmd.extend(['-s', options['projection']])
    
    if options.get('layers'):
        if isinstance(options['layers'], list):
            for layer in options['layers']:
                cmd.extend(['-l', layer])
        else:
            cmd.extend(['-l', options['layers']])
    
    if options.get('tag_layer_and_zoom', False):
        cmd.append('-c')
    
    if options.get('force', False):
        cmd.append('-f')
    
    if options.get('integer_coords', False):
        cmd.append('-I')
    
    if options.get('fraction_coords', False):
        cmd.append('-F')
    
    # Add input file and tile coordinates
    cmd.extend([str(input_path), str(zoom), str(x), str(y)])
    
    try:
        # Execute tippecanoe-decode
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Write output to file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(result.stdout)
        
        return {
            "success": True,
            "message": f"Successfully decoded tile {zoom}/{x}/{y} to {output_path.name}",
            "input_file": input_path,
            "output_file": output_path,
            "tile": {"zoom": zoom, "x": x, "y": y},
            "command": ' '.join(cmd)
        }
        
    except subprocess.CalledProcessError as e:
        return {
            "success": False,
            "message": f"tippecanoe-decode error: {e.stderr if e.stderr else str(e)}",
            "command": ' '.join(cmd)
        }
    except Exception as e:
        return {"success": False, "message": f"Error: {str(e)}"}

def get_pmtiles_stats(input_path, **options):
    """Get statistics about a PMTiles/MBTiles file
    
    Args:
        input_path (str|Path): Path to input PMTiles/MBTiles file
        **options: Additional options for tippecanoe-decode
    
    Returns:
        dict: Statistics as JSON or error information
    """
    input_path = Path(input_path)
    
    if not input_path.exists():
        return {"success": False, "message": f"Input file not found: {input_path}"}
    
    # Build tippecanoe-decode command for stats
    cmd = ['tippecanoe-decode', '-S']
    
    # Add layer filtering if specified
    if options.get('layers'):
        if isinstance(options['layers'], list):
            for layer in options['layers']:
                cmd.extend(['-l', layer])
        else:
            cmd.extend(['-l', options['layers']])
    
    # Add input file
    cmd.append(str(input_path))
    
    try:
        # Execute tippecanoe-decode
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        
        # Parse JSON output
        stats = json.loads(result.stdout)
        
        return {
            "success": True,
            "stats": stats,
            "input_file": input_path
        }
        
    except subprocess.CalledProcessError as e:
        return {
            "success": False,
            "message": f"tippecanoe-decode error: {e.stderr if e.stderr else str(e)}",
            "command": ' '.join(cmd)
        }
    except json.JSONDecodeError as e:
        return {
            "success": False,
            "message": f"Failed to parse stats JSON: {str(e)}"
        }
    except Exception as e:
        return {"success": False, "message": f"Error: {str(e)}"}

def batch_decode_pmtiles(input_dir, output_dir=None, parallel=True, verbose=True, **options):
    """Decode multiple PMTiles files in a directory
    
    Args:
        input_dir (str|Path): Directory containing PMTiles files
        output_dir (str|Path): Output directory for GeoJSON files
        parallel (bool): Use parallel processing
        verbose (bool): Show progress information
        **options: Additional options for tippecanoe-decode
    
    Returns:
        dict: Results including processed files and errors
    """
    input_dir = Path(input_dir)
    
    if output_dir is None:
        output_dir = OUTPUT_DIR
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all PMTiles/MBTiles files
    pmtiles_files = []
    for pattern in ['*.pmtiles', '*.mbtiles']:
        pmtiles_files.extend(input_dir.glob(pattern))
    
    if not pmtiles_files:
        message = f"No PMTiles/MBTiles files found in {input_dir}"
        return {"success": False, "message": message, "processed_files": [], "errors": []}
    
    if verbose:
        print(f"Found {len(pmtiles_files)} files to decode:")
        for f in pmtiles_files:
            print(f"  {f.name}")
    
    results = {
        "success": True,
        "processed_files": [],
        "errors": [],
        "total_files": len(pmtiles_files)
    }
    
    def process_single_pmtiles(pmtiles_file):
        """Process a single PMTiles file"""
        output_path = output_dir / f"{pmtiles_file.stem}_decoded.geojson"
        return decode_pmtiles_to_geojson(pmtiles_file, output_path, **options)
    
    # Process files
    if parallel and len(pmtiles_files) > 1:
        # Parallel processing
        with ProcessPoolExecutor(max_workers=min(4, len(pmtiles_files))) as executor:
            # Submit all jobs
            future_to_file = {
                executor.submit(process_single_pmtiles, pmtiles_file): pmtiles_file
                for pmtiles_file in pmtiles_files
            }
            
            # Process results with progress bar
            if verbose:
                progress_bar = tqdm(total=len(pmtiles_files), desc="Decoding files", unit="file")
            
            for future in as_completed(future_to_file):
                pmtiles_file = future_to_file[future]
                try:
                    result = future.result()
                    if result["success"]:
                        results["processed_files"].append({
                            "input_file": pmtiles_file.name,
                            "output_file": result.get("output_file"),
                        })
                        if verbose:
                            tqdm.write(f"✓ {pmtiles_file.name} -> {result.get('output_file', 'unknown').name}")
                    else:
                        results["errors"].append({
                            "file": pmtiles_file.name,
                            "error": result["message"]
                        })
                        if verbose:
                            tqdm.write(f"✗ {pmtiles_file.name}: {result['message']}")
                except Exception as e:
                    error_msg = f"Unexpected error: {str(e)}"
                    results["errors"].append({
                        "file": pmtiles_file.name,
                        "error": error_msg
                    })
                    if verbose:
                        tqdm.write(f"✗ {pmtiles_file.name}: {error_msg}")
                
                if verbose:
                    progress_bar.update(1)
            
            if verbose:
                progress_bar.close()
    
    else:
        # Sequential processing
        if verbose:
            progress_bar = tqdm(pmtiles_files, desc="Decoding files", unit="file")
        else:
            progress_bar = pmtiles_files
        
        for pmtiles_file in progress_bar:
            result = process_single_pmtiles(pmtiles_file)
            
            if result["success"]:
                results["processed_files"].append({
                    "input_file": pmtiles_file.name,
                    "output_file": result.get("output_file"),
                })
                if verbose:
                    tqdm.write(f"✓ {pmtiles_file.name} -> {result.get('output_file', 'unknown').name}")
            else:
                results["errors"].append({
                    "file": pmtiles_file.name,
                    "error": result["message"]
                })
                if verbose:
                    tqdm.write(f"✗ {pmtiles_file.name}: {result['message']}")
    
    # Set overall success status
    if results["errors"]:
        results["success"] = False
    
    if verbose:
        print(f"\n=== DECODE PROCESSING COMPLETE ===")
        print(f"Decoded: {len(results['processed_files'])}/{results['total_files']} files")
        if results["errors"]:
            print(f"Errors: {len(results['errors'])}")
    
    return results

def main():
    """Main entry point for command line usage"""
    parser = argparse.ArgumentParser(
        description='Decode PMTiles/MBTiles back to GeoJSON using tippecanoe-decode',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument('--input', required=True,
                        help='Input PMTiles/MBTiles file or directory')
    parser.add_argument('--output',
                        help='Output GeoJSON file or directory')
    parser.add_argument('--batch', action='store_true',
                        help='Process all PMTiles files in input directory')
    
    # Individual tile options
    parser.add_argument('--tile-zoom', type=int,
                        help='Zoom level for individual tile extraction')
    parser.add_argument('--tile-x', type=int,
                        help='X coordinate for individual tile extraction')
    parser.add_argument('--tile-y', type=int,
                        help='Y coordinate for individual tile extraction')
    
    # tippecanoe-decode options
    parser.add_argument('--projection', choices=['EPSG:4326', 'EPSG:3857'],
                        default='EPSG:4326',
                        help='Output projection')
    parser.add_argument('--max-zoom', type=int,
                        help='Maximum zoom level to decode')
    parser.add_argument('--min-zoom', type=int,
                        help='Minimum zoom level to decode')
    parser.add_argument('--layers', action='append',
                        help='Decode only specified layers (can be used multiple times)')
    parser.add_argument('--tag-layer-and-zoom', action='store_true',
                        help='Include layer and zoom in feature properties')
    parser.add_argument('--stats-only', action='store_true',
                        help='Only output statistics, not GeoJSON')
    parser.add_argument('--force', action='store_true',
                        help='Force decoding even with geometry problems')
    parser.add_argument('--integer-coords', action='store_true',
                        help='Use integer tile coordinates')
    parser.add_argument('--fraction-coords', action='store_true',
                        help='Use fractional tile coordinates')
    
    # Processing options
    parser.add_argument('--no-parallel', action='store_true',
                        help='Disable parallel processing for batch operations')
    parser.add_argument('--verbose', action='store_true', default=True,
                        help='Show detailed progress information')
    
    args = parser.parse_args()
    
    # Check if tippecanoe-decode is available
    if not check_tippecanoe_decode():
        print("Error: tippecanoe-decode not found in PATH")
        print("Please install tippecanoe: https://github.com/felt/tippecanoe")
        sys.exit(1)
    
    # Prepare options for tippecanoe-decode
    decode_options = {
        'projection': args.projection,
        'max_zoom': args.max_zoom,
        'min_zoom': args.min_zoom,
        'layers': args.layers,
        'tag_layer_and_zoom': args.tag_layer_and_zoom,
        'stats_only': args.stats_only,
        'force': args.force,
        'integer_coords': args.integer_coords,
        'fraction_coords': args.fraction_coords
    }
    
    # Remove None values
    decode_options = {k: v for k, v in decode_options.items() if v is not None}
    
    input_path = Path(args.input)
    
    # Handle different operation modes
    if args.stats_only:
        # Just get statistics
        if input_path.is_file():
            result = get_pmtiles_stats(input_path, **decode_options)
            if result["success"]:
                print(json.dumps(result["stats"], indent=2))
            else:
                print(f"Error: {result['message']}")
                sys.exit(1)
        else:
            print("Error: --stats-only requires a single file input")
            sys.exit(1)
    
    elif all(x is not None for x in [args.tile_zoom, args.tile_x, args.tile_y]):
        # Individual tile extraction
        if not input_path.is_file():
            print("Error: Individual tile extraction requires a single file input")
            sys.exit(1)
        
        result = decode_individual_tile(
            input_path, args.tile_zoom, args.tile_x, args.tile_y,
            args.output, **decode_options
        )
        
        if result["success"]:
            print(f"Successfully decoded tile {args.tile_zoom}/{args.tile_x}/{args.tile_y}")
            print(f"Output: {result['output_file']}")
        else:
            print(f"Error: {result['message']}")
            sys.exit(1)
    
    elif args.batch or input_path.is_dir():
        # Batch processing
        if not input_path.is_dir():
            print("Error: Batch processing requires a directory input")
            sys.exit(1)
        
        results = batch_decode_pmtiles(
            input_path, args.output,
            parallel=not args.no_parallel,
            verbose=args.verbose,
            **decode_options
        )
        
        if not results["success"]:
            print(f"Batch processing completed with {len(results['errors'])} errors:")
            for error in results["errors"]:
                print(f"  - {error['file']}: {error['error']}")
            sys.exit(1)
        else:
            print(f"Successfully decoded {len(results['processed_files'])} files")
    
    else:
        # Single file processing
        if not input_path.is_file():
            print(f"Error: Input file not found: {input_path}")
            sys.exit(1)
        
        result = decode_pmtiles_to_geojson(input_path, args.output, **decode_options)
        
        if result["success"]:
            print(f"Successfully decoded {input_path.name}")
            print(f"Output: {result['output_file']}")
        else:
            print(f"Error: {result['message']}")
            sys.exit(1)

if __name__ == "__main__":
    main()
