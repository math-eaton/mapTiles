#!/usr/bin/env python3
"""
downloadOverture.py - Download and process source data from Overture Maps

This module handles downloading geospatial data from Overture Maps using DuckDB
and SQL queries. It can be used standalone or imported into other scripts.

Usage:
    python downloadOverture.py --extent="20.0,-7.0,26.0,-3.0" --buffer=0.2
    
    # From another script:
    from downloadOverture import download_overture_data
    download_overture_data(extent=(20.0, -7.0, 26.0, -3.0), buffer_degrees=0.2)
"""

import duckdb
import argparse
import sys
from pathlib import Path
from tqdm import tqdm
import mercantile


def snap_to_tile_bounds(extent, zoom=16):
    """Snap extent to align with slippy tile boundaries to prevent rendering artifacts"""
    xmin, ymin, xmax, ymax = extent
    tiles = list(mercantile.tiles(xmin, ymin, xmax, ymax, zoom))
    if not tiles:
        return extent

    snapped = mercantile.bounds(tiles[0])
    for t in tiles[1:]:
        b = mercantile.bounds(t)
        snapped = mercantile.LngLatBbox(
            min(snapped.west, b.west),
            min(snapped.south, b.south),
            max(snapped.east, b.east),
            max(snapped.north, b.north)
        )
    return (snapped.west, snapped.south, snapped.east, snapped.north)

def get_db_url(sql_section):
    """Extract URL and metadata from SQL section for progress reporting"""
    lines = sql_section.strip().split('\n')
    
    for line in lines:
        if 'read_parquet(' in line:
            # Extract URL from read_parquet call
            import re
            url_match = re.search(r"read_parquet\('([^']+)'", line)
            if url_match:
                url = url_match.group(1)
                
                # Extract data type from URL or TO clause
                data_type = "unknown"
                if 'theme=' in url:
                    theme_match = re.search(r'theme=([^/]+)', url)
                    type_match = re.search(r'type=([^/]+)', url)
                    if theme_match and type_match:
                        data_type = f"{theme_match.group(1)}/{type_match.group(1)}"
                
                # Extract output file from TO clause
                output_file = "unknown"
                for to_line in lines:
                    if "TO '" in to_line:
                        to_match = re.search(r"TO '([^']+)'", to_line)
                        if to_match:
                            output_file = Path(to_match.group(1)).name
                            break
                
                return {
                    "url": url,
                    "description": data_type,
                    "output_file": output_file
                }
    
    return None

def download_overture_data(extent, buffer_degrees=0, template_path=None, verbose=True,
                           project_root=None, overture_data_dir=None):
    """Download and process source data from Overture Maps
    
    Args:
        extent (tuple): (xmin, ymin, xmax, ymax) in WGS84 coordinates
        buffer_degrees (float): Buffer around extent in degrees (default: 0)
        template_path (str|Path): Path to SQL template file (default: tilequeries.sql)
        verbose (bool): Show progress information (default: True)
        project_root (str|Path|None): Optional project root path to override module default
        overture_data_dir (str|Path|None): Optional data directory path to override module default

    Returns:
        dict: Results including processed files and any errors
    """
    # Resolve optional path overrides; fall back to module-level defaults if not provided
    if project_root is not None:
        PROJECT_ROOT = Path(project_root)
    else:
        PROJECT_ROOT = Path(__file__).resolve().parent.parent

    if overture_data_dir is not None:
        overture_data_dir = Path(overture_data_dir)
    else:
        # Use disk-based path structure
        # Structure: /mnt/disk02/gis/mapTiles/data/1-input/overture/
        DATA_DISK = Path("/mnt/disk02/gis/mapTiles")
        DATA_DIR = DATA_DISK / "data"
        INPUT_DIR = DATA_DIR / "1-input"
        overture_data_dir = INPUT_DIR / "overture"

    # Snap extent to tile boundaries to prevent rendering artifacts
    snapped_extent = snap_to_tile_bounds(extent, zoom=16)
    extent_xmin, extent_ymin, extent_xmax, extent_ymax = snapped_extent
    
    if verbose:
        print("=== DOWNLOADING SOURCE DATA ===")
        print(f"Raw extent: {extent}")
        print(f"Snapped extent: {snapped_extent}")
        print(f"Map extent: {extent_xmin}, {extent_ymin} to {extent_xmax}, {extent_ymax}")
    
    # Calculate buffered extent for data download
    buffered_xmin = extent_xmin - buffer_degrees
    buffered_xmax = extent_xmax + buffer_degrees
    buffered_ymin = extent_ymin - buffer_degrees
    buffered_ymax = extent_ymax + buffer_degrees
    
    if verbose:
        print(f"Download extent (buffered): {buffered_xmin}, {buffered_ymin} to {buffered_xmax}, {buffered_ymax}")
        print(f"Buffer: {buffer_degrees} degrees (~{buffer_degrees * 111:.1f}km)")
        print()
    
    # Ensure directories exist
    overture_data_dir.mkdir(parents=True, exist_ok=True)
    
    # Read the SQL template file
    if template_path is None:
        template_path = Path(__file__).parent / 'tilequeries.sql'
    else:
        template_path = Path(template_path)
    
    if not template_path.exists():
        raise FileNotFoundError(f"SQL template file not found: {template_path}")
    
    with open(template_path, 'r') as file:
        template_content = file.read()

    # Replace template variables with actual paths
    sql_content = template_content.replace('{{overture_data_dir}}', str(overture_data_dir))

    # Replace the extent variables with buffered extent
    sql_content = sql_content.replace('$extent_xmin', str(buffered_xmin))
    sql_content = sql_content.replace('$extent_xmax', str(buffered_xmax))
    sql_content = sql_content.replace('$extent_ymin', str(buffered_ymin))
    sql_content = sql_content.replace('$extent_ymax', str(buffered_ymax))

    # Split the SQL content into sections based on '-- break'
    sql_sections = sql_content.split('-- break')

    # Connect to DuckDB
    conn = duckdb.connect()
    
    results = {
        "success": True,
        "processed_sections": 0,
        "errors": [],
        "output_files": []
    }

    # Create a progress bar for the overall process
    valid_sections = [s for s in sql_sections if s.strip() and not s.strip().startswith('SET extent_')]
    
    if verbose:
        progress_bar = tqdm(total=len(valid_sections), desc="Overall progress", unit="section", position=0, leave=True)
    
    try:
        # Execute each section
        for i, section in enumerate(sql_sections):
            section = section.strip()
            if section and not section.startswith('SET extent_'):  # Skip empty sections and SET commands
                # Extract URL and data type from the section
                url_info = get_db_url(section)
                if url_info and verbose:
                    tqdm.write(f"Executing {url_info['description']}")
                    tqdm.write(f"  -> Querying: {url_info['url']}")
                    tqdm.write(f"  -> Output: {url_info['output_file']}")
                    results["output_files"].append(url_info['output_file'])
                elif verbose:
                    tqdm.write(f"Executing section {i + 1}...")
                
                try:
                    # Execute the SQL section
                    conn.execute(section)
                    results["processed_sections"] += 1
                    
                    if verbose:
                        progress_bar.update(1)
                    
                except Exception as e:
                    error_msg = f"Error executing section {i + 1}: {str(e)}"
                    results["errors"].append(error_msg)
                    if verbose:
                        tqdm.write(f"ERROR: {error_msg}")
                    
    finally:
        # Close the connection
        conn.close()
        if verbose:
            progress_bar.close()
            tqdm.write("=== SOURCE DATA DOWNLOAD COMPLETE ===\n")
    
    if results["errors"]:
        results["success"] = False
    
    return results

def main():
    """Main entry point for command line usage"""
    parser = argparse.ArgumentParser(
        description='Download geospatial data from Overture Maps',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument('--extent', required=True,
                        help='Extent as "xmin,ymin,xmax,ymax" in WGS84 coordinates')
    parser.add_argument('--buffer', type=float, default=0.2,
                        help='Buffer around extent in degrees')
    parser.add_argument('--template', 
                        help='Path to SQL template file (default: tilequeries.sql)')
    parser.add_argument('--verbose', action='store_true', default=True,
                        help='Show detailed progress information')
    
    args = parser.parse_args()
    
    # Parse extent
    try:
        extent_parts = args.extent.split(',')
        if len(extent_parts) != 4:
            raise ValueError("Extent must have 4 values")
        extent = tuple(float(x) for x in extent_parts)
    except ValueError as e:
        print(f"Error parsing extent: {e}")
        print("Extent format: xmin,ymin,xmax,ymax")
        sys.exit(1)
    
    # Run the download
    results = download_overture_data(
        extent=extent,
        buffer_degrees=args.buffer,
        template_path=args.template,
    )
    
    if results["success"]:
        print(f"Successfully processed {results['processed_sections']} sections")
        if results["output_files"]:
            print(f"Created files: {', '.join(results['output_files'])}")
    else:
        print(f"Download completed with {len(results['errors'])} errors:")
        for error in results["errors"]:
            print(f"  - {error}")
        sys.exit(1)

if __name__ == "__main__":
    main()
