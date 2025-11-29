#!/usr/bin/env python3
"""
convertToFlatGeobuf.py - Convert GeoParquet files to FlatGeobuf format

FlatGeobuf is optimal for large-scale tile generation:
- Streaming read capability (low memory footprint)
- Built-in spatial indexing (fast queries)
- Compact binary format (~30-50% smaller than GeoJSON)
- Native tippecanoe support (v2.17+)
- Perfect for continent/world-scale processing

Usage:
    python convertToFlatGeobuf.py --input-dir=/path/to/parquet --output-dir=/path/to/fgb
    
    # From another script:
    from convertToFlatGeobuf import convert_parquet_to_fgb, batch_convert_directory
"""

import sys
from pathlib import Path
from typing import Union, List, Tuple, Optional
import geopandas as gpd
from tqdm import tqdm

def convert_parquet_to_fgb(
    input_path: Union[str, Path],
    output_path: Optional[Union[str, Path]] = None,
    overwrite: bool = False,
    verbose: bool = True,
    cleanup_source: bool = False
) -> Tuple[bool, str, Optional[Path]]:
    """
    Convert a single GeoParquet file to FlatGeobuf format.
    
    Args:
        input_path: Path to input .parquet file
        output_path: Path to output .fgb file (auto-generated if None)
        overwrite: Whether to overwrite existing files
        verbose: Print progress information
        cleanup_source: Remove source file after successful conversion (saves disk space)
        
    Returns:
        Tuple of (success, message, output_path)
    """
    input_path = Path(input_path)
    
    if not input_path.exists():
        return False, f"Input file not found: {input_path}", None
    
    # Auto-generate output path if not provided
    if output_path is None:
        output_path = input_path.with_suffix('.fgb')
    else:
        output_path = Path(output_path)
    
    # Check if output already exists
    if output_path.exists() and not overwrite:
        if verbose:
            print(f"⊘ Skipping {input_path.name} (already converted)")
        return True, "Already exists", output_path
    
    try:
        if verbose:
            print(f"Converting {input_path.name}...", end=" ", flush=True)
        
        # Read GeoParquet
        gdf = gpd.read_parquet(input_path)
        
        # Ensure CRS is set (FlatGeobuf requires CRS)
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
            if verbose:
                print("[assumed EPSG:4326]", end=" ", flush=True)
        
        # Write as FlatGeobuf with spatial index
        gdf.to_file(output_path, driver='FlatGeobuf', SPATIAL_INDEX='YES')
        
        # Get file stats
        input_size_mb = input_path.stat().st_size / 1024 / 1024
        output_size_mb = output_path.stat().st_size / 1024 / 1024
        compression_pct = ((input_size_mb - output_size_mb) / input_size_mb) * 100
        
        if verbose:
            print(f"✓ {len(gdf):,} features ({output_size_mb:.1f} MB, {compression_pct:+.0f}%)")
        
        # Remove source file if requested (saves disk space)
        if cleanup_source and input_path.exists():
            input_path.unlink()
            if verbose:
                print(f"  → Removed source file: {input_path.name} (saved {input_size_mb:.1f} MB)")
        
        return True, f"Converted {len(gdf)} features", output_path
        
    except Exception as e:
        if verbose:
            print(f"✗ Error: {e}")
        return False, str(e), None


def batch_convert_directory(
    input_dir: Union[str, Path],
    output_dir: Optional[Union[str, Path]] = None,
    pattern: str = "*.parquet",
    overwrite: bool = False,
    verbose: bool = True,
    parallel: bool = False,
    cleanup_source: bool = False
) -> dict:
    """
    Convert all GeoParquet files in a directory to FlatGeobuf format.
    
    Args:
        input_dir: Directory containing .parquet files
        output_dir: Directory for .fgb output (same as input if None)
        pattern: Glob pattern for finding parquet files
        overwrite: Whether to overwrite existing files
        verbose: Print progress information
        parallel: Use parallel processing (experimental)
        cleanup_source: Remove source files after successful conversion (saves disk space)
        
    Returns:
        Dictionary with conversion results
    """
    input_dir = Path(input_dir)
    
    if output_dir is None:
        output_dir = input_dir
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all parquet files
    parquet_files = sorted(input_dir.glob(pattern))
    
    if not parquet_files:
        return {
            "success": False,
            "message": f"No files matching '{pattern}' found in {input_dir}",
            "total_files": 0,
            "converted": 0,
            "skipped": 0,
            "errors": []
        }
    
    if verbose:
        print(f"Found {len(parquet_files)} GeoParquet files to convert")
        print(f"Input:  {input_dir}")
        print(f"Output: {output_dir}\n")
    
    results = {
        "success": True,
        "total_files": len(parquet_files),
        "converted": 0,
        "skipped": 0,
        "errors": [],
        "output_files": [],
        "cleaned_up": 0 if cleanup_source else None
    }
    
    # Process files
    use_tqdm = verbose
    iterator = tqdm(parquet_files, desc="Converting to FlatGeobuf") if use_tqdm else parquet_files
    
    for parquet_file in iterator:
        output_file = output_dir / parquet_file.with_suffix('.fgb').name
        
        success, message, output_path = convert_parquet_to_fgb(
            input_path=parquet_file,
            output_path=output_file,
            overwrite=overwrite,
            verbose=False,  # Suppress individual messages when batch processing
            cleanup_source=cleanup_source
        )
        
        if success:
            if "Already exists" in message:
                results["skipped"] += 1
            else:
                results["converted"] += 1
                results["output_files"].append(output_path)
                if cleanup_source:
                    results["cleaned_up"] += 1
                
                if verbose and not use_tqdm:  # Only print if not using tqdm
                    print(f"✓ {parquet_file.name} → {output_file.name}")
        else:
            results["errors"].append({
                "file": parquet_file.name,
                "error": message
            })
            results["success"] = False
            
            if verbose and not use_tqdm:
                print(f"✗ {parquet_file.name}: {message}")
    
    # Print summary
    if verbose:
        print(f"\n{'='*60}")
        print(f"Conversion Summary:")
        print(f"  Total files:     {results['total_files']}")
        print(f"  Converted:       {results['converted']}")
        print(f"  Skipped:         {results['skipped']}")
        print(f"  Errors:          {len(results['errors'])}")
        if cleanup_source:
            print(f"  Cleaned up:      {results['cleaned_up']} source files removed")
        
        if results['output_files']:
            total_size_mb = sum(f.stat().st_size for f in results['output_files']) / 1024 / 1024
            print(f"  Total FGB size:  {total_size_mb:.1f} MB")
            print(f"\n✓ FlatGeobuf files ready for tippecanoe")
    
    return results


def main():
    """Command-line interface for the converter."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Convert GeoParquet files to FlatGeobuf format for efficient tile generation"
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        required=True,
        help="Directory containing GeoParquet (.parquet) files"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for FlatGeobuf (.fgb) files (default: same as input)"
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default="*.parquet",
        help="Glob pattern for finding parquet files (default: *.parquet)"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing FlatGeobuf files"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress output"
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove source files after successful conversion (saves disk space)"
    )
    
    args = parser.parse_args()
    
    results = batch_convert_directory(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        pattern=args.pattern,
        overwrite=args.overwrite,
        verbose=not args.quiet,
        cleanup_source=args.cleanup
    )
    
    sys.exit(0 if results["success"] else 1)


if __name__ == "__main__":
    main()
