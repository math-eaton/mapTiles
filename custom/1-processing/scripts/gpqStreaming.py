#!/usr/bin/env python3
"""
gpqStreaming.py - Stream GeoParquet directly to Tippecanoe using GPQ

GPQ (GeoParquet Query) by Planet Labs provides efficient streaming from GeoParquet
to GeoJSON/GeoJSONSeq format, eliminating the need for intermediate FlatGeobuf files.

Benefits:
- No intermediate file conversion (GeoParquet → tippecanoe directly)
- Saves disk space (no .fgb files for Overture data)
- Faster processing (one less I/O step)
- Memory efficient streaming

Installation:
    pip install gpq
    # or
    cargo install gpq

Usage:
    python gpqStreaming.py --input=data.parquet --output=tiles.pmtiles
    
    # From another script:
    from gpqStreaming import process_geoparquet_to_tiles
    process_geoparquet_to_tiles(input_path="data.parquet", output_path="tiles.pmtiles")
"""

import subprocess
import sys
from pathlib import Path
from typing import Union, Optional, Tuple, List
import shutil

def check_gpq_installed() -> Tuple[bool, Optional[str]]:
    """
    Check if gpq is installed and available in PATH.
    
    Returns:
        Tuple of (is_installed, version_string)
    """
    try:
        # Try 'gpq version' command (Homebrew/binary installation)
        result = subprocess.run(
            ['gpq', 'version'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            version = result.stdout.strip()
            return True, f"gpq {version}"
        return False, None
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False, None


def stream_geoparquet_to_tippecanoe(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    layer_name: str,
    tippecanoe_args: List[str],
    verbose: bool = True
) -> Tuple[bool, str]:
    """
    Stream GeoParquet to tippecanoe using gpq convert.
    
    This creates a pipeline: gpq convert → tippecanoe
    No intermediate files are created - data streams through pipe.
    
    Args:
        input_path: Path to .parquet file
        output_path: Path to output .pmtiles file
        layer_name: Name for the output layer
        tippecanoe_args: List of tippecanoe command arguments (excluding input/output)
        verbose: Print progress information
        
    Returns:
        Tuple of (success, message)
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    
    if not input_path.exists():
        return False, f"Input file not found: {input_path}"
    
    # Check gpq is installed
    gpq_installed, gpq_version = check_gpq_installed()
    if not gpq_installed:
        return False, "gpq not installed. Install with: pip install gpq (or cargo install gpq)"
    
    if verbose:
        print(f"Processing {input_path.name} with gpq → tippecanoe...")
        print(f"  Using gpq {gpq_version}")
    
    try:
        # Build gpq command: gpq convert input.parquet --to=geojsonseq
        gpq_cmd = [
            'gpq',
            'convert',
            str(input_path),
            '--to=geojsonseq'  # Stream as GeoJSONSeq (newline-delimited)
        ]
        
        # Build tippecanoe command
        tipp_cmd = [
            'tippecanoe',
            '-o', str(output_path),
            '-l', layer_name,
            '--force',  # Overwrite existing output
            *tippecanoe_args  # Additional tippecanoe settings
        ]
        
        if verbose:
            print(f"  GPQ: {' '.join(gpq_cmd)}")
            print(f"  Tippecanoe: {' '.join(tipp_cmd[:6])}... [+{len(tippecanoe_args)} args]")
        
        # Create output directory if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Execute pipeline: gpq | tippecanoe
        # gpq converts GeoParquet → GeoJSONSeq and pipes to tippecanoe
        with subprocess.Popen(gpq_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as gpq_proc:
            with subprocess.Popen(
                tipp_cmd,
                stdin=gpq_proc.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            ) as tipp_proc:
                # Allow gpq_proc to receive SIGPIPE if tipp_proc exits
                gpq_proc.stdout.close()
                
                # Wait for completion
                tipp_stdout, tipp_stderr = tipp_proc.communicate()
                gpq_proc.wait()
                
                # Check for errors
                if tipp_proc.returncode != 0:
                    error_msg = tipp_stderr.decode('utf-8', errors='replace')
                    return False, f"Tippecanoe error: {error_msg}"
                
                if gpq_proc.returncode != 0:
                    gpq_stderr = gpq_proc.stderr.read().decode('utf-8', errors='replace')
                    return False, f"GPQ error: {gpq_stderr}"
        
        # Verify output was created
        if not output_path.exists():
            return False, "Output file was not created"
        
        output_size_mb = output_path.stat().st_size / 1024 / 1024
        
        if verbose:
            print(f"  ✓ Created {output_path.name} ({output_size_mb:.1f} MB)")
        
        return True, f"Successfully created {output_path.name}"
        
    except Exception as e:
        return False, f"Pipeline error: {str(e)}"


def get_layer_name_from_path(file_path: Path) -> str:
    """Extract layer name from file path (e.g., 'roads.parquet' → 'roads')"""
    return file_path.stem


def main():
    """Command-line interface for GPQ streaming"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Stream GeoParquet to PMTiles using GPQ + Tippecanoe",
        epilog="Example: python gpqStreaming.py --input=roads.parquet --output=roads.pmtiles"
    )
    parser.add_argument(
        '--input',
        type=str,
        required=True,
        help="Input GeoParquet file (.parquet)"
    )
    parser.add_argument(
        '--output',
        type=str,
        required=True,
        help="Output PMTiles file (.pmtiles)"
    )
    parser.add_argument(
        '--layer',
        type=str,
        default=None,
        help="Layer name (default: derived from filename)"
    )
    parser.add_argument(
        '--min-zoom',
        type=int,
        default=0,
        help="Minimum zoom level (default: 0)"
    )
    parser.add_argument(
        '--max-zoom',
        type=int,
        default=14,
        help="Maximum zoom level (default: 14)"
    )
    parser.add_argument(
        '--drop-densest-as-needed',
        action='store_true',
        help="Enable tippecanoe's feature dropping for large datasets"
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help="Suppress progress output"
    )
    
    args = parser.parse_args()
    
    # Check gpq installation
    gpq_installed, gpq_version = check_gpq_installed()
    if not gpq_installed:
        print("ERROR: gpq is not installed")
        print("\nInstall gpq:")
        print("  pip install gpq")
        print("  # or")
        print("  cargo install gpq")
        sys.exit(1)
    
    # Derive layer name if not provided
    input_path = Path(args.input)
    layer_name = args.layer or get_layer_name_from_path(input_path)
    
    # Build tippecanoe arguments
    tipp_args = [
        f'-Z{args.min_zoom}',
        f'-z{args.max_zoom}',
    ]
    
    if args.drop_densest_as_needed:
        tipp_args.append('--drop-densest-as-needed')
    
    # Execute streaming pipeline
    success, message = stream_geoparquet_to_tippecanoe(
        input_path=args.input,
        output_path=args.output,
        layer_name=layer_name,
        tippecanoe_args=tipp_args,
        verbose=not args.quiet
    )
    
    if success:
        print(f"\n✓ {message}")
        sys.exit(0)
    else:
        print(f"\n✗ {message}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
