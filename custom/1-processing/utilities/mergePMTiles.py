#!/usr/bin/env python3
"""
PMTiles Merger with Pre-Processing Analysis

Merges individual PMTiles layers from a directory into a single output archive.
Includes pre-processing analysis to ensure proper alignment of zoom levels, 
tile sizes, and extents before merging.

Based on tile-join from the Tippecanoe/Protomaps toolkit.

Usage:
    python mergePMTiles.py -i input_dir -o output.pmtiles
    python mergePMTiles.py -r file1.pmtiles file2.pmtiles -o merged.pmtiles --force
    python mergePMTiles.py -i input_dir -o output.pmtiles --overzoom --buffer=16
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class PMTilesMetadata:
    """Store and analyze PMTiles metadata."""
    
    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.metadata = None
        self.minzoom = None
        self.maxzoom = None
        self.bounds = None
        self.layers = []
        self.center = None
        self.tile_stats = {}
        
    def load_metadata(self) -> bool:
        """Load metadata from PMTiles file using pmtiles CLI."""
        try:
            # Get JSON metadata using pmtiles show --metadata command
            result = subprocess.run(
                ['pmtiles', 'show', str(self.filepath), '--metadata'],
                capture_output=True,
                text=True,
                check=True
            )
            
            # Parse the JSON output
            data = json.loads(result.stdout)
            self.metadata = data
            
            # Extract key information from PMTiles metadata format
            # Parse bounds from string format "minlon,minlat,maxlon,maxlat"
            bounds_str = data.get('antimeridian_adjusted_bounds', '')
            if bounds_str:
                try:
                    bounds_parts = [float(x) for x in bounds_str.split(',')]
                    self.bounds = bounds_parts
                except (ValueError, IndexError):
                    self.bounds = [-180, -85.0511, 180, 85.0511]
            else:
                self.bounds = [-180, -85.0511, 180, 85.0511]
            
            # Extract layer information from vector_layers
            self.layers = data.get('vector_layers', [])
            
            # Get zoom levels from vector_layers
            if self.layers:
                # Get min and max zoom from all layers
                minzooms = [layer.get('minzoom', 0) for layer in self.layers]
                maxzooms = [layer.get('maxzoom', 14) for layer in self.layers]
                self.minzoom = min(minzooms) if minzooms else 0
                self.maxzoom = max(maxzooms) if maxzooms else 14
            else:
                self.minzoom = 0
                self.maxzoom = 14
            
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to read metadata from {self.filepath}: {e}")
            logger.debug(f"stdout: {e.stdout}, stderr: {e.stderr}")
            return False
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse metadata JSON from {self.filepath}: {e}")
            return False
        except FileNotFoundError:
            logger.error("pmtiles CLI tool not found. Please install it first.")
            logger.info("Install: npm install -g pmtiles or download from https://github.com/protomaps/go-pmtiles")
            return False
    
    def get_layer_names(self) -> List[str]:
        """Get list of layer names in this PMTiles file."""
        return [layer.get('id', layer.get('name', 'unknown')) for layer in self.layers]
    
    def __repr__(self):
        return f"PMTiles({self.filepath.name}, z{self.minzoom}-{self.maxzoom}, layers={len(self.layers)})"


class PMTilesAnalyzer:
    """Analyze multiple PMTiles files for compatibility before merging."""
    
    def __init__(self, files: List[Path]):
        self.files = files
        self.metadata_list: List[PMTilesMetadata] = []
        self.analysis_results = {
            'compatible': True,
            'warnings': [],
            'errors': [],
            'info': []
        }
    
    def analyze(self) -> Dict:
        """Analyze all files and check for compatibility issues."""
        logger.info(f"Analyzing {len(self.files)} PMTiles files...")
        
        # Load metadata for all files
        for filepath in self.files:
            meta = PMTilesMetadata(filepath)
            if meta.load_metadata():
                self.metadata_list.append(meta)
            else:
                self.analysis_results['errors'].append(
                    f"Could not load metadata from {filepath.name}"
                )
                self.analysis_results['compatible'] = False
        
        if not self.metadata_list:
            self.analysis_results['errors'].append("No valid PMTiles files found")
            self.analysis_results['compatible'] = False
            return self.analysis_results
        
        # Run compatibility checks
        self._check_zoom_levels()
        self._check_bounds()
        self._check_layers()
        self._check_tile_sizes()
        
        return self.analysis_results
    
    def _check_zoom_levels(self):
        """Check if zoom levels are aligned across files."""
        minzooms = [m.minzoom for m in self.metadata_list]
        maxzooms = [m.maxzoom for m in self.metadata_list]
        
        min_minzoom = min(minzooms)
        max_minzoom = max(minzooms)
        min_maxzoom = min(maxzooms)
        max_maxzoom = max(maxzooms)
        
        self.analysis_results['info'].append(
            f"Zoom ranges: min={min_minzoom}-{max_minzoom}, max={min_maxzoom}-{max_maxzoom}"
        )
        
        if len(set(maxzooms)) > 1:
            self.analysis_results['warnings'].append(
                f"Different maxzoom values detected: {set(maxzooms)}. "
                "Consider using --overzoom to scale up lower-zoom tilesets."
            )
    
    def _check_bounds(self):
        """Check if geographic bounds overlap or align."""
        if len(self.metadata_list) < 2:
            return
        
        # Calculate overall bounding box
        all_bounds = [m.bounds for m in self.metadata_list]
        
        # Check for each file
        for i, meta in enumerate(self.metadata_list):
            logger.debug(f"{meta.filepath.name}: bounds={meta.bounds}")
        
        # Calculate union of bounds
        min_lon = min(b[0] for b in all_bounds)
        min_lat = min(b[1] for b in all_bounds)
        max_lon = max(b[2] for b in all_bounds)
        max_lat = max(b[3] for b in all_bounds)
        
        self.analysis_results['info'].append(
            f"Combined bounds: [{min_lon:.4f}, {min_lat:.4f}, {max_lon:.4f}, {max_lat:.4f}]"
        )
        
        # Check if bounds are identical (might indicate same source)
        if len(set(str(b) for b in all_bounds)) == 1:
            self.analysis_results['info'].append(
                "All files have identical bounds (likely from same geographic area)"
            )
    
    def _check_layers(self):
        """Check for layer name conflicts."""
        layer_map = {}
        
        for meta in self.metadata_list:
            for layer_name in meta.get_layer_names():
                if layer_name not in layer_map:
                    layer_map[layer_name] = []
                layer_map[layer_name].append(meta.filepath.name)
        
        # Report layers
        self.analysis_results['info'].append(
            f"Total unique layers: {len(layer_map)}"
        )
        
        # Check for duplicate layers across files
        for layer_name, files in layer_map.items():
            if len(files) > 1:
                self.analysis_results['warnings'].append(
                    f"Layer '{layer_name}' appears in multiple files: {', '.join(files)}. "
                    "These will be merged in the output."
                )
    
    def _check_tile_sizes(self):
        """Check if tile sizes are consistent."""
        # This is a placeholder - actual tile size checking would require
        # inspecting individual tiles, which is more complex
        self.analysis_results['info'].append(
            "Tile size analysis: Individual tile inspection not implemented. "
            "tile-join will handle oversized tiles based on --no-tile-size-limit flag."
        )
    
    def print_report(self):
        """Print analysis report to console."""
        print("\n" + "="*70)
        print("PMTiles Merge Analysis Report")
        print("="*70 + "\n")
        
        print(f"Files to merge: {len(self.metadata_list)}")
        for meta in self.metadata_list:
            print(f"  • {meta.filepath.name}")
            print(f"    Zoom: {meta.minzoom}-{meta.maxzoom}, Layers: {len(meta.layers)}")
        
        print("\n" + "-"*70)
        
        if self.analysis_results['info']:
            print("\nINFORMATION:")
            for info in self.analysis_results['info']:
                print(f"  ℹ {info}")
        
        if self.analysis_results['warnings']:
            print("\nWARNINGS:")
            for warning in self.analysis_results['warnings']:
                print(f"  ⚠ {warning}")
        
        if self.analysis_results['errors']:
            print("\nERRORS:")
            for error in self.analysis_results['errors']:
                print(f"  ✗ {error}")
        
        print("\n" + "="*70)
        
        if self.analysis_results['compatible']:
            print("✓ Files are compatible for merging")
        else:
            print("✗ Compatibility issues detected - merge may fail")
        
        print("="*70 + "\n")


def find_pmtiles_files(directory: Path, exclude_output: Optional[Path] = None) -> List[Path]:
    """Find all .pmtiles files in a directory, optionally excluding the output file."""
    pmtiles_files = list(directory.glob("*.pmtiles"))
    
    # Exclude the output file if it's in the same directory
    if exclude_output and exclude_output.parent == directory:
        pmtiles_files = [f for f in pmtiles_files if f.resolve() != exclude_output.resolve()]
    
    logger.info(f"Found {len(pmtiles_files)} PMTiles files in {directory}")
    return sorted(pmtiles_files)


def build_tile_join_command(
    input_files: List[Path],
    output_file: Path,
    force: bool = False,
    overzoom: bool = False,
    buffer: Optional[int] = None,
    attribution: Optional[str] = None,
    name: Optional[str] = None,
    description: Optional[str] = None,
    include_layers: Optional[List[str]] = None,
    exclude_layers: Optional[List[str]] = None,
    rename_layers: Optional[List[Tuple[str, str]]] = None,
    min_zoom: Optional[int] = None,
    max_zoom: Optional[int] = None,
    no_tile_size_limit: bool = False,
    no_tile_compression: bool = False,
    no_tile_stats: bool = False,
) -> List[str]:
    """Build the tile-join command with specified options."""
    
    cmd = ['tile-join']
    
    # Output file
    cmd.extend(['-o', str(output_file)])
    
    # Input files
    for input_file in input_files:
        cmd.append(str(input_file))
    
    # Force overwrite
    if force:
        cmd.append('-f')
    
    # Overzooming
    if overzoom:
        cmd.append('--overzoom')
    
    if buffer is not None:
        cmd.extend(['-b', str(buffer)])
    
    # Metadata
    if attribution:
        cmd.extend(['-A', attribution])
    
    if name:
        cmd.extend(['-n', name])
    
    if description:
        cmd.extend(['-N', description])
    
    # Layer filtering
    if include_layers:
        for layer in include_layers:
            cmd.extend(['-l', layer])
    
    if exclude_layers:
        for layer in exclude_layers:
            cmd.extend(['-L', layer])
    
    if rename_layers:
        for old_name, new_name in rename_layers:
            cmd.extend(['-R', f'{old_name}:{new_name}'])
    
    # Zoom levels
    if min_zoom is not None:
        cmd.extend(['-Z', str(min_zoom)])
    
    if max_zoom is not None:
        cmd.extend(['-z', str(max_zoom)])
    
    # Tile size and compression
    if no_tile_size_limit:
        cmd.append('-pk')
    
    if no_tile_compression:
        cmd.append('-pC')
    
    if no_tile_stats:
        cmd.append('-pg')
    
    return cmd


def run_tile_join(cmd: List[str]) -> bool:
    """Execute the tile-join command."""
    logger.info(f"Executing: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
        
        if result.stdout:
            logger.info(result.stdout)
        
        logger.info("✓ Merge completed successfully")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"✗ tile-join failed with exit code {e.returncode}")
        if e.stderr:
            logger.error(f"Error output: {e.stderr}")
        if e.stdout:
            logger.info(f"Standard output: {e.stdout}")
        return False
    except FileNotFoundError:
        logger.error("tile-join command not found. Please install Tippecanoe.")
        logger.info("Install: https://github.com/felt/tippecanoe")
        return False


def parse_rename_layers(rename_str_list: List[str]) -> List[Tuple[str, str]]:
    """Parse layer rename strings in format 'old:new'."""
    result = []
    for rename_str in rename_str_list:
        if ':' not in rename_str:
            logger.warning(f"Invalid rename format: {rename_str}. Expected 'old:new'")
            continue
        old, new = rename_str.split(':', 1)
        result.append((old.strip(), new.strip()))
    return result


def main():
    parser = argparse.ArgumentParser(
        description='Merge PMTiles files with pre-processing analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Merge all PMTiles in a directory
  %(prog)s -i tiles/ -o merged.pmtiles
  
  # Merge specific files with overzooming
  %(prog)s -r layer1.pmtiles layer2.pmtiles -o merged.pmtiles --overzoom
  
  # Merge with layer filtering
  %(prog)s -i tiles/ -o merged.pmtiles -l buildings -l roads
  
  # Merge with custom metadata
  %(prog)s -i tiles/ -o merged.pmtiles -n "My Map" -A "© OpenStreetMap"
        """
    )
    
    # Input sources (mutually exclusive)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        '-i', '--input-dir',
        type=Path,
        help='Directory containing PMTiles files to merge'
    )
    input_group.add_argument(
        '-r', '--read-from',
        nargs='+',
        type=Path,
        help='Specific PMTiles files to merge'
    )
    
    # Output
    parser.add_argument(
        '-o', '--output',
        type=Path,
        required=True,
        help='Output PMTiles file'
    )
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        help='Overwrite output file if it exists'
    )
    
    # Analysis options
    parser.add_argument(
        '--skip-analysis',
        action='store_true',
        help='Skip pre-processing analysis and proceed directly to merge'
    )
    
    # Overzooming
    parser.add_argument(
        '--overzoom',
        action='store_true',
        help='Scale up tiles from lower maxzoom sources to match highest maxzoom'
    )
    parser.add_argument(
        '-b', '--buffer',
        type=int,
        help='Tile buffer size in pixels for overzoomed tiles'
    )
    
    # Metadata
    parser.add_argument(
        '-A', '--attribution',
        help='Attribution string for the tileset'
    )
    parser.add_argument(
        '-n', '--name',
        help='Name for the tileset'
    )
    parser.add_argument(
        '-N', '--description',
        help='Description for the tileset'
    )
    
    # Layer operations
    parser.add_argument(
        '-l', '--layer',
        action='append',
        dest='include_layers',
        help='Include only specified layers (can be used multiple times)'
    )
    parser.add_argument(
        '-L', '--exclude-layer',
        action='append',
        dest='exclude_layers',
        help='Exclude specified layers (can be used multiple times)'
    )
    parser.add_argument(
        '-R', '--rename-layer',
        action='append',
        dest='rename_layers',
        help='Rename layer in format old:new (can be used multiple times)'
    )
    
    # Zoom levels
    parser.add_argument(
        '-z', '--maximum-zoom',
        type=int,
        help='Maximum zoom level to include'
    )
    parser.add_argument(
        '-Z', '--minimum-zoom',
        type=int,
        help='Minimum zoom level to include'
    )
    
    # Tile options
    parser.add_argument(
        '-pk', '--no-tile-size-limit',
        action='store_true',
        help="Don't skip tiles larger than 500K"
    )
    parser.add_argument(
        '-pC', '--no-tile-compression',
        action='store_true',
        help="Don't compress PBF vector tile data"
    )
    parser.add_argument(
        '-pg', '--no-tile-stats',
        action='store_true',
        help="Don't generate tilestats metadata"
    )
    
    # Logging
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Determine input files
    if args.input_dir:
        if not args.input_dir.is_dir():
            logger.error(f"Input directory does not exist: {args.input_dir}")
            sys.exit(1)
        input_files = find_pmtiles_files(args.input_dir, exclude_output=args.output)
    else:
        input_files = args.read_from
    
    if not input_files:
        logger.error("No PMTiles files found to merge")
        sys.exit(1)
    
    # Verify all input files exist
    for f in input_files:
        if not f.exists():
            logger.error(f"Input file does not exist: {f}")
            sys.exit(1)
    
    # Check if output exists and force not specified
    if args.output.exists() and not args.force:
        logger.error(f"Output file already exists: {args.output}")
        logger.error("Use --force to overwrite")
        sys.exit(1)
    
    # Run pre-processing analysis unless skipped
    if not args.skip_analysis:
        analyzer = PMTilesAnalyzer(input_files)
        results = analyzer.analyze()
        analyzer.print_report()
        
        if not results['compatible']:
            response = input("\nCompatibility issues detected. Continue anyway? [y/N]: ")
            if response.lower() not in ['y', 'yes']:
                logger.info("Merge cancelled by user")
                sys.exit(0)
    
    # Parse rename layers
    rename_layers = None
    if args.rename_layers:
        rename_layers = parse_rename_layers(args.rename_layers)
    
    # Build tile-join command
    cmd = build_tile_join_command(
        input_files=input_files,
        output_file=args.output,
        force=args.force,
        overzoom=args.overzoom,
        buffer=args.buffer,
        attribution=args.attribution,
        name=args.name,
        description=args.description,
        include_layers=args.include_layers,
        exclude_layers=args.exclude_layers,
        rename_layers=rename_layers,
        min_zoom=args.minimum_zoom,
        max_zoom=args.maximum_zoom,
        no_tile_size_limit=args.no_tile_size_limit,
        no_tile_compression=args.no_tile_compression,
        no_tile_stats=args.no_tile_stats,
    )
    
    # Execute merge
    logger.info("\nStarting merge operation...")
    success = run_tile_join(cmd)
    
    if success:
        # Verify output was created
        if args.output.exists():
            size_mb = args.output.stat().st_size / (1024 * 1024)
            logger.info(f"✓ Output file created: {args.output} ({size_mb:.2f} MB)")
        sys.exit(0)
    else:
        logger.error("✗ Merge operation failed")
        sys.exit(1)


if __name__ == '__main__':
    main()
