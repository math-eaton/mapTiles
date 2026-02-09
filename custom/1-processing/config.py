"""
Configuration module for the geospatial data processing pipeline.

This module centralizes all path configuration and project settings,
making it easy to import and use across notebooks and scripts.
"""

from pathlib import Path
import os

# Load .env from repository root
def load_environment():
    """Load .env file from repository root (monorepo-wide configuration)."""
    try:
        from dotenv import load_dotenv
        # This file is at: /basemap/1-processing/config.py
        # Repository root is: /basemap/
        repo_root = Path(__file__).resolve().parent.parent
        env_file = repo_root / '.env'
        if env_file.exists():
            load_dotenv(env_file)
            return True
    except ImportError:
        # python-dotenv not installed, continue with system env vars
        pass
    return False

# Load environment before defining paths
load_environment()

# Detect project root - works both from notebooks and scripts
def get_project_root():
    """
    Dynamically find the project root (1-processing directory).
    Works from notebooks, scripts, or any subdirectory.
    """
    current = Path(__file__).resolve().parent
    # This file is in 1-processing/config.py
    return current


# Initialize paths
PROJECT_ROOT = get_project_root()

# Data disk - check environment variable first, then fall back to default
# If DATA_DISK is relative (like '.'), resolve it relative to the repository root
data_disk_env = os.environ.get("DATA_DISK", "/mnt/pool/gis/mapTiles")
if data_disk_env.startswith(('.', '..')):
    # Relative path - resolve from repository root (parent of PROJECT_ROOT)
    repo_root = PROJECT_ROOT.parent
    DATA_DISK = (repo_root / data_disk_env).resolve()
else:
    # Absolute path
    DATA_DISK = Path(data_disk_env)

SCRIPTS_DIR = PROJECT_ROOT / "scripts"
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
UTILITIES_DIR = PROJECT_ROOT / "utilities"

# Data directories on disk - simpler structure without 1-processing subfolder
DATA_DIR = DATA_DISK / "data"
INPUT_DIR = DATA_DIR / "1-input"
OVERTURE_DATA_DIR = INPUT_DIR / "overture"
GRID3_DATA_DIR = INPUT_DIR / "grid3"
SCRATCH_DIR = DATA_DIR / "2-scratch"
OUTPUT_DIR = DATA_DIR / "3-pmtiles"
TILE_DIR = OUTPUT_DIR  # Alias for consistency with scripts

# Template paths
TILE_QUERIES_TEMPLATE = SCRIPTS_DIR / "tilequeries.sql"
TIPPECANOE_TEMPLATE = SCRIPTS_DIR / "tippecanoe.template"


# Default processing configuration
def _get_env_required(name):
    val = os.environ.get(name)
    if val is None:
        raise RuntimeError(
            "Required environment variable(s) not set. "
            "Please set EXTENT_WEST, EXTENT_SOUTH, EXTENT_EAST, EXTENT_NORTH in your .env or environment."
        )
    try:
        return float(val)
    except ValueError:
        raise RuntimeError(f"Environment variable '{name}' must be a valid number, got: {val!r}")

# Read required extent values (error out if any are missing or invalid)
_west = _get_env_required("EXTENT_WEST")
_south = _get_env_required("EXTENT_SOUTH")
_east = _get_env_required("EXTENT_EAST")
_north = _get_env_required("EXTENT_NORTH")

# Basic sanity checks
if not (_west < _east and _south < _north):
    raise RuntimeError(
        "Invalid extent coordinates: ensure EXTENT_WEST < EXTENT_EAST and EXTENT_SOUTH < EXTENT_NORTH."
    )

# Optional buffer (defaults to 0.0 if not set)
_buffer_degrees = float(os.environ.get("EXTENT_BUFFER", "0.0"))

DEFAULT_CONFIG = {
    "paths": {
        "project_root": PROJECT_ROOT,
        "scripts_dir": SCRIPTS_DIR,
        "notebooks_dir": NOTEBOOKS_DIR,
        "utilities_dir": UTILITIES_DIR,
        "data_dir": DATA_DIR,
        "input_dir": INPUT_DIR,
        "overture_data_dir": OVERTURE_DATA_DIR,
        "grid3_data_dir": GRID3_DATA_DIR,
        "scratch_dir": SCRATCH_DIR,
        "output_dir": OUTPUT_DIR,
        "tile_dir": TILE_DIR,
        "template_path": TILE_QUERIES_TEMPLATE,
        "tippecanoe_template": TIPPECANOE_TEMPLATE,
    },
    "extent": {
        # Now required: will raise if not provided
        "coordinates": (_west, _south, _east, _north),
        "buffer_degrees": _buffer_degrees
    },
    "download": {
        "verbose": True,
        "output_formats": ["*.parquet", "*.geojson", "*.geojsonseq"]
    },
    "fgb_conversion": {
        "enabled": True,
        "input_pattern": "*.parquet",
        "overwrite": False,  # Don't re-convert existing FGB files
        "verbose": True,
        "output_suffix": ".fgb",
        "cleanup_source": False  # Remove source .parquet files after successful conversion to save disk space
    },
    "conversion": {
        "input_patterns": ["*.parquet", "*.shp", "*.gpkg", "*.gdb", "*.sqlite", "*.db", "*.geojson", "*.json"],
        "output_suffix": ".geojsonseq",
        "reproject_crs": "EPSG:4326",
        "overwrite": True,
        "verbose": True
    },
    "tiling": {
        "input_dirs": [SCRATCH_DIR],  # Read FlatGeobuf files from scratch directory
        "output_dir": OUTPUT_DIR,
        "parallel": True,
        "overwrite": True,
        "verbose": True,
        "create_tilejson": True,
        "filter_pattern": "*.fgb"  # Prioritize FlatGeobuf files for optimal performance
    }
}


def ensure_directories():
    """Create all necessary directories if they don't exist."""
    directories = [
        DATA_DIR,
        INPUT_DIR,
        OVERTURE_DATA_DIR,
        GRID3_DATA_DIR,
        SCRATCH_DIR,
        OUTPUT_DIR,
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
    
    return True


def get_config():
    """
    Get a copy of the default configuration.
    Modifying the returned dict won't affect the default.
    """
    import copy
    return copy.deepcopy(DEFAULT_CONFIG)


def print_config_summary(config=None):
    """Print a summary of the current configuration."""
    if config is None:
        config = DEFAULT_CONFIG
    
    print("PROJECT CONFIGURATION")
    print("=" * 60)
    print(f"Project root:        {config['paths']['project_root']}")
    print(f"Scripts directory:   {config['paths']['scripts_dir']}")
    print(f"Notebooks directory: {config['paths']['notebooks_dir']}")
    print(f"Data directory:      {config['paths']['data_dir']}")
    print(f"Scratch directory:   {config['paths']['scratch_dir']}")
    print(f"Output directory:    {config['paths']['output_dir']}")
    print(f"Overture data:       {config['paths']['overture_data_dir']}")
    print(f"GRID3 data:         {config['paths']['grid3_data_dir']}")
    print()
    print(f"Processing extent:   {config['extent']['coordinates']}")
    print(f"Buffer degrees:      {config['extent']['buffer_degrees']}")
    
    extent = config['extent']['coordinates']
    area_deg2 = (extent[2] - extent[0]) * (extent[3] - extent[1])
    area_km2 = area_deg2 * 111 * 111  # Rough conversion
    print(f"Area:                {area_deg2:.4f} degree² (~{area_km2:.0f} km²)")
    print("=" * 60)


if __name__ == "__main__":
    # When run directly, print configuration and create directories
    ensure_directories()
    print_config_summary()
    print("\n✓ All directories created")
    print(f"\nTo use in your code:")
    print("  from config import get_config, SCRIPTS_DIR, OUTPUT_DIR")
    print("  config = get_config()")
