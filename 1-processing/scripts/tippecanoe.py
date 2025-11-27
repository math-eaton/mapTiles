"""
Tippecanoe configuration template for layer-specific settings.

Simple 1:1 mapping between layers and their optimized tippecanoe parameters.
Import this into runCreateTiles.py to get settings for each layer.

Usage:
    from tippecanoe import get_layer_settings
    settings = get_layer_settings('buildings.fgb')  # Automatically matches 'buildings.geojsonseq'
    
Note: get_layer_settings() matches on base filename, ignoring extensions.
      So 'buildings.fgb' will match 'buildings.geojsonseq' in LAYER_SETTINGS.

Shared Boundary Handling:
    Administrative layers (health_areas, health_zones, provinces) use:
    - --no-polygon-splitting: Keeps polygons intact across tile boundaries
    - --no-simplification-of-shared-nodes: Ensures shared boundaries are simplified 
      identically in adjacent features (replaces deprecated --detect-shared-borders)
    - --coalesce-densest-as-needed: Merges features while maintaining coverage
    
    This creates properly nested boundary polygons where adjacent administrative
    units share exact boundary coordinates, similar to TopoJSON topology.
"""

# Direct mapping of layer files to their tippecanoe settings
# Extension-agnostic: 'buildings.geojsonseq' will match 'buildings.fgb', 'buildings.geojson', etc.
LAYER_SETTINGS = {
    # Building footprints - high detail at close zooms
    'buildings.fgb': [
        '--no-polygon-splitting',
        '--detect-shared-borders',
        '--simplification=5',  # Increased from 4 for better tile sizes
        '--drop-rate=0.2',  # Increased from 0.05 to reduce features
        '--coalesce-smallest-as-needed',
        '--drop-densest-as-needed',
        '--extend-zooms-if-still-dropping-maximum=15',
        '--minimum-zoom=11',
    ],

    # Infrastructure polygons
    'infrastructure.fgb': [
        '--no-polygon-splitting',
        '--detect-shared-borders',
        '--simplification=8',  # Added for geometry simplification
        '--drop-rate=0.2',  # Increased from 0.1
        '--coalesce-densest-as-needed',
        '--drop-densest-as-needed',
        # '--minimum-zoom=9',  # Increased from 8 to reduce lower zoom tiles
        # '--maximum-zoom=13',  # Reduced from 15, supersample beyond
        # '--maximum-tile-bytes=2097152' 
    ],

    # Land use polygons 
    'land_use.fgb': [
        '--no-polygon-splitting',
        '--detect-shared-borders',
        '--simplification=4',  # Reduced from 10 for better detail preservation
        '--drop-rate=0.15',  # Reduced from 0.40 to keep more features
        '--low-detail=10',  # Increased from 8 to preserve detail at lower zooms
        '--full-detail=13',  # Increased from 12 for better detail at mid-zooms
        '--minimum-detail=10',  # Increased from 10
        '--extend-zooms-if-still-dropping-maximum=14',
        '--coalesce-densest-as-needed',
        '--drop-densest-as-needed',
    ],

    'land_cover.fgb': [
        '--no-polygon-splitting',
        '--detect-shared-borders',
        '--simplification=4',  # Reduced from 10 for better detail preservation
        '--drop-rate=0.2',  # Reduced from 0.40 to keep more features
        # '--full-detail=14',  # Increased from 12 for better detail at mid-zooms
        '--minimum-detail=11',  # Increased from 10
        # '--no-duplication',
        '--hilbert',
        '--coalesce-densest-as-needed',
        '--drop-densest-as-needed',
        '--extend-zooms-if-still-dropping-maximum=13',
    ],

    'land_residential.fgb': [
        '--no-polygon-splitting',
        '--detect-shared-borders',
        '--simplification=10',  # Added for geometry simplification
        '--drop-rate=0.2',  # Increased from 0.1
        '--coalesce-densest-as-needed',
        '--drop-densest-as-needed',
        '--extend-zooms-if-still-dropping-maximum=13',
    ],

    'land.fgb': [
        '--no-polygon-splitting',
        '--detect-shared-borders',
        '--simplification=4',  # Reduced from 10 for better detail preservation
        '--drop-rate=0.2',
        '--low-detail=9',  # Added to preserve detail at lower zooms
        '--full-detail=13',  # Added for better detail at mid-zooms
        '--minimum-detail=11',  # Added to ensure minimum detail level
        '--coalesce-densest-as-needed',
        '--drop-densest-as-needed',
    ],

    # Roads - linear features with line-specific optimizations
    'roads.fgb': [
        '--no-line-simplification',
        # '--drop-rate=0.15',
        # '--drop-smallest',
        '--simplification=1', 
        # '--minimum-detail=5',  # Added to ensure minimum detail level
        '--no-simplification-of-shared-nodes',
        # '--no-clipping',
        '--extend-zooms-if-still-dropping-maximum=15',
        '--coalesce-smallest-as-needed',
        # '--maximum-tile-bytes=4194304',  # Increased limit to 4MB for road density
        # '--drop-densest-as-needed',  # Drop densest features when tiles get too large
        '-j', '{"*":["any",[">=","$zoom",11],["!=","class","path"]]}',  # Exclude class=path below zoom 11
    ],

    # Water polygons - enhanced detail at zoom 13+
    'water.fgb': [
        # '--no-polygon-splitting',
        '--detect-shared-borders',
        '--simplification=1', 
        # '--drop-rate=0.15', 
        # '--extend-zooms-if-still-dropping-maximum=15',
        # '--no-clipping',
        '--hilbert',
        '--drop-densest-as-needed',
        '--no-simplification-of-shared-nodes',
        # '--maximum-tile-bytes=4194304',
        # '--minimum-zoom=7',
        '--buffer=8',
        '--maximum-zoom=13',
        # '-j', '{"*":["all",["any",[">=","$zoom",12],["!=","class","stream"]],["any",[">=","$zoom",10],["==","$type","Polygon"]]]}',  # Any streams below zoom 12, only polygons below zoom 10
    ],

    # # Point features - places and placenames
    # 'places.geojson': [
    #     '--cluster-distance=10',
    #     '--drop-rate=0.0',
    #     '--no-feature-limit',
    #     '--extend-zooms-if-still-dropping',
    #     # '--maximum-zoom=16'
    # ],

    # 'placenames.geojson': [
    #     '--cluster-distance=10',
    #     '--drop-rate=0.0',
    #     '--no-feature-limit',
    #     '--extend-zooms-if-still-dropping',
    #     # '--maximum-zoom=16'
    # ],

    # Administrative boundaries - health areas
    # Nested administrative polygons requiring shared boundary topology
    'health_areas.fgb': [
        '--no-polygon-splitting',  # Keep polygons intact across tile boundaries
        '--no-simplification-of-shared-nodes',  # Preserve shared boundaries identically
        '--simplification=1',
        '--low-detail=8',
        '--full-detail=12',
        '--coalesce-densest-as-needed',  # Merge features when needed, maintaining coverage
        '--extend-zooms-if-still-dropping-maximum=16',
        '--no-tiny-polygon-reduction',
    ],



    # Administrative boundaries - health zones
    # Higher-level nested administrative polygons
    'health_zones.fgb': [
        '--no-polygon-splitting',  # Keep polygons intact across tile boundaries
        '--no-simplification-of-shared-nodes',  # Preserve shared boundaries identically
        '--simplification=1', 
        '--low-detail=8',
        '--full-detail=12',
        '--coalesce-densest-as-needed',  # Merge features when needed, maintaining coverage
        '--extend-zooms-if-still-dropping-maximum=16',
        '--no-tiny-polygon-reduction',
    ],

    # Health zone centroids - point labels for interior placement
    # One point per health zone, guaranteed inside polygon
    'health_zones_centroids.fgb': [
        '--drop-rate=0.0',  # Never drop - one label per zone
        '--minimum-zoom=5',  # Start 2 levels earlier than areas (matches interior label config)
        '--maximum-zoom=16',
        '--no-feature-limit',  # Ensure all centroid points are included
        '--no-tile-size-limit',  # Small point layer, allow all features
    ],

    # Health area centroids - point labels for interior placement  
    # One point per health area, guaranteed inside polygon
    'health_areas_centroids.fgb': [
        '--drop-rate=0.0',  # Never drop - one label per area
        '--minimum-zoom=7',  # Start at overview level (matches interior label config)
        '--maximum-zoom=16',
        '--no-feature-limit',  # Ensure all centroid points are included
        '--no-tile-size-limit',  # Small point layer, allow all features
    ],

    # Water centerlines - linear features for labeling elongated water bodies
    # Medial axis lines through lakes, reservoirs, and other polygonal water features
    # Label-only layer: simplification is acceptable for text placement
    # 'water_centerlines.fgb': [
    #     '--simplification=4',  # Simplify geometry - labels don't need precise curves
    #     '--drop-rate=0.1',  # Drop smaller features at lower zooms
    #     '--minimum-zoom=10',  # Start showing centerline labels at mid-zoom
    #     '--maximum-zoom=16',
    #     # '--buffer=64',  # Large buffer to prevent label clipping
    #     '--drop-smallest-as-needed',  # Drop smallest water bodies when tiles too large
    #     '--coalesce-smallest-as-needed',  # Merge small nearby features
    #     '--extend-zooms-if-still-dropping-maximum=14'  # Ensure features appear eventually
    # ],

    # Settlement extents - very numerous small polygons
    # Heavily optimized for lower zoom levels due to high feature count
    # Filtered by type: Built-up Area (z10+), Small Settlement Area (z11+), Hamlet (z12+)
    'settlement_extents.fgb': [
        '--no-polygon-splitting',
        '--no-simplification-of-shared-nodes',
        '--simplification=5',  # Higher simplification for many small features
        '--drop-rate=0.2', 
        '--minimum-detail=8',
        '--coalesce-smallest-as-needed',  # Merge smallest settlements at low zooms
        '--drop-smallest-as-needed',  # Drop smallest when tiles too large
        '--gamma=1.4',  # Reduce density of clustered settlements
        '--extend-zooms-if-still-dropping-maximum=14',
        ],

    # Administrative boundaries - provinces (top-level admin units)
    # Large-scale administrative boundaries with strict topology preservation
    'provinces.fgb': [
        '--no-polygon-splitting',  # Essential for continuous coverage
        '--no-simplification-of-shared-nodes',  # Preserve provincial boundaries exactly
        '--simplification=10',  # Higher simplification for large-scale features
        '--drop-rate=0.0',  # Never drop provincial boundaries
        '--low-detail=6',
        '--full-detail=12',
        '--coalesce-densest-as-needed',  # Maintain full coverage
        '--extend-zooms-if-still-dropping',
        # '--maximum-zoom=12',
        # '--minimum-zoom=3',  # Visible from very low zoom levels
    ],
    
}

# Base tippecanoe command flags that apply to all layers
BASE_COMMAND = [
    # '--buffer=8',
    '-zg',
    '-Bg',
    '--drop-smallest',
    # '--maximum-tile-bytes=2097152',  # default for all layers
    '--preserve-input-order',
    '--coalesce-densest-as-needed',
    '--drop-fraction-as-needed',
    '--drop-densest-as-needed',  # Added for better tile size management
    '-P'  # Show progress
]

def get_layer_settings(filename):
    """
    Get tippecanoe settings for a specific layer file.
    
    Extension-agnostic but requires exact base name match.
    'buildings.fgb' will match 'buildings.geojsonseq' settings.
    'land.fgb' will NOT match 'land_residential.fgb' settings.
    
    Args:
        filename (str): Name of the layer file
        
    Returns:
        list: Tippecanoe command arguments for this layer
    """
    import os
    
    # Get base name without extension
    base_name = os.path.splitext(filename)[0]
    
    # Look for exact base name match in LAYER_SETTINGS
    for template_filename, settings in LAYER_SETTINGS.items():
        template_base = os.path.splitext(template_filename)[0]
        # Require exact match of base name (not partial/substring match)
        if base_name == template_base:
            return settings
    
    # No match found
    return []

def build_tippecanoe_command(input_file, output_file, layer_name, extent=None):
    """
    Build complete tippecanoe command for a layer.
    
    Args:
        input_file (str): Path to input GeoJSON/GeoJSONSeq file
        output_file (str): Path to output PMTiles file  
        layer_name (str): Layer name for the tiles
        extent (tuple): Optional bounding box (xmin, ymin, xmax, ymax)
        
    Returns:
        list: Complete command arguments for subprocess
    """
    import os
    
    filename = os.path.basename(input_file)
    
    # Start with base command
    cmd = ['tippecanoe', '-fo', output_file, '-l', layer_name] + BASE_COMMAND
    
    # Add layer-specific settings
    layer_settings = get_layer_settings(filename)
    cmd.extend(layer_settings)
    
    # Add extent clipping if provided
    if extent:
        xmin, ymin, xmax, ymax = extent
        cmd.extend(['--clip-bounding-box', f'{xmin},{ymin},{xmax},{ymax}'])
    
    # Add input file
    cmd.append(input_file)
    
    return cmd
