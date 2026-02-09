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
        '--hilbert',
        '-y', 'height'
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
        '-y', 'subtype'
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
        '--extend-zooms-if-still-dropping-maximum=14',
    ],

    'land_residential.fgb': [
        '--hilbert',
        '--no-polygon-splitting',
        '--detect-shared-borders',
        # '--simplification=5',  # Added for geometry simplification
        # '--drop-rate=0.2',  # Increased from 0.1
        '--coalesce-densest-as-needed',
        '--drop-densest-as-needed',
        '--extend-zooms-if-still-dropping-maximum=16',
        '-y', 'subtype'
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
        '--minimum-zoom=7',
        # '--maximum-zoom=15',
        '--buffer=16',
        '--hilbert',
        # '--no-line-simplification',
        # '--drop-smallest',
        # '--minimum-detail=5',  # Added to ensure minimum detail level
        '--no-simplification-of-shared-nodes',
        '--no-clipping',
        '--extend-zooms-if-still-dropping-maximum=16',
        '--coalesce-smallest-as-needed',
        # '--simplification=5',
        '-P',
        '-y', 'class',  
        '-y', 'subclass',
        '-y', 'subtype',
        # '--maximum-tile-bytes=4194304',  # Increased limit to 4MB for road density
        # '--drop-densest-as-needed',  # Drop densest features when tiles get too large
        '-j', '{"*":["any",[">=","$zoom",10],["!=","class","path"]]}',  # Exclude class=path below zoom 10
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
        '-j', '{"*":["all",["any",[">=","$zoom",12],["!=","class","stream"]],["any",[">=","$zoom",10],["==","$type","Polygon"]]]}',  # Any streams below zoom 12, only polygons below zoom 10
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
        '--extend-zooms-if-still-dropping-maximum=15',
        '--no-tiny-polygon-reduction',
        '-y', 'airesante',
        '-y', 'zonesante',
        '-y', 'province'
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
        '--extend-zooms-if-still-dropping-maximum=15',
        '--no-tiny-polygon-reduction',
        '-y', 'zonesante',
        '-y', 'province'
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
        '-y', 'type'
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

def extract_cartography_zoom_range(input_file):
    """
    Extract min_zoom and max_zoom from Overture cartography properties.
    
    Reads the first 1000 features from a FlatGeobuf/GeoJSON file and finds
    the min/max zoom levels. Supports both flattened columns (min_zoom, max_zoom)
    from DuckDB extraction or nested cartography struct.
    
    Also checks for sort_key and level properties which are useful for z-ordering
    in the rendering pipeline.
    
    Args:
        input_file (str): Path to input file (FlatGeobuf, GeoJSON, or GeoJSONSeq)
        
    Returns:
        tuple: (min_zoom, max_zoom) or (None, None) if not found
    """
    import os
    try:
        file_ext = os.path.splitext(input_file)[1].lower()
        
        if file_ext == '.fgb':
            # Use fiona for FlatGeobuf (streaming, efficient)
            try:
                import fiona
                min_zoom, max_zoom = None, None
                sample_count = 0
                max_samples = 1000
                
                with fiona.open(input_file, 'r') as src:
                    for feature in src:
                        if sample_count >= max_samples:
                            break
                        
                        props = feature.get('properties', {})
                        
                        # Try flattened columns first (from DuckDB extraction)
                        feat_min = props.get('min_zoom')
                        feat_max = props.get('max_zoom')
                        
                        # Fall back to nested cartography struct
                        if feat_min is None or feat_max is None:
                            cartography = props.get('cartography', {})
                            if isinstance(cartography, dict):
                                feat_min = feat_min or cartography.get('min_zoom')
                                feat_max = feat_max or cartography.get('max_zoom')
                        
                        if feat_min is not None:
                            min_zoom = feat_min if min_zoom is None else min(min_zoom, feat_min)
                        if feat_max is not None:
                            max_zoom = feat_max if max_zoom is None else max(max_zoom, feat_max)
                        
                        sample_count += 1
                
                return (min_zoom, max_zoom)
            except ImportError:
                pass  # Fall through to return None
        
        elif file_ext in ['.geojson', '.json', '.geojsonseq']:
            # Handle GeoJSON/GeoJSONSeq
            import json
            min_zoom, max_zoom = None, None
            sample_count = 0
            max_samples = 1000
            
            with open(input_file, 'r') as f:
                if file_ext == '.geojsonseq':
                    # Line-delimited GeoJSON
                    for line in f:
                        if sample_count >= max_samples:
                            break
                        try:
                            feature = json.loads(line.strip())
                            props = feature.get('properties', {})
                            
                            # Try flattened columns first
                            feat_min = props.get('min_zoom')
                            feat_max = props.get('max_zoom')
                            
                            # Fall back to nested cartography struct
                            if feat_min is None or feat_max is None:
                                cartography = props.get('cartography', {})
                                if isinstance(cartography, dict):
                                    feat_min = feat_min or cartography.get('min_zoom')
                                    feat_max = feat_max or cartography.get('max_zoom')
                            
                            if feat_min is not None:
                                min_zoom = feat_min if min_zoom is None else min(min_zoom, feat_min)
                            if feat_max is not None:
                                max_zoom = feat_max if max_zoom is None else max(max_zoom, feat_max)
                            
                            sample_count += 1
                        except json.JSONDecodeError:
                            continue
                else:
                    # Standard GeoJSON
                    data = json.load(f)
                    features = data.get('features', [])
                    
                    for feature in features[:max_samples]:
                        props = feature.get('properties', {})
                        
                        # Try flattened columns first
                        feat_min = props.get('min_zoom')
                        feat_max = props.get('max_zoom')
                        
                        # Fall back to nested cartography struct
                        if feat_min is None or feat_max is None:
                            cartography = props.get('cartography', {})
                            if isinstance(cartography, dict):
                                feat_min = feat_min or cartography.get('min_zoom')
                                feat_max = feat_max or cartography.get('max_zoom')
                        
                        if feat_min is not None:
                            min_zoom = feat_min if min_zoom is None else min(min_zoom, feat_min)
                        if feat_max is not None:
                            max_zoom = feat_max if max_zoom is None else max(max_zoom, feat_max)
            
            return (min_zoom, max_zoom)
    
    except Exception as e:
        # Silently fail - not all layers have cartography properties
        pass
    
    return (None, None)


def build_tippecanoe_command(input_file, output_file, layer_name, extent=None, use_overture_zooms=True):
    """
    Build complete tippecanoe command for a layer.
    
    Automatically extracts cartography properties (min_zoom, max_zoom, sort_key) and level
    from Overture Maps data when available. These properties are used for:
    - min_zoom/max_zoom: Optimal zoom range for each feature
    - sort_key: Z-ordering/drawing priority within tiles
    - level: Vertical level for multi-level features (buildings, infrastructure)
    
    All these properties are preserved in the output PMTiles for use by the map renderer.
    
    Args:
        input_file (str): Path to input GeoJSON/GeoJSONSeq file
        output_file (str): Path to output PMTiles file  
        layer_name (str): Layer name for the tiles
        extent (tuple): Optional bounding box (xmin, ymin, xmax, ymax)
        use_overture_zooms (bool): If True, extract and use zoom levels from Overture cartography properties
        
    Returns:
        list: Complete command arguments for subprocess
    """
    import os
    
    # Convert Windows paths to WSL format if needed
    def to_wsl_path(path):
        if len(path) >= 2 and path[1] == ':':
            drive = path[0].lower()
            rest = path[2:].replace('\\', '/')
            return f'/mnt/{drive}{rest}'
        return path
    
    input_file = to_wsl_path(input_file)
    output_file = to_wsl_path(output_file)
    
    filename = os.path.basename(input_file)
    
    # Start with base command
    cmd = ['tippecanoe', '-fo', output_file, '-l', layer_name] + BASE_COMMAND
    
    # Add layer-specific settings
    layer_settings = get_layer_settings(filename)
    cmd.extend(layer_settings)
    
    # Try to extract and apply Overture cartography zoom levels
    if use_overture_zooms:
        min_zoom, max_zoom = extract_cartography_zoom_range(input_file)
        
        if min_zoom is not None or max_zoom is not None:
            # Check if layer_settings already has zoom constraints
            has_min_zoom = any('--minimum-zoom' in str(s) for s in layer_settings)
            has_max_zoom = any('--maximum-zoom' in str(s) for s in layer_settings)
            
            # Apply Overture zoom levels if not already constrained by layer settings
            if min_zoom is not None and not has_min_zoom:
                cmd.extend(['--minimum-zoom', str(min_zoom)])
                print(f"  ℹ Using Overture min_zoom={min_zoom} for {layer_name}")
            
            if max_zoom is not None and not has_max_zoom:
                cmd.extend(['--maximum-zoom', str(max_zoom)])
                print(f"  ℹ Using Overture max_zoom={max_zoom} for {layer_name}")
    
    # Add extent clipping if provided
    if extent:
        xmin, ymin, xmax, ymax = extent
        cmd.extend(['--clip-bounding-box', f'{xmin},{ymin},{xmax},{ymax}'])
    
    # Add input file
    cmd.append(input_file)
    
    return cmd
