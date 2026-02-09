"""
Generate polygon centroids and centerlines for label positioning.

Creates point features at the interior centroid of each polygon with optimal
rotation angles for text labels, and centerline features along the medial axis 
of polygons. Preserves all attributes. Useful for placing labels on 
administrative boundaries and water features.

Usage:
    python generateLabels.py input.fgb output_centroids.fgb
    python generateLabels.py --centerlines water.fgb water_centerlines.fgb
    
Features:
    - Centroids: Uses true interior centroids (guaranteed inside polygon)
    - Label rotation: Calculates optimal text angle from minimum rotated rectangle
    - Centerlines: Generates medial axis skeleton from polygon boundaries
    - Preserves all original attributes
    - Handles multipolygons correctly
    - Fast processing with spatial indexing
"""

import sys
from pathlib import Path
import geopandas as gpd
from shapely.geometry import Point, LineString, MultiLineString
from shapely.validation import make_valid
from shapely.ops import linemerge, unary_union, snap
import numpy as np
import warnings
# warnings.filterwarnings('ignore')

try:
    from centerline.geometry import Centerline
    CENTERLINE_AVAILABLE = True
except ImportError:
    CENTERLINE_AVAILABLE = False
    warnings.warn("centerline library not available. Install with: pip install centerline")

def _work_crs_gdf(gdf):
    """
    Reproject GeoDataFrame to a planar CRS (meters) if needed.
    Returns (reprojected_gdf, original_crs) or (gdf, None) if already planar.
    """
    source_crs = gdf.crs
    
    # Check if already in a projected CRS (units in meters)
    if source_crs and source_crs.is_projected:
        # Already planar
        return gdf, None
    
    # Need to reproject to a suitable UTM or local projection
    # Use the centroid to pick an appropriate UTM zone
    centroid = gdf.geometry.unary_union.centroid
    lon, lat = centroid.x, centroid.y
    
    # Calculate UTM zone from longitude
    utm_zone = int((lon + 180) / 6) + 1
    hemisphere = 'north' if lat >= 0 else 'south'
    
    # Create EPSG code for UTM zone
    # Northern hemisphere: 32600 + zone, Southern: 32700 + zone
    epsg_code = 32600 + utm_zone if hemisphere == 'north' else 32700 + utm_zone
    
    try:
        gdf_proj = gdf.to_crs(epsg=epsg_code)
        return gdf_proj, source_crs
    except Exception as e:
        warnings.warn(f"Failed to reproject to UTM: {e}. Using original CRS.")
        return gdf, None

def generate_centroids(input_file, output_file, verbose=True):
    """
    Generate centroid points from polygon features with optimal rotation angles.
    
    Creates point features at the interior centroid of each polygon and calculates
    the optimal rotation angle for text labels based on the polygon's oriented
    bounding box (minimum rotated rectangle). This ensures labels align with the
    natural orientation of elongated or diagonal polygons.
    
    Args:
        input_file (str/Path): Input polygon file (FlatGeobuf, GeoJSON, etc.)
        output_file (str/Path): Output point file for centroids
        verbose (bool): Print progress messages
        
    Returns:
        dict: Results with success status and feature count
        
    Output attributes:
        - All original polygon attributes are preserved
        - label_rotation (float): Optimal rotation angle in degrees (-90 to 90)
          Calculated from the minimum rotated rectangle's longest edge
    """
    input_path = Path(input_file)
    output_path = Path(output_file)
    
    if verbose:
        print(f"Generating centroids from: {input_path.name}")
    
    try:
        # Read polygon file
        if verbose:
            print("  Reading polygons...")
        gdf = gpd.read_file(input_path)
        original_count = len(gdf)
        
        if verbose:
            print(f"  Found {original_count:,} features")
        
        # Filter to polygon geometries only
        gdf = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]
        polygon_count = len(gdf)
        
        if polygon_count == 0:
            return {
                'success': False,
                'error': 'No polygon features found in input file',
                'feature_count': 0
            }
        
        if polygon_count < original_count and verbose:
            print(f"  Filtered to {polygon_count:,} polygon features")
        
        # Generate representative points (guaranteed to be inside polygon)
        # This is better than geometric centroid which can fall outside complex polygons
        if verbose:
            print("  Computing interior centroids with rotation angles...")
        
        # Calculate rotation angle for best-fit orientation
        def calculate_rotation_angle(geom):
            """Calculate the optimal rotation angle for a polygon using minimum rotated rectangle."""
            try:
                # Get the minimum rotated rectangle (oriented bounding box)
                min_rect = geom.minimum_rotated_rectangle
                
                # Get coordinates of the rectangle
                coords = list(min_rect.exterior.coords)
                
                # Calculate angle between first two points (longest edge)
                dx = coords[1][0] - coords[0][0]
                dy = coords[1][1] - coords[0][1]
                
                # Calculate lengths of both edges
                edge1_len = np.sqrt(dx**2 + dy**2)
                dx2 = coords[2][0] - coords[1][0]
                dy2 = coords[2][1] - coords[1][1]
                edge2_len = np.sqrt(dx2**2 + dy2**2)
                
                # Use the longer edge for rotation calculation
                if edge2_len > edge1_len:
                    dx, dy = dx2, dy2
                
                # Calculate angle in degrees (counterclockwise from east)
                angle = np.degrees(np.arctan2(dy, dx))
                
                # Normalize to [-90, 90] range for text rotation
                # This ensures text is never upside down
                while angle > 90:
                    angle -= 180
                while angle < -90:
                    angle += 180
                
                return angle
            except Exception:
                return 0.0  # Default to no rotation if calculation fails
        
        # Store original geometries for rotation calculation
        original_geoms = gdf.geometry.copy()
        
        # Generate centroids
        gdf['geometry'] = gdf.geometry.representative_point()
        
        # Calculate rotation angles based on original polygon orientation
        if verbose:
            print("  Calculating optimal rotation angles...")
        gdf['label_rotation'] = original_geoms.apply(calculate_rotation_angle)
        
        # Verify all geometries are now points
        assert all(gdf.geometry.type == 'Point'), "Failed to convert all features to points"
        
        # Create output directory if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write to file
        if verbose:
            print(f"  Writing centroids to: {output_path.name}")
        
        # Use FlatGeobuf driver for .fgb files, otherwise auto-detect
        if output_path.suffix.lower() == '.fgb':
            gdf.to_file(output_path, driver='FlatGeobuf')
        else:
            gdf.to_file(output_path)
        
        if verbose:
            print(f"✓ Generated {len(gdf):,} centroid points")
        
        return {
            'success': True,
            'feature_count': len(gdf),
            'input_file': str(input_path),
            'output_file': str(output_path)
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'feature_count': 0,
            'input_file': str(input_path)
        }

def generate_centerlines(input_file, output_file, simplify_tolerance=5.0, 
                        border_density=300.0, verbose=True):
    """
    Generate centerline features from polygon features using medial axis.
    
    Creates linear features along the approximate center of each polygon,
    useful for labeling elongated water features like lakes and rivers.
    
    Uses the centerline library for accurate medial axis computation.
    Automatically reprojects to planar CRS (meters) for accurate computation.
    
    Args:
        input_file (str/Path): Input polygon file (FlatGeobuf, GeoJSON, etc.)
        output_file (str/Path): Output line file for centerlines
        simplify_tolerance (float): Simplification tolerance in METERS after reprojection (default 5.0)
        border_density (float): Border point density factor (default 300). 
                               Higher = more boundary points = better accuracy for winding rivers.
                               Range: 100 (low detail) to 500+ (high detail).
        verbose (bool): Print progress messages
        
    Returns:
        dict: Results with success status and feature count
    """
    input_path = Path(input_file)
    output_path = Path(output_file)
    
    if verbose:
        print(f"Generating centerlines from: {input_path.name}")
        print("  Expecting planar units (meters). Will reproject if needed.")
    
    try:
        # Read polygon file
        if verbose:
            print("  Reading polygons...")
        gdf = gpd.read_file(input_path)
        original_count = len(gdf)
        
        if verbose:
            print(f"  Found {original_count:,} features")
        
        # Reproject to planar CRS if needed
        gdf, source_crs = _work_crs_gdf(gdf)
        
        # Filter to polygon geometries only
        gdf = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]
        polygon_count = len(gdf)
        
        if polygon_count == 0:
            return {
                'success': False,
                'error': 'No polygon features found in input file',
                'feature_count': 0
            }
        
        if polygon_count < original_count and verbose:
            print(f"  Filtered to {polygon_count:,} polygon features")
        
        # Require centerline library (no fallback pipeline)
        if not CENTERLINE_AVAILABLE:
            return {
                'success': False,
                'error': 'centerline library is required for centerline generation',
                'feature_count': 0
            }

        if verbose:
            print("  Computing polygon centerlines...")

        centerlines = []
        for idx, row in gdf.iterrows():
            try:
                geom = row.geometry

                # Handle MultiPolygon by processing largest polygon
                if geom.geom_type == 'MultiPolygon':
                    geom = max(geom.geoms, key=lambda g: g.area)

                # Fix only if invalid
                if not geom.is_valid:
                    geom = make_valid(geom)

                # (optional) remove tiny holes if water polygons are noisy
                # import pygeoops as pgo
                # geom = pgo.remove_inner_rings(geom, min_area_to_keep=50.0)

                # Simplify if requested (in meters)
                if simplify_tolerance and simplify_tolerance > 0:
                    geom = geom.simplify(simplify_tolerance, preserve_topology=True)

                # Skip unhelpful shapes (too narrow or circular)
                bounds = geom.bounds
                w = bounds[2] - bounds[0]
                h = bounds[3] - bounds[1]
                short = min(w, h)
                if short < 5.0:
                    continue
                aspect = (max(w, h) / max(1e-9, short))
                if aspect < 1.4:
                    continue

                # Skip very small polygons (area in square meters)
                if geom.area < 1.0 or len(list(geom.exterior.coords)) < 4:
                    continue

                # Compute spacing from a point budget
                perimeter = geom.length
                area = geom.area
                complexity = perimeter / (4 * (area ** 0.5)) if area > 0 else 1.0
                complexity = min(2.0, max(0.6, complexity))

                BASE_PTS = int(50 * border_density) if border_density else 1000
                BASE_PTS = max(400, min(2000, BASE_PTS))
                target_pts = int(BASE_PTS * complexity)
                target_pts = max(600, min(2000, target_pts))

                interpolation = perimeter / target_pts
                interpolation = max(1.0, min(interpolation, 50.0))
                est_pts = max(4, int(perimeter / interpolation))
                if est_pts > 10000:
                    interpolation = max(interpolation, perimeter / 10000.0)

                # Build centerline
                try:
                    raw = Centerline(geom, interpolation_distance=interpolation).geometry
                except Exception as e:
                    if verbose:
                        print(f"  Centerline error (feature {idx}), skipping: {e}")
                    continue

                if raw.is_empty:
                    continue

                # Stitch ridges (cheap first)
                try:
                    snapped = snap(raw, raw, interpolation * 1.25)
                    merged = linemerge(snapped)
                    if merged.is_empty:
                        merged = linemerge(unary_union(snapped))  # expensive; only if needed
                    centerline = merged if not merged.is_empty else raw
                except Exception:
                    centerline = raw

                if centerline.is_empty:
                    continue

                # Normalize to a LineString (take longest segment for MultiLineString)
                if centerline.geom_type == 'MultiLineString':
                    centerline = max(centerline.geoms, key=lambda g: g.length)
                elif centerline.geom_type == 'GeometryCollection':
                    lines = [g for g in centerline.geoms if g.geom_type in ('LineString', 'MultiLineString')]
                    if not lines:
                        continue
                    if lines[0].geom_type == 'MultiLineString':
                        lines = [l for ml in lines for l in ml.geoms]
                    centerline = max(lines, key=lambda g: g.length)

                centerline_row = row.copy()
                centerline_row['geometry'] = centerline
                centerlines.append(centerline_row)

            except Exception as e:
                if verbose:
                    print(f"  Skipped feature {idx}: {e}")
                continue
        
        if not centerlines:
            return {
                'success': False,
                'error': 'No centerlines could be generated',
                'feature_count': 0
            }
        
        # Create GeoDataFrame from centerlines
        centerline_gdf = gpd.GeoDataFrame(centerlines, crs=gdf.crs)
        
        # Restore original CRS if we reprojected
        if source_crs is not None and centerline_gdf.crs != source_crs:
            if verbose:
                print("  Reprojecting back to original CRS...")
            centerline_gdf = centerline_gdf.to_crs(source_crs)
        
        # Verify all geometries are lines
        line_types = centerline_gdf.geometry.type.unique()
        if not all(lt in ('LineString', 'MultiLineString') for lt in line_types):
            if verbose:
                print(f"  Warning: Unexpected geometry types: {line_types}")
        
        # Create output directory if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write to file
        if verbose:
            print(f"  Writing centerlines to: {output_path.name}")
        
        # Use FlatGeobuf driver for .fgb files, otherwise auto-detect
        if output_path.suffix.lower() == '.fgb':
            centerline_gdf.to_file(output_path, driver='FlatGeobuf')
        else:
            centerline_gdf.to_file(output_path)
        
        if verbose:
            print(f"✓ Generated {len(centerline_gdf):,} centerline features")
        
        return {
            'success': True,
            'feature_count': len(centerline_gdf),
            'input_file': str(input_path),
            'output_file': str(output_path)
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'feature_count': 0,
            'input_file': str(input_path)
        }


def _create_fallback_centerline(geom):
    """
    Create a simple fallback centerline when centerline library is unavailable.
    
    Args:
        geom: Shapely Polygon geometry
        
    Returns:
        LineString or MultiLineString centerline geometry
    """
    # Simple fallback: create centerline from centroid to longest axis
    centroid = geom.centroid
    bounds = geom.bounds
    width = bounds[2] - bounds[0]
    height = bounds[3] - bounds[1]
    
    if width > height:
        # Horizontal centerline
        centerline = LineString([
            (bounds[0], centroid.y),
            (bounds[2], centroid.y)
        ])
    else:
        # Vertical centerline
        centerline = LineString([
            (centroid.x, bounds[1]),
            (centroid.x, bounds[3])
        ])
    
    # Clip to polygon interior
    centerline = centerline.intersection(geom)
    
    return centerline

def batch_generate_centerlines(input_dir, output_dir, layers=None, suffix='_centerlines', 
                               simplify_tolerance=5.0, border_density=300.0, verbose=True):
    """
    Generate centerlines for multiple polygon layers.
    
    Args:
        input_dir (str/Path): Directory containing input polygon files
        output_dir (str/Path): Directory for output centerline files
        layers (list): List of layer names to process (without extension)
                      If None, processes all .fgb files
        suffix (str): Suffix to append to output filenames
        simplify_tolerance (float): Simplification tolerance in METERS (default 5.0)
        border_density (float): Border point density factor (default 300).
                               Higher values = more boundary points = better accuracy.
                               Try 400-500 for very winding rivers.
        verbose (bool): Print progress messages
        
    Returns:
        dict: Results with counts and any errors
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    # Create output directory
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Determine which files to process
    if layers:
        input_files = [input_path / f"{layer}.fgb" for layer in layers]
        input_files = [f for f in input_files if f.exists()]
    else:
        input_files = list(input_path.glob("*.fgb"))
    
    if verbose:
        print(f"Processing {len(input_files)} polygon layers for centerlines\n")
    
    results = {
        'total_layers': len(input_files),
        'successful': 0,
        'failed': 0,
        'layers': []
    }
    
    for input_file in input_files:
        output_file = output_path / f"{input_file.stem}{suffix}.fgb"
        
        result = generate_centerlines(input_file, output_file, 
                                      simplify_tolerance=simplify_tolerance,
                                      border_density=border_density,
                                      verbose=verbose)
        results['layers'].append(result)
        
        if result['success']:
            results['successful'] += 1
        else:
            results['failed'] += 1
            if verbose:
                print(f"✗ Error: {result.get('error', 'Unknown error')}")
        
        if verbose:
            print()  # Blank line between layers
    
    return results


def batch_generate_centroids(input_dir, output_dir, layers=None, suffix='_centroids', verbose=True):
    """
    Generate centroids for multiple polygon layers.
    
    Args:
        input_dir (str/Path): Directory containing input polygon files
        output_dir (str/Path): Directory for output centroid files
        layers (list): List of layer names to process (without extension)
                      If None, processes all .fgb files
        suffix (str): Suffix to append to output filenames
        verbose (bool): Print progress messages
        
    Returns:
        dict: Results with counts and any errors
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    # Create output directory
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Determine which files to process
    if layers:
        input_files = [input_path / f"{layer}.fgb" for layer in layers]
        input_files = [f for f in input_files if f.exists()]
    else:
        input_files = list(input_path.glob("*.fgb"))
    
    if verbose:
        print(f"Processing {len(input_files)} polygon layers for centroids\n")
    
    results = {
        'total_layers': len(input_files),
        'successful': 0,
        'failed': 0,
        'layers': []
    }
    
    for input_file in input_files:
        output_file = output_path / f"{input_file.stem}{suffix}.fgb"
        
        result = generate_centroids(input_file, output_file, verbose=verbose)
        results['layers'].append(result)
        
        if result['success']:
            results['successful'] += 1
        else:
            results['failed'] += 1
            if verbose:
                print(f"✗ Error: {result.get('error', 'Unknown error')}")
        
        if verbose:
            print()  # Blank line between layers
    
    return results

def main():
    """Command-line interface for centroid and centerline generation."""
    if len(sys.argv) < 3:
        print("Usage:")
        print("  Centroids:   python generateLabels.py <input_polygons.fgb> <output_centroids.fgb>")
        print("  Centerlines: python generateLabels.py --centerlines <input_polygons.fgb> <output_centerlines.fgb>")
        print("\nExamples:")
        print("  python generateLabels.py health_zones.fgb health_zones_centroids.fgb")
        print("  python generateLabels.py --centerlines water.fgb water_centerlines.fgb")
        sys.exit(1)
    
    # Check for --centerlines flag
    if sys.argv[1] == '--centerlines':
        if len(sys.argv) < 4:
            print("Error: --centerlines requires input and output file arguments")
            sys.exit(1)
        
        input_file = sys.argv[2]
        output_file = sys.argv[3]
        simplify = float(sys.argv[4]) if len(sys.argv) > 4 else 0.0001
        
        result = generate_centerlines(input_file, output_file, 
                                      simplify_tolerance=simplify,
                                      verbose=True)
        
        if not result['success']:
            print(f"\n✗ Failed: {result['error']}")
            sys.exit(1)
        
        print(f"\n✓ Success: {result['feature_count']:,} centerlines generated")
    else:
        # Default: generate centroids
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        
        result = generate_centroids(input_file, output_file, verbose=True)
        
        if not result['success']:
            print(f"\n✗ Failed: {result['error']}")
            sys.exit(1)
        
        print(f"\n✓ Success: {result['feature_count']:,} centroids generated")

if __name__ == '__main__':
    main()
