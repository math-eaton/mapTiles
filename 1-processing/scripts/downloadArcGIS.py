"""
Download and process ArcGIS Feature Server data.

This module provides functionality to download geospatial data from ArcGIS REST API endpoints
and convert them to formats suitable for tile generation (GeoJSON or FlatGeobuf).

Features:
- Automatic pagination for large datasets (handles >1000 feature limit)
- Smart fallback to spatial chunking for non-paginated services
- Spatial filtering by bounding box
- Direct GeoJSON download or conversion to FlatGeobuf
- Progress tracking for large downloads
- Robust error handling and retry logic
- Connection testing and diagnostics

Spatial Chunking Strategy:
For services that don't support pagination (e.g., older ArcGIS servers), the module
automatically detects this limitation and switches to spatial chunking. This divides
the bounding box into smaller chunks, downloads each chunk separately, and deduplicates
features that appear in multiple chunks. This ensures complete downloads even from
non-paginated services with large datasets.

Example ArcGIS Feature Server URLs:
- https://services3.arcgis.com/BU6Aadhn6tbBEdyk/arcgis/rest/services/GRID3_COD_Settlement_Extents_v3_1/FeatureServer/0
- https://services3.arcgis.com/BU6Aadhn6tbBEdyk/arcgis/rest/services/GRID3_COD_health_zones_v7_0/FeatureServer/0

Usage:
    from scripts import download_arcgis_data, test_service_connection
    
    # Test connection first
    test_service_connection(service_url)
    
    # Download with extent filtering (automatically handles pagination or chunking)
    result = download_arcgis_data(
        service_url="https://services3.arcgis.com/.../FeatureServer/0",
        output_path="data/health_zones.geojson",
        extent=(27.0, -8.0, 30.5, -2.0),  # (lon_min, lat_min, lon_max, lat_max)
        output_format="fgb"  # or "geojson"
    )
"""

import requests
import json
from pathlib import Path
from typing import Optional, Tuple, Dict, Any
from tqdm import tqdm
import geopandas as gpd
from shapely.geometry import shape
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing


def test_service_connection(service_url: str, verbose: bool = True) -> Dict[str, Any]:
    """
    Test connection to ArcGIS Feature Server and retrieve service metadata.
    
    This is useful for diagnosing connection issues before attempting downloads.
    
    Args:
        service_url: Base URL of the Feature Server endpoint
        verbose: Print diagnostic information
        
    Returns:
        Dictionary with connection test results
    """
    base_url = service_url.rstrip('/query').rstrip('/')
    
    if verbose:
        print(f"\n[TEST] Testing connection to: {base_url}")
    
    result = {
        'accessible': False,
        'service_url': base_url,
        'response_time_ms': None,
        'error': None,
        'metadata': {}
    }
    
    try:
        # Test 1: Get service metadata (not /query endpoint)
        start_time = time.time()
        response = requests.get(f"{base_url}?f=json", timeout=10)
        response_time = (time.time() - start_time) * 1000  # Convert to ms
        
        result['response_time_ms'] = round(response_time, 2)
        
        if verbose:
            print(f"[TEST] Response status: {response.status_code}")
            print(f"[TEST] Response time: {result['response_time_ms']} ms")
        
        if response.status_code == 403:
            result['error'] = "Access forbidden (HTTP 403) - May require authentication or service is restricted"
            if verbose:
                print(f"[ERROR] {result['error']}")
            return result
        elif response.status_code == 404:
            result['error'] = "Service not found (HTTP 404) - Check URL"
            if verbose:
                print(f"[ERROR] {result['error']}")
            return result
        elif response.status_code == 503:
            result['error'] = "Service unavailable (HTTP 503) - Server may be down or overloaded"
            if verbose:
                print(f"[ERROR] {result['error']}")
            return result
        
        response.raise_for_status()
        
        data = response.json()
        
        if 'error' in data:
            result['error'] = f"Service error: {data['error']}"
            if verbose:
                print(f"[ERROR] {result['error']}")
            return result
        
        # Extract metadata
        result['accessible'] = True
        result['metadata'] = {
            'name': data.get('name', 'Unknown'),
            'type': data.get('type', 'Unknown'),
            'geometryType': data.get('geometryType', 'Unknown'),
            'maxRecordCount': data.get('maxRecordCount', 'Unknown'),
            'supportsPagination': data.get('supportsPagination', False),
            'supportsStatistics': data.get('supportsStatistics', False),
            'capabilities': data.get('capabilities', 'Unknown')
        }
        
        if verbose:
            print(f"[TEST] ✓ Service accessible")
            print(f"[TEST] Service name: {result['metadata']['name']}")
            print(f"[TEST] Geometry type: {result['metadata']['geometryType']}")
            print(f"[TEST] Max record count: {result['metadata']['maxRecordCount']}")
            print(f"[TEST] Supports pagination: {result['metadata']['supportsPagination']}")
            
            if not result['metadata']['supportsPagination']:
                print(f"[WARNING] Service does not support pagination - downloads may be limited")
        
        return result
        
    except requests.exceptions.Timeout:
        result['error'] = "Connection timeout (10 seconds)"
        if verbose:
            print(f"[ERROR] {result['error']}")
    except requests.exceptions.ConnectionError as e:
        result['error'] = f"Connection error: {str(e)}"
        if verbose:
            print(f"[ERROR] {result['error']}")
    except Exception as e:
        result['error'] = f"Unexpected error: {str(e)}"
        if verbose:
            print(f"[ERROR] {result['error']}")
    
    return result


def parse_arcgis_url(url: str) -> Tuple[str, Dict[str, str]]:
    """
    Parse ArcGIS Feature Server URL and extract base URL and query parameters.
    
    Args:
        url: Full ArcGIS Feature Server URL (may include query parameters)
        
    Returns:
        Tuple of (base_url, query_params_dict)
        
    Example:
        >>> parse_arcgis_url("https://services3.arcgis.com/.../FeatureServer/0/query?where=1=1&f=geojson")
        ("https://services3.arcgis.com/.../FeatureServer/0/query", {"where": "1=1", "f": "geojson"})
    """
    from urllib.parse import urlparse, parse_qs
    
    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    
    # Ensure base URL ends with /query
    if not base_url.endswith('/query'):
        base_url = base_url.rstrip('/') + '/query'
    
    # Parse query parameters
    query_params = {k: v[0] if len(v) == 1 else v for k, v in parse_qs(parsed.query).items()}
    
    return base_url, query_params


def get_feature_count(service_url: str, where_clause: str = "1=1", extent: Optional[Tuple[float, float, float, float]] = None, verbose: bool = False, timeout: int = 120) -> int:
    """
    Get total feature count from ArcGIS Feature Server.
    
    Args:
        service_url: Base URL of the Feature Server endpoint
        where_clause: SQL where clause for filtering
        extent: Optional bounding box (lon_min, lat_min, lon_max, lat_max)
        verbose: Enable detailed diagnostic logging
        timeout: Request timeout in seconds (default: 120 for large datasets)
        
    Returns:
        Total number of features matching the query
    """
    base_url, _ = parse_arcgis_url(service_url)
    
    params = {
        'where': where_clause,
        'returnCountOnly': 'true',
        'f': 'json'
    }
    
    if extent:
        lon_min, lat_min, lon_max, lat_max = extent
        params['geometry'] = f"{lon_min},{lat_min},{lon_max},{lat_max}"
        params['geometryType'] = 'esriGeometryEnvelope'
        params['spatialRel'] = 'esriSpatialRelIntersects'
        params['inSR'] = '4326'
    
    if verbose:
        print(f"\n[DEBUG] Requesting feature count from: {base_url}")
        print(f"[DEBUG] Request parameters: {json.dumps(params, indent=2)}")
    
    try:
        response = requests.get(base_url, params=params, timeout=timeout)
        
        if verbose:
            print(f"[DEBUG] Response status code: {response.status_code}")
            print(f"[DEBUG] Response headers: {dict(response.headers)}")
            
            # Check for rate limiting headers
            if 'X-RateLimit-Limit' in response.headers:
                print(f"[DEBUG] Rate limit: {response.headers['X-RateLimit-Limit']}")
            if 'X-RateLimit-Remaining' in response.headers:
                print(f"[DEBUG] Rate limit remaining: {response.headers['X-RateLimit-Remaining']}")
            if 'Retry-After' in response.headers:
                print(f"[WARNING] Rate limited! Retry after: {response.headers['Retry-After']} seconds")
        
        response.raise_for_status()
        
        data = response.json()
        
        if verbose:
            print(f"[DEBUG] Response JSON keys: {list(data.keys())}")
        
        # Check for ArcGIS API errors
        if 'error' in data:
            error_msg = f"ArcGIS API Error: {data['error']}"
            if verbose:
                print(f"[ERROR] {error_msg}")
                print(f"[ERROR] Full error response: {json.dumps(data['error'], indent=2)}")
            raise Exception(error_msg)
        
        count = data.get('count', 0)
        
        if verbose:
            print(f"[DEBUG] Feature count returned: {count}")
        
        return count
        
    except requests.exceptions.HTTPError as e:
        if verbose:
            print(f"[ERROR] HTTP Error: {e}")
            print(f"[ERROR] Response content: {response.text[:500]}")
        raise
    except requests.exceptions.Timeout as e:
        if verbose:
            print(f"[ERROR] Request timeout after {timeout} seconds")
            print(f"[ERROR] Failed to get feature count: {e}")
        raise
    except Exception as e:
        if verbose:
            print(f"[ERROR] Unexpected error: {e}")
        raise


def create_spatial_chunks(extent: Tuple[float, float, float, float], num_chunks: int) -> list:
    """
    Divide a bounding box into smaller chunks for spatial downloading.
    
    This is useful for services that don't support pagination but have large datasets.
    By dividing the extent into smaller chunks, we can stay under the max record count.
    
    Args:
        extent: Bounding box (lon_min, lat_min, lon_max, lat_max)
        num_chunks: Number of chunks to create (will create a grid)
        
    Returns:
        List of chunk extents, each as (lon_min, lat_min, lon_max, lat_max)
    """
    lon_min, lat_min, lon_max, lat_max = extent
    
    # Calculate grid dimensions (try to make it square-ish)
    grid_size = int(num_chunks ** 0.5)
    if grid_size * grid_size < num_chunks:
        grid_size += 1
    
    lon_step = (lon_max - lon_min) / grid_size
    lat_step = (lat_max - lat_min) / grid_size
    
    chunks = []
    for i in range(grid_size):
        for j in range(grid_size):
            chunk_lon_min = lon_min + (i * lon_step)
            chunk_lon_max = lon_min + ((i + 1) * lon_step)
            chunk_lat_min = lat_min + (j * lat_step)
            chunk_lat_max = lat_min + ((j + 1) * lat_step)
            
            chunks.append((chunk_lon_min, chunk_lat_min, chunk_lon_max, chunk_lat_max))
    
    return chunks


def download_with_spatial_chunking(
    service_url: str,
    where_clause: str,
    extent: Tuple[float, float, float, float],
    max_record_count: int,
    verbose: bool = True
) -> list:
    """
    Download features using spatial chunking for non-paginated services.
    
    For services that don't support pagination, this divides the extent into
    smaller chunks and downloads each chunk separately.
    
    Args:
        service_url: Base URL of the Feature Server endpoint
        where_clause: SQL where clause for filtering
        extent: Bounding box (lon_min, lat_min, lon_max, lat_max)
        max_record_count: Max features per request (from service metadata)
        verbose: Show progress messages
        
    Returns:
        List of all features combined from all chunks
    """
    if verbose:
        print(f"\n[INFO] Using spatial chunking strategy for non-paginated service")
    
    # First, get total count to estimate number of chunks needed
    try:
        total_count = get_feature_count(service_url, where_clause, extent, verbose=False, timeout=timeout)
    except:
        total_count = max_record_count * 2  # Assume we need at least 2 chunks
    
    # Calculate how many chunks we need
    # Add 20% safety margin to account for uneven distribution
    estimated_chunks = max(4, int((total_count / max_record_count) * 1.2))
    
    if verbose:
        print(f"[INFO] Estimated features: {total_count:,}")
        print(f"[INFO] Max records per request: {max_record_count:,}")
        print(f"[INFO] Creating {estimated_chunks} spatial chunks")
    
    chunks = create_spatial_chunks(extent, estimated_chunks)
    
    if verbose:
        print(f"[INFO] Processing {len(chunks)} chunks...")
    
    all_features = []
    feature_ids_seen = set()  # Track IDs to avoid duplicates at chunk boundaries
    
    for idx, chunk_extent in enumerate(chunks, 1):
        if verbose:
            lon_min, lat_min, lon_max, lat_max = chunk_extent
            print(f"\n[INFO] Chunk {idx}/{len(chunks)}: ({lon_min:.4f}, {lat_min:.4f}) to ({lon_max:.4f}, {lat_max:.4f})")
        
        try:
            # Try to get count for this chunk
            chunk_count = get_feature_count(service_url, where_clause, chunk_extent, verbose=False, timeout=timeout)
            
            if chunk_count == 0:
                if verbose:
                    print(f"[INFO] Chunk {idx}: 0 features, skipping")
                continue
            
            if chunk_count > max_record_count:
                if verbose:
                    print(f"[WARNING] Chunk {idx} has {chunk_count:,} features (exceeds max {max_record_count:,})")
                    print(f"[WARNING] This chunk may be incomplete. Consider increasing chunk count.")
            
            # Download from this chunk (will be limited by max_record_count)
            base_url, existing_params = parse_arcgis_url(service_url)
            
            params = {
                'where': where_clause,
                'outFields': '*',
                'returnGeometry': 'true',
                'f': 'geojson',
                'resultRecordCount': max_record_count,
                'outSR': '4326'
            }
            
            # Add spatial filter for this chunk
            lon_min, lat_min, lon_max, lat_max = chunk_extent
            params['geometry'] = f"{lon_min},{lat_min},{lon_max},{lat_max}"
            params['geometryType'] = 'esriGeometryEnvelope'
            params['spatialRel'] = 'esriSpatialRelIntersects'
            params['inSR'] = '4326'
            
            params.update(existing_params)
            
            response = requests.get(base_url, params=params, timeout=timeout)
            response.raise_for_status()
            
            data = response.json()
            
            if 'error' in data:
                if verbose:
                    print(f"[WARNING] Chunk {idx} error: {data['error']}")
                continue
            
            chunk_features = data.get('features', [])
            
            # Deduplicate features (some may appear in multiple chunks)
            new_features = 0
            for feature in chunk_features:
                # Try to get a unique ID for the feature
                feature_id = None
                if 'id' in feature:
                    feature_id = feature['id']
                elif 'properties' in feature:
                    # Try common ID field names
                    for id_field in ['OBJECTID', 'FID', 'id', 'ID', 'objectid']:
                        if id_field in feature['properties']:
                            feature_id = feature['properties'][id_field]
                            break
                
                # If we have an ID, check for duplicates
                if feature_id is not None:
                    if feature_id in feature_ids_seen:
                        continue  # Skip duplicate
                    feature_ids_seen.add(feature_id)
                
                all_features.append(feature)
                new_features += 1
            
            if verbose:
                print(f"[INFO] Chunk {idx}: {new_features:,} features added ({len(chunk_features) - new_features} duplicates)")
        
        except Exception as e:
            if verbose:
                print(f"[ERROR] Chunk {idx} failed: {e}")
            continue
    
    if verbose:
        print(f"\n[INFO] Spatial chunking complete: {len(all_features):,} total features")
    
    return all_features


def esri_json_to_geojson(esri_feature: dict) -> dict:
    """
    Convert ESRI JSON feature to GeoJSON feature.
    
    Args:
        esri_feature: Feature in ESRI JSON format
    
    Returns:
        Feature in GeoJSON format
    """
    geojson_feature = {
        'type': 'Feature',
        'properties': esri_feature.get('attributes', {}),
        'geometry': None
    }
    
    # Convert ESRI geometry to GeoJSON geometry
    if 'geometry' in esri_feature and esri_feature['geometry']:
        esri_geom = esri_feature['geometry']
        
        # Point
        if 'x' in esri_geom and 'y' in esri_geom:
            geojson_feature['geometry'] = {
                'type': 'Point',
                'coordinates': [esri_geom['x'], esri_geom['y']]
            }
        
        # Polyline (LineString or MultiLineString)
        elif 'paths' in esri_geom:
            if len(esri_geom['paths']) == 1:
                geojson_feature['geometry'] = {
                    'type': 'LineString',
                    'coordinates': esri_geom['paths'][0]
                }
            else:
                geojson_feature['geometry'] = {
                    'type': 'MultiLineString',
                    'coordinates': esri_geom['paths']
                }
        
        # Polygon (Polygon or MultiPolygon)
        elif 'rings' in esri_geom:
            # Group rings into polygons (exterior + holes)
            polygons = []
            current_polygon = []
            
            for ring in esri_geom['rings']:
                # Check if ring is clockwise (exterior) or counter-clockwise (hole)
                # In ESRI JSON, exterior rings are clockwise, holes are counter-clockwise
                area = sum((ring[i][0] * ring[i+1][1] - ring[i+1][0] * ring[i][1]) 
                          for i in range(len(ring)-1))
                
                if area < 0:  # Exterior ring (clockwise)
                    if current_polygon:  # Save previous polygon
                        polygons.append(current_polygon)
                    current_polygon = [ring]
                else:  # Hole (counter-clockwise)
                    if current_polygon:
                        current_polygon.append(ring)
            
            if current_polygon:  # Save last polygon
                polygons.append(current_polygon)
            
            if len(polygons) == 1:
                geojson_feature['geometry'] = {
                    'type': 'Polygon',
                    'coordinates': polygons[0]
                }
            else:
                geojson_feature['geometry'] = {
                    'type': 'MultiPolygon',
                    'coordinates': polygons
                }
    
    return geojson_feature


def download_features_paginated(
    service_url: str,
    where_clause: str = "1=1",
    extent: Optional[Tuple[float, float, float, float]] = None,
    max_record_count: int = 1000,
    verbose: bool = False,
    timeout: int = 60,
    max_workers: Optional[int] = None
) -> list:
    """
    Download all features from ArcGIS Feature Server with pagination or spatial chunking.
    
    Automatically detects and handles both GeoJSON and ESRI JSON formats.
    
    ArcGIS Feature Servers typically limit responses to 1000-2000 features per request.
    This function handles pagination automatically using resultOffset.
    
    For services that don't support pagination, it automatically falls back to spatial
    chunking (dividing the extent into smaller chunks).
    
    Args:
        service_url: Base URL of the Feature Server endpoint
        where_clause: SQL where clause for filtering (default: "1=1" for all features)
        extent: Optional bounding box (lon_min, lat_min, lon_max, lat_max) in WGS84
        max_record_count: Maximum features per request (default: 1000)
        verbose: Show progress bar and diagnostic messages
        max_workers: Number of parallel download threads
        
    Returns:
        List of GeoJSON features
    """
    base_url, existing_params = parse_arcgis_url(service_url)
    
    # X parallel workers
    # todo: >1 workers triggers 400 error
    if max_workers is None:
        max_workers = 1
    
    if verbose:
        print(f"\n[INFO] Starting download from: {base_url}")
        print(f"[INFO] Where clause: {where_clause}")
        if extent:
            print(f"[INFO] Spatial extent: {extent}")
        print(f"[INFO] Parallel workers: {max_workers}")
    
    # Check if service supports pagination
    supports_pagination = True
    service_max_records = max_record_count
    
    try:
        # Get service metadata to check pagination support
        metadata_url = base_url.replace('/query', '')
        metadata_response = requests.get(f"{metadata_url}?f=json", timeout=10)
        if metadata_response.status_code == 200:
            metadata = metadata_response.json()
            supports_pagination = metadata.get('supportsPagination', True)
            service_max_records = metadata.get('maxRecordCount', max_record_count)
            
            if verbose:
                print(f"[INFO] Service supports pagination: {supports_pagination}")
                print(f"[INFO] Service max record count: {service_max_records}")
    except:
        # If we can't get metadata, assume pagination is supported
        if verbose:
            print(f"[INFO] Could not retrieve service metadata, assuming pagination supported")
    
    # Get total count first
    try:
        total_count = get_feature_count(service_url, where_clause, extent, verbose=verbose, timeout=timeout)
    except Exception as e:
        if verbose:
            print(f"[ERROR] Failed to get feature count: {e}")
        raise
    
    if verbose:
        print(f"[INFO] Total features to download: {total_count:,}")
    
    if total_count == 0:
        if verbose:
            print(f"[WARNING] No features found matching query")
        return []
    
    # If pagination not supported AND we have more features than max, use spatial chunking
    if not supports_pagination and total_count > service_max_records:
        if extent is None:
            if verbose:
                print(f"[ERROR] Service doesn't support pagination and no extent provided for chunking")
                print(f"[ERROR] Download limited to {service_max_records:,} features")
            # Fall through to regular download (will be limited)
        else:
            if verbose:
                print(f"[INFO] Service doesn't support pagination ({total_count:,} > {service_max_records:,})")
                print(f"[INFO] Switching to spatial chunking strategy...")
            return download_with_spatial_chunking(
                service_url=service_url,
                where_clause=where_clause,
                extent=extent,
                max_record_count=service_max_records,
                verbose=verbose
            )
    
    # Use service's max record count for optimal chunk sizes
    chunk_size = service_max_records
    
    # Track which format works (GeoJSON or ESRI JSON)
    # Start with GeoJSON, fall back to JSON if it fails
    output_format = 'geojson'
    
    # Standard pagination approach with parallel downloading
    def download_chunk(offset: int, chunk_num: int, retry_count: int = 0) -> Tuple[int, list, Optional[str]]:
        """Download a single chunk of features with retry logic. Returns (offset, features, error)."""
        nonlocal output_format
        max_retries = 3
        retry_delay = 2  # seconds
        
        params = {
            'where': where_clause,
            'outFields': '*',
            'returnGeometry': 'true',
            'f': output_format,  # Use working format (geojson or json)
            'resultOffset': offset,
            'resultRecordCount': chunk_size,
            'outSR': '4326'
        }
        
        # Add spatial filter if extent provided
        if extent:
            lon_min, lat_min, lon_max, lat_max = extent
            params['geometry'] = f"{lon_min},{lat_min},{lon_max},{lat_max}"
            params['geometryType'] = 'esriGeometryEnvelope'
            params['spatialRel'] = 'esriSpatialRelIntersects'
            params['inSR'] = '4326'
        
        params.update(existing_params)
        
        # DEBUG: Print parameters for first chunk or on error
        if verbose and (offset == 0 or retry_count > 0):
            print(f"\n[DEBUG] Chunk {chunk_num} (offset {offset}):")
            print(f"  URL: {base_url}")
            print(f"  Parameters:")
            for key, value in sorted(params.items()):
                print(f"    {key}: {value}")
        
        try:
            response = requests.get(base_url, params=params, timeout=timeout)
            
            # DEBUG: Print response details on error
            if response.status_code != 200 and verbose:
                print(f"\n[DEBUG] HTTP {response.status_code} Response:")
                print(f"  URL: {response.url}")
                print(f"  Headers: {dict(response.headers)}")
                try:
                    print(f"  Body: {response.text[:500]}")
                except:
                    pass
            
            # Handle rate limiting (HTTP 429)
            if response.status_code == 429:
                if retry_count < max_retries:
                    # Exponential backoff: 2s, 4s, 8s
                    wait_time = retry_delay * (2 ** retry_count)
                    
                    # Check Retry-After header if provided
                    if 'Retry-After' in response.headers:
                        wait_time = max(wait_time, int(response.headers['Retry-After']))
                    
                    if verbose:
                        print(f"\n[WARNING] Rate limited at offset {offset}, retrying in {wait_time}s (attempt {retry_count + 1}/{max_retries})")
                    
                    time.sleep(wait_time)
                    return download_chunk(offset, chunk_num, retry_count + 1)
                else:
                    return (offset, [], f"Rate limit exceeded after {max_retries} retries")
            
            response.raise_for_status()
            
            data = response.json()
            
            if 'error' in data:
                # If GeoJSON format failed with HTTP 400, try ESRI JSON format
                if data['error'].get('code') == 400 and output_format == 'geojson' and retry_count == 0:
                    if verbose:
                        print(f"\n[INFO] GeoJSON format not supported, switching to ESRI JSON format")
                    output_format = 'json'  # Switch to ESRI JSON (nonlocal will update for all chunks)
                    return download_chunk(offset, chunk_num, retry_count + 1)  # Retry with JSON
                
                error_msg = f"ArcGIS API Error at offset {offset}: {data['error']}"
                if verbose:
                    print(f"\n[ERROR] API returned error for chunk {chunk_num}:")
                    print(f"  Error details: {data['error']}")
                    print(f"  Full response: {data}")
                    print(f"  Request URL: {response.url}")
                return (offset, [], error_msg)
            
            features = data.get('features', [])
            
            # If we got ESRI JSON format, convert to GeoJSON
            if output_format == 'json' and features:
                features = [esri_json_to_geojson(f) for f in features]
            
            # Check rate limit headers and warn if getting close
            if 'x-esri-org-request-units-per-min' in response.headers:
                units_info = response.headers['x-esri-org-request-units-per-min']
                # Parse: "usage=2;max=6000"
                try:
                    parts = dict(item.split('=') for item in units_info.split(';'))
                    usage = int(parts.get('usage', 0))
                    max_units = int(parts.get('max', 0))
                    if max_units > 0 and usage > max_units * 0.8:  # >80% of limit
                        if verbose:
                            print(f"\n[WARNING] High API usage: {usage}/{max_units} units/min")
                except:
                    pass
            
            return (offset, features, None)
            
        except requests.exceptions.Timeout:
            if retry_count < max_retries:
                wait_time = retry_delay * (2 ** retry_count)
                if verbose:
                    print(f"\n[WARNING] Timeout at offset {offset}, retrying in {wait_time}s")
                time.sleep(wait_time)
                return download_chunk(offset, chunk_num, retry_count + 1)
            return (offset, [], f"Timeout after {max_retries} retries")
            
        except Exception as e:
            if retry_count < max_retries:
                wait_time = retry_delay * (2 ** retry_count)
                if verbose:
                    print(f"\n[WARNING] Error at offset {offset}: {e}, retrying in {wait_time}s")
                time.sleep(wait_time)
                return download_chunk(offset, chunk_num, retry_count + 1)
            return (offset, [], str(e))
    
    # Calculate offsets for all chunks
    offsets = list(range(0, total_count, chunk_size))
    
    if verbose:
        print(f"[INFO] Downloading {len(offsets)} chunks of {chunk_size} features each")
        print(f"[INFO] Using {max_workers} parallel workers")
        print(f"[INFO] Rate limit protection: 3 retries with exponential backoff")
    
    # Download chunks in parallel with rate limiting protection
    all_features = []
    errors = []
    chunk_delay = 0.1  # Small delay between chunk submissions (100ms) to avoid burst rate limiting
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit download tasks with small delays to avoid burst rate limiting
        future_to_offset = {}
        for idx, offset in enumerate(offsets):
            future = executor.submit(download_chunk, offset, idx)
            future_to_offset[future] = offset
            # Small delay between submissions to spread out initial requests
            if idx < len(offsets) - 1:  # Don't delay after last submission
                time.sleep(chunk_delay)
        
        # Setup progress bar
        pbar = tqdm(total=total_count, desc="Downloading features", disable=not verbose)
        
        # Collect results as they complete
        for future in as_completed(future_to_offset):
            offset, features, error = future.result()
            
            if error:
                errors.append(f"Chunk at offset {offset}: {error}")
                if verbose:
                    print(f"\n[ERROR] {errors[-1]}")
            else:
                all_features.extend(features)
                pbar.update(len(features))
        
        pbar.close()
    
    if verbose:
        print(f"\n[INFO] Downloaded {len(all_features):,} features in {len(offsets)} chunks")
        if errors:
            print(f"[WARNING] {len(errors)} chunks had errors:")
            for error in errors[:5]:  # Show first 5 errors
                print(f"  - {error}")
            if len(errors) > 5:
                print(f"  ... and {len(errors) - 5} more errors")
    
    return all_features


def download_arcgis_data(
    service_url: str,
    output_path: str,
    extent: Optional[Tuple[float, float, float, float]] = None,
    where_clause: str = "1=1",
    output_format: str = "geojson",
    verbose: bool = False,
    timeout: int = 120,
    max_workers: Optional[int] = None
) -> Dict[str, Any]:
    """
    Download data from ArcGIS Feature Server and save to file.
    
    Args:
        service_url: ArcGIS Feature Server URL (e.g., "https://.../FeatureServer/0")
        output_path: Path to save the output file
        extent: Optional bounding box (lon_min, lat_min, lon_max, lat_max) in WGS84
        where_clause: SQL where clause for attribute filtering (default: "1=1")
        output_format: Output format - "geojson" or "fgb" (FlatGeobuf)
        verbose: Show progress messages
        timeout: Request timeout in seconds (default: 120 for large/slow datasets)
        max_workers: Number of parallel download threads (default: auto-detect based on CPU count)
        
    Returns:
        Dictionary with download results:
        {
            'success': bool,
            'feature_count': int,
            'output_file': str,
            'output_format': str,
            'extent_used': tuple or None,
            'error': str (if failed)
        }
        
    Example:
        >>> result = download_arcgis_data(
        ...     service_url="https://services3.arcgis.com/.../FeatureServer/0",
        ...     output_path="data/settlements.geojson",
        ...     extent=(27.0, -8.0, 30.5, -2.0),
        ...     output_format="geojson"
        ... )
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if verbose:
        print(f"\n{'='*70}")
        print(f"DOWNLOADING FROM ARCGIS FEATURE SERVER")
        print(f"{'='*70}")
        print(f"  URL: {service_url}")
        if extent:
            lon_min, lat_min, lon_max, lat_max = extent
            print(f"  Extent:")
            print(f"    West:  {lon_min}")
            print(f"    South: {lat_min}")
            print(f"    East:  {lon_max}")
            print(f"    North: {lat_max}")
        print(f"  Where: {where_clause}")
        print(f"  Output: {output_path}")
        print(f"  Format: {output_format}")
        print(f"{'='*70}")
    
    try:
        # Download features with pagination (parallel enabled)
        features = download_features_paginated(
            service_url=service_url,
            where_clause=where_clause,
            extent=extent,
            verbose=verbose,
            timeout=timeout,
            max_workers=max_workers
        )
        
        if not features:
            error_msg = 'No features downloaded (query returned 0 results or failed)'
            if verbose:
                print(f"\n[ERROR] {error_msg}")
            return {
                'success': False,
                'feature_count': 0,
                'output_file': None,
                'output_format': output_format,
                'extent_used': extent,
                'error': error_msg
            }
        
        # Create GeoJSON structure
        geojson = {
            'type': 'FeatureCollection',
            'features': features
        }
        
        if output_format.lower() == 'geojson':
            # Save as GeoJSON
            if verbose:
                print(f"\n[INFO] Writing GeoJSON to disk...")
            
            with open(output_path, 'w') as f:
                json.dump(geojson, f)
            
            if verbose:
                file_size_mb = output_path.stat().st_size / 1024 / 1024
                print(f"[SUCCESS] ✓ Saved {len(features):,} features to {output_path}")
                print(f"[INFO] File size: {file_size_mb:.2f} MB")
        
        elif output_format.lower() == 'fgb':
            # Convert to FlatGeobuf using geopandas
            if verbose:
                print(f"\n[INFO] Converting to FlatGeobuf...")
            
            gdf = gpd.GeoDataFrame.from_features(features, crs='EPSG:4326')
            
            # Ensure output path has .fgb extension
            if not str(output_path).endswith('.fgb'):
                output_path = output_path.with_suffix('.fgb')
            
            gdf.to_file(output_path, driver='FlatGeobuf')
            
            if verbose:
                file_size_mb = output_path.stat().st_size / 1024 / 1024
                print(f"[SUCCESS] ✓ Saved {len(features):,} features to {output_path}")
                print(f"[INFO] File size: {file_size_mb:.2f} MB")
        
        else:
            raise ValueError(f"Unsupported output format: {output_format}. Use 'geojson' or 'fgb'")
        
        return {
            'success': True,
            'feature_count': len(features),
            'output_file': str(output_path),
            'output_format': output_format,
            'extent_used': extent
        }
        
    except Exception as e:
        error_msg = str(e)
        if verbose:
            print(f"\n[ERROR] ✗ Error downloading ArcGIS data: {error_msg}")
            import traceback
            print(f"[ERROR] Full traceback:\n{traceback.format_exc()}")
        
        return {
            'success': False,
            'feature_count': 0,
            'output_file': None,
            'output_format': output_format,
            'extent_used': extent,
            'error': error_msg
        }


def batch_download_arcgis_layers(
    layer_configs: list,
    output_dir: str,
    extent: Optional[Tuple[float, float, float, float]] = None,
    output_format: str = "geojson",
    verbose: bool = True,
    timeout: int = 120,
    max_workers: Optional[int] = None
) -> Dict[str, Any]:
    """
    Download multiple ArcGIS Feature Server layers.
    
    Args:
        layer_configs: List of layer configurations, each with:
            - 'url': Feature Server URL
            - 'name': Output filename (without extension)
            - 'where': Optional SQL where clause (default: "1=1")
        output_dir: Directory to save output files
        extent: Optional bounding box applied to all layers
        output_format: Output format - "geojson" or "fgb"
        verbose: Show progress messages
        timeout: Request timeout in seconds (default: 120 for large/slow datasets)
        max_workers: Number of parallel download threads (default: auto-detect based on CPU count)
        
    Returns:
        Dictionary with batch download results
        
    Example:
        >>> layers = [
        ...     {
        ...         'url': 'https://.../GRID3_COD_Settlement_Extents_v3_1/FeatureServer/0',
        ...         'name': 'settlements',
        ...         'where': '1=1'
        ...     },
        ...     {
        ...         'url': 'https://.../GRID3_COD_health_zones_v7_0/FeatureServer/0',
        ...         'name': 'health_zones'
        ...     }
        ... ]
        >>> results = batch_download_arcgis_layers(
        ...     layer_configs=layers,
        ...     output_dir='data/arcgis',
        ...     extent=(27.0, -8.0, 30.5, -2.0)
        ... )
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    results = {
        'total_layers': len(layer_configs),
        'successful': 0,
        'failed': 0,
        'layers': []
    }
    
    if verbose:
        print(f"\n{'='*70}")
        print(f"BATCH DOWNLOADING {len(layer_configs)} ARCGIS LAYERS")
        print(f"{'='*70}")
        print(f"Output directory: {output_dir}")
        print(f"Output format: {output_format}")
        if extent:
            lon_min, lat_min, lon_max, lat_max = extent
            print(f"Spatial filter:")
            print(f"  West:  {lon_min}")
            print(f"  South: {lat_min}")
            print(f"  East:  {lon_max}")
            print(f"  North: {lat_max}")
        print(f"{'='*70}\n")
    
    for idx, config in enumerate(layer_configs, 1):
        layer_name = config.get('name', 'layer')
        layer_url = config['url']
        where_clause = config.get('where', '1=1')
        
        # Determine output path
        extension = '.fgb' if output_format.lower() == 'fgb' else '.geojson'
        output_path = output_dir / f"{layer_name}{extension}"
        
        if verbose:
            print(f"\n{'='*70}")
            print(f"LAYER {idx}/{len(layer_configs)}: {layer_name}")
            print(f"{'='*70}")
        
        result = download_arcgis_data(
            service_url=layer_url,
            output_path=str(output_path),
            extent=extent,
            where_clause=where_clause,
            output_format=output_format,
            verbose=verbose,
            timeout=timeout,
            max_workers=max_workers
        )
        
        results['layers'].append({
            'name': layer_name,
            **result
        })
        
        if result['success']:
            results['successful'] += 1
        else:
            results['failed'] += 1
            if verbose:
                print(f"\n[ERROR] Failed to download {layer_name}: {result.get('error', 'Unknown error')}")
    
    if verbose:
        print(f"\n{'='*70}")
        print(f"BATCH DOWNLOAD SUMMARY")
        print(f"{'='*70}")
        print(f"Total layers: {results['total_layers']}")
        print(f"Successful:   {results['successful']} ✓")
        print(f"Failed:       {results['failed']} ✗")
        
        if results['failed'] > 0:
            print(f"\nFailed layers:")
            for layer in results['layers']:
                if not layer['success']:
                    print(f"  ✗ {layer['name']}: {layer.get('error', 'Unknown error')}")
        
        print(f"{'='*70}\n")
    
    return results


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Download ArcGIS Feature Server data')
    parser.add_argument('url', help='ArcGIS Feature Server URL')
    parser.add_argument('output', help='Output file path')
    parser.add_argument('--extent', help='Bounding box: lon_min,lat_min,lon_max,lat_max')
    parser.add_argument('--where', default='1=1', help='SQL where clause')
    parser.add_argument('--format', choices=['geojson', 'fgb'], default='geojson',
                       help='Output format (default: geojson)')
    
    args = parser.parse_args()
    
    extent = None
    if args.extent:
        extent = tuple(map(float, args.extent.split(',')))
    
    result = download_arcgis_data(
        service_url=args.url,
        output_path=args.output,
        extent=extent,
        where_clause=args.where,
        output_format=args.format,
        verbose=True
    )
    
    exit(0 if result['success'] else 1)
