#!/usr/bin/env python3
"""
Geometry Utilities v1 - Helper Functions for Spatial Operations
Utilities for geometry validation, conversion, and processing
"""

import json
import logging
from typing import Dict, List, Optional, Tuple

from shapely.geometry import Point, Polygon, shape
from shapely.validation import make_valid
import numpy as np

logger = logging.getLogger(__name__)

def validate_geometry(geometry: Dict) -> bool:
    """
    Validate GeoJSON geometry
    
    Args:
        geometry: GeoJSON geometry dictionary
        
    Returns:
        True if geometry is valid
    """
    try:
        geom = shape(geometry)
        return geom.is_valid and not geom.is_empty
    except Exception as e:
        logger.debug(f"Geometry validation failed: {e}")
        return False

def fix_geometry(geometry: Dict) -> Optional[Dict]:
    """
    Attempt to fix invalid geometry
    
    Args:
        geometry: GeoJSON geometry dictionary
        
    Returns:
        Fixed geometry dictionary or None if unfixable
    """
    try:
        geom = shape(geometry)
        if not geom.is_valid:
            fixed_geom = make_valid(geom)
            if fixed_geom.is_valid and not fixed_geom.is_empty:
                return json.loads(json.dumps(fixed_geom.__geo_interface__))
        return geometry
    except Exception as e:
        logger.warning(f"Failed to fix geometry: {e}")
        return None

def calculate_geometry_area_acres(geometry: Dict) -> float:
    """
    Calculate geometry area in acres (approximate for WGS84)
    
    Args:
        geometry: GeoJSON geometry dictionary
        
    Returns:
        Area in acres
    """
    try:
        geom = shape(geometry)
        # For WGS84, approximate area calculation
        # This is rough - for precise area calculation, would need to reproject
        bounds = geom.bounds
        # Approximate degrees to meters conversion at mid-latitude
        lat_center = (bounds[1] + bounds[3]) / 2
        meters_per_degree_lat = 111320  # Approximately constant
        meters_per_degree_lon = 111320 * np.cos(np.radians(lat_center))
        
        # Simple area calculation (not accurate for large areas)
        area_deg_squared = geom.area
        area_m_squared = area_deg_squared * meters_per_degree_lat * meters_per_degree_lon
        area_acres = area_m_squared * 0.000247105  # m² to acres
        
        return area_acres
    except Exception as e:
        logger.warning(f"Failed to calculate geometry area: {e}")
        return 0.0

def get_geometry_centroid(geometry: Dict) -> Optional[Tuple[float, float]]:
    """
    Get geometry centroid coordinates
    
    Args:
        geometry: GeoJSON geometry dictionary
        
    Returns:
        Tuple of (longitude, latitude) or None if failed
    """
    try:
        geom = shape(geometry)
        centroid = geom.centroid
        return (centroid.x, centroid.y)
    except Exception as e:
        logger.debug(f"Failed to calculate centroid: {e}")
        return None

def geometry_to_postgis(geometry: Dict) -> str:
    """
    Convert GeoJSON geometry to PostGIS-compatible WKT
    
    Args:
        geometry: GeoJSON geometry dictionary
        
    Returns:
        WKT string for PostGIS
    """
    try:
        geom = shape(geometry)
        return geom.wkt
    except Exception as e:
        logger.error(f"Failed to convert geometry to PostGIS: {e}")
        return ""

def simplify_geometry(geometry: Dict, tolerance: float = 0.0001) -> Optional[Dict]:
    """
    Simplify geometry to reduce complexity
    
    Args:
        geometry: GeoJSON geometry dictionary
        tolerance: Simplification tolerance in degrees
        
    Returns:
        Simplified geometry dictionary or None if failed
    """
    try:
        geom = shape(geometry)
        simplified = geom.simplify(tolerance, preserve_topology=True)
        if simplified.is_valid and not simplified.is_empty:
            return json.loads(json.dumps(simplified.__geo_interface__))
        return geometry
    except Exception as e:
        logger.debug(f"Failed to simplify geometry: {e}")
        return geometry

def buffer_geometry(geometry: Dict, buffer_degrees: float) -> Optional[Dict]:
    """
    Create buffer around geometry
    
    Args:
        geometry: GeoJSON geometry dictionary
        buffer_degrees: Buffer distance in degrees
        
    Returns:
        Buffered geometry dictionary or None if failed
    """
    try:
        geom = shape(geometry)
        buffered = geom.buffer(buffer_degrees)
        if buffered.is_valid and not buffered.is_empty:
            return json.loads(json.dumps(buffered.__geo_interface__))
        return None
    except Exception as e:
        logger.debug(f"Failed to buffer geometry: {e}")
        return None

def geometry_intersects_bounds(geometry: Dict, bounds: Tuple[float, float, float, float]) -> bool:
    """
    Check if geometry intersects with bounding box
    
    Args:
        geometry: GeoJSON geometry dictionary
        bounds: Tuple of (min_lon, min_lat, max_lon, max_lat)
        
    Returns:
        True if geometry intersects bounds
    """
    try:
        geom = shape(geometry)
        bounds_geom = Polygon([
            (bounds[0], bounds[1]),  # min_lon, min_lat
            (bounds[0], bounds[3]),  # min_lon, max_lat
            (bounds[2], bounds[3]),  # max_lon, max_lat
            (bounds[2], bounds[1]),  # max_lon, min_lat
            (bounds[0], bounds[1])   # close polygon
        ])
        return geom.intersects(bounds_geom)
    except Exception as e:
        logger.debug(f"Failed to check geometry bounds intersection: {e}")
        return False

def validate_coordinates(lon: float, lat: float) -> bool:
    """
    Validate longitude and latitude values
    
    Args:
        lon: Longitude
        lat: Latitude
        
    Returns:
        True if coordinates are valid
    """
    return -180 <= lon <= 180 and -90 <= lat <= 90