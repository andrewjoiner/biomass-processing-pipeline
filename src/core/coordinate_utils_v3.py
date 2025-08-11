#!/usr/bin/env python3
"""
Coordinate Utilities v1 - WGS84 ↔ UTM Transformations
Fixes critical coordinate system mismatch issues from original pipeline
"""

import logging
import math
import re
from typing import Dict, List, Optional, Tuple

import numpy as np
from pyproj import CRS, Transformer
from shapely.geometry import Point, Polygon, box
from shapely.ops import transform

logger = logging.getLogger(__name__)

class CoordinateTransformer:
    """
    High-performance coordinate transformation utilities for biomass processing
    Fixes the critical WGS84 ↔ UTM mismatch that prevented tile matching
    """
    
    def __init__(self):
        # Cache transformers to avoid repeated initialization
        self._transformer_cache = {}
        
        # Common CRS definitions
        self.wgs84 = CRS.from_epsg(4326)
        
        # Sentinel-2 MGRS grid zones for efficient lookup
        self.mgrs_zones = self._initialize_mgrs_zones()
        
    def _initialize_mgrs_zones(self) -> Dict[str, Dict]:
        """Initialize MGRS zone definitions for Sentinel-2 tiles"""
        return {
            # Iowa region (primary test area)
            '15TUL': {'utm_zone': 15, 'utm_epsg': 32615, 'lat_band': 'T'},
            '15TUM': {'utm_zone': 15, 'utm_epsg': 32615, 'lat_band': 'T'},
            '15TUN': {'utm_zone': 15, 'utm_epsg': 32615, 'lat_band': 'T'},
            
            # Additional common zones
            '14TQL': {'utm_zone': 14, 'utm_epsg': 32614, 'lat_band': 'T'},
            '16TCK': {'utm_zone': 16, 'utm_epsg': 32616, 'lat_band': 'T'},
            '16TDK': {'utm_zone': 16, 'utm_epsg': 32616, 'lat_band': 'T'},
        }
    
    def get_transformer(self, from_crs: str, to_crs: str) -> Transformer:
        """
        Get cached transformer for coordinate conversion
        
        Args:
            from_crs: Source CRS (e.g., 'EPSG:4326', 'EPSG:32615')
            to_crs: Target CRS (e.g., 'EPSG:4326', 'EPSG:32615')
            
        Returns:
            Transformer object for coordinate conversion
        """
        cache_key = f"{from_crs}_to_{to_crs}"
        
        if cache_key not in self._transformer_cache:
            self._transformer_cache[cache_key] = Transformer.from_crs(
                from_crs, to_crs, always_xy=True
            )
        
        return self._transformer_cache[cache_key]
    
    def wgs84_to_utm(self, lon: float, lat: float, utm_epsg: int) -> Tuple[float, float]:
        """
        Convert WGS84 coordinates to UTM
        
        Args:
            lon: Longitude in WGS84
            lat: Latitude in WGS84
            utm_epsg: UTM EPSG code (e.g., 32615 for UTM Zone 15N)
            
        Returns:
            Tuple of (easting, northing) in UTM coordinates
        """
        transformer = self.get_transformer('EPSG:4326', f'EPSG:{utm_epsg}')
        return transformer.transform(lon, lat)
    
    def utm_to_wgs84(self, easting: float, northing: float, utm_epsg: int) -> Tuple[float, float]:
        """
        Convert UTM coordinates to WGS84
        
        Args:
            easting: UTM easting coordinate
            northing: UTM northing coordinate
            utm_epsg: UTM EPSG code (e.g., 32615 for UTM Zone 15N)
            
        Returns:
            Tuple of (longitude, latitude) in WGS84
        """
        transformer = self.get_transformer(f'EPSG:{utm_epsg}', 'EPSG:4326')
        return transformer.transform(easting, northing)
    
    def bounds_wgs84_to_utm(self, bounds: Tuple[float, float, float, float], 
                           utm_epsg: int) -> Tuple[float, float, float, float]:
        """
        Convert WGS84 bounds to UTM bounds
        CRITICAL FIX: This resolves the coordinate mismatch in tile selection
        
        Args:
            bounds: WGS84 bounds (min_lon, min_lat, max_lon, max_lat)
            utm_epsg: UTM EPSG code
            
        Returns:
            UTM bounds (min_easting, min_northing, max_easting, max_northing)
        """
        min_lon, min_lat, max_lon, max_lat = bounds
        
        # Transform all four corners to ensure correct bounds
        corners = [
            (min_lon, min_lat),  # Bottom-left
            (min_lon, max_lat),  # Top-left
            (max_lon, min_lat),  # Bottom-right
            (max_lon, max_lat)   # Top-right
        ]
        
        utm_corners = []
        for lon, lat in corners:
            easting, northing = self.wgs84_to_utm(lon, lat, utm_epsg)
            utm_corners.append((easting, northing))
        
        # Find min/max of transformed corners
        eastings = [corner[0] for corner in utm_corners]
        northings = [corner[1] for corner in utm_corners]
        
        return (min(eastings), min(northings), max(eastings), max(northings))
    
    def bounds_utm_to_wgs84(self, bounds: Tuple[float, float, float, float], 
                           utm_epsg: int) -> Tuple[float, float, float, float]:
        """
        Convert UTM bounds to WGS84 bounds
        
        Args:
            bounds: UTM bounds (min_easting, min_northing, max_easting, max_northing)
            utm_epsg: UTM EPSG code
            
        Returns:
            WGS84 bounds (min_lon, min_lat, max_lon, max_lat)
        """
        min_easting, min_northing, max_easting, max_northing = bounds
        
        # Transform all four corners
        corners = [
            (min_easting, min_northing),
            (min_easting, max_northing),
            (max_easting, min_northing),
            (max_easting, max_northing)
        ]
        
        wgs84_corners = []
        for easting, northing in corners:
            lon, lat = self.utm_to_wgs84(easting, northing, utm_epsg)
            wgs84_corners.append((lon, lat))
        
        # Find min/max of transformed corners
        lons = [corner[0] for corner in wgs84_corners]
        lats = [corner[1] for corner in wgs84_corners]
        
        return (min(lons), min(lats), max(lons), max(lats))
    
    def determine_utm_zone(self, lon: float, lat: float) -> int:
        """
        Determine UTM zone from WGS84 coordinates
        
        Args:
            lon: Longitude in WGS84
            lat: Latitude in WGS84
            
        Returns:
            UTM zone number
        """
        return int((lon + 180) // 6) + 1
    
    def get_utm_epsg(self, utm_zone: int, is_northern: bool = True) -> int:
        """
        Get EPSG code for UTM zone
        
        Args:
            utm_zone: UTM zone number (1-60)
            is_northern: True for northern hemisphere, False for southern
            
        Returns:
            EPSG code for the UTM zone
        """
        if is_northern:
            return 32600 + utm_zone
        else:
            return 32700 + utm_zone
    
    def parse_sentinel2_tile_id(self, tile_id: str) -> Dict:
        """
        Parse Sentinel-2 MGRS tile ID to get UTM information
        
        Args:
            tile_id: Sentinel-2 tile ID (e.g., '15TUL')
            
        Returns:
            Dictionary with UTM zone information
        """
        match = re.match(r'(\d{2})([A-Z])([A-Z]{2})', tile_id)
        if not match:
            raise ValueError(f"Invalid Sentinel-2 tile ID: {tile_id}")
        
        utm_zone = int(match.group(1))
        lat_band = match.group(2)
        grid_square = match.group(3)
        
        # Determine if northern hemisphere (simplified)
        is_northern = lat_band >= 'N'
        utm_epsg = self.get_utm_epsg(utm_zone, is_northern)
        
        return {
            'tile_id': tile_id,
            'utm_zone': utm_zone,
            'lat_band': lat_band,
            'grid_square': grid_square,
            'utm_epsg': utm_epsg,
            'is_northern': is_northern
        }
    
    def bounds_intersect(self, bounds1: Tuple[float, float, float, float], 
                        bounds2: Tuple[float, float, float, float]) -> bool:
        """
        Check if two bounding boxes intersect (same CRS assumed)
        CRITICAL FIX: Proper bounds intersection check
        
        Args:
            bounds1: First bounding box
            bounds2: Second bounding box
            
        Returns:
            True if bounds intersect, False otherwise
        """
        min_x1, min_y1, max_x1, max_y1 = bounds1
        min_x2, min_y2, max_x2, max_y2 = bounds2
        
        return not (max_x1 < min_x2 or max_x2 < min_x1 or 
                   max_y1 < min_y2 or max_y2 < min_y1)
    
    def get_sentinel2_tiles_for_bounds(self, wgs84_bounds: Tuple[float, float, float, float],
                                     available_tiles: List[str]) -> List[Dict]:
        """
        Get Sentinel-2 tiles that intersect with WGS84 bounds
        CRITICAL FIX: Proper coordinate transformation for tile selection
        
        Args:
            wgs84_bounds: WGS84 bounds to search
            available_tiles: List of available Sentinel-2 tile IDs
            
        Returns:
            List of tiles with intersection information
        """
        intersecting_tiles = []
        min_lon, min_lat, max_lon, max_lat = wgs84_bounds
        
        # Calculate target UTM zone from bounds center
        center_lon = (min_lon + max_lon) / 2
        center_lat = (min_lat + max_lat) / 2
        target_utm_zone = int((center_lon + 180) / 6) + 1
        
        # Determine target latitude bands from bounds
        target_lat_bands = set()
        for lat in [min_lat, max_lat, center_lat]:
            if lat < 0:
                # Southern hemisphere bands (future enhancement)
                continue
            elif lat < 24:
                target_lat_bands.add('Q')  # 16-24N
            elif lat < 32:
                target_lat_bands.add('R')  # 24-32N
            elif lat < 40:
                target_lat_bands.add('S')  # 32-40N
            elif lat < 48:
                target_lat_bands.add('T')  # 40-48N
            elif lat < 56:
                target_lat_bands.add('U')  # 48-56N
            elif lat < 64:
                target_lat_bands.add('V')  # 56-64N
        
        logger.info(f"Target area: UTM zone {target_utm_zone}, latitude bands {target_lat_bands}")
        
        for tile_id in available_tiles:
            try:
                tile_info = self.parse_sentinel2_tile_id(tile_id)
                
                zone = tile_info['utm_zone']
                band = tile_info.get('lat_band') or tile_info.get('utm_band')
                
                # Check if tile intersects with target geographic area
                zone_matches = zone in [target_utm_zone - 1, target_utm_zone, target_utm_zone + 1]
                band_matches = band in target_lat_bands
                
                if zone_matches and band_matches:
                    intersecting_tiles.append({
                        'tile_id': tile_id,
                        'utm_epsg': tile_info['utm_epsg'],
                        'utm_zone': zone,
                        'lat_band': band,
                        'wgs84_bounds': wgs84_bounds
                    })
                    
            except Exception as e:
                logger.warning(f"Could not process tile {tile_id}: {e}")
                continue
        
        logger.info(f"Found {len(intersecting_tiles)} intersecting tiles")
        return intersecting_tiles
    
    def get_worldcover_tiles_for_bounds(self, wgs84_bounds: Tuple[float, float, float, float]) -> List[str]:
        """
        Get WorldCover tile names that intersect with WGS84 bounds
        WorldCover uses 3° x 3° tiles in WGS84
        
        Args:
            wgs84_bounds: WGS84 bounds to search
            
        Returns:
            List of WorldCover tile names
        """
        min_lon, min_lat, max_lon, max_lat = wgs84_bounds
        
        # WorldCover tiles are 3° x 3° starting at multiples of 3
        tiles = []
        
        # Find tile grid coordinates
        min_tile_lon = int(math.floor(min_lon / 3.0)) * 3
        max_tile_lon = int(math.floor(max_lon / 3.0)) * 3
        min_tile_lat = int(math.floor(min_lat / 3.0)) * 3
        max_tile_lat = int(math.floor(max_lat / 3.0)) * 3
        
        for tile_lat in range(min_tile_lat, max_tile_lat + 3, 3):
            for tile_lon in range(min_tile_lon, max_tile_lon + 3, 3):
                # Format tile name: N39W096 style
                ns = 'N' if tile_lat >= 0 else 'S'
                ew = 'E' if tile_lon >= 0 else 'W'
                
                tile_name = f"{ns}{abs(tile_lat):02d}{ew}{abs(tile_lon):03d}"
                tiles.append(tile_name)
        
        return tiles
    
    def validate_coordinates(self, lon: float, lat: float) -> bool:
        """
        Validate WGS84 coordinates
        
        Args:
            lon: Longitude
            lat: Latitude
            
        Returns:
            True if coordinates are valid
        """
        return -180 <= lon <= 180 and -90 <= lat <= 90


# Global coordinate transformer instance
coordinate_transformer = CoordinateTransformer()