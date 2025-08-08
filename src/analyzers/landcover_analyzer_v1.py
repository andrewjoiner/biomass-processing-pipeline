#!/usr/bin/env python3
"""
Land Cover Analyzer v1 - Sub-Parcel Land Use Segmentation
Advanced satellite data processing for accurate biomass allocation within parcels
"""

import logging
import os
import tempfile
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import rasterio
import rasterio.features
import rasterio.mask
from shapely.geometry import shape
from shapely.ops import transform
import pyproj

from ..config.database_config_v1 import WORLDCOVER_CLASSES
from ..core.blob_manager_v1 import blob_manager

logger = logging.getLogger(__name__)

class LandCoverAnalyzer:
    """
    Advanced land cover analyzer for sub-parcel land use segmentation
    Uses WorldCover 10m data + Sentinel-2 NDVI for accurate biomass allocation
    """
    
    def __init__(self):
        self.blob_manager = blob_manager
        self.tile_cache_dir = '/tmp/landcover_cache_v2'  # Use new cache directory
        self._ensure_cache_directory()
        
        # Land cover classification thresholds
        self.landcover_thresholds = {
            'forest_min_pixels': 5,        # Minimum 5 pixels (500m²) for forest classification
            'crop_min_pixels': 10,         # Minimum 10 pixels (1000m²) for crop field classification
            'ndvi_forest_min': 0.5,        # Minimum NDVI for healthy forest
            'ndvi_crop_min': 0.3,          # Minimum NDVI for active cropland
            'fragmentation_threshold': 0.8  # Minimum coverage for non-fragmented areas
        }
        
        # WorldCover class mappings for biomass analysis
        self.biomass_landcover_mapping = {
            10: 'forest',      # Tree cover
            40: 'cropland',    # Cropland
            30: 'grassland',   # Grassland (potential pasture/hay)
            20: 'shrubland',   # Shrubland (low biomass potential)
            50: 'developed',   # Built-up (no biomass)
            60: 'barren',      # Bare/sparse vegetation (no biomass)
            80: 'water',       # Water (no biomass)
            90: 'wetland',     # Wetland (special biomass considerations)
        }
    
    def _ensure_cache_directory(self):
        """Create tile cache directory if it doesn't exist"""
        os.makedirs(self.tile_cache_dir, exist_ok=True)
    
    def analyze_parcel_landcover(self, parcel_geometry: Dict, parcel_id: str = None) -> Optional[Dict]:
        """
        Perform comprehensive sub-parcel land cover analysis
        
        Args:
            parcel_geometry: GeoJSON geometry dictionary
            parcel_id: Optional parcel identifier for caching
            
        Returns:
            Land cover analysis dictionary with sub-parcel breakdowns
        """
        try:
            # Step 1: Get WorldCover tiles covering the parcel
            worldcover_tiles = self._get_worldcover_tiles_for_parcel(parcel_geometry)
            
            if not worldcover_tiles:
                logger.warning(f"No WorldCover tiles available for parcel {parcel_id}")
                return None
            
            # Step 2: Process WorldCover data for sub-parcel analysis
            landcover_analysis = self._process_worldcover_subparcel(parcel_geometry, worldcover_tiles)
            
            if not landcover_analysis:
                logger.warning(f"WorldCover processing failed for parcel {parcel_id}")
                return None
            
            # Step 3: Enhance with Sentinel-2 NDVI data (if available)
            ndvi_analysis = self._enhance_with_sentinel2_ndvi(parcel_geometry, parcel_id)
            
            # Step 4: Create comprehensive land cover record
            landcover_record = {
                'parcel_id': parcel_id,
                'total_parcel_area_m2': landcover_analysis['total_area_m2'],
                'total_parcel_acres': landcover_analysis['total_area_m2'] * 0.000247105,
                
                # Land cover breakdown by type
                'landcover_breakdown': landcover_analysis['landcover_breakdown'],
                'landcover_percentages': landcover_analysis['landcover_percentages'],
                
                # Biomass-relevant area calculations
                'forest_area_acres': landcover_analysis.get('forest_area_acres', 0.0),
                'forest_percentage': landcover_analysis.get('forest_percentage', 0.0),
                'cropland_area_acres': landcover_analysis.get('cropland_area_acres', 0.0),
                'cropland_percentage': landcover_analysis.get('cropland_percentage', 0.0),
                'grassland_area_acres': landcover_analysis.get('grassland_area_acres', 0.0),
                'grassland_percentage': landcover_analysis.get('grassland_percentage', 0.0),
                
                # Non-productive areas
                'developed_area_acres': landcover_analysis.get('developed_area_acres', 0.0),
                'water_area_acres': landcover_analysis.get('water_area_acres', 0.0),
                'other_area_acres': landcover_analysis.get('other_area_acres', 0.0),
                
                # Data quality metrics
                'pixel_count_total': landcover_analysis['pixel_count'],
                'data_completeness': landcover_analysis['data_completeness'],
                'fragmentation_index': landcover_analysis.get('fragmentation_index', 0.0),
                
                # Analysis metadata
                'worldcover_tiles_used': len(worldcover_tiles),
                'has_ndvi_data': ndvi_analysis is not None,
                'analysis_timestamp': datetime.now().isoformat(),
                'processing_method': 'WorldCover_10m_Subparcel'
            }
            
            # Add NDVI analysis if available
            if ndvi_analysis:
                landcover_record.update({
                    'ndvi_statistics': ndvi_analysis['ndvi_stats'],
                    'vegetation_health_score': ndvi_analysis['vegetation_health'],
                    'ndvi_forest_correlation': ndvi_analysis.get('forest_correlation', 0.0),
                    'ndvi_crop_correlation': ndvi_analysis.get('crop_correlation', 0.0)
                })
            
            logger.debug(f"Land cover analysis complete for parcel {parcel_id}: "
                        f"{landcover_record['forest_percentage']:.1f}% forest, "
                        f"{landcover_record['cropland_percentage']:.1f}% cropland")
            
            return landcover_record
            
        except Exception as e:
            logger.error(f"Error in land cover analysis for parcel {parcel_id}: {e}")
            return None
    
    def _get_worldcover_tiles_for_parcel(self, parcel_geometry: Dict) -> List[str]:
        """
        Identify WorldCover tiles needed to cover the parcel
        
        Args:
            parcel_geometry: GeoJSON geometry dictionary
            
        Returns:
            List of WorldCover tile identifiers
        """
        try:
            # Convert geometry to shapely for bounds calculation
            parcel_shape = shape(parcel_geometry)
            bounds = parcel_shape.bounds  # (minx, miny, maxx, maxy)
            
            # WorldCover tiles are 3° x 3° - calculate tile grid
            tiles_needed = []
            
            # Calculate tile bounds (simplified - WorldCover uses specific tile naming)
            min_lon, min_lat, max_lon, max_lat = bounds
            
            # Round to WorldCover tile boundaries (3-degree grid)
            # WorldCover tiles are aligned to 3-degree grid starting from 0
            import math
            tile_min_lon = math.floor(min_lon / 3) * 3
            tile_min_lat = math.floor(min_lat / 3) * 3
            tile_max_lon = math.ceil(max_lon / 3) * 3
            tile_max_lat = math.ceil(max_lat / 3) * 3
            
            # Generate tile names covering the parcel
            for lon in range(int(tile_min_lon), int(tile_max_lon), 3):
                for lat in range(int(tile_min_lat), int(tile_max_lat), 3):
                    # WorldCover tile naming convention (simplified)
                    if lat >= 0:
                        lat_str = f"N{lat:02d}"
                    else:
                        lat_str = f"S{abs(lat):02d}"
                    
                    if lon >= 0:
                        lon_str = f"E{lon:03d}"
                    else:
                        lon_str = f"W{abs(lon):03d}"
                    
                    tile_name = f"ESA_WorldCover_10m_2021_v200_{lat_str}{lon_str}.tif"
                    tiles_needed.append(tile_name)
            
            logger.debug(f"Identified {len(tiles_needed)} WorldCover tiles for parcel")
            return tiles_needed
            
        except Exception as e:
            logger.error(f"Error identifying WorldCover tiles: {e}")
            return []
    
    def _process_worldcover_subparcel(self, parcel_geometry: Dict, tile_names: List[str]) -> Optional[Dict]:
        """
        Process WorldCover tiles for detailed sub-parcel land cover analysis
        
        Args:
            parcel_geometry: GeoJSON geometry dictionary
            tile_names: List of WorldCover tile names
            
        Returns:
            Detailed land cover analysis dictionary
        """
        try:
            parcel_shape = shape(parcel_geometry)
            
            # Download and cache WorldCover tiles
            cached_tiles = []
            for tile_name in tile_names:
                cached_path = self._download_and_cache_tile(tile_name)
                if cached_path and os.path.exists(cached_path):
                    cached_tiles.append(cached_path)
            
            if not cached_tiles:
                logger.warning("No WorldCover tiles could be downloaded/cached")
                return None
            
            # Process each tile and aggregate results
            total_pixel_count = 0
            landcover_pixel_counts = {}
            total_area_m2 = 0
            
            for tile_path in cached_tiles:
                tile_analysis = self._analyze_tile_for_parcel(tile_path, parcel_shape)
                
                if tile_analysis:
                    total_pixel_count += tile_analysis['pixel_count']
                    total_area_m2 += tile_analysis['area_m2']
                    
                    # Aggregate pixel counts by land cover class
                    for lc_class, pixel_count in tile_analysis['class_pixels'].items():
                        if lc_class not in landcover_pixel_counts:
                            landcover_pixel_counts[lc_class] = 0
                        landcover_pixel_counts[lc_class] += pixel_count
            
            if total_pixel_count == 0:
                logger.warning("No valid pixels found in WorldCover tiles")
                return None
            
            # Calculate land cover breakdown
            landcover_breakdown = {}
            landcover_percentages = {}
            
            for lc_class, pixel_count in landcover_pixel_counts.items():
                # Each WorldCover pixel is 10m x 10m = 100 m²
                area_m2 = pixel_count * 100
                area_acres = area_m2 * 0.000247105
                percentage = (pixel_count / total_pixel_count) * 100
                
                landcover_name = WORLDCOVER_CLASSES.get(lc_class, f'Class_{lc_class}')
                
                landcover_breakdown[landcover_name] = {
                    'pixel_count': pixel_count,
                    'area_m2': area_m2,
                    'area_acres': round(area_acres, 3),
                    'percentage': round(percentage, 2)
                }
                landcover_percentages[landcover_name] = round(percentage, 2)
            
            # Calculate specific biomass-relevant areas
            forest_pixels = landcover_pixel_counts.get(10, 0)  # Tree cover
            cropland_pixels = landcover_pixel_counts.get(40, 0)  # Cropland
            grassland_pixels = landcover_pixel_counts.get(30, 0)  # Grassland
            developed_pixels = landcover_pixel_counts.get(50, 0)  # Built-up
            water_pixels = landcover_pixel_counts.get(80, 0)  # Water
            
            # Convert pixels to acres for easy use
            forest_area_acres = forest_pixels * 100 * 0.000247105
            cropland_area_acres = cropland_pixels * 100 * 0.000247105
            grassland_area_acres = grassland_pixels * 100 * 0.000247105
            developed_area_acres = developed_pixels * 100 * 0.000247105
            water_area_acres = water_pixels * 100 * 0.000247105
            
            # Calculate fragmentation index (measure of land use mixing)
            fragmentation_index = self._calculate_fragmentation_index(landcover_pixel_counts, total_pixel_count)
            
            return {
                'total_area_m2': total_area_m2,
                'pixel_count': total_pixel_count,
                'data_completeness': min(1.0, total_pixel_count / (total_area_m2 / 100)),  # Expected pixels vs actual
                
                'landcover_breakdown': landcover_breakdown,
                'landcover_percentages': landcover_percentages,
                
                # Biomass-relevant areas
                'forest_area_acres': round(forest_area_acres, 3),
                'forest_percentage': round((forest_pixels / total_pixel_count) * 100, 2),
                'cropland_area_acres': round(cropland_area_acres, 3),
                'cropland_percentage': round((cropland_pixels / total_pixel_count) * 100, 2),
                'grassland_area_acres': round(grassland_area_acres, 3),
                'grassland_percentage': round((grassland_pixels / total_pixel_count) * 100, 2),
                
                # Non-productive areas
                'developed_area_acres': round(developed_area_acres, 3),
                'water_area_acres': round(water_area_acres, 3),
                'other_area_acres': round((total_area_m2 * 0.000247105) - forest_area_acres - cropland_area_acres - grassland_area_acres - developed_area_acres - water_area_acres, 3),
                
                'fragmentation_index': fragmentation_index
            }
            
        except Exception as e:
            logger.error(f"Error processing WorldCover tiles: {e}")
            return None
    
    def _download_and_cache_tile(self, tile_name: str) -> Optional[str]:
        """
        Download and cache WorldCover tile, return path to cached file
        
        Args:
            tile_name: WorldCover tile identifier
            
        Returns:
            Path to cached tile file or None if failed
        """
        try:
            cache_path = os.path.join(self.tile_cache_dir, tile_name)
            
            # Return cached file if it exists
            if os.path.exists(cache_path):
                logger.debug(f"Using cached WorldCover tile: {tile_name}")
                return cache_path
            
            # Download tile from Azure blob storage - need raw bytes, not processed data
            logger.debug(f"Downloading WorldCover tile: {tile_name}")
            
            # Construct blob name
            if tile_name.startswith('ESA_WorldCover') and tile_name.endswith('.tif'):
                blob_name = f"worldcover_2021/{tile_name}"
            else:
                blob_name = f"worldcover_2021/ESA_WorldCover_10m_2021_v200_{tile_name}.tif"
            
            # Download raw bytes directly
            logger.info(f"Downloading WorldCover tile from blob: {blob_name}")
            raw_tile_data = self.blob_manager.download_blob_to_memory('worldcover-data', blob_name)
            
            if raw_tile_data and len(raw_tile_data) > 0:
                # Cache the raw TIFF bytes (only if we got actual data)
                with open(cache_path, 'wb') as f:
                    f.write(raw_tile_data)
                logger.info(f"Successfully cached WorldCover tile: {cache_path} ({len(raw_tile_data)} bytes)")
                return cache_path
            else:
                logger.error(f"Downloaded tile data is empty or None for {blob_name}")
                # Remove any empty file that might have been created
                if os.path.exists(cache_path):
                    os.remove(cache_path)
                return None
                
        except Exception as e:
            logger.error(f"Error downloading/caching tile {tile_name}: {e}")
            return None
    
    def _analyze_tile_for_parcel(self, tile_path: str, parcel_shape) -> Optional[Dict]:
        """
        Analyze a single WorldCover tile for the parcel area
        
        Args:
            tile_path: Path to WorldCover tile file
            parcel_shape: Shapely geometry of parcel
            
        Returns:
            Tile analysis dictionary or None if failed
        """
        try:
            with rasterio.open(tile_path) as dataset:
                # Mask the raster data to the parcel boundary
                parcel_data, parcel_transform = rasterio.mask.mask(
                    dataset, [parcel_shape], crop=True, filled=True, nodata=0
                )
                
                # Flatten the array and remove nodata values
                parcel_pixels = parcel_data[0].flatten()
                valid_pixels = parcel_pixels[parcel_pixels != 0]  # Remove nodata
                
                if len(valid_pixels) == 0:
                    return None
                
                # Count pixels by land cover class
                unique_classes, class_counts = np.unique(valid_pixels, return_counts=True)
                class_pixels = dict(zip(unique_classes.astype(int), class_counts.astype(int)))
                
                # Calculate total area covered by valid pixels
                pixel_area_m2 = dataset.res[0] * dataset.res[1]  # Usually 100 m² for 10m resolution
                total_area_m2 = len(valid_pixels) * pixel_area_m2
                
                return {
                    'pixel_count': len(valid_pixels),
                    'area_m2': total_area_m2,
                    'class_pixels': class_pixels
                }
                
        except Exception as e:
            logger.error(f"Error analyzing tile {tile_path}: {e}")
            return None
    
    def _calculate_fragmentation_index(self, landcover_counts: Dict, total_pixels: int) -> float:
        """
        Calculate fragmentation index - measure of land use diversity
        
        Args:
            landcover_counts: Dictionary of land cover class pixel counts
            total_pixels: Total number of pixels
            
        Returns:
            Fragmentation index between 0 (single land use) and 1 (highly mixed)
        """
        if total_pixels == 0 or len(landcover_counts) <= 1:
            return 0.0
        
        # Calculate Shannon diversity index normalized to 0-1
        shannon_index = 0.0
        for pixel_count in landcover_counts.values():
            if pixel_count > 0:
                proportion = pixel_count / total_pixels
                shannon_index -= proportion * np.log(proportion)
        
        # Normalize by maximum possible diversity (log of number of classes)
        max_diversity = np.log(len(landcover_counts))
        if max_diversity > 0:
            return shannon_index / max_diversity
        else:
            return 0.0
    
    def _enhance_with_sentinel2_ndvi(self, parcel_geometry: Dict, parcel_id: str = None) -> Optional[Dict]:
        """
        Enhance land cover analysis with Sentinel-2 NDVI data
        
        Args:
            parcel_geometry: GeoJSON geometry dictionary
            parcel_id: Optional parcel identifier
            
        Returns:
            NDVI analysis dictionary or None if not available
        """
        try:
            # Try to get Sentinel-2 data for the parcel (function only takes geometry parameter)
            sentinel_data = self.blob_manager.get_sentinel2_data_for_parcel(parcel_geometry)
            
            if not sentinel_data:
                logger.debug(f"No Sentinel-2 data available for parcel {parcel_id}")
                return None
            
            # Calculate NDVI statistics
            ndvi_values = sentinel_data.get('ndvi_values', [])
            if not ndvi_values:
                return None
            
            ndvi_array = np.array(ndvi_values)
            valid_ndvi = ndvi_array[(ndvi_array >= -1) & (ndvi_array <= 1)]  # Valid NDVI range
            
            if len(valid_ndvi) == 0:
                return None
            
            # Calculate NDVI statistics
            ndvi_stats = {
                'mean': float(np.mean(valid_ndvi)),
                'median': float(np.median(valid_ndvi)),
                'std': float(np.std(valid_ndvi)),
                'min': float(np.min(valid_ndvi)),
                'max': float(np.max(valid_ndvi)),
                'pixel_count': len(valid_ndvi)
            }
            
            # Calculate vegetation health score (0-1 scale)
            vegetation_health = self._calculate_vegetation_health_score(ndvi_stats)
            
            # Assess correlation with land cover types
            forest_correlation = self._assess_ndvi_forest_correlation(ndvi_stats)
            crop_correlation = self._assess_ndvi_crop_correlation(ndvi_stats)
            
            return {
                'ndvi_stats': ndvi_stats,
                'vegetation_health': vegetation_health,
                'forest_correlation': forest_correlation,
                'crop_correlation': crop_correlation,
                'acquisition_date': sentinel_data.get('acquisition_date'),
                'cloud_coverage': sentinel_data.get('cloud_coverage', 0)
            }
            
        except Exception as e:
            logger.error(f"Error enhancing with Sentinel-2 NDVI: {e}")
            return None
    
    def _calculate_vegetation_health_score(self, ndvi_stats: Dict) -> float:
        """Calculate overall vegetation health score from NDVI statistics"""
        mean_ndvi = ndvi_stats['mean']
        
        # Map NDVI to health score (0-1)
        if mean_ndvi >= 0.6:
            return 0.9  # Excellent vegetation health
        elif mean_ndvi >= 0.4:
            return 0.7  # Good vegetation health
        elif mean_ndvi >= 0.2:
            return 0.5  # Moderate vegetation health
        elif mean_ndvi >= 0.0:
            return 0.3  # Poor vegetation health
        else:
            return 0.1  # Very poor vegetation health
    
    def _assess_ndvi_forest_correlation(self, ndvi_stats: Dict) -> float:
        """Assess how well NDVI values correlate with expected forest characteristics"""
        mean_ndvi = ndvi_stats['mean']
        std_ndvi = ndvi_stats['std']
        
        # Forests typically have NDVI 0.5-0.9 with moderate variability
        if 0.5 <= mean_ndvi <= 0.9 and std_ndvi <= 0.15:
            return 0.9  # Strong forest correlation
        elif 0.4 <= mean_ndvi <= 0.95 and std_ndvi <= 0.2:
            return 0.7  # Moderate forest correlation
        else:
            return 0.3  # Weak forest correlation
    
    def _assess_ndvi_crop_correlation(self, ndvi_stats: Dict) -> float:
        """Assess how well NDVI values correlate with expected crop characteristics"""
        mean_ndvi = ndvi_stats['mean']
        std_ndvi = ndvi_stats['std']
        
        # Crops can have variable NDVI depending on growth stage and type
        if 0.3 <= mean_ndvi <= 0.8 and std_ndvi >= 0.05:
            return 0.8  # Good crop correlation (shows variability)
        elif 0.2 <= mean_ndvi <= 0.9:
            return 0.6  # Moderate crop correlation
        else:
            return 0.2  # Weak crop correlation
    
    def get_biomass_allocation_factors(self, landcover_record: Dict) -> Dict:
        """
        Calculate biomass allocation factors based on sub-parcel land cover
        
        Args:
            landcover_record: Land cover analysis record
            
        Returns:
            Dictionary of biomass allocation factors by type
        """
        total_acres = landcover_record['total_parcel_acres']
        
        if total_acres == 0:
            return {'forest_factor': 0.0, 'crop_factor': 0.0, 'grassland_factor': 0.0}
        
        return {
            # Allocation factors (0-1) for applying biomass estimates
            'forest_factor': landcover_record['forest_area_acres'] / total_acres,
            'crop_factor': landcover_record['cropland_area_acres'] / total_acres,
            'grassland_factor': landcover_record['grassland_area_acres'] / total_acres,
            'developed_factor': landcover_record['developed_area_acres'] / total_acres,
            'water_factor': landcover_record['water_area_acres'] / total_acres,
            
            # Absolute areas for reference
            'forest_acres': landcover_record['forest_area_acres'],
            'cropland_acres': landcover_record['cropland_area_acres'],
            'grassland_acres': landcover_record['grassland_area_acres'],
            'productive_acres': (
                landcover_record['forest_area_acres'] + 
                landcover_record['cropland_area_acres'] + 
                landcover_record['grassland_area_acres']
            ),
            'total_acres': total_acres
        }
    
    def validate_landcover_analysis(self, landcover_record: Dict) -> Dict:
        """
        Validate land cover analysis results
        
        Args:
            landcover_record: Land cover analysis record
            
        Returns:
            Validation results dictionary
        """
        validation = {
            'valid': True,
            'warnings': [],
            'errors': []
        }
        
        # Check data completeness
        data_completeness = landcover_record.get('data_completeness', 0)
        if data_completeness < 0.8:
            validation['warnings'].append(f"Low data completeness: {data_completeness:.2%}")
        
        # Check for reasonable land cover percentages
        total_percentage = sum(landcover_record['landcover_percentages'].values())
        if abs(total_percentage - 100) > 5:
            validation['warnings'].append(f"Land cover percentages sum to {total_percentage:.1f}% (expected ~100%)")
        
        # Check for very high fragmentation
        fragmentation = landcover_record.get('fragmentation_index', 0)
        if fragmentation > 0.8:
            validation['warnings'].append(f"High land use fragmentation: {fragmentation:.2f}")
        
        # Check pixel count reasonableness
        pixel_count = landcover_record.get('pixel_count_total', 0)
        expected_pixels = landcover_record['total_parcel_acres'] * 4047 / 100  # Approx pixels for 10m resolution
        if pixel_count < expected_pixels * 0.5:
            validation['warnings'].append(f"Low pixel count: {pixel_count} (expected ~{expected_pixels:.0f})")
        
        return validation


# Global land cover analyzer instance
landcover_analyzer = LandCoverAnalyzer()