#!/usr/bin/env python3
"""
Blob Manager v1 - Azure SDK with Coordinate-Aware Tile Management
High-performance blob storage manager that fixes coordinate transformation issues
"""

import io
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.mask import mask
from rasterio.warp import transform_bounds
from shapely.geometry import shape
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

from ..config.azure_config_v3 import (
    get_azure_config,
    get_sentinel2_config,
    get_worldcover_config,
    get_blob_paths
)
from .coordinate_utils_v3 import coordinate_transformer

logger = logging.getLogger(__name__)

class BlobManager:
    """
    High-performance Azure blob manager with coordinate-aware tile management
    Fixes critical coordinate transformation issues from original pipeline
    """
    
    def __init__(self):
        self.config = get_azure_config()
        self.sentinel2_config = get_sentinel2_config()
        self.worldcover_config = get_worldcover_config()
        self.blob_paths = get_blob_paths()
        
        # Initialize Azure blob client
        self._initialize_blob_client()
        
        # In-memory tile cache with spatial indexing
        self.sentinel2_cache = {}  # {tile_key: {bands, metadata, bounds}}
        self.worldcover_cache = {}  # {tile_name: {data, metadata, bounds}}
        
        # County tile index for streaming (metadata only, no tile data)
        self.county_tile_index = {}  # {tile_id: {blob_paths, bounds, transform}}
        
        # Performance tracking
        self.stats = {
            'downloads': 0,
            'cache_hits': 0,
            'total_bytes': 0,
            'total_time': 0.0
        }
    
    def _initialize_blob_client(self):
        """Initialize Azure blob service client with authentication"""
        try:
            # Try account key first
            if self.config['account_key']:
                logger.info(f"Attempting account key authentication for {self.config['account_url']}")
                self.blob_client = BlobServiceClient(
                    account_url=self.config['account_url'],
                    credential=self.config['account_key']
                )
                logger.info("✅ Using Azure storage account key authentication")
                
                # Test the connection
                try:
                    # Try to get a specific container to verify credentials work
                    container_client = self.blob_client.get_container_client('worldcover-data')
                    container_exists = container_client.exists()
                    logger.info(f"✅ Account key authentication verified - worldcover-data container exists: {container_exists}")
                except Exception as test_e:
                    logger.error(f"❌ Account key authentication failed during verification: {test_e}")
                    raise test_e
                    
            else:
                logger.error("No account key found in configuration")
                raise ValueError("AZURE_STORAGE_KEY environment variable is required")
                
        except Exception as e:
            logger.error(f"Failed to initialize Azure blob client: {e}")
            raise
    
    def download_blob_to_memory(self, container: str, blob_name: str) -> Optional[bytes]:
        """
        Download blob directly to memory with retry logic
        
        Args:
            container: Container name
            blob_name: Blob path/name
            
        Returns:
            Blob content as bytes or None if failed
        """
        start_time = time.time()
        
        try:
            blob_client = self.blob_client.get_blob_client(
                container=container,
                blob=blob_name
            )
            
            # Download blob content
            blob_data = blob_client.download_blob().readall()
            
            # Update stats
            self.stats['downloads'] += 1
            self.stats['total_bytes'] += len(blob_data)
            self.stats['total_time'] += time.time() - start_time
            
            logger.debug(f"Downloaded {blob_name} ({len(blob_data)} bytes)")
            return blob_data
            
        except ResourceNotFoundError:
            logger.warning(f"Blob not found: {container}/{blob_name}")
            return None
        except Exception as e:
            logger.error(f"Failed to download {container}/{blob_name}: {e}")
            return None
    
    def load_raster_from_blob(self, container: str, blob_name: str) -> Optional[Dict]:
        """
        Load raster data from blob into memory
        
        Args:
            container: Container name
            blob_name: Blob path/name
            
        Returns:
            Dictionary with raster data and metadata or None if failed
        """
        blob_data = self.download_blob_to_memory(container, blob_name)
        if not blob_data:
            return None
        
        try:
            with MemoryFile(blob_data) as memfile:
                with memfile.open() as dataset:
                    # Read raster data
                    data = dataset.read(1)  # Read first band
                    
                    # Get metadata
                    metadata = {
                        'crs': dataset.crs,
                        'transform': dataset.transform,
                        'bounds': dataset.bounds,
                        'shape': data.shape,
                        'dtype': data.dtype,
                        'nodata': dataset.nodata
                    }
                    
                    return {
                        'data': data,
                        'metadata': metadata,
                        'blob_name': blob_name
                    }
                    
        except Exception as e:
            logger.error(f"Failed to read raster from {blob_name}: {e}")
            return None
    
    def analyze_county_satellite_requirements(self, county_bounds: Tuple[float, float, float, float],
                                            period: str = 'august') -> Dict:
        """
        Analyze satellite data requirements for county without downloading tiles
        STREAMING ARCHITECTURE: Creates lightweight tile index for on-demand access
        
        Args:
            county_bounds: WGS84 bounds (min_lon, min_lat, max_lon, max_lat)
            period: Time period ('june', 'august', 'october')
            
        Returns:
            Dictionary with tile analysis results
        """
        logger.info(f"Analyzing satellite data requirements for bounds: {county_bounds}")
        
        try:
            from ..core.coordinate_utils_v3 import coordinate_transformer
            
            # Get available tiles from blob storage (metadata only)
            available_tiles = self._get_available_sentinel2_tiles()
            
            # Find intersecting tiles using coordinate transformer
            intersecting_tiles = coordinate_transformer.get_sentinel2_tiles_for_bounds(
                county_bounds, available_tiles
            )
            
            logger.info(f"Found {len(intersecting_tiles)} tiles required for county bounds")
            
            # Build tile index without downloading data
            tiles_indexed = 0
            estimated_data_size = 0
            
            for tile_info in intersecting_tiles:
                tile_id = tile_info['tile_id']
                
                # Get actual available date for this tile
                tile_date = self._get_available_date_for_tile(tile_id, period)
                
                # Build blob paths for all bands
                blob_paths = {}
                for band in self.sentinel2_config['bands']:
                    blob_paths[band] = self.blob_paths['sentinel2'].format(
                        period=self.sentinel2_config['periods'][period],
                        tile_id=tile_id,
                        date=tile_date,
                        band=band
                    )
                
                # Store tile metadata in index
                self.county_tile_index[tile_id] = {
                    'blob_paths': blob_paths,
                    'wgs84_bounds': tile_info.get('wgs84_bounds'),
                    'utm_bounds': tile_info.get('utm_bounds'),
                    'utm_epsg': tile_info.get('utm_epsg', 4326),
                    'date': tile_date,
                    'period': period
                }
                
                tiles_indexed += 1
                estimated_data_size += 250 * 4  # Estimate 250MB per band × 4 bands
            
            analysis_result = {
                'tiles_required': tiles_indexed,
                'estimated_data_size_mb': estimated_data_size,
                'estimated_data_size_gb': estimated_data_size / 1024,
                'period': period,
                'county_bounds': county_bounds
            }
            
            logger.info(f"✅ County satellite analysis complete: {tiles_indexed} tiles, "
                       f"~{estimated_data_size/1024:.1f}GB if downloaded")
            
            return analysis_result
            
        except Exception as e:
            logger.error(f"Failed to analyze satellite requirements: {e}")
            return {
                'tiles_required': 0,
                'estimated_data_size_mb': 0,
                'error': str(e)
            }

    def download_sentinel2_county_tiles(self, county_bounds: Tuple[float, float, float, float],
                                      period: str = 'august') -> Dict:
        """
        Download all Sentinel-2 tiles covering county bounds
        CRITICAL FIX: Uses proper coordinate transformations
        
        Args:
            county_bounds: WGS84 bounds (min_lon, min_lat, max_lon, max_lat)
            period: Time period ('june', 'august', 'october')
            
        Returns:
            Dictionary with download statistics
        """
        logger.info(f"Downloading Sentinel-2 tiles for bounds: {county_bounds}")
        
        # Get available tiles from blob storage
        available_tiles = self._get_available_sentinel2_tiles()
        
        # Find intersecting tiles using coordinate transformer
        intersecting_tiles = coordinate_transformer.get_sentinel2_tiles_for_bounds(
            county_bounds, available_tiles
        )
        
        logger.info(f"Found {len(intersecting_tiles)} intersecting tiles")
        
        # Download all required tiles for complete coverage
        tiles_to_download = intersecting_tiles
        logger.info(f"Will download {len(tiles_to_download)} tiles for complete coverage")
        
        downloaded = 0
        errors = 0
        
        for tile_info in tiles_to_download:
            tile_id = tile_info['tile_id']
            
            # Download all 4 bands for this tile
            bands_data = {}
            
            # Determine actual available date for this tile
            tile_date = self._get_available_date_for_tile(tile_id, period)
            
            for band in self.sentinel2_config['bands']:
                blob_name = self.blob_paths['sentinel2'].format(
                    period=self.sentinel2_config['periods'][period],
                    tile_id=tile_id,
                    date=tile_date,
                    band=band
                )
                
                raster_data = self.load_raster_from_blob(
                    self.config['containers']['sentinel2'], 
                    blob_name
                )
                
                if raster_data:
                    bands_data[band] = raster_data
                else:
                    errors += 1
            
            if len(bands_data) == 4:  # All bands downloaded successfully
                cache_key = f"{tile_id}_{period}_{tile_date}"
                
                # Handle missing utm_bounds gracefully (coordinate transformer issue)
                utm_bounds = tile_info.get('utm_bounds')
                if utm_bounds is None:
                    # Calculate utm_bounds from wgs84_bounds if missing
                    wgs84_bounds = tile_info.get('wgs84_bounds')
                    if wgs84_bounds:
                        utm_bounds = wgs84_bounds  # Fallback to wgs84_bounds
                        logger.debug(f"Using wgs84_bounds as fallback for utm_bounds for tile {tile_id}")
                
                self.sentinel2_cache[cache_key] = {
                    'tile_id': tile_id,
                    'period': period,
                    'date': tile_date,
                    'bands': bands_data,
                    'utm_epsg': tile_info.get('utm_epsg', 4326),
                    'utm_bounds': utm_bounds,
                    'wgs84_bounds': tile_info.get('wgs84_bounds')
                }
                downloaded += 1
                logger.info(f"Cached Sentinel-2 tile {tile_id}")
            else:
                logger.warning(f"Incomplete download for tile {tile_id}")
        
        return {
            'sentinel2_tiles': downloaded,
            'errors': errors,
            'cache_size': len(self.sentinel2_cache)
        }
    
    def download_worldcover_county_tiles(self, county_bounds: Tuple[float, float, float, float]) -> Dict:
        """
        Download WorldCover tiles covering county bounds
        
        Args:
            county_bounds: WGS84 bounds (min_lon, min_lat, max_lon, max_lat)
            
        Returns:
            Dictionary with download statistics
        """
        logger.info(f"Downloading WorldCover tiles for bounds: {county_bounds}")
        
        # Get required WorldCover tiles
        tile_names = coordinate_transformer.get_worldcover_tiles_for_bounds(county_bounds)
        
        logger.info(f"Found {len(tile_names)} WorldCover tiles needed")
        
        downloaded = 0
        errors = 0
        
        for tile_name in tile_names:
            # Check if already cached
            if tile_name in self.worldcover_cache:
                self.stats['cache_hits'] += 1
                continue
            
            blob_name = self.worldcover_config['container_path'] + '/' + \
                       self.worldcover_config['tile_pattern'].format(
                           lat=tile_name[:3], lon=tile_name[3:]
                       )
            
            raster_data = self.load_raster_from_blob(
                self.config['containers']['worldcover'],
                blob_name
            )
            
            if raster_data:
                self.worldcover_cache[tile_name] = {
                    'tile_name': tile_name,
                    'data': raster_data['data'],
                    'metadata': raster_data['metadata'],
                    'blob_name': blob_name
                }
                downloaded += 1
                logger.info(f"Cached WorldCover tile {tile_name}")
            else:
                errors += 1
        
        return {
            'worldcover_tiles': downloaded,
            'errors': errors,
            'cache_size': len(self.worldcover_cache)
        }
    
    def download_worldcover_tile(self, tile_name: str) -> Optional[Dict]:
        """
        Download a single WorldCover tile
        
        Args:
            tile_name: WorldCover tile name (can be short form like 'N39_W114' or full filename)
            
        Returns:
            Dictionary with tile data or None if failed
        """
        # Check if already cached
        if tile_name in self.worldcover_cache:
            self.stats['cache_hits'] += 1
            return self.worldcover_cache[tile_name]
        
        # Handle both short form and full filename
        if tile_name.startswith('ESA_WorldCover') and tile_name.endswith('.tif'):
            # Already a full filename, add directory prefix
            blob_name = f"worldcover_2021/{tile_name.replace('_Map', '')}"
        else:
            # Short form, construct full filename with correct path
            blob_name = f"worldcover_2021/ESA_WorldCover_10m_2021_v200_{tile_name}.tif"
        
        logger.info(f"Downloading WorldCover tile: {blob_name}")
        
        raster_data = self.load_raster_from_blob(
            self.config['containers']['worldcover'],
            blob_name
        )
        
        if raster_data:
            tile_data = {
                'tile_name': tile_name,
                'data': raster_data['data'],
                'metadata': raster_data['metadata'],
                'blob_name': blob_name
            }
            
            # Cache the tile
            self.worldcover_cache[tile_name] = tile_data
            logger.info(f"Successfully cached WorldCover tile {tile_name}")
            return tile_data
        else:
            logger.error(f"Failed to download WorldCover tile: {tile_name}")
            return None
    
    def get_sentinel2_data_for_parcel_streaming(self, parcel_geometry: Dict) -> Optional[Dict]:
        """
        Stream Sentinel-2 data for a specific parcel without pre-downloading entire tiles
        STREAMING ARCHITECTURE: Downloads only the pixels needed for the parcel
        
        Args:
            parcel_geometry: GeoJSON geometry dictionary
            
        Returns:
            Dictionary with clipped Sentinel-2 data or None
        """
        try:
            from ..core.coordinate_utils_v3 import coordinate_transformer
            from shapely.geometry import shape
            
            # Convert geometry to shapely
            geom = shape(parcel_geometry)
            parcel_bounds = geom.bounds  # WGS84 bounds (min_lon, min_lat, max_lon, max_lat)
            
            # Find tiles from our county index that intersect this parcel
            intersecting_tile_ids = []
            for tile_id, tile_info in self.county_tile_index.items():
                if tile_info.get('wgs84_bounds'):
                    if coordinate_transformer.bounds_intersect(parcel_bounds, tile_info['wgs84_bounds']):
                        intersecting_tile_ids.append(tile_id)
            
            if not intersecting_tile_ids:
                logger.debug("No indexed tiles found for parcel - index may be empty")
                return None
            
            logger.debug(f"Streaming from {len(intersecting_tile_ids)} indexed tiles for parcel")
            
            # Stream data from each intersecting tile
            all_clipped_bands = {}
            
            # Initialize band data containers
            for band in self.sentinel2_config['bands']:
                all_clipped_bands[band] = {'data_arrays': [], 'metadata': None}
            
            # Process each intersecting tile
            for tile_id in intersecting_tile_ids:
                tile_info = self.county_tile_index[tile_id]
                
                # Stream each band for this tile
                for band in self.sentinel2_config['bands']:
                    blob_path = tile_info['blob_paths'][band]
                    
                    try:
                        # Stream only the needed data from this tile/band
                        clipped_data = self._stream_parcel_window_from_tile(
                            blob_path, parcel_geometry, tile_info
                        )
                        
                        if clipped_data is not None:
                            all_clipped_bands[band]['data_arrays'].append(clipped_data)
                            if all_clipped_bands[band]['metadata'] is None:
                                all_clipped_bands[band]['metadata'] = {
                                    'crs': clipped_data.get('crs', 'EPSG:4326'),
                                    'nodata': clipped_data.get('nodata', 0)
                                }
                                
                    except Exception as e:
                        logger.warning(f"Failed to stream {band} from tile {tile_id}: {e}")
                        continue
            
            # Merge data from multiple tiles (use largest array for each band)
            final_bands = {}
            for band in self.sentinel2_config['bands']:
                if all_clipped_bands[band]['data_arrays']:
                    # Use the array with the most pixels
                    largest_array = max(all_clipped_bands[band]['data_arrays'], 
                                      key=lambda x: x['data'].size if x.get('data') is not None else 0)
                    final_bands[band] = largest_array
            
            if not final_bands:
                logger.debug("No valid Sentinel-2 data streamed for parcel")
                return None
            
            # Return structured result similar to cached approach
            return {
                'bands': final_bands,
                'metadata': all_clipped_bands[list(final_bands.keys())[0]]['metadata'],
                'source': 'streaming'
            }
            
        except Exception as e:
            logger.error(f"Failed to stream Sentinel-2 data for parcel: {e}")
            return None

    def _stream_parcel_window_from_tile(self, blob_path: str, parcel_geometry: Dict, 
                                      tile_info: Dict) -> Optional[Dict]:
        """
        Stream only the pixel window needed for a parcel from a Sentinel-2 tile
        PHASE 1: Uses full tile download and clipping (fallback approach)
        TODO PHASE 2: Implement true pixel window range requests
        
        Args:
            blob_path: Blob path for the Sentinel-2 band file
            parcel_geometry: GeoJSON geometry dictionary  
            tile_info: Tile information with bounds and metadata
            
        Returns:
            Dictionary with clipped raster data or None
        """
        try:
            # PHASE 1 IMPLEMENTATION: Download full tile and clip
            # This maintains compatibility while we test the architecture
            blob_data = self.download_blob_to_memory(
                self.config['containers']['sentinel2'], 
                blob_path
            )
            
            if not blob_data:
                logger.debug(f"Could not download blob: {blob_path}")
                return None
            
            # Process the tile data in memory and clip to parcel
            with MemoryFile(blob_data) as memfile:
                with memfile.open() as dataset:
                    # Transform geometry to raster CRS if needed
                    if dataset.crs != 'EPSG:4326':
                        from rasterio.warp import transform_geom
                        transformed_geom = transform_geom('EPSG:4326', dataset.crs, parcel_geometry)
                    else:
                        transformed_geom = parcel_geometry
                    
                    # Clip raster to geometry
                    from rasterio.mask import mask
                    clipped_data, clipped_transform = mask(
                        dataset, [transformed_geom], crop=True, nodata=dataset.nodata
                    )
                    
                    if clipped_data[0] is not None and clipped_data[0].size > 0:
                        return {
                            'data': clipped_data[0],
                            'transform': clipped_transform,
                            'crs': dataset.crs,
                            'nodata': dataset.nodata,
                            'source_blob': blob_path
                        }
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to stream window from {blob_path}: {e}")
            return None

    def get_sentinel2_data_for_parcel(self, parcel_geometry: Dict) -> Optional[Dict]:
        """
        Get Sentinel-2 data for a specific parcel from cache
        CRITICAL FIX: Proper coordinate handling for parcel-tile matching
        
        Args:
            parcel_geometry: GeoJSON geometry dictionary
            
        Returns:
            Dictionary with clipped Sentinel-2 data or None
        """
        try:
            # Convert geometry to shapely
            geom = shape(parcel_geometry)
            parcel_bounds = geom.bounds  # WGS84 bounds
            
            # Find cached tiles that intersect this parcel
            matching_tiles = []
            for cache_key, tile_data in self.sentinel2_cache.items():
                # Use coordinate transformer to check intersection properly
                if coordinate_transformer.bounds_intersect(parcel_bounds, tile_data['wgs84_bounds']):
                    matching_tiles.append(tile_data)
            
            if not matching_tiles:
                logger.debug("No Sentinel-2 tiles found for parcel")
                return None
            
            # Process ALL matching tiles for complete coverage
            all_clipped_bands = {}
            
            # Initialize band data containers
            for band in self.sentinel2_config['bands']:
                all_clipped_bands[band] = {'data_arrays': [], 'metadata': None}
            
            # Process each intersecting tile
            for tile_data in matching_tiles:
                # Clip each band to parcel geometry for this tile
                for band, raster_data in tile_data['bands'].items():
                    try:
                        with MemoryFile() as memfile:
                            # Create temporary raster
                            with memfile.open(
                                driver='GTiff',
                                height=raster_data['metadata']['shape'][0],
                                width=raster_data['metadata']['shape'][1],
                                count=1,
                                dtype=raster_data['metadata']['dtype'],
                                crs=raster_data['metadata']['crs'],
                                transform=raster_data['metadata']['transform']
                            ) as dataset:
                                dataset.write(raster_data['data'], 1)
                            
                            # Reopen for clipping
                            with memfile.open() as dataset:
                                # Transform geometry to raster CRS if needed
                                if dataset.crs != 'EPSG:4326':
                                    from rasterio.warp import transform_geom
                                    transformed_geom = transform_geom('EPSG:4326', dataset.crs, parcel_geometry)
                                else:
                                    transformed_geom = parcel_geometry
                                
                                # Clip raster to geometry
                                clipped_data, clipped_transform = mask(
                                    dataset, [transformed_geom], crop=True, nodata=dataset.nodata
                                )
                                
                                # Store clipped data from this tile
                                if clipped_data[0] is not None and clipped_data[0].size > 0:
                                    all_clipped_bands[band]['data_arrays'].append({
                                        'data': clipped_data[0],
                                        'transform': clipped_transform,
                                        'crs': dataset.crs,
                                        'tile_id': tile_data['tile_id']
                                    })
                                    if all_clipped_bands[band]['metadata'] is None:
                                        all_clipped_bands[band]['metadata'] = {
                                            'crs': dataset.crs,
                                            'nodata': dataset.nodata
                                        }
                                
                    except Exception as e:
                        logger.warning(f"Failed to clip {band} band from tile {tile_data['tile_id']}: {e}")
                        continue
            
            # Merge data from multiple tiles (simplified approach - use first valid data)
            # TODO: Implement proper mosaicking for overlapping tiles
            final_bands = {}
            for band in self.sentinel2_config['bands']:
                if all_clipped_bands[band]['data_arrays']:
                    # For now, use the largest clipped array
                    best_data = max(all_clipped_bands[band]['data_arrays'], 
                                   key=lambda x: x['data'].size)
                    final_bands[band] = {
                        'data': best_data['data'],
                        'transform': best_data['transform'],
                        'crs': best_data['crs']
                    }
            
            if len(final_bands) > 0:
                return {
                    'tile_ids': [tile['tile_id'] for tile in matching_tiles],
                    'bands': final_bands,
                    'acquisition_date': matching_tiles[0]['date']  # Use first tile's date
                }
            
        except Exception as e:
            logger.error(f"Error getting Sentinel-2 data for parcel: {e}")
        
        return None
    
    def get_worldcover_data_for_parcel(self, parcel_geometry: Dict) -> Optional[Dict]:
        """
        Get WorldCover data for a specific parcel from cache
        
        Args:
            parcel_geometry: GeoJSON geometry dictionary
            
        Returns:
            Dictionary with WorldCover analysis or None
        """
        try:
            geom = shape(parcel_geometry)
            parcel_bounds = geom.bounds  # WGS84 bounds
            
            # Find cached WorldCover tiles that intersect this parcel
            forest_pixels = 0
            total_pixels = 0
            
            for tile_name, tile_data in self.worldcover_cache.items():
                # Simple bounds check (could be enhanced)
                tile_bounds = tile_data['metadata']['bounds']
                
                if coordinate_transformer.bounds_intersect(parcel_bounds, tile_bounds):
                    try:
                        # Clip WorldCover data to parcel
                        with MemoryFile() as memfile:
                            with memfile.open(
                                driver='GTiff',
                                height=tile_data['metadata']['shape'][0],
                                width=tile_data['metadata']['shape'][1],
                                count=1,
                                dtype=tile_data['metadata']['dtype'],
                                crs=tile_data['metadata']['crs'],
                                transform=tile_data['metadata']['transform']
                            ) as dataset:
                                dataset.write(tile_data['data'], 1)
                            
                            with memfile.open() as dataset:
                                clipped_data, _ = mask(
                                    dataset, [parcel_geometry], crop=True, nodata=dataset.nodata
                                )
                                
                                # Count forest pixels (class 10)
                                valid_pixels = clipped_data[0] != dataset.nodata
                                forest_mask = clipped_data[0] == self.worldcover_config['forest_class']
                                
                                forest_pixels += np.sum(forest_mask)
                                total_pixels += np.sum(valid_pixels)
                                
                    except Exception as e:
                        logger.warning(f"Failed to process WorldCover tile {tile_name}: {e}")
                        continue
            
            if total_pixels > 0:
                forest_percentage = (forest_pixels / total_pixels) * 100
                # Convert pixels to area (10m resolution = 100 m² per pixel)
                forest_area_m2 = forest_pixels * 100
                forest_area_acres = forest_area_m2 * 0.000247105  # m² to acres
                
                return {
                    'forest_pixels': forest_pixels,
                    'total_pixels': total_pixels,
                    'forest_percentage': forest_percentage,
                    'forest_area_acres': forest_area_acres
                }
            
        except Exception as e:
            logger.error(f"Error getting WorldCover data for parcel: {e}")
        
        return None
    
    def get_cache_stats(self) -> Dict:
        """Get cache and performance statistics"""
        return {
            'sentinel2_tiles_cached': len(self.sentinel2_cache),
            'worldcover_tiles_cached': len(self.worldcover_cache),
            'total_downloads': self.stats['downloads'],
            'cache_hits': self.stats['cache_hits'],
            'total_bytes_downloaded': self.stats['total_bytes'],
            'total_download_time': self.stats['total_time']
        }
    
    def _get_available_sentinel2_tiles(self) -> List[str]:
        """
        Get list of available Sentinel-2 tiles from blob storage
        
        Returns:
            List of available tile IDs
        """
        try:
            container_client = self.blob_client.get_container_client(
                self.config['containers']['sentinel2']
            )
            
            tile_ids = set()
            for blob in container_client.list_blobs():
                # Extract tile ID from filenames like 'sentinel2_august/12TUL_20240830_B02.tif'
                if '/' in blob.name:
                    filename = blob.name.split('/')[-1]
                    if '_' in filename:
                        tile_id = filename.split('_')[0]
                        tile_ids.add(tile_id)
            
            available_tiles = sorted(list(tile_ids))
            logger.info(f"Found {len(available_tiles)} available Sentinel-2 tiles")
            return available_tiles
            
        except Exception as e:
            logger.error(f"Failed to get available Sentinel-2 tiles: {e}")
            # Fallback to empty list
            return []
    
    def _get_available_date_for_tile(self, tile_id: str, period: str) -> str:
        """
        Get available date for a specific Sentinel-2 tile
        
        Args:
            tile_id: Sentinel-2 tile ID (e.g., '12STA')
            period: Period name (e.g., 'august')
            
        Returns:
            Available date string (e.g., '20240829') or None if not found
        """
        try:
            container_client = self.blob_client.get_container_client(
                self.config['containers']['sentinel2']
            )
            
            period_folder = self.sentinel2_config['periods'][period]
            prefix = f"{period_folder}/{tile_id}_"
            
            # Find the first available date for this tile
            for blob in container_client.list_blobs(name_starts_with=prefix):
                # Extract date from filename like 'sentinel2_august/12STA_20240829_B02.tif'
                filename = blob.name.split('/')[-1]
                parts = filename.split('_')
                if len(parts) >= 3:
                    date_str = parts[1]  # Should be '20240829'
                    if len(date_str) == 8 and date_str.isdigit():
                        logger.debug(f"Found date {date_str} for tile {tile_id}")
                        return date_str
            
            logger.warning(f"No available date found for tile {tile_id} in period {period}")
            return '20240829'  # Fallback date
            
        except Exception as e:
            logger.error(f"Error finding date for tile {tile_id}: {e}")
            return '20240829'  # Fallback date
    
    def get_required_tiles_for_parcels(self, parcel_geometries: List[Dict]) -> Dict[str, List[str]]:
        """
        Calculate minimal set of tiles required for a list of parcels
        
        Args:
            parcel_geometries: List of GeoJSON geometry dictionaries
            
        Returns:
            Dictionary with 'sentinel2' and 'worldcover' tile lists
        """
        if not parcel_geometries:
            return {'sentinel2': [], 'worldcover': []}
        
        try:
            # Calculate combined bounds of all parcels
            from shapely.geometry import shape
            from shapely.ops import unary_union
            
            # Convert to shapely geometries and get union
            shapes = []
            for geom in parcel_geometries:
                try:
                    shapes.append(shape(geom))
                except Exception as e:
                    logger.warning(f"Invalid parcel geometry: {e}")
                    continue
            
            if not shapes:
                return {'sentinel2': [], 'worldcover': []}
            
            # Get combined bounds
            combined_geom = unary_union(shapes)
            combined_bounds = combined_geom.bounds  # (min_x, min_y, max_x, max_y)
            
            # Get required Sentinel-2 tiles
            available_s2_tiles = self._get_available_sentinel2_tiles()
            required_s2_tiles = coordinate_transformer.get_sentinel2_tiles_for_bounds(
                combined_bounds, available_s2_tiles
            )
            s2_tile_ids = [tile['tile_id'] for tile in required_s2_tiles]
            
            # Get required WorldCover tiles
            required_wc_tiles = coordinate_transformer.get_worldcover_tiles_for_bounds(
                combined_bounds
            )
            
            logger.info(f"Required tiles for {len(parcel_geometries)} parcels: "
                       f"{len(s2_tile_ids)} Sentinel-2, {len(required_wc_tiles)} WorldCover")
            
            return {
                'sentinel2': s2_tile_ids,
                'worldcover': required_wc_tiles
            }
            
        except Exception as e:
            logger.error(f"Error calculating required tiles: {e}")
            return {'sentinel2': [], 'worldcover': []}
    
    def clear_cache(self):
        """Clear all cached tile data"""
        self.sentinel2_cache.clear()
        self.worldcover_cache.clear()
        logger.info("Cleared tile cache")


# Global blob manager instance
blob_manager = BlobManager()