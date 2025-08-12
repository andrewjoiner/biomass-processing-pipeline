#!/usr/bin/env python3
"""
Optimized County Processor v1 - High Performance Batch Processing
Implements county-level batch processing with proper economies of scale
"""

import gc
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import partial
from typing import Dict, List, Optional, Tuple

import numpy as np
import rasterio
import rasterio.features
import rasterio.mask
from shapely.geometry import shape, Point
from shapely.ops import unary_union
import geopandas as gpd

from ..config.database_config_v3 import get_database_queries, CDL_CODES, CROP_BIOMASS_DATA
from ..config.processing_config_v3 import get_processing_config
from ..core.database_manager_v3 import database_manager
from ..core.blob_manager_v3 import blob_manager
from .comprehensive_biomass_processor_v3 import ComprehensiveBiomassProcessor

logger = logging.getLogger(__name__)

class OptimizedCountyProcessor:
    """
    High-performance county processor that pre-loads shared data
    and processes parcels in vectorized batches
    """
    
    def __init__(self):
        self.db_manager = database_manager
        self.blob_manager = blob_manager
        self.processing_config = get_processing_config()
        
        # Initialize comprehensive processor for individual parcel analysis
        self.comprehensive_processor = ComprehensiveBiomassProcessor()
        
        # County-level cached data
        self.county_data = {
            'required_tiles': None,
            'county_bounds': None,
            'cdl_spatial_index': None,
            'fia_spatial_index': None,
            'parcel_gdf': None
        }
        
        # Performance tracking
        self.processing_stats = {
            'setup_time': 0,
            'parcel_processing_time': 0,
            'parcels_processed': 0,
            'batch_times': []
        }
    
    def process_county_optimized(self, state_fips: str, county_fips: str, 
                                max_parcels: Optional[int] = None,
                                batch_size: int = 100,  # Reduced from 1000 to optimize memory usage
                                max_workers: int = 4) -> Dict:
        """
        Process entire county with optimized batch operations and parallel processing
        
        Args:
            state_fips: State FIPS code
            county_fips: County FIPS code  
            max_parcels: Optional limit on parcels to process
            batch_size: Number of parcels to process per batch (default: 100, optimized for memory)
            max_workers: Maximum concurrent parcel processing threads (default: 4)
            
        Returns:
            Processing results dictionary
        """
        start_time = time.time()
        logger.info(f"🚀 Starting optimized processing for county {state_fips}{county_fips}")
        
        try:
            # Phase 1: County-level pre-processing (setup shared data)
            setup_start = time.time()
            setup_success = self._setup_county_data(state_fips, county_fips, max_parcels)
            setup_time = time.time() - setup_start
            self.processing_stats['setup_time'] = setup_time
            
            if not setup_success:
                return {
                    'success': False,
                    'error': 'County setup failed',
                    'processing_time': time.time() - start_time
                }
            
            logger.info(f"✅ County setup completed in {setup_time:.1f}s")
            
            # Phase 2: Batch process parcels with parallel processing
            processing_start = time.time()
            parcel_results = self._process_parcels_in_batches(batch_size, max_workers)
            processing_time = time.time() - processing_start
            self.processing_stats['parcel_processing_time'] = processing_time
            
            # Phase 3: Aggregate results
            total_time = time.time() - start_time
            results_summary = self._aggregate_results(parcel_results, total_time)
            
            # Cleanup
            self._cleanup_county_cache()
            
            logger.info(f"🎉 County processing completed in {total_time:.1f}s")
            logger.info(f"📊 Processed {len(parcel_results)} parcels at {len(parcel_results)/total_time:.1f} parcels/second")
            
            return {
                'success': True,
                'processing_summary': results_summary,
                'parcel_results': parcel_results,
                'performance_stats': self.processing_stats,
                'total_processing_time': total_time
            }
            
        except Exception as e:
            logger.error(f"💥 Error in optimized county processing: {e}")
            return {
                'success': False,
                'error': str(e),
                'processing_time': time.time() - start_time
            }
    
    def _setup_county_data(self, state_fips: str, county_fips: str, 
                          max_parcels: Optional[int]) -> bool:
        """
        Pre-load and cache all county-level data for batch processing
        """
        logger.info("📊 Setting up county-level data...")
        
        try:
            # Step 1: Load county parcels with geometries
            logger.info("📍 Loading county parcels...")
            parcels = self.db_manager.get_county_parcels(state_fips, county_fips, 
                                                        limit=max_parcels)
            if not parcels:
                logger.error("No parcels found for county")
                return False
            
            # Convert to GeoDataFrame for efficient spatial operations
            geometries = []
            parcel_data = []
            
            for parcel in parcels:
                try:
                    geom = shape(parcel['geometry'])
                    geometries.append(geom)
                    parcel_data.append({
                        'parcel_id': parcel['parcelid'],
                        'state_fips': state_fips,
                        'county_fips': county_fips,
                        'acres': parcel.get('acres', 0),
                        'centroid_lon': parcel.get('centroid_lon', 0),
                        'centroid_lat': parcel.get('centroid_lat', 0),
                        'postgis_geometry': parcel.get('postgis_geometry', '')
                    })
                except Exception as e:
                    logger.warning(f"Invalid geometry for parcel {parcel.get('parcelid')}: {e}")
                    continue
            
            if not geometries:
                logger.error("No valid geometries found")
                return False
            
            # Create GeoDataFrame
            self.county_data['parcel_gdf'] = gpd.GeoDataFrame(
                parcel_data, geometry=geometries, crs='EPSG:4326'
            )
            
            logger.info(f"📍 Loaded {len(geometries)} parcels")
            
            # Step 2: Download satellite data for county (like comprehensive processor)
            logger.info("🗺️ Analyzing spatial tile requirements...")
            county_bounds = self.db_manager.get_county_bounds(state_fips, county_fips)
            
            # Calculate required tiles for all parcels
            parcel_geometries = [parcel['geometry'] for parcel in parcels]
            required_tiles = self.blob_manager.get_required_tiles_for_parcels(parcel_geometries)
            
            logger.info(f"📊 Tile analysis: {len(required_tiles['sentinel2'])} Sentinel-2 tiles, "
                       f"{len(required_tiles['worldcover'])} WorldCover tiles required")
            
            # Analyze satellite data requirements (STREAMING ARCHITECTURE)
            logger.info("🗂️ Analyzing satellite tile requirements for county...")
            try:
                satellite_analysis = self.blob_manager.analyze_county_satellite_requirements(county_bounds)
                # Still download WorldCover tiles (they're smaller and needed for spatial index)
                worldcover_stats = self.blob_manager.download_worldcover_county_tiles(county_bounds)
                logger.info(f"✅ Analyzed {satellite_analysis['tiles_required']} Sentinel-2 tiles "
                           f"(~{satellite_analysis['estimated_data_size_gb']:.1f}GB), "
                           f"downloaded {worldcover_stats['worldcover_tiles']} WorldCover tiles")
                logger.info("📡 Sentinel-2 data will be streamed on-demand for each parcel")
            except Exception as e:
                logger.warning(f"Satellite analysis failed: {e}")
                satellite_analysis = {'tiles_required': 0, 'estimated_data_size_gb': 0}
            
            # Store requirements for reference
            self.county_data['required_tiles'] = required_tiles
            self.county_data['county_bounds'] = county_bounds
            
            # Step 3: Create spatial indices for fast lookups
            logger.info("🗂️ Building spatial indices...")
            self._build_spatial_indices(state_fips, county_fips, county_bounds)
            
            return True
            
        except Exception as e:
            logger.error(f"Error in county setup: {e}")
            return False
    
    def _build_spatial_indices(self, state_fips: str, county_fips: str, 
                              county_bounds: Tuple[float, float, float, float]):
        """
        Build spatial indices for CDL and FIA data within county
        """
        try:
            # CDL spatial index - pre-compute all crop intersections
            logger.info("🌾 Building CDL spatial index...")
            
            with self.db_manager.get_connection('crops') as conn:
                cursor = conn.cursor()
                
                # Get all CDL data within county bounds
                cursor.execute("""
                    SELECT 
                        crop_code,
                        ST_AsGeoJSON(geometry) as geometry,
                        ST_Area(geometry) as area_m2
                    FROM cdl.us_cdl_data 
                    WHERE crop_code NOT IN (111, 112, 121, 122, 123, 124, 131)
                    AND ST_Intersects(geometry, ST_MakeEnvelope(%s, %s, %s, %s, 4326))
                """, county_bounds)
                
                cdl_records = cursor.fetchall()
                logger.info(f"🌾 Found {len(cdl_records)} CDL records in county")
                
                # Build CDL GeoDataFrame
                if cdl_records:
                    cdl_geometries = []
                    cdl_data = []
                    
                    for record in cdl_records:
                        try:
                            geom = shape(json.loads(record['geometry']))
                            cdl_geometries.append(geom)
                            cdl_data.append({
                                'crop_code': record['crop_code'],
                                'area_m2': record['area_m2']
                            })
                        except Exception as e:
                            logger.warning(f"Failed to parse CDL geometry for crop {record.get('crop_code', 'unknown')}: {e}")
                            logger.debug(f"Raw geometry data: {record['geometry'][:100]}...")
                            continue
                    
                    if cdl_geometries:
                        self.county_data['cdl_gdf'] = gpd.GeoDataFrame(
                            cdl_data, geometry=cdl_geometries, crs='EPSG:4326'
                        )
                        # Create spatial index for fast intersections
                        self.county_data['cdl_gdf'].sindex
            
            # FIA spatial index - pre-load nearby forest plots
            logger.info("🌲 Building FIA spatial index...")
            
            with self.db_manager.get_connection('forestry') as conn:
                cursor = conn.cursor()
                
                # Get FIA plots within expanded county bounds (with buffer for search radius)
                buffer = self.processing_config.get('fia_search_radius_degrees', 0.1)
                expanded_bounds = (
                    county_bounds[0] - buffer, county_bounds[1] - buffer,
                    county_bounds[2] + buffer, county_bounds[3] + buffer
                )
                
                cursor.execute("""
                    SELECT 
                        cn as plot_cn, lat, lon, statecd, countycd,
                        plot as plot_id, invyr as inventory_year
                    FROM forestry.plot_local
                    WHERE lat BETWEEN %s AND %s 
                    AND lon BETWEEN %s AND %s
                    AND lat IS NOT NULL AND lon IS NOT NULL
                """, (expanded_bounds[1], expanded_bounds[3], 
                     expanded_bounds[0], expanded_bounds[2]))
                
                fia_plots = cursor.fetchall()
                logger.info(f"🌲 Found {len(fia_plots)} FIA plots in expanded county area")
                
                if fia_plots:
                    fia_points = []
                    fia_data = []
                    
                    for plot in fia_plots:
                        point = Point(plot['lon'], plot['lat'])
                        fia_points.append(point)
                        fia_data.append(dict(plot))
                    
                    self.county_data['fia_gdf'] = gpd.GeoDataFrame(
                        fia_data, geometry=fia_points, crs='EPSG:4326'
                    )
                    # Create spatial index
                    self.county_data['fia_gdf'].sindex
                    
                    # Pre-load tree data for these plots
                    plot_cns = [plot['plot_cn'] for plot in fia_plots]
                    trees = self.db_manager.get_fia_trees_for_plots(plot_cns)
                    
                    # Index trees by plot_cn for fast lookup
                    self.county_data['fia_trees_by_plot'] = {}
                    for tree in trees:
                        plot_cn = tree['plt_cn']
                        if plot_cn not in self.county_data['fia_trees_by_plot']:
                            self.county_data['fia_trees_by_plot'][plot_cn] = []
                        self.county_data['fia_trees_by_plot'][plot_cn].append(tree)
                    
                    logger.info(f"🌲 Pre-loaded {len(trees)} tree records")
                        
        except Exception as e:
            logger.error(f"Error building spatial indices: {e}")
    
    def _process_parcels_in_batches(self, batch_size: int, max_workers: int = 4) -> List[Dict]:
        """
        Process parcels in optimized batches using pre-loaded data with parallel processing
        
        Args:
            batch_size: Number of parcels per batch
            max_workers: Maximum concurrent threads for parcel processing
        """
        parcel_gdf = self.county_data['parcel_gdf']
        total_parcels = len(parcel_gdf)
        all_results = []
        
        logger.info(f"🔄 Processing {total_parcels} parcels in batches of {batch_size}")
        
        for i in range(0, total_parcels, batch_size):
            batch_start = time.time()
            batch_end = min(i + batch_size, total_parcels)
            batch_gdf = parcel_gdf.iloc[i:batch_end]
            
            logger.info(f"📦 Processing batch {i//batch_size + 1}: parcels {i+1}-{batch_end}")
            
            # Process this batch with parallel processing
            batch_results = self._process_parcel_batch(batch_gdf, max_workers)
            
            # Save batch results to database immediately after processing
            if batch_results:
                try:
                    logger.info(f"💾 Saving batch {i//batch_size + 1} to database...")
                    save_success = self._save_batch_results_to_database(batch_results, i//batch_size + 1)
                    if save_success:
                        logger.info(f"✅ Batch {i//batch_size + 1} saved to database successfully")
                    else:
                        logger.error(f"❌ Failed to save batch {i//batch_size + 1} to database")
                except Exception as e:
                    logger.error(f"❌ Error saving batch {i//batch_size + 1} to database: {e}")
            
            all_results.extend(batch_results)
            
            batch_time = time.time() - batch_start
            self.processing_stats['batch_times'].append(batch_time)
            
            parcels_in_batch = len(batch_results)
            rate = parcels_in_batch / batch_time if batch_time > 0 else 0
            logger.info(f"📦 Batch completed: {parcels_in_batch} parcels in {batch_time:.1f}s ({rate:.1f} parcels/sec)")
            
            # Force garbage collection between batches
            gc.collect()
        
        self.processing_stats['parcels_processed'] = len(all_results)
        return all_results
    
    def _process_parcel_batch(self, batch_gdf: gpd.GeoDataFrame, max_workers: int = 4) -> List[Dict]:
        """
        Process a batch of parcels using parallel comprehensive parcel analysis
        Uses concurrent processing with the working V3 analyzers
        
        Args:
            batch_gdf: GeoDataFrame containing parcels to process
            max_workers: Maximum number of concurrent worker threads
        """
        logger.debug(f"🔍 Processing batch of {len(batch_gdf)} parcels with {max_workers} concurrent workers")
        
        # Extract state/county FIPS from the first parcel
        first_row = batch_gdf.iloc[0]
        state_fips = first_row.get('state_fips', '17')  # Default to Illinois if not found
        county_fips = first_row.get('county_fips', '113')  # Default to McLean if not found
        
        # Create a partial function with fixed state/county FIPS
        process_single_parcel = partial(self._process_single_parcel_from_row, state_fips, county_fips)
        
        batch_results = []
        
        try:
            # Process parcels in parallel
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all parcels to the thread pool
                future_to_parcel = {}
                for idx, row in batch_gdf.iterrows():
                    future = executor.submit(process_single_parcel, row)
                    future_to_parcel[future] = row['parcel_id']
                
                # Collect results as they complete
                successful_results = 0
                for future in as_completed(future_to_parcel):
                    parcel_id = future_to_parcel[future]
                    try:
                        parcel_result = future.result(timeout=300)  # 5 minute timeout per parcel
                        if parcel_result:
                            batch_results.append(parcel_result)
                            successful_results += 1
                            logger.debug(f"✅ Parallel analysis successful for parcel {parcel_id}")
                        else:
                            logger.debug(f"⚠️ Parallel analysis returned no result for parcel {parcel_id}")
                    except Exception as e:
                        logger.error(f"❌ Parallel analysis failed for parcel {parcel_id}: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error in parallel batch processing: {e}")
            import traceback
            traceback.print_exc()
            
        logger.info(f"🎯 Parallel batch processing complete: {len(batch_results)} successful results from {len(batch_gdf)} parcels ({max_workers} workers)")
        return batch_results

    def _process_single_parcel_from_row(self, state_fips: str, county_fips: str, row) -> Optional[Dict]:
        """
        Process a single parcel from a GeoDataFrame row (helper for parallel processing)
        
        Args:
            state_fips: State FIPS code
            county_fips: County FIPS code  
            row: Single row from GeoDataFrame containing parcel data
            
        Returns:
            Parcel analysis result or None if failed
        """
        parcel_id = row['parcel_id']
        
        # Create parcel dictionary in format expected by comprehensive processor
        parcel = {
            'parcelid': parcel_id,
            'geometry': None,  # Will be filled from GeoDataFrame geometry
            'postgis_geometry': row.get('postgis_geometry', ''),
            'acres': row['acres'],
            'centroid_lon': row['centroid_lon'],
            'centroid_lat': row['centroid_lat']
        }
        
        # Extract geometry from GeoDataFrame
        try:
            geom = row.geometry
            if geom:
                # Convert to GeoJSON format expected by analyzers
                parcel['geometry'] = geom.__geo_interface__
            else:
                logger.warning(f"No geometry found for parcel {parcel_id}")
                return None
        except Exception as e:
            logger.warning(f"Failed to extract geometry for parcel {parcel_id}: {e}")
            return None
        
        # Process parcel with comprehensive V3 analysis
        try:
            parcel_result = self.comprehensive_processor.process_single_parcel_comprehensive(
                parcel, state_fips, county_fips
            )
            return parcel_result
        except Exception as e:
            logger.error(f"❌ V3 analysis failed for parcel {parcel_id}: {e}")
            return None
    
    def _save_batch_results_to_database(self, batch_results: List[Dict], batch_number: int) -> bool:
        """
        Save batch results to biomass output database
        
        Args:
            batch_results: List of parcel results from batch processing
            batch_number: Batch number for tracking
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not batch_results:
                logger.warning(f"No results to save for batch {batch_number}")
                return True
            
            logger.info(f"💾 Saving {len(batch_results)} results from batch {batch_number} to database...")
            
            # Use existing database manager's save_biomass_results method
            success = self.db_manager.save_biomass_results(batch_results)
            
            if success:
                logger.info(f"✅ Successfully saved batch {batch_number} ({len(batch_results)} records)")
            else:
                logger.error(f"❌ Failed to save batch {batch_number} to database")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error saving batch {batch_number} to database: {e}")
            return False
    
    
    def _aggregate_results(self, parcel_results: List[Dict], total_time: float) -> Dict:
        """
        Aggregate processing results into summary statistics
        """
        if not parcel_results:
            return {'error': 'No results to aggregate'}
        
        total_parcels = len(parcel_results)
        total_biomass = sum(
            result.get('forest_biomass_tons', 0) + 
            result.get('crop_yield_tons', 0) + 
            result.get('crop_residue_tons', 0)
            for result in parcel_results
        )
        
        # Calculate data quality metrics
        parcels_with_forest = sum(1 for result in parcel_results if result.get('forest_biomass_tons', 0) > 0)
        parcels_with_crops = sum(1 for result in parcel_results if result.get('crop_yield_tons', 0) > 0)
        forest_coverage_rate = parcels_with_forest / total_parcels if total_parcels > 0 else 0
        crop_coverage_rate = parcels_with_crops / total_parcels if total_parcels > 0 else 0
        
        # Calculate average confidence
        confidence_scores = [result.get('confidence_score', 0) for result in parcel_results if result.get('confidence_score', 0) > 0]
        average_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
        
        return {
            'parcels_processed': total_parcels,
            'processing_errors': 0,  # Will track errors properly
            'parcels_per_second': total_parcels / total_time if total_time > 0 else 0,
            'total_biomass_tons': total_biomass,
            'average_biomass_per_parcel': total_biomass / total_parcels if total_parcels > 0 else 0,
            'setup_time_seconds': self.processing_stats['setup_time'],
            'processing_time_seconds': self.processing_stats['parcel_processing_time'],
            'average_confidence': average_confidence,
            'forest_coverage_rate': forest_coverage_rate,
            'crop_coverage_rate': crop_coverage_rate
        }
    
    def _cleanup_county_cache(self):
        """
        Clean up county-level cached data to free memory
        """
        self.county_data.clear()
        gc.collect()
        logger.debug("🧹 County cache cleaned up")

# Create global instance
optimized_county_processor = OptimizedCountyProcessor()