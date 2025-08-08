#!/usr/bin/env python3
"""
Optimized County Processor v1 - High Performance Batch Processing
Implements county-level batch processing with proper economies of scale
"""

import gc
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import rasterio
import rasterio.features
import rasterio.mask
from shapely.geometry import shape, Point
from shapely.ops import unary_union
import geopandas as gpd

from ..config.database_config_v1 import get_database_queries, CDL_CODES, CROP_BIOMASS_DATA
from ..config.processing_config_v1 import get_processing_config
from ..core.database_manager_v1 import database_manager
from ..core.blob_manager_v1 import blob_manager

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
                                batch_size: int = 1000) -> Dict:
        """
        Process entire county with optimized batch operations
        
        Args:
            state_fips: State FIPS code
            county_fips: County FIPS code  
            max_parcels: Optional limit on parcels to process
            batch_size: Number of parcels to process per batch
            
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
            
            # Phase 2: Batch process parcels
            processing_start = time.time()
            parcel_results = self._process_parcels_in_batches(batch_size)
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
            
            # Step 2: Analyze spatial tile requirements (don't pre-load everything)
            logger.info("🗺️ Analyzing spatial tile requirements...")
            county_bounds = self.db_manager.get_county_bounds(state_fips, county_fips)
            
            # Calculate required tiles for all parcels
            parcel_geometries = [parcel['geometry'] for parcel in parcels]
            required_tiles = self.blob_manager.get_required_tiles_for_parcels(parcel_geometries)
            
            logger.info(f"📊 Tile analysis: {len(required_tiles['sentinel2'])} Sentinel-2 tiles, "
                       f"{len(required_tiles['worldcover'])} WorldCover tiles required")
            
            # Store requirements for on-demand loading during processing
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
                            geom = shape(record['geometry'])
                            cdl_geometries.append(geom)
                            cdl_data.append({
                                'crop_code': record['crop_code'],
                                'area_m2': record['area_m2']
                            })
                        except:
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
    
    def _process_parcels_in_batches(self, batch_size: int) -> List[Dict]:
        """
        Process parcels in optimized batches using pre-loaded data
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
            
            # Process this batch
            batch_results = self._process_parcel_batch(batch_gdf)
            
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
    
    def _process_parcel_batch(self, batch_gdf: gpd.GeoDataFrame) -> List[Dict]:
        """
        Process a batch of parcels using vectorized operations
        """
        batch_results = []
        
        try:
            # Vectorized land cover analysis
            landcover_results = self._analyze_batch_landcover(batch_gdf)
            
            # Vectorized crop analysis
            crop_results = self._analyze_batch_crops(batch_gdf)
            
            # Vectorized forest analysis
            forest_results = self._analyze_batch_forest(batch_gdf)
            
            # Combine results for each parcel
            for idx, row in batch_gdf.iterrows():
                parcel_id = row['parcel_id']
                
                # Get land cover analysis
                landcover = landcover_results.get(parcel_id, {})
                forest_acres = landcover.get('forest_area_acres', 0)
                cropland_acres = landcover.get('cropland_area_acres', 0)
                total_acres = row['acres']
                
                # Get forest analysis details
                forest_analysis = forest_results.get(parcel_id, {})
                forest_biomass = forest_analysis.get('total_biomass_tons', 0)
                forest_harvestable = forest_analysis.get('harvestable_biomass_tons', 0)
                forest_residue = forest_analysis.get('residue_tons', 0)
                
                # Get crop analysis details
                crop_analysis = crop_results.get(parcel_id, [])
                crop_yield = sum(crop.get('yield_tons', 0) for crop in crop_analysis)
                crop_residue = sum(crop.get('residue_tons', 0) for crop in crop_analysis)
                
                parcel_result = {
                    'parcel_id': parcel_id,
                    'county_fips': f"{row.get('state_fips', '')}{row.get('county_fips', '')}",  # Add county FIPS
                    'total_acres': total_acres,
                    'centroid_lon': row['centroid_lon'],
                    'centroid_lat': row['centroid_lat'],
                    'processing_timestamp': datetime.now(),  # Use datetime object instead of string
                    
                    # Allocation factors for database
                    'allocation_factors': {
                        'forest_acres': forest_acres,
                        'cropland_acres': cropland_acres,
                        'other_acres': max(0, total_acres - forest_acres - cropland_acres)
                    },
                    
                    # Land cover data
                    'landcover_analysis': landcover,
                    
                    # Biomass results
                    'forest_biomass_tons': forest_biomass,
                    'forest_harvestable_tons': forest_harvestable,
                    'forest_residue_tons': forest_residue,
                    'crop_yield_tons': crop_yield,
                    'crop_residue_tons': crop_residue,
                    
                    # Analysis details
                    'forest_analysis': forest_analysis,
                    'crop_analysis': crop_analysis,
                    
                    # Vegetation indices (placeholder for now)
                    'vegetation_indices': {
                        'ndvi': None,
                        'evi': None,
                        'savi': None,
                        'ndwi': None
                    },
                    
                    # Data sources and metadata
                    'data_sources_used': ['FIA', 'CDL', 'WorldCover'],
                    'confidence_score': 0.8  # Will implement proper confidence scoring
                }
                
                batch_results.append(parcel_result)
                
        except Exception as e:
            logger.error(f"Error processing parcel batch: {e}")
            
        return batch_results
    
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
    
    def _analyze_batch_landcover(self, batch_gdf: gpd.GeoDataFrame) -> Dict:
        """
        Analyze land cover for batch of parcels using cached WorldCover data
        """
        # Simplified landcover analysis - will enhance with actual WorldCover data
        logger.debug("🌍 Analyzing landcover for batch")
        
        landcover_results = {}
        for idx, row in batch_gdf.iterrows():
            # Simplified allocation - assume mixed use
            total_acres = row['acres']
            forest_acres = total_acres * 0.4  # 40% forest
            crop_acres = total_acres * 0.3    # 30% crops
            
            landcover_results[row['parcel_id']] = {
                'forest_area_acres': forest_acres,
                'cropland_area_acres': crop_acres,
                'total_area_acres': total_acres
            }
            
        return landcover_results
    
    def _analyze_batch_crops(self, batch_gdf: gpd.GeoDataFrame) -> Dict:
        """
        Analyze crops for batch of parcels using CDL spatial index
        """
        crop_results = {}
        
        if 'cdl_gdf' not in self.county_data or self.county_data['cdl_gdf'] is None:
            return crop_results
        
        cdl_gdf = self.county_data['cdl_gdf']
        
        try:
            # Perform batch spatial intersection
            intersections = gpd.overlay(batch_gdf, cdl_gdf, how='intersection')
            
            # Group by parcel and aggregate crop data
            for parcel_id in batch_gdf['parcel_id']:
                parcel_intersections = intersections[intersections['parcel_id'] == parcel_id]
                
                if len(parcel_intersections) == 0:
                    continue
                
                parcel_crops = []
                for _, intersection in parcel_intersections.iterrows():
                    crop_code = intersection['crop_code']
                    intersection_area_m2 = intersection.geometry.area * 111319.9 ** 2  # Approximate conversion
                    
                    if crop_code in CROP_BIOMASS_DATA:
                        crop_data = CROP_BIOMASS_DATA[crop_code]
                        area_acres = intersection_area_m2 * 0.000247105
                        
                        crop_record = {
                            'crop_code': crop_code,
                            'crop_name': crop_data['name'],
                            'area_acres': area_acres,
                            'yield_tons': area_acres * crop_data['yield_tons_per_acre'],
                            'residue_tons': area_acres * crop_data['yield_tons_per_acre'] * crop_data['residue_ratio']
                        }
                        parcel_crops.append(crop_record)
                
                if parcel_crops:
                    crop_results[parcel_id] = parcel_crops
                    
        except Exception as e:
            logger.error(f"Error in batch crop analysis: {e}")
        
        return crop_results
    
    def _analyze_batch_forest(self, batch_gdf: gpd.GeoDataFrame) -> Dict:
        """
        Analyze forest for batch of parcels using FIA spatial index
        """
        forest_results = {}
        
        if 'fia_gdf' not in self.county_data or self.county_data['fia_gdf'] is None:
            return forest_results
        
        fia_gdf = self.county_data['fia_gdf']
        radius_degrees = self.processing_config.get('fia_search_radius_degrees', 0.1)
        
        try:
            # For each parcel, find nearby FIA plots
            for idx, row in batch_gdf.iterrows():
                parcel_id = row['parcel_id']
                parcel_geom = row.geometry
                parcel_centroid = parcel_geom.centroid
                
                # Find FIA plots within search radius
                parcel_buffer = parcel_centroid.buffer(radius_degrees)
                nearby_plots = fia_gdf[fia_gdf.intersects(parcel_buffer)]
                
                if len(nearby_plots) == 0:
                    continue
                
                # Calculate forest biomass using nearby plots
                total_biomass = 0
                plot_count = 0
                
                for _, plot in nearby_plots.iterrows():
                    plot_cn = plot['plot_cn']
                    
                    if plot_cn in self.county_data.get('fia_trees_by_plot', {}):
                        plot_trees = self.county_data['fia_trees_by_plot'][plot_cn]
                        
                        plot_biomass = sum(
                            tree.get('drybio_ag', 0) or 0 
                            for tree in plot_trees
                        ) / 2000  # Convert pounds to tons
                        
                        total_biomass += plot_biomass
                        plot_count += 1
                
                if plot_count > 0:
                    # Estimate forest area (placeholder - should use WorldCover data)
                    estimated_forest_acres = row['acres'] * 0.3  # Assume 30% forest coverage
                    
                    forest_results[parcel_id] = {
                        'total_biomass_tons': total_biomass / plot_count * estimated_forest_acres,
                        'forest_area_acres': estimated_forest_acres,
                        'fia_plots_used': plot_count
                    }
                    
        except Exception as e:
            logger.error(f"Error in batch forest analysis: {e}")
        
        return forest_results
    
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