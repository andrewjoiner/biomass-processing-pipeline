#!/usr/bin/env python3
"""
County Processor v1 - County-Wide Biomass Processing Pipeline
Clean implementation of the county-first approach with all critical fixes applied
"""

import csv
import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Optional

from ..analyzers.forest_analyzer_v3 import forest_analyzer
from ..analyzers.crop_analyzer_v3 import crop_analyzer
from ..analyzers.landcover_analyzer_v3 import landcover_analyzer
from ..analyzers.vegetation_analyzer_v3 import vegetation_analyzer
from ..config.processing_config_v3 import get_processing_config, get_output_schema
from ..core.database_manager_v3 import database_manager
from ..core.blob_manager_v3 import blob_manager
from ..utils.logging_utils_v1 import ProcessingMetrics, get_processing_logger
from ..utils.geometry_utils_v1 import validate_geometry, get_geometry_centroid

logger = logging.getLogger(__name__)

class CountyProcessor:
    """
    County-wide biomass processing with optimized tile management and bulk operations
    Implements all critical fixes from original pipeline issues
    """
    
    def __init__(self, output_dir: str = 'results'):
        self.output_dir = output_dir
        self.processing_config = get_processing_config()
        self.output_schema = get_output_schema()
        
        # Initialize components
        self.db_manager = database_manager
        self.blob_manager = blob_manager
        self.vegetation_analyzer = vegetation_analyzer
        self.crop_analyzer = crop_analyzer
        self.forest_analyzer = forest_analyzer
        
        # Processing metrics
        self.metrics = ProcessingMetrics('county_processor')
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
    
    def process_county(self, fips_state: str, fips_county: str, 
                      parcel_limit: Optional[int] = None) -> Dict:
        """
        Process all parcels in a county using the optimized county-first approach
        
        Args:
            fips_state: 2-digit state FIPS code (e.g., '19')
            fips_county: 3-digit county FIPS code (e.g., '055')
            parcel_limit: Optional limit on number of parcels to process
            
        Returns:
            Processing statistics dictionary
        """
        processing_id = f"{fips_state}{fips_county}_{int(time.time())}"
        proc_logger = get_processing_logger('county_processor', processing_id)
        
        proc_logger.info(f"Starting county processing for FIPS {fips_state}{fips_county}")
        start_time = time.time()
        
        # Initialize processing statistics
        stats = {
            'county_fips': f"{fips_state}{fips_county}",
            'processing_id': processing_id,
            'start_time': datetime.now().isoformat(),
            'parcels_loaded': 0,
            'parcels_processed': 0,
            'parcels_failed': 0,
            'parcels_skipped': 0,
            'crop_records': 0,
            'forest_records': 0,
            'tile_download_time': 0.0,
            'processing_time': 0.0,
            'total_time': 0.0,
            'output_files': []
        }
        
        try:
            # Step 1: Get county bounds and validate
            print(f"🗺️  STEP 1: Getting county bounds for FIPS {fips_state}{fips_county}...")
            proc_logger.info("Getting county bounds...")
            county_bounds = self.db_manager.get_county_bounds(fips_state, fips_county)
            
            if not county_bounds:
                print(f"❌ Could not determine bounds for county {fips_state}{fips_county}")
                proc_logger.error(f"Could not determine bounds for county {fips_state}{fips_county}")
                stats['error'] = 'county_bounds_not_found'
                return stats
            
            print(f"✅ County bounds found: {county_bounds}")
            proc_logger.info(f"County bounds: {county_bounds}")
            self.metrics.set_gauge('county_bounds_width', county_bounds[2] - county_bounds[0])
            self.metrics.set_gauge('county_bounds_height', county_bounds[3] - county_bounds[1])
            
            # Step 2: Download all tiles for county (CRITICAL FIX: proper coordinate handling)
            print(f"🛰️  STEP 2: Downloading satellite tiles for county...")
            proc_logger.info("Downloading tiles for county...")
            tile_start = time.time()
            
            # Download Sentinel-2 tiles
            sentinel2_stats = self.blob_manager.download_sentinel2_county_tiles(
                county_bounds, period=self.processing_config['sentinel2_period']
            )
            
            # Download WorldCover tiles
            worldcover_stats = self.blob_manager.download_worldcover_county_tiles(county_bounds)
            
            tile_download_time = time.time() - tile_start
            stats['tile_download_time'] = tile_download_time
            
            proc_logger.info(f"Downloaded {sentinel2_stats['sentinel2_tiles']} Sentinel-2 tiles and "
                           f"{worldcover_stats['worldcover_tiles']} WorldCover tiles in {tile_download_time:.1f}s")
            
            self.metrics.set_gauge('sentinel2_tiles_downloaded', sentinel2_stats['sentinel2_tiles'])
            self.metrics.set_gauge('worldcover_tiles_downloaded', worldcover_stats['worldcover_tiles'])
            
            # Step 3: Load parcels for county
            print(f"📦 STEP 3: Loading parcels from database...")
            proc_logger.info("Loading parcels from database...")
            parcels = self.db_manager.get_county_parcels(fips_state, fips_county, parcel_limit)
            
            if not parcels:
                print(f"❌ No parcels found for county {fips_state}{fips_county}")
                proc_logger.warning(f"No parcels found for county {fips_state}{fips_county}")
                stats['parcels_loaded'] = 0
                return stats
            
            stats['parcels_loaded'] = len(parcels)
            print(f"✅ Loaded {len(parcels)} parcels for processing")
            proc_logger.info(f"Loaded {len(parcels)} parcels for processing")
            
            # Step 4: Bulk CDL analysis (OPTIMIZATION: county-wide query)
            print(f"🌾 STEP 4: Performing bulk CDL analysis...")
            proc_logger.info("Performing bulk CDL analysis...")
            crop_intersections_bulk = self.crop_analyzer.analyze_county_crops_bulk(
                fips_state, fips_county, parcels
            )
            print(f"✅ Bulk CDL analysis completed for {len(crop_intersections_bulk)} parcels")
            proc_logger.info(f"Bulk CDL analysis completed for {len(crop_intersections_bulk)} parcels")
            
            # Step 5: Process parcels in batches
            print(f"⚙️  STEP 5: Processing {len(parcels)} parcels in batches...")
            processing_start = time.time()
            batch_size = self.processing_config['batch_size']
            batch_results = []
            
            total_batches = (len(parcels) + batch_size - 1) // batch_size
            print(f"📊 Will process {total_batches} batches of {batch_size} parcels each")
            
            for i in range(0, len(parcels), batch_size):
                batch = parcels[i:i + batch_size]
                batch_number = i // batch_size + 1
                
                print(f"🔄 Processing batch {batch_number}/{total_batches} ({len(batch)} parcels)")
                proc_logger.info(f"Processing batch {batch_number} ({len(batch)} parcels)")
                
                batch_result = self._process_parcel_batch(
                    batch, crop_intersections_bulk, proc_logger
                )
                
                batch_results.extend(batch_result)
                
                # Update statistics
                stats['parcels_processed'] += len([r for r in batch_result if r['status'] == 'success'])
                stats['parcels_failed'] += len([r for r in batch_result if r['status'] == 'failed'])
                stats['parcels_skipped'] += len([r for r in batch_result if r['status'] == 'skipped'])
                
                # Save results periodically
                if len(batch_results) >= self.processing_config['save_frequency']:
                    output_file = self._save_batch_results(
                        batch_results, fips_state, fips_county, batch_number
                    )
                    if output_file:
                        stats['output_files'].append(output_file)
                    
                    # Count records
                    for result in batch_results:
                        if result['status'] == 'success':
                            stats['crop_records'] += len(result.get('crop_records', []))
                            stats['forest_records'] += len(result.get('forest_records', []))
                    
                    batch_results.clear()  # Clear to free memory
                    proc_logger.info(f"Saved batch results, processed {stats['parcels_processed']} parcels total")
            
            # Save any remaining results
            if batch_results:
                output_file = self._save_batch_results(
                    batch_results, fips_state, fips_county, 'final'
                )
                if output_file:
                    stats['output_files'].append(output_file)
                
                # Count final records
                for result in batch_results:
                    if result['status'] == 'success':
                        stats['crop_records'] += len(result.get('crop_records', []))
                        stats['forest_records'] += len(result.get('forest_records', []))
            
            stats['processing_time'] = time.time() - processing_start
            stats['total_time'] = time.time() - start_time
            
            # Log final statistics
            proc_logger.info(f"County processing completed: {stats['parcels_processed']} processed, "
                           f"{stats['parcels_failed']} failed, {stats['parcels_skipped']} skipped")
            proc_logger.info(f"Generated {stats['crop_records']} crop records and "
                           f"{stats['forest_records']} forest records")
            proc_logger.info(f"Total processing time: {stats['total_time']:.1f}s")
            
            self.metrics.set_gauge('parcels_processed', stats['parcels_processed'])
            self.metrics.set_gauge('total_processing_time', stats['total_time'])
            self.metrics.log_metrics(proc_logger)
            
            return stats
            
        except Exception as e:
            proc_logger.error(f"County processing failed: {e}")
            stats['error'] = str(e)
            stats['total_time'] = time.time() - start_time
            return stats
    
    def _process_parcel_batch(self, parcels: List[Dict], crop_intersections_bulk: Dict,
                            proc_logger: logging.Logger) -> List[Dict]:
        """
        Process a batch of parcels
        
        Args:
            parcels: List of parcel dictionaries
            crop_intersections_bulk: Bulk CDL intersection results
            proc_logger: Logger instance
            
        Returns:
            List of processing results
        """
        batch_results = []
        
        for parcel in parcels:
            try:
                result = self._process_single_parcel(parcel, crop_intersections_bulk)
                batch_results.append(result)
                
                if result['status'] == 'success':
                    self.metrics.increment('parcels_processed_success')
                elif result['status'] == 'failed':
                    self.metrics.increment('parcels_processed_failed')
                else:
                    self.metrics.increment('parcels_processed_skipped')
                    
            except Exception as e:
                proc_logger.warning(f"Failed to process parcel {parcel['parcel_id']}: {e}")
                batch_results.append({
                    'parcel_id': parcel['parcel_id'],
                    'status': 'failed',
                    'error': str(e)
                })
                self.metrics.increment('parcels_processed_failed')
        
        return batch_results
    
    def _process_single_parcel(self, parcel: Dict, crop_intersections_bulk: Dict) -> Dict:
        """
        Process a single parcel for biomass analysis
        
        Args:
            parcel: Parcel dictionary with geometry and metadata
            crop_intersections_bulk: Bulk CDL intersection results
            
        Returns:
            Processing result dictionary
        """
        parcel_id = parcel['parcel_id']
        
        # Validate parcel geometry
        if not validate_geometry(parcel['geometry']):
            return {
                'parcel_id': parcel_id,
                'status': 'skipped',
                'reason': 'invalid_geometry'
            }
        
        # Skip very small parcels
        if parcel['acres'] < self.processing_config['min_parcel_area_acres']:
            return {
                'parcel_id': parcel_id,
                'status': 'skipped',
                'reason': 'too_small'
            }
        
        result = {
            'parcel_id': parcel_id,
            'status': 'processing',
            'parcel_acres': parcel['acres'],
            'centroid_lon': parcel['centroid_lon'],
            'centroid_lat': parcel['centroid_lat'],
            'crop_records': [],
            'forest_records': [],
            'processing_time': 0.0
        }
        
        processing_start = time.time()
        
        try:
            # Step 1: Analyze vegetation indices
            vegetation_indices = self.vegetation_analyzer.analyze_parcel_vegetation(parcel['geometry'])
            
            # Step 2: Analyze crops using bulk CDL data
            crop_records = crop_intersections_bulk.get(parcel_id, [])
            if crop_records:
                # Add vegetation correlation to crop records
                for crop_record in crop_records:
                    if vegetation_indices:
                        crop_record.update(self.crop_analyzer._assess_vegetation_correlation(
                            crop_record['source_code'], vegetation_indices
                        ))
                result['crop_records'] = crop_records
            
            # Step 3: Analyze forest coverage
            forest_record = self.forest_analyzer.analyze_parcel_forest(
                parcel['geometry'],
                parcel['postgis_geometry'],
                parcel['acres'],
                vegetation_indices
            )
            
            if forest_record:
                result['forest_records'] = [forest_record]
            
            # Step 4: Update result status
            result['processing_time'] = time.time() - processing_start
            
            if result['crop_records'] or result['forest_records']:
                result['status'] = 'success'
                result['vegetation_indices'] = vegetation_indices
            else:
                result['status'] = 'skipped'
                result['reason'] = 'no_biomass_detected'
            
            return result
            
        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            result['processing_time'] = time.time() - processing_start
            return result
    
    def _save_batch_results(self, batch_results: List[Dict], fips_state: str, 
                          fips_county: str, batch_identifier) -> Optional[str]:
        """
        Save batch results directly to PostgreSQL database following FIA MPC schema
        
        Args:
            batch_results: List of processing results
            fips_state: State FIPS code
            fips_county: County FIPS code
            batch_identifier: Batch number or identifier
            
        Returns:
            Summary message about records saved or None if failed
        """
        try:
            # Connect to the biomass_output database (where we'll store biomass analysis tables)
            with self.db_manager.get_connection('biomass_output') as conn:
                cursor = conn.cursor()
                
                records_saved = 0
                parcels_processed = 0
                
                for result in batch_results:
                    if result['status'] != 'success':
                        continue
                    
                    parcels_processed += 1
                    
                    # Step 1: Insert parcel summary record
                    summary_id = self._insert_parcel_summary(
                        cursor, result, fips_state, fips_county
                    )
                    
                    if not summary_id:
                        logger.warning(f"Failed to insert summary for parcel {result['parcel_id']}")
                        continue
                    
                    # Step 2: Insert crop details if available
                    crop_records_inserted = self._insert_crop_details(
                        cursor, summary_id, result.get('crop_records', [])
                    )
                    
                    # Step 3: Insert forestry metrics if available  
                    forestry_records_inserted = self._insert_forestry_metrics(
                        cursor, summary_id, result.get('forest_records', []), result['parcel_id']
                    )
                    
                    records_saved += 1 + crop_records_inserted + forestry_records_inserted
                
                # Commit all changes
                conn.commit()
                
                logger.info(f"Successfully saved {records_saved} database records for "
                          f"{parcels_processed} parcels in batch {batch_identifier}")
                
                return f"Database: {records_saved} records saved for {parcels_processed} parcels"
                
        except Exception as e:
            logger.error(f"Failed to save batch results to database: {e}")
            # Fallback to CSV if database fails
            return self._save_batch_results_csv_fallback(
                batch_results, fips_state, fips_county, batch_identifier
            )
    
    def _insert_parcel_summary(self, cursor, result: Dict, fips_state: str, fips_county: str) -> Optional[int]:
        """Insert parcel summary record and return the ID"""
        try:
            # Calculate summary metrics
            total_crop_acres = sum(
                crop.get('area_acres', 0) for crop in result.get('crop_records', [])
            )
            total_crop_residue = sum(
                crop.get('harvestable_residue_tons', 0) for crop in result.get('crop_records', [])
                if crop.get('harvestable_residue_tons')
            )
            total_forest_biomass = sum(
                forest.get('total_biomass_tons', 0) for forest in result.get('forest_records', [])
                if forest.get('total_biomass_tons')
            )
            forest_residue = sum(
                forest.get('residue_biomass_tons', 0) for forest in result.get('forest_records', [])  
                if forest.get('residue_biomass_tons')
            )
            
            # Insert summary record
            cursor.execute("""
                INSERT INTO parcel_crop_summary 
                (parcel_id, county_fips, year, processing_date, total_acres, 
                 centroid_lon, centroid_lat, total_crop_acres, total_crop_residue_tons,
                 total_forest_biomass_tons, forest_residue_tons, data_sources)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (parcel_id, year) DO UPDATE SET
                    processing_date = EXCLUDED.processing_date,
                    total_crop_acres = EXCLUDED.total_crop_acres,
                    total_crop_residue_tons = EXCLUDED.total_crop_residue_tons,
                    total_forest_biomass_tons = EXCLUDED.total_forest_biomass_tons,
                    forest_residue_tons = EXCLUDED.forest_residue_tons,
                    data_sources = EXCLUDED.data_sources
                RETURNING id
            """, (
                result['parcel_id'],
                f"{fips_state}{fips_county}",
                2024,  # Current processing year
                datetime.now(),
                result['parcel_acres'],
                result['centroid_lon'],
                result['centroid_lat'],
                total_crop_acres,
                total_crop_residue,
                total_forest_biomass,
                forest_residue,
                ['CDL', 'FIA', 'WorldCover', 'Sentinel2']
            ))
            
            summary_id = cursor.fetchone()[0]
            return summary_id
            
        except Exception as e:
            logger.error(f"Failed to insert parcel summary: {e}")
            return None
    
    def _insert_crop_details(self, cursor, summary_id: int, crop_records: List[Dict]) -> int:
        """Insert crop detail records and return count inserted"""
        if not crop_records:
            return 0
        
        try:
            # Clear existing crop details for this summary
            cursor.execute("DELETE FROM parcel_crop_details WHERE parcel_summary_id = %s", (summary_id,))
            
            records_inserted = 0
            for crop in crop_records:
                cursor.execute("""
                    INSERT INTO parcel_crop_details
                    (parcel_summary_id, crop_code, crop_name, area_acres, area_percentage,
                     yield_tons, residue_tons_wet, residue_tons_dry, harvestable_residue_tons,
                     moisture_content, residue_ratio, confidence_score, data_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    summary_id,
                    crop.get('source_code', 0),
                    crop.get('source_name', 'Unknown'),
                    crop.get('area_acres', 0),
                    crop.get('coverage_percent', 0),
                    crop.get('yield_tons', 0),
                    crop.get('residue_tons_wet', 0),
                    crop.get('residue_tons_dry', 0),
                    crop.get('harvestable_residue_tons', 0),
                    crop.get('moisture_content', 0),
                    crop.get('residue_ratio', 0),
                    crop.get('confidence_score', 0.5),
                    'CDL'
                ))
                records_inserted += 1
            
            return records_inserted
            
        except Exception as e:
            logger.error(f"Failed to insert crop details: {e}")
            return 0
    
    def _insert_forestry_metrics(self, cursor, summary_id: int, forest_records: List[Dict], 
                               parcel_id: str) -> int:
        """Insert forestry metrics records and return count inserted"""
        if not forest_records:
            return 0
        
        try:
            # Clear existing forestry metrics for this summary
            cursor.execute("DELETE FROM parcel_forestry_metrics WHERE parcel_summary_id = %s", (summary_id,))
            
            records_inserted = 0
            for forest in forest_records:
                cursor.execute("""
                    INSERT INTO parcel_forestry_metrics
                    (parcel_summary_id, parcel_id, total_biomass_tons, bole_biomass_tons,
                     residue_biomass_tons, stand_age_avg, forest_type_dominant,
                     harvest_probability, last_treatment_years, confidence_score,
                     fia_plots_used, estimation_method, data_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    summary_id,
                    parcel_id,
                    forest.get('total_biomass_tons', 0),
                    forest.get('bole_biomass_tons', 0),
                    forest.get('residue_biomass_tons', 0),
                    forest.get('stand_age_avg', 0),
                    forest.get('forest_type_dominant', 'Unknown'),
                    forest.get('harvest_probability', 0),
                    forest.get('last_treatment_years', 0),
                    forest.get('confidence_score', 0.5),
                    forest.get('fia_plots_used', 0),
                    forest.get('estimation_method', 'Regional_Estimate'),
                    forest.get('data_source', 'WorldCover')
                ))
                records_inserted += 1
            
            return records_inserted
        
        except Exception as e:
            logger.error(f"Failed to insert forestry metrics: {e}")
            return 0
    
    def _save_batch_results_csv_fallback(self, batch_results: List[Dict], fips_state: str, 
                                       fips_county: str, batch_identifier) -> Optional[str]:
        """Fallback CSV save method if database insertion fails"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"biomass_inventory_FALLBACK_{fips_state}{fips_county}_batch_{batch_identifier}_{timestamp}.csv"
            filepath = os.path.join(self.output_dir, filename)
            
            # Convert results to CSV records (simplified)
            csv_records = []
            
            for result in batch_results:
                if result['status'] != 'success':
                    continue
                
                base_record = {
                    'parcel_id': result['parcel_id'],
                    'county_fips': f"{fips_state}{fips_county}",
                    'processing_date': datetime.now().isoformat(),
                    'parcel_acres': result['parcel_acres'],
                    'centroid_lat': result['centroid_lat'],
                    'centroid_lon': result['centroid_lon'],
                    'crop_records_count': len(result.get('crop_records', [])),
                    'forest_records_count': len(result.get('forest_records', []))
                }
                csv_records.append(base_record)
            
            # Write simplified CSV
            if csv_records:
                with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=list(csv_records[0].keys()))
                    writer.writeheader()
                    writer.writerows(csv_records)
                
                logger.warning(f"Database insert failed, saved {len(csv_records)} records to fallback CSV: {filename}")
                return filepath
            
            return None
                
        except Exception as e:
            logger.error(f"Even CSV fallback failed: {e}")
            return None


# Global county processor factory function
def create_county_processor(output_dir: str = 'results') -> CountyProcessor:
    """Create a new county processor instance"""
    return CountyProcessor(output_dir)