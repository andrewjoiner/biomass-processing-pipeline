#!/usr/bin/env python3
"""
Comprehensive Biomass Processor v1 - Integrated US-Scale Biomass Analysis
Orchestrates forestry, crop, and satellite data for accurate biomass volume estimation
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from ..analyzers.forest_analyzer_v1 import forest_analyzer
from ..analyzers.crop_analyzer_v1 import crop_analyzer
from ..analyzers.landcover_analyzer_v1 import landcover_analyzer
from ..analyzers.vegetation_analyzer_v1 import vegetation_analyzer
from ..core.database_manager_v1 import database_manager
from ..core.blob_manager_v1 import blob_manager
from ..config.processing_config_v1 import get_processing_config

logger = logging.getLogger(__name__)

class ComprehensiveBiomassProcessor:
    """
    Comprehensive biomass processor for US-scale analysis
    Integrates satellite-based land segmentation with FIA forest data and CDL crop data
    """
    
    def __init__(self):
        self.db_manager = database_manager
        self.processing_config = get_processing_config()
        
        # Analyzer instances
        self.forest_analyzer = forest_analyzer
        self.crop_analyzer = crop_analyzer
        self.landcover_analyzer = landcover_analyzer
        self.vegetation_analyzer = vegetation_analyzer
        
        # Processing statistics
        self.stats = {
            'parcels_processed': 0,
            'parcels_with_forest': 0,
            'parcels_with_crops': 0,
            'processing_errors': 0,
            'total_forest_biomass_tons': 0.0,
            'total_crop_yield_tons': 0.0,
            'total_crop_residue_tons': 0.0,
            'start_time': None,
            'end_time': None
        }
    
    def process_county_comprehensive(self, fips_state: str, fips_county: str, 
                                   max_parcels: Optional[int] = None,
                                   enable_parallel: bool = True,
                                   batch_size: int = 500,
                                   resume_from_checkpoint: bool = True) -> Dict:
        """
        Process all parcels in a county with comprehensive biomass analysis using batch processing
        
        Args:
            fips_state: 2-digit state FIPS code
            fips_county: 3-digit county FIPS code
            max_parcels: Optional limit on parcels to process
            enable_parallel: Whether to enable parallel processing
            batch_size: Number of parcels to process in each batch (default 500)
            resume_from_checkpoint: Whether to resume from existing checkpoint
            
        Returns:
            Processing results summary
        """
        logger.info(f"Starting comprehensive biomass processing for county {fips_state}{fips_county}")
        self.stats['start_time'] = datetime.now()
        
        try:
            # Step 1: Pre-download satellite data for county (once per county)
            county_bounds = self.db_manager.get_county_bounds(fips_state, fips_county)
            if county_bounds:
                logger.info(f"Pre-downloading satellite tiles for county bounds: {county_bounds}")
                try:
                    sentinel2_stats = blob_manager.download_sentinel2_county_tiles(county_bounds)
                    worldcover_stats = blob_manager.download_worldcover_county_tiles(county_bounds)
                    logger.info(f"Downloaded {sentinel2_stats['sentinel2_tiles']} Sentinel-2 tiles, "
                               f"{worldcover_stats['worldcover_tiles']} WorldCover tiles")
                except Exception as e:
                    logger.warning(f"Satellite tile download failed: {e}")
            else:
                logger.warning(f"Could not determine county bounds for {fips_state}{fips_county}")
            
            # Step 2: Process parcels in batches (with checkpoint support)
            all_parcel_results = self._process_county_in_batches(
                fips_state, fips_county, max_parcels, batch_size, enable_parallel, resume_from_checkpoint
            )
            
            if not all_parcel_results:
                logger.warning(f"No parcels successfully processed for county {fips_state}{fips_county}")
                return {'success': False, 'message': 'No parcels successfully processed'}
            
            # Step 3: Save all results to database (batch save for efficiency)
            logger.info(f"Saving {len(all_parcel_results)} parcel results to database...")
            success = self.db_manager.save_biomass_results(all_parcel_results)
            if not success:
                logger.error("Failed to save results to database")
            
            # Step 4: Generate summary statistics
            self.stats['end_time'] = datetime.now()
            processing_summary = self._generate_processing_summary(fips_state, fips_county, all_parcel_results)
            
            logger.info(f"County processing complete: {len(all_parcel_results)} parcels processed successfully")
            return processing_summary
            
        except Exception as e:
            logger.error(f"Error processing county {fips_state}{fips_county}: {e}")
            return {'success': False, 'error': str(e)}
    
    def _process_county_in_batches(self, fips_state: str, fips_county: str, 
                                 max_parcels: Optional[int], batch_size: int, 
                                 enable_parallel: bool, resume_from_checkpoint: bool = True) -> List[Dict]:
        """
        Process all parcels in a county using batch processing for memory efficiency with checkpointing
        
        Args:
            fips_state: State FIPS code
            fips_county: County FIPS code
            max_parcels: Optional limit on total parcels
            batch_size: Size of each processing batch
            enable_parallel: Whether to use parallel processing within batches
            resume_from_checkpoint: Whether to resume from existing checkpoint
            
        Returns:
            List of all processed parcel results
        """
        logger.info(f"Starting batch processing for county {fips_state}{fips_county}")
        
        all_parcel_results = []
        processed_count = 0
        batch_num = 0
        errors_count = 0
        
        # Check for existing checkpoint
        if resume_from_checkpoint:
            checkpoint = self.db_manager.get_checkpoint(fips_state, fips_county)
            if checkpoint:
                processed_count = checkpoint['parcel_offset']
                batch_num = checkpoint['batch_num']
                errors_count = checkpoint['errors_count']
                logger.info(f"Resuming from checkpoint: batch {batch_num}, offset {processed_count}, "
                           f"previous errors: {errors_count}")
            else:
                logger.info("No checkpoint found - starting fresh processing")
        
        while True:
            batch_num += 1
            
            # Calculate remaining parcels to process
            remaining_parcels = None
            if max_parcels:
                remaining_parcels = max_parcels - processed_count
                if remaining_parcels <= 0:
                    break
                # Don't exceed the requested limit
                current_batch_size = min(batch_size, remaining_parcels)
            else:
                current_batch_size = batch_size
            
            # Load next batch of parcels
            logger.info(f"Loading batch {batch_num} (offset: {processed_count}, limit: {current_batch_size})")
            
            # Get parcels with offset using LIMIT and OFFSET
            batch_parcels = self._get_county_parcels_batch(
                fips_state, fips_county, processed_count, current_batch_size
            )
            
            if not batch_parcels:
                logger.info(f"No more parcels found after {processed_count} parcels - processing complete")
                break
            
            logger.info(f"Batch {batch_num}: Processing {len(batch_parcels)} parcels")
            
            # Process this batch
            try:
                if enable_parallel and len(batch_parcels) > 5:
                    batch_results = self._process_parcels_parallel(batch_parcels, fips_state, fips_county)
                else:
                    batch_results = self._process_parcels_sequential(batch_parcels, fips_state, fips_county)
                
                # Add successful results
                all_parcel_results.extend(batch_results)
                successful_in_batch = len(batch_results)
                
                # Update counters
                processed_count += len(batch_parcels)
                errors_in_batch = len(batch_parcels) - successful_in_batch
                errors_count += errors_in_batch
                
                logger.info(f"Batch {batch_num} complete: {successful_in_batch}/{len(batch_parcels)} successful "
                           f"(Total processed: {len(all_parcel_results)}, Total errors: {errors_count})")
                
                # Create checkpoint after successful batch
                self.db_manager.create_checkpoint(
                    fips_state, fips_county, batch_num, processed_count, 
                    len(all_parcel_results), errors_count
                )
                
                # Memory cleanup after each batch
                import gc
                gc.collect()
                
                # If this batch was smaller than requested batch size, we've reached the end
                if len(batch_parcels) < current_batch_size:
                    logger.info("Reached end of parcels (partial batch returned)")
                    break
                    
            except Exception as e:
                logger.error(f"Error processing batch {batch_num}: {e}")
                errors_count += len(batch_parcels)
                processed_count += len(batch_parcels)
                # Continue with next batch
                continue
        
        logger.info(f"County batch processing complete: {len(all_parcel_results)} parcels processed successfully, "
                   f"{errors_count} errors across {batch_num} batches")
        
        # Mark county as completed
        self.db_manager.complete_county_processing(fips_state, fips_county)
        
        # Clear satellite data cache after county completion
        logger.info("Clearing satellite data cache after county completion")
        blob_manager.clear_cache()
        
        return all_parcel_results
    
    def _get_county_parcels_batch(self, fips_state: str, fips_county: str, 
                                offset: int, limit: int) -> List[Dict]:
        """
        Get a batch of parcels for county with offset for batch processing
        
        Args:
            fips_state: State FIPS code
            fips_county: County FIPS code
            offset: Number of parcels to skip
            limit: Number of parcels to return
            
        Returns:
            List of parcel dictionaries
        """
        try:
            # Use the new database batch method with proper OFFSET support
            batch_parcels = self.db_manager.get_county_parcels_batch(
                fips_state, fips_county, offset, limit,
                min_acres=self.processing_config.get('min_parcel_acres', 0.1),
                max_acres=self.processing_config.get('max_parcel_acres', 10000)
            )
            
            return batch_parcels
            
        except Exception as e:
            logger.error(f"Error loading parcel batch (offset: {offset}, limit: {limit}): {e}")
            return []
    
    def _process_parcels_parallel(self, parcels: List[Dict], fips_state: str, fips_county: str) -> List[Dict]:
        """Process parcels in parallel using thread pool"""
        logger.info(f"Processing {len(parcels)} parcels in parallel")
        
        parcel_results = []
        max_workers = min(8, len(parcels))  # Limit concurrent threads
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit parcel processing tasks
            future_to_parcel = {
                executor.submit(self.process_single_parcel_comprehensive, parcel, fips_state, fips_county): parcel
                for parcel in parcels
            }
            
            # Process completed futures
            for future in as_completed(future_to_parcel):
                parcel = future_to_parcel[future]
                try:
                    result = future.result()
                    if result:
                        parcel_results.append(result)
                        self._update_processing_stats(result)
                    else:
                        # Count failed parcels that returned None
                        self.stats['processing_errors'] += 1
                    
                    # Log progress every 100 parcels
                    if len(parcel_results) % 100 == 0:
                        logger.info(f"Processed {len(parcel_results)}/{len(parcels)} parcels "
                                   f"({self.stats['processing_errors']} errors)")
                        
                except Exception as e:
                    logger.error(f"Error processing parcel {parcel.get('parcelid', 'unknown')}: {e}")
                    self.stats['processing_errors'] += 1
                    # Continue processing other parcels
        
        logger.info(f"Parallel processing complete: {len(parcel_results)} successful, {self.stats['processing_errors']} errors")
        return parcel_results
    
    def _process_parcels_sequential(self, parcels: List[Dict], fips_state: str, fips_county: str) -> List[Dict]:
        """Process parcels sequentially"""
        logger.info(f"Processing {len(parcels)} parcels sequentially")
        
        parcel_results = []
        
        for i, parcel in enumerate(parcels):
            try:
                result = self.process_single_parcel_comprehensive(parcel, fips_state, fips_county)
                if result:
                    parcel_results.append(result)
                    self._update_processing_stats(result)
                else:
                    # Count failed parcels that returned None
                    self.stats['processing_errors'] += 1
                
                # Log progress
                if (i + 1) % 50 == 0:
                    logger.info(f"Processed {i + 1}/{len(parcels)} parcels "
                               f"({len(parcel_results)} successful, {self.stats['processing_errors']} errors)")
                    
            except Exception as e:
                logger.error(f"Error processing parcel {parcel.get('parcelid', 'unknown')}: {e}")
                self.stats['processing_errors'] += 1
                # Continue processing other parcels
        
        logger.info(f"Sequential processing complete: {len(parcel_results)} successful, {self.stats['processing_errors']} errors")
        return parcel_results
    
    def process_single_parcel_comprehensive(self, parcel: Dict, fips_state: str, fips_county: str) -> Optional[Dict]:
        """
        Process a single parcel with comprehensive biomass analysis
        
        Args:
            parcel: Parcel dictionary with geometry and metadata
            fips_state: State FIPS code
            fips_county: County FIPS code
            
        Returns:
            Comprehensive parcel analysis results or None if failed
        """
        try:
            parcel_id = parcel['parcelid']
            parcel_geometry = parcel['geometry']
            parcel_postgis_geometry = parcel['postgis_geometry']
            parcel_acres = parcel['acres']
            
            # Step 1: Sub-parcel land cover analysis
            landcover_analysis = self.landcover_analyzer.analyze_parcel_landcover(parcel_geometry, parcel_id)
            
            if not landcover_analysis:
                logger.debug(f"No land cover data for parcel {parcel_id}")
                return None
            
            # Step 2: Get biomass allocation factors
            allocation_factors = self.landcover_analyzer.get_biomass_allocation_factors(landcover_analysis)
            
            # Step 3: Vegetation analysis (NDVI, etc.)
            vegetation_indices = None
            try:
                vegetation_indices = self.vegetation_analyzer.analyze_parcel_vegetation(parcel_geometry)
            except Exception as e:
                logger.debug(f"Vegetation analysis failed for parcel {parcel_id}: {e}")
            
            # Step 4: Forest biomass analysis (only if forest land present)
            forest_analysis = None
            if allocation_factors['forest_acres'] > 0.1:  # At least 0.1 acres of forest
                forest_analysis = self.forest_analyzer.analyze_parcel_forest(
                    parcel_geometry, 
                    parcel_postgis_geometry, 
                    allocation_factors['forest_acres'],  # Use actual forest area, not total parcel
                    vegetation_indices
                )
                
                # Apply land cover allocation
                if forest_analysis:
                    forest_analysis = self._apply_forest_landcover_allocation(forest_analysis, allocation_factors)
            
            # Step 5: Crop analysis (only if cropland present)
            crop_analysis = None
            if allocation_factors['cropland_acres'] > 0.1:  # At least 0.1 acres of cropland
                crop_records = self.crop_analyzer.analyze_parcel_crops(parcel_postgis_geometry, vegetation_indices)
                
                if crop_records:
                    # Apply land cover allocation to crop estimates
                    crop_analysis = self._apply_crop_landcover_allocation(crop_records, allocation_factors)
            
            # Step 6: Create comprehensive parcel result
            parcel_result = {
                'parcel_id': parcel_id,
                'county_fips': f"{fips_state}{fips_county}",
                'total_acres': parcel_acres,
                'centroid_lon': parcel.get('centroid_lon', 0),
                'centroid_lat': parcel.get('centroid_lat', 0),
                
                # Land cover breakdown
                'landcover_analysis': landcover_analysis,
                'allocation_factors': allocation_factors,
                
                # Biomass analysis results
                'forest_analysis': forest_analysis,
                'crop_analysis': crop_analysis,
                'vegetation_indices': vegetation_indices,
                
                # Summary totals
                'forest_biomass_tons': forest_analysis.get('total_standing_biomass_tons', 0) if forest_analysis else 0,
                'forest_harvestable_tons': forest_analysis.get('total_harvestable_biomass_tons', 0) if forest_analysis else 0,
                'forest_residue_tons': forest_analysis.get('forest_residue_biomass_tons', 0) if forest_analysis else 0,
                'crop_yield_tons': sum(crop.get('yield_tons', 0) for crop in (crop_analysis or [])),
                'crop_residue_tons': sum(crop.get('harvestable_residue_tons', 0) for crop in (crop_analysis or [])),
                
                # Processing metadata
                'processing_timestamp': datetime.now().isoformat(),
                'data_sources_used': self._get_data_sources_used(forest_analysis, crop_analysis, landcover_analysis),
                'confidence_score': self._calculate_overall_confidence(forest_analysis, crop_analysis, landcover_analysis)
            }
            
            return parcel_result
            
        except Exception as e:
            logger.error(f"Error in comprehensive parcel processing for {parcel.get('parcelid', 'unknown')}: {e}")
            return None
    
    def _apply_forest_landcover_allocation(self, forest_analysis: Dict, allocation_factors: Dict) -> Dict:
        """
        Apply land cover allocation factors to forest biomass estimates
        Adjusts estimates based on actual forest coverage within parcel
        """
        # The forest analyzer already uses the actual forest area from land cover analysis
        # So we don't need to scale down further, but we can add confidence adjustments
        
        # Adjust confidence based on forest coverage percentage
        forest_coverage = allocation_factors['forest_factor']
        
        if forest_coverage < 0.1:  # Less than 10% forest
            forest_analysis['confidence_score'] *= 0.7  # Reduce confidence
        elif forest_coverage > 0.8:  # More than 80% forest
            forest_analysis['confidence_score'] *= 1.1  # Increase confidence slightly
            forest_analysis['confidence_score'] = min(forest_analysis['confidence_score'], 0.95)  # Cap at 95%
        
        # Add allocation metadata
        forest_analysis['landcover_allocation'] = {
            'forest_factor': forest_coverage,
            'forest_acres_used': allocation_factors['forest_acres'],
            'total_parcel_acres': allocation_factors['total_acres']
        }
        
        return forest_analysis
    
    def _apply_crop_landcover_allocation(self, crop_records: List[Dict], allocation_factors: Dict) -> List[Dict]:
        """
        Apply land cover allocation factors to crop biomass estimates
        Adjusts estimates based on actual cropland coverage within parcel
        """
        cropland_factor = allocation_factors['crop_factor']
        
        # Scale crop areas based on actual cropland coverage
        for crop in crop_records:
            # Adjust area estimates based on satellite-derived cropland percentage
            original_area = crop['area_acres']
            adjusted_area = original_area * cropland_factor
            scale_factor = adjusted_area / original_area if original_area > 0 else 1.0
            
            # Scale all biomass estimates proportionally
            crop['area_acres'] = round(adjusted_area, 3)
            crop['yield_tons'] = round(crop['yield_tons'] * scale_factor, 2)
            crop['residue_tons_wet'] = round(crop['residue_tons_wet'] * scale_factor, 2)
            crop['residue_tons_dry'] = round(crop['residue_tons_dry'] * scale_factor, 2)
            crop['harvestable_residue_tons'] = round(crop['harvestable_residue_tons'] * scale_factor, 2)
            
            # Adjust confidence based on cropland coverage
            if cropland_factor < 0.3:  # Less than 30% cropland
                crop['confidence_score'] *= 0.8
            elif cropland_factor > 0.7:  # More than 70% cropland
                crop['confidence_score'] *= 1.1
                crop['confidence_score'] = min(crop['confidence_score'], 0.95)
            
            # Add allocation metadata
            crop['landcover_allocation'] = {
                'cropland_factor': cropland_factor,
                'original_area_acres': original_area,
                'scale_factor_applied': scale_factor
            }
        
        return crop_records
    
    def _get_data_sources_used(self, forest_analysis: Optional[Dict], crop_analysis: Optional[List[Dict]], 
                             landcover_analysis: Optional[Dict]) -> List[str]:
        """Determine which data sources were used in the analysis"""
        sources = ['WorldCover_10m']  # Always use WorldCover for land segmentation
        
        if forest_analysis:
            sources.append(forest_analysis.get('data_sources', 'FIA'))
        
        if crop_analysis:
            sources.append('CDL_Crops')
        
        if landcover_analysis and landcover_analysis.get('has_ndvi_data'):
            sources.append('Sentinel2_NDVI')
        
        return sources
    
    def _calculate_overall_confidence(self, forest_analysis: Optional[Dict], crop_analysis: Optional[List[Dict]],
                                   landcover_analysis: Dict) -> float:
        """Calculate overall confidence score for parcel analysis"""
        confidence_scores = []
        
        # Land cover analysis confidence
        data_completeness = landcover_analysis.get('data_completeness', 0.8)
        landcover_confidence = min(data_completeness, 0.9)  # Cap at 90%
        confidence_scores.append(landcover_confidence)
        
        # Forest analysis confidence
        if forest_analysis:
            confidence_scores.append(forest_analysis.get('confidence_score', 0.5))
        
        # Crop analysis confidence
        if crop_analysis:
            crop_confidences = [crop.get('confidence_score', 0.5) for crop in crop_analysis]
            if crop_confidences:
                confidence_scores.append(sum(crop_confidences) / len(crop_confidences))
        
        # Return weighted average
        if confidence_scores:
            return round(sum(confidence_scores) / len(confidence_scores), 3)
        else:
            return 0.5  # Default moderate confidence
    
    def _update_processing_stats(self, result: Dict):
        """Update processing statistics"""
        self.stats['parcels_processed'] += 1
        
        if result.get('forest_analysis'):
            self.stats['parcels_with_forest'] += 1
            self.stats['total_forest_biomass_tons'] += result.get('forest_biomass_tons', 0)
        
        if result.get('crop_analysis'):
            self.stats['parcels_with_crops'] += 1
            self.stats['total_crop_yield_tons'] += result.get('crop_yield_tons', 0)
            self.stats['total_crop_residue_tons'] += result.get('crop_residue_tons', 0)
    
    def _generate_processing_summary(self, fips_state: str, fips_county: str, results: List[Dict]) -> Dict:
        """Generate comprehensive processing summary"""
        if not self.stats['start_time'] or not self.stats['end_time']:
            processing_time_seconds = 0
        else:
            processing_time_seconds = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        return {
            'success': True,
            'county_fips': f"{fips_state}{fips_county}",
            'processing_summary': {
                'parcels_processed': len(results),
                'parcels_with_forest': self.stats['parcels_with_forest'],
                'parcels_with_crops': self.stats['parcels_with_crops'],
                'processing_errors': self.stats['processing_errors'],
                'processing_time_seconds': round(processing_time_seconds, 2),
                'parcels_per_second': round(len(results) / processing_time_seconds, 3) if processing_time_seconds > 0 else 0
            },
            'biomass_totals': {
                'total_forest_biomass_tons': round(self.stats['total_forest_biomass_tons'], 2),
                'total_crop_yield_tons': round(self.stats['total_crop_yield_tons'], 2),
                'total_crop_residue_tons': round(self.stats['total_crop_residue_tons'], 2),
                'total_biomass_tons': round(
                    self.stats['total_forest_biomass_tons'] + 
                    self.stats['total_crop_yield_tons'] + 
                    self.stats['total_crop_residue_tons'], 2
                )
            },
            'data_quality': {
                'average_confidence': round(
                    sum(r.get('confidence_score', 0) for r in results) / len(results), 3
                ) if results else 0,
                'forest_coverage_rate': round(self.stats['parcels_with_forest'] / len(results), 3) if results else 0,
                'crop_coverage_rate': round(self.stats['parcels_with_crops'] / len(results), 3) if results else 0
            },
            'processing_timestamp': datetime.now().isoformat(),
            'results_saved_to_database': True
        }
    
    def process_test_parcels(self, fips_state: str, fips_county: str, limit: int = 5) -> Dict:
        """
        Process a small number of test parcels for validation
        
        Args:
            fips_state: State FIPS code
            fips_county: County FIPS code  
            limit: Number of test parcels to process
            
        Returns:
            Test processing results
        """
        logger.info(f"Running test processing for {limit} parcels in county {fips_state}{fips_county}")
        
        # Reset stats for test run
        self.stats = {key: 0 if isinstance(val, (int, float)) else None for key, val in self.stats.items()}
        
        return self.process_county_comprehensive(fips_state, fips_county, max_parcels=limit, enable_parallel=False)
    
    def get_processing_status(self) -> Dict:
        """Get current processing status and statistics"""
        return {
            'current_stats': self.stats.copy(),
            'processing_config': self.processing_config,
            'analyzer_status': {
                'forest_analyzer': 'ready',
                'crop_analyzer': 'ready',
                'landcover_analyzer': 'ready',
                'vegetation_analyzer': 'ready'
            },
            'database_connections': self.db_manager.test_connections()
        }


# Global comprehensive processor instance
comprehensive_biomass_processor = ComprehensiveBiomassProcessor()