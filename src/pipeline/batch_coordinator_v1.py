#!/usr/bin/env python3
"""
Batch Coordinator v1 - Multi-County Processing Coordination
Coordinates processing of multiple counties with resource management and monitoring
"""

import json
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional

from ..config.processing_config_v1 import get_processing_config, get_state_processing_order
from ..pipeline.county_processor_v1 import create_county_processor
from ..utils.logging_utils_v1 import ProcessingMetrics, get_processing_logger

logger = logging.getLogger(__name__)

class BatchCoordinator:
    """
    Coordinates multi-county biomass processing with resource management
    Handles state-by-state and county-by-county processing workflows
    """
    
    def __init__(self, output_dir: str = 'results', max_workers: int = 4):
        self.output_dir = output_dir
        self.max_workers = max_workers
        self.processing_config = get_processing_config()
        self.state_processing_order = get_state_processing_order()
        
        # Processing metrics
        self.metrics = ProcessingMetrics('batch_coordinator')
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Processing state file
        self.state_file = os.path.join(output_dir, 'processing_state.json')
        self.processing_state = self._load_processing_state()
    
    def process_state(self, state_code: str, county_limit: Optional[int] = None,
                     parcel_limit_per_county: Optional[int] = None) -> Dict:
        """
        Process all counties in a state
        
        Args:
            state_code: 2-letter state code (e.g., 'IA')
            county_limit: Optional limit on number of counties to process
            parcel_limit_per_county: Optional limit on parcels per county
            
        Returns:
            Processing statistics dictionary
        """
        processing_id = f"state_{state_code}_{int(time.time())}"
        proc_logger = get_processing_logger('batch_coordinator', processing_id)
        
        proc_logger.info(f"Starting state processing for {state_code}")
        start_time = time.time()
        
        # Get counties for state (this would need to be implemented with actual county list)
        counties = self._get_counties_for_state(state_code)
        
        if county_limit:
            counties = counties[:county_limit]
        
        stats = {
            'state_code': state_code,
            'processing_id': processing_id,
            'start_time': datetime.now().isoformat(),
            'total_counties': len(counties),
            'counties_processed': 0,
            'counties_failed': 0,
            'counties_skipped': 0,
            'total_parcels_processed': 0,
            'total_crop_records': 0,
            'total_forest_records': 0,
            'processing_time': 0.0,
            'county_results': []
        }
        
        try:
            if not counties:
                proc_logger.warning(f"No counties found for state {state_code}")
                return stats
            
            proc_logger.info(f"Processing {len(counties)} counties in {state_code}")
            
            # Process counties in parallel or sequential based on configuration
            if self.max_workers > 1:
                county_results = self._process_counties_parallel(
                    counties, parcel_limit_per_county, proc_logger
                )
            else:
                county_results = self._process_counties_sequential(
                    counties, parcel_limit_per_county, proc_logger
                )
            
            # Aggregate results
            for result in county_results:
                stats['county_results'].append(result)
                
                if result.get('error'):
                    stats['counties_failed'] += 1
                elif result.get('parcels_processed', 0) > 0:
                    stats['counties_processed'] += 1
                    stats['total_parcels_processed'] += result.get('parcels_processed', 0)
                    stats['total_crop_records'] += result.get('crop_records', 0)
                    stats['total_forest_records'] += result.get('forest_records', 0)
                else:
                    stats['counties_skipped'] += 1
            
            stats['processing_time'] = time.time() - start_time
            
            # Save processing state
            self._update_processing_state(state_code, stats)
            
            proc_logger.info(f"State {state_code} processing completed: "
                           f"{stats['counties_processed']} counties processed, "
                           f"{stats['total_parcels_processed']} parcels total")
            
            return stats
            
        except Exception as e:
            proc_logger.error(f"State processing failed: {e}")
            stats['error'] = str(e)
            stats['processing_time'] = time.time() - start_time
            return stats
    
    def process_multiple_states(self, state_codes: List[str], 
                              county_limit_per_state: Optional[int] = None,
                              parcel_limit_per_county: Optional[int] = None) -> Dict:
        """
        Process multiple states in sequence
        
        Args:
            state_codes: List of state codes to process
            county_limit_per_state: Optional limit on counties per state
            parcel_limit_per_county: Optional limit on parcels per county
            
        Returns:
            Aggregated processing statistics
        """
        processing_id = f"multi_state_{int(time.time())}"
        proc_logger = get_processing_logger('batch_coordinator', processing_id)
        
        proc_logger.info(f"Starting multi-state processing for {len(state_codes)} states")
        start_time = time.time()
        
        aggregate_stats = {
            'processing_id': processing_id,
            'start_time': datetime.now().isoformat(),
            'states_requested': state_codes,
            'states_processed': 0,
            'states_failed': 0,
            'total_counties_processed': 0,
            'total_parcels_processed': 0,
            'total_crop_records': 0,
            'total_forest_records': 0,
            'processing_time': 0.0,
            'state_results': []
        }
        
        try:
            for state_code in state_codes:
                proc_logger.info(f"Processing state {state_code}")
                
                state_result = self.process_state(
                    state_code, county_limit_per_state, parcel_limit_per_county
                )
                
                aggregate_stats['state_results'].append(state_result)
                
                if state_result.get('error'):
                    aggregate_stats['states_failed'] += 1
                    proc_logger.error(f"State {state_code} processing failed: {state_result['error']}")
                else:
                    aggregate_stats['states_processed'] += 1
                    aggregate_stats['total_counties_processed'] += state_result.get('counties_processed', 0)
                    aggregate_stats['total_parcels_processed'] += state_result.get('total_parcels_processed', 0)
                    aggregate_stats['total_crop_records'] += state_result.get('total_crop_records', 0)
                    aggregate_stats['total_forest_records'] += state_result.get('total_forest_records', 0)
                    
                    proc_logger.info(f"State {state_code} completed successfully")
            
            aggregate_stats['processing_time'] = time.time() - start_time
            
            proc_logger.info(f"Multi-state processing completed: {aggregate_stats['states_processed']} states, "
                           f"{aggregate_stats['total_counties_processed']} counties, "
                           f"{aggregate_stats['total_parcels_processed']} parcels")
            
            return aggregate_stats
            
        except Exception as e:
            proc_logger.error(f"Multi-state processing failed: {e}")
            aggregate_stats['error'] = str(e)
            aggregate_stats['processing_time'] = time.time() - start_time
            return aggregate_stats
    
    def process_corn_belt_states(self, county_limit_per_state: Optional[int] = None,
                               parcel_limit_per_county: Optional[int] = None) -> Dict:
        """
        Process corn belt states (high priority agricultural areas)
        
        Args:
            county_limit_per_state: Optional limit on counties per state
            parcel_limit_per_county: Optional limit on parcels per county
            
        Returns:
            Processing statistics
        """
        corn_belt_states = self.state_processing_order['phase_1_corn_belt']
        logger.info(f"Processing corn belt states: {corn_belt_states}")
        
        return self.process_multiple_states(
            corn_belt_states, county_limit_per_state, parcel_limit_per_county
        )
    
    def _process_counties_parallel(self, counties: List[Dict], 
                                 parcel_limit: Optional[int],
                                 proc_logger: logging.Logger) -> List[Dict]:
        """
        Process counties in parallel using ProcessPoolExecutor
        
        Args:
            counties: List of county dictionaries
            parcel_limit: Optional parcel limit per county
            proc_logger: Logger instance
            
        Returns:
            List of county processing results
        """
        results = []
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit county processing jobs
            future_to_county = {
                executor.submit(
                    self._process_single_county_wrapper,
                    county['fips_state'], 
                    county['fips_county'],
                    parcel_limit
                ): county for county in counties
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_county):
                county = future_to_county[future]
                county_id = f"{county['fips_state']}{county['fips_county']}"
                
                try:
                    result = future.result()
                    results.append(result)
                    proc_logger.info(f"County {county_id} completed: "
                                   f"{result.get('parcels_processed', 0)} parcels processed")
                except Exception as e:
                    proc_logger.error(f"County {county_id} failed: {e}")
                    results.append({
                        'county_fips': county_id,
                        'error': str(e),
                        'parcels_processed': 0
                    })
        
        return results
    
    def _process_counties_sequential(self, counties: List[Dict],
                                   parcel_limit: Optional[int],
                                   proc_logger: logging.Logger) -> List[Dict]:
        """
        Process counties sequentially
        
        Args:
            counties: List of county dictionaries
            parcel_limit: Optional parcel limit per county
            proc_logger: Logger instance
            
        Returns:
            List of county processing results
        """
        results = []
        
        for i, county in enumerate(counties, 1):
            county_id = f"{county['fips_state']}{county['fips_county']}"
            proc_logger.info(f"Processing county {i}/{len(counties)}: {county_id}")
            
            try:
                result = self._process_single_county_wrapper(
                    county['fips_state'], county['fips_county'], parcel_limit
                )
                results.append(result)
                proc_logger.info(f"County {county_id} completed: "
                               f"{result.get('parcels_processed', 0)} parcels processed")
            except Exception as e:
                proc_logger.error(f"County {county_id} failed: {e}")
                results.append({
                    'county_fips': county_id,
                    'error': str(e),
                    'parcels_processed': 0
                })
        
        return results
    
    def _process_single_county_wrapper(self, fips_state: str, fips_county: str,
                                     parcel_limit: Optional[int]) -> Dict:
        """
        Wrapper function for single county processing (for multiprocessing)
        
        Args:
            fips_state: State FIPS code
            fips_county: County FIPS code
            parcel_limit: Optional parcel limit
            
        Returns:
            County processing result
        """
        processor = create_county_processor(self.output_dir)
        return processor.process_county(fips_state, fips_county, parcel_limit)
    
    def _get_counties_for_state(self, state_code: str) -> List[Dict]:
        """
        Get list of counties for a state
        
        Args:
            state_code: 2-letter state code
            
        Returns:
            List of county dictionaries with FIPS codes
        """
        # This is a simplified implementation - in production, this would
        # query a database or use a comprehensive county list
        
        # State FIPS codes mapping (simplified)
        state_fips_mapping = {
            'IA': '19',  # Iowa
            'IL': '17',  # Illinois
            'IN': '18',  # Indiana
            'OH': '39',  # Ohio
            'CA': '06',  # California
            'TX': '48',  # Texas
        }
        
        fips_state = state_fips_mapping.get(state_code)
        if not fips_state:
            logger.warning(f"Unknown state code: {state_code}")
            return []
        
        # For Delaware County, IA (test case)
        if state_code == 'IA':
            return [
                {'fips_state': '19', 'fips_county': '055', 'name': 'Delaware County'},
                # Add more Iowa counties as needed
            ]
        
        # For other states, return empty list (would need full county database)
        logger.warning(f"County list not implemented for state {state_code}")
        return []
    
    def _load_processing_state(self) -> Dict:
        """Load processing state from file"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load processing state: {e}")
        
        return {
            'states_completed': [],
            'counties_completed': [],
            'last_updated': None
        }
    
    def _update_processing_state(self, state_code: str, stats: Dict):
        """Update and save processing state"""
        try:
            self.processing_state['last_updated'] = datetime.now().isoformat()
            
            if state_code not in self.processing_state['states_completed']:
                self.processing_state['states_completed'].append(state_code)
            
            # Add completed counties
            for county_result in stats.get('county_results', []):
                county_fips = county_result.get('county_fips')
                if county_fips and county_result.get('parcels_processed', 0) > 0:
                    if county_fips not in self.processing_state['counties_completed']:
                        self.processing_state['counties_completed'].append(county_fips)
            
            with open(self.state_file, 'w') as f:
                json.dump(self.processing_state, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to update processing state: {e}")
    
    def get_processing_status(self) -> Dict:
        """Get current processing status"""
        return {
            'processing_state': self.processing_state,
            'metrics': self.metrics.get_metrics(),
            'output_directory': self.output_dir,
            'max_workers': self.max_workers
        }


# Global batch coordinator factory function
def create_batch_coordinator(output_dir: str = 'results', max_workers: int = 4) -> BatchCoordinator:
    """Create a new batch coordinator instance"""
    return BatchCoordinator(output_dir, max_workers)