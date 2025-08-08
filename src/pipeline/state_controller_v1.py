#!/usr/bin/env python3
"""
State Controller v1 - Nationwide State-by-State Processing Orchestrator
Manages processing of all US states with county-level coordination and failure recovery
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .comprehensive_biomass_processor_v1 import comprehensive_biomass_processor
from ..core.database_manager_v1 import database_manager

logger = logging.getLogger(__name__)

class StateController:
    """
    State-level processing controller for nationwide biomass analysis
    Coordinates state-by-state, county-by-county processing with recovery
    """
    
    def __init__(self):
        self.processor = comprehensive_biomass_processor
        self.db_manager = database_manager
        
        # Processing statistics
        self.stats = {
            'states_processed': 0,
            'counties_processed': 0,
            'total_parcels_processed': 0,
            'total_errors': 0,
            'start_time': None,
            'current_state': None,
            'current_county': None
        }
        
        # US State FIPS codes (50 states + DC)
        self.us_states = {
            '01': 'Alabama', '02': 'Alaska', '04': 'Arizona', '05': 'Arkansas', '06': 'California',
            '08': 'Colorado', '09': 'Connecticut', '10': 'Delaware', '11': 'District of Columbia',
            '12': 'Florida', '13': 'Georgia', '15': 'Hawaii', '16': 'Idaho', '17': 'Illinois',
            '18': 'Indiana', '19': 'Iowa', '20': 'Kansas', '21': 'Kentucky', '22': 'Louisiana',
            '23': 'Maine', '24': 'Maryland', '25': 'Massachusetts', '26': 'Michigan', '27': 'Minnesota',
            '28': 'Mississippi', '29': 'Missouri', '30': 'Montana', '31': 'Nebraska', '32': 'Nevada',
            '33': 'New Hampshire', '34': 'New Jersey', '35': 'New Mexico', '36': 'New York',
            '37': 'North Carolina', '38': 'North Dakota', '39': 'Ohio', '40': 'Oklahoma',
            '41': 'Oregon', '42': 'Pennsylvania', '44': 'Rhode Island', '45': 'South Carolina',
            '46': 'South Dakota', '47': 'Tennessee', '48': 'Texas', '49': 'Utah', '50': 'Vermont',
            '51': 'Virginia', '53': 'Washington', '54': 'West Virginia', '55': 'Wisconsin', '56': 'Wyoming'
        }
    
    def process_all_states(self, start_state: Optional[str] = None, 
                         max_states: Optional[int] = None,
                         batch_size: int = 500) -> Dict:
        """
        Process all US states sequentially
        
        Args:
            start_state: Optional state FIPS to start from (for resuming)
            max_states: Optional limit on number of states to process
            batch_size: Batch size for parcel processing within counties
            
        Returns:
            Processing summary dictionary
        """
        logger.info("Starting nationwide state-by-state processing")
        self.stats['start_time'] = datetime.now()
        
        # Determine which states to process
        states_to_process = list(self.us_states.keys())
        if start_state:
            # Start from specified state
            if start_state in states_to_process:
                start_index = states_to_process.index(start_state)
                states_to_process = states_to_process[start_index:]
                logger.info(f"Resuming processing from state {start_state} ({self.us_states[start_state]})")
            else:
                logger.error(f"Invalid start state FIPS: {start_state}")
                return {'success': False, 'error': 'Invalid start state'}
        
        if max_states:
            states_to_process = states_to_process[:max_states]
        
        logger.info(f"Will process {len(states_to_process)} states")
        
        # Process each state
        processed_states = 0
        failed_states = []
        
        for state_fips in states_to_process:
            state_name = self.us_states[state_fips]
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing State {processed_states + 1}/{len(states_to_process)}: "
                       f"{state_name} (FIPS: {state_fips})")
            logger.info(f"{'='*60}")
            
            self.stats['current_state'] = f"{state_fips} ({state_name})"
            
            try:
                state_result = self.process_single_state(state_fips, batch_size)
                if state_result['success']:
                    processed_states += 1
                    self.stats['states_processed'] = processed_states
                    self.stats['counties_processed'] += state_result['counties_processed']
                    self.stats['total_parcels_processed'] += state_result['total_parcels_processed']
                    logger.info(f"✅ State {state_name} completed successfully: "
                               f"{state_result['counties_processed']} counties, "
                               f"{state_result['total_parcels_processed']} parcels")
                else:
                    logger.error(f"❌ State {state_name} failed: {state_result.get('error', 'Unknown error')}")
                    failed_states.append(state_fips)
                    
            except Exception as e:
                logger.error(f"❌ Critical error processing state {state_name}: {e}")
                failed_states.append(state_fips)
                continue
            
            # Log overall progress
            remaining_states = len(states_to_process) - processed_states - len(failed_states)
            logger.info(f"\n📊 PROGRESS UPDATE:")
            logger.info(f"   ✅ States completed: {processed_states}")
            logger.info(f"   ❌ States failed: {len(failed_states)}")
            logger.info(f"   ⏳ States remaining: {remaining_states}")
            logger.info(f"   📈 Counties processed: {self.stats['counties_processed']}")
            logger.info(f"   📈 Total parcels: {self.stats['total_parcels_processed']:,}")
        
        # Generate final summary
        return self._generate_nationwide_summary(processed_states, failed_states, states_to_process)
    
    def process_single_state(self, state_fips: str, batch_size: int = 500) -> Dict:
        """
        Process all counties in a single state
        
        Args:
            state_fips: 2-digit state FIPS code
            batch_size: Batch size for county processing
            
        Returns:
            State processing summary
        """
        state_name = self.us_states.get(state_fips, f"State_{state_fips}")
        logger.info(f"Starting processing for {state_name}")
        
        # Get all counties in this state
        counties = self._get_state_counties(state_fips)
        if not counties:
            logger.warning(f"No counties found for state {state_fips}")
            return {'success': False, 'error': 'No counties found'}
        
        logger.info(f"Found {len(counties)} counties in {state_name}")
        
        # Process each county
        counties_processed = 0
        counties_failed = []
        total_parcels_processed = 0
        
        for i, county_fips in enumerate(counties):
            logger.info(f"\nProcessing County {i + 1}/{len(counties)}: "
                       f"{state_fips}{county_fips} ({state_name})")
            
            self.stats['current_county'] = f"{state_fips}{county_fips}"
            
            try:
                county_result = self.processor.process_county_comprehensive(
                    state_fips, county_fips, 
                    batch_size=batch_size,
                    resume_from_checkpoint=True
                )
                
                if county_result['success']:
                    counties_processed += 1
                    parcels_in_county = county_result['processing_summary']['parcels_processed']
                    total_parcels_processed += parcels_in_county
                    logger.info(f"✅ County {state_fips}{county_fips} completed: {parcels_in_county} parcels")
                else:
                    logger.error(f"❌ County {state_fips}{county_fips} failed: "
                               f"{county_result.get('error', 'Unknown error')}")
                    counties_failed.append(county_fips)
                    
            except Exception as e:
                logger.error(f"❌ Critical error processing county {state_fips}{county_fips}: {e}")
                counties_failed.append(county_fips)
                continue
            
            # Log state progress every 10 counties
            if (i + 1) % 10 == 0:
                remaining = len(counties) - counties_processed - len(counties_failed)
                logger.info(f"   State Progress: {counties_processed}/{len(counties)} counties completed, "
                           f"{len(counties_failed)} failed, {remaining} remaining")
        
        success = counties_processed > 0
        return {
            'success': success,
            'state_fips': state_fips,
            'state_name': state_name,
            'counties_processed': counties_processed,
            'counties_failed': len(counties_failed),
            'total_counties': len(counties),
            'failed_counties': counties_failed,
            'total_parcels_processed': total_parcels_processed
        }
    
    def _get_state_counties(self, state_fips: str) -> List[str]:
        """
        Get list of county FIPS codes for a state
        
        Args:
            state_fips: 2-digit state FIPS code
            
        Returns:
            List of 3-digit county FIPS codes
        """
        try:
            with self.db_manager.get_connection('parcels') as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT DISTINCT fipscounty 
                    FROM parcels 
                    WHERE fipsstate = %s 
                    AND geometry IS NOT NULL
                    ORDER BY fipscounty
                """, (state_fips,))
                
                counties = [row['fipscounty'] for row in cursor.fetchall()]
                logger.debug(f"Found {len(counties)} counties in state {state_fips}")
                return counties
                
        except Exception as e:
            logger.error(f"Error getting counties for state {state_fips}: {e}")
            return []
    
    def _generate_nationwide_summary(self, processed_states: int, failed_states: List[str], 
                                   all_states: List[str]) -> Dict:
        """Generate comprehensive nationwide processing summary"""
        end_time = datetime.now()
        total_time = (end_time - self.stats['start_time']).total_seconds() if self.stats['start_time'] else 0
        
        return {
            'success': processed_states > 0,
            'summary': {
                'total_states_attempted': len(all_states),
                'states_completed': processed_states,
                'states_failed': len(failed_states),
                'success_rate': round((processed_states / len(all_states)) * 100, 2) if all_states else 0,
                
                'counties_processed': self.stats['counties_processed'],
                'total_parcels_processed': self.stats['total_parcels_processed'],
                'total_errors': self.stats['total_errors'],
                
                'processing_time_hours': round(total_time / 3600, 2),
                'parcels_per_hour': round(self.stats['total_parcels_processed'] / (total_time / 3600), 0) if total_time > 0 else 0
            },
            'failed_states': [{'fips': fips, 'name': self.us_states[fips]} for fips in failed_states],
            'processing_start': self.stats['start_time'].isoformat() if self.stats['start_time'] else None,
            'processing_end': end_time.isoformat(),
            'nationwide_completion': f"{processed_states}/{len(all_states)} states completed"
        }
    
    def get_processing_status(self) -> Dict:
        """Get current processing status"""
        return {
            'current_stats': self.stats.copy(),
            'current_state': self.stats.get('current_state'),
            'current_county': self.stats.get('current_county'),
            'processor_status': self.processor.get_processing_status()
        }
    
    def resume_processing(self, failed_states_only: bool = False) -> Dict:
        """
        Resume processing from failed states or continue from last checkpoint
        
        Args:
            failed_states_only: If True, only process previously failed states
            
        Returns:
            Processing results
        """
        if failed_states_only:
            # Get failed states from previous run (would need to be stored in database)
            logger.info("Resume mode: failed states only (implementation needed)")
            # This would require storing state-level checkpoints in database
            return {'success': False, 'error': 'Failed states resume not implemented'}
        else:
            # Resume from checkpoints (county-level resumption already implemented)
            logger.info("Resume mode: continuing with existing checkpoints")
            return self.process_all_states()


# Global state controller instance
state_controller = StateController()