#!/usr/bin/env python3
"""
Multi-VM Coordinator - Distribute state processing across multiple VMs
Coordinates nationwide processing across multiple Azure VMs for maximum throughput
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from ..core.database_manager_v1 import database_manager

logger = logging.getLogger(__name__)

class MultiVMCoordinator:
    """
    Coordinates biomass processing across multiple Azure VMs
    Each VM processes a subset of states to maximize nationwide throughput
    """
    
    def __init__(self):
        self.db_manager = database_manager
        
        # US States grouped for optimal VM distribution
        # Group states by expected parcel count and geographic region for load balancing
        self.vm_state_assignments = {
            'vm_1_southeast': ['12', '13', '37', '45', '47'],  # FL, GA, NC, SC, TN
            'vm_2_texas_southwest': ['48', '35', '40', '05'],  # TX, NM, OK, AR  
            'vm_3_california_west': ['06', '41', '53', '32'],  # CA, OR, WA, NV
            'vm_4_midwest': ['17', '18', '19', '26', '39'],    # IL, IN, IA, MI, OH
            'vm_5_northeast': ['36', '42', '25', '09', '23'],  # NY, PA, MA, CT, ME
            'vm_6_plains': ['20', '31', '38', '46', '27'],     # KS, NE, ND, SD, MN
            'vm_7_mountain_west': ['30', '56', '08', '49', '16'], # MT, WY, CO, UT, ID
            'vm_8_southeast_2': ['01', '28', '22', '21'],      # AL, MS, LA, KY
            'vm_9_mid_atlantic': ['51', '24', '11', '10', '34'], # VA, MD, DC, DE, NJ
            'vm_10_remaining': ['02', '15', '04', '33', '44', '50', '54', '55'] # AK, HI, AZ, NH, RI, VT, WV, WI
        }
        
        # State names for logging
        self.state_names = {
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
    
    def generate_vm_assignments(self, output_dir: str = 'vm_assignments') -> Dict:
        """
        Generate processing assignments for each VM
        
        Args:
            output_dir: Directory to save assignment files
            
        Returns:
            Dictionary with VM assignments and statistics
        """
        logger.info("Generating VM processing assignments for nationwide deployment")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Calculate expected workload for each VM
        vm_workloads = {}
        total_states = 0
        
        for vm_id, state_list in self.vm_state_assignments.items():
            state_info = []
            estimated_parcels = 0
            
            for state_fips in state_list:
                state_name = self.state_names[state_fips]
                # Get county count for this state
                counties = self._get_state_counties(state_fips)
                county_count = len(counties)
                
                # Rough estimation: average 1000 parcels per county (varies widely)
                # This could be refined with actual database queries
                estimated_parcels_state = county_count * 1000
                estimated_parcels += estimated_parcels_state
                
                state_info.append({
                    'fips': state_fips,
                    'name': state_name,
                    'counties': county_count,
                    'estimated_parcels': estimated_parcels_state
                })
            
            vm_workloads[vm_id] = {
                'states': state_info,
                'total_states': len(state_list),
                'total_counties': sum(s['counties'] for s in state_info),
                'estimated_total_parcels': estimated_parcels
            }
            total_states += len(state_list)
        
        # Generate individual VM assignment files
        assignment_files = {}
        
        for vm_id, workload in vm_workloads.items():
            # Create VM-specific assignment file
            assignment = {
                'vm_id': vm_id,
                'assignment_generated': datetime.now().isoformat(),
                'processing_instructions': {
                    'command': 'python run_nationwide_processing.py --vm-mode',
                    'batch_size': 500,
                    'resume_checkpoints': True
                },
                'states_assigned': workload['states'],
                'workload_summary': {
                    'total_states': workload['total_states'],
                    'total_counties': workload['total_counties'],
                    'estimated_parcels': workload['estimated_total_parcels']
                }
            }
            
            # Save VM assignment file
            filename = f"{output_dir}/{vm_id}_assignment.json"
            with open(filename, 'w') as f:
                json.dump(assignment, f, indent=2)
            
            assignment_files[vm_id] = filename
            
            logger.info(f"Generated assignment for {vm_id}: "
                       f"{workload['total_states']} states, "
                       f"{workload['total_counties']} counties, "
                       f"~{workload['estimated_total_parcels']:,} parcels")
        
        # Generate master coordination file
        master_assignment = {
            'coordination_info': {
                'total_vms': len(self.vm_state_assignments),
                'total_states_assigned': total_states,
                'assignment_strategy': 'geographic_and_load_balanced',
                'created_timestamp': datetime.now().isoformat()
            },
            'vm_assignments': vm_workloads,
            'deployment_instructions': {
                'setup_steps': [
                    "1. Deploy identical VM instances with this codebase",
                    "2. Configure database connections (same credentials)",
                    "3. Copy VM-specific assignment file to each VM",
                    "4. Run: python run_vm_processing.py --assignment-file vm_X_assignment.json",
                    "5. Monitor progress via database checkpoints"
                ],
                'coordination_notes': [
                    "Each VM processes its assigned states independently",
                    "Database checkpoints enable progress tracking across VMs", 
                    "Failed states can be reassigned to available VMs",
                    "No coordination required between VMs during processing"
                ]
            }
        }
        
        master_file = f"{output_dir}/master_vm_coordination.json"
        with open(master_file, 'w') as f:
            json.dump(master_assignment, f, indent=2)
        
        logger.info(f"Master coordination file saved: {master_file}")
        logger.info(f"Total VMs: {len(self.vm_state_assignments)}, Total states: {total_states}")
        
        return {
            'success': True,
            'total_vms': len(self.vm_state_assignments),
            'total_states': total_states,
            'assignment_files': assignment_files,
            'master_file': master_file,
            'output_directory': output_dir
        }
    
    def get_vm_assignment_for_current_vm(self, assignment_file: str) -> Optional[Dict]:
        """
        Load VM assignment from file for current VM
        
        Args:
            assignment_file: Path to VM assignment JSON file
            
        Returns:
            VM assignment dictionary or None if failed
        """
        try:
            with open(assignment_file, 'r') as f:
                assignment = json.load(f)
            
            logger.info(f"Loaded assignment for {assignment['vm_id']}")
            logger.info(f"States to process: {len(assignment['states_assigned'])}")
            
            return assignment
            
        except Exception as e:
            logger.error(f"Failed to load VM assignment file {assignment_file}: {e}")
            return None
    
    def process_vm_assignment(self, assignment_file: str, batch_size: int = 500) -> Dict:
        """
        Process all states assigned to this VM
        
        Args:
            assignment_file: Path to VM assignment file
            batch_size: Batch size for processing
            
        Returns:
            Processing results summary
        """
        # Load assignment
        assignment = self.get_vm_assignment_for_current_vm(assignment_file)
        if not assignment:
            return {'success': False, 'error': 'Failed to load VM assignment'}
        
        vm_id = assignment['vm_id']
        states_to_process = assignment['states_assigned']
        
        logger.info(f"🚀 Starting processing for {vm_id}")
        logger.info(f"📋 Processing {len(states_to_process)} assigned states")
        
        # Import state controller here to avoid circular imports
        from .state_controller_v1 import state_controller
        
        # Process each assigned state
        vm_results = {
            'vm_id': vm_id,
            'processing_start': datetime.now().isoformat(),
            'states_completed': 0,
            'states_failed': [],
            'total_counties': 0,
            'total_parcels': 0
        }
        
        for i, state_info in enumerate(states_to_process):
            state_fips = state_info['fips']
            state_name = state_info['name']
            
            logger.info(f"\n{vm_id} - Processing state {i + 1}/{len(states_to_process)}: "
                       f"{state_name} ({state_fips})")
            
            try:
                # Process single state
                state_result = state_controller.process_single_state(state_fips, batch_size)
                
                if state_result['success']:
                    vm_results['states_completed'] += 1
                    vm_results['total_counties'] += state_result['counties_processed']
                    vm_results['total_parcels'] += state_result['total_parcels_processed']
                    logger.info(f"✅ {vm_id} - {state_name} completed: "
                               f"{state_result['counties_processed']} counties, "
                               f"{state_result['total_parcels_processed']} parcels")
                else:
                    logger.error(f"❌ {vm_id} - {state_name} failed: {state_result.get('error')}")
                    vm_results['states_failed'].append(state_fips)
                    
            except Exception as e:
                logger.error(f"💥 {vm_id} - Critical error processing {state_name}: {e}")
                vm_results['states_failed'].append(state_fips)
                continue
        
        vm_results['processing_end'] = datetime.now().isoformat()
        vm_results['success'] = vm_results['states_completed'] > 0
        
        # Save VM results
        results_file = f"logs/{vm_id}_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs('logs', exist_ok=True)
        
        with open(results_file, 'w') as f:
            json.dump(vm_results, f, indent=2)
        
        logger.info(f"🏁 {vm_id} processing complete:")
        logger.info(f"   ✅ States completed: {vm_results['states_completed']}/{len(states_to_process)}")
        logger.info(f"   ❌ States failed: {len(vm_results['states_failed'])}")
        logger.info(f"   📊 Total parcels: {vm_results['total_parcels']:,}")
        logger.info(f"   📄 Results saved: {results_file}")
        
        return vm_results
    
    def _get_state_counties(self, state_fips: str) -> List[str]:
        """Get counties for a state (reused from state_controller)"""
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
                return counties
                
        except Exception as e:
            logger.error(f"Error getting counties for state {state_fips}: {e}")
            return []
    
    def monitor_nationwide_progress(self) -> Dict:
        """
        Monitor processing progress across all VMs by checking database checkpoints
        
        Returns:
            Nationwide progress summary
        """
        try:
            with self.db_manager.get_connection('biomass_output') as conn:
                cursor = conn.cursor()
                
                # Get all processing checkpoints
                cursor.execute("""
                    SELECT county_fips, status, parcels_processed, errors_count,
                           checkpoint_timestamp
                    FROM processing_checkpoints
                    ORDER BY checkpoint_timestamp DESC
                """)
                
                checkpoints = cursor.fetchall()
                
                # Aggregate by state
                state_progress = {}
                total_counties_in_progress = 0
                total_counties_completed = 0
                total_parcels = 0
                
                for checkpoint in checkpoints:
                    county_fips = checkpoint['county_fips']
                    state_fips = county_fips[:2]
                    state_name = self.state_names.get(state_fips, f'State_{state_fips}')
                    
                    if state_fips not in state_progress:
                        state_progress[state_fips] = {
                            'state_name': state_name,
                            'counties_in_progress': 0,
                            'counties_completed': 0,
                            'total_parcels': 0
                        }
                    
                    if checkpoint['status'] == 'completed':
                        state_progress[state_fips]['counties_completed'] += 1
                        total_counties_completed += 1
                    else:
                        state_progress[state_fips]['counties_in_progress'] += 1
                        total_counties_in_progress += 1
                    
                    state_progress[state_fips]['total_parcels'] += checkpoint['parcels_processed']
                    total_parcels += checkpoint['parcels_processed']
                
                return {
                    'nationwide_summary': {
                        'total_counties_completed': total_counties_completed,
                        'total_counties_in_progress': total_counties_in_progress,
                        'total_parcels_processed': total_parcels,
                        'states_with_activity': len(state_progress)
                    },
                    'state_progress': state_progress,
                    'last_updated': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error monitoring nationwide progress: {e}")
            return {'error': str(e)}


# Global multi-VM coordinator instance
multi_vm_coordinator = MultiVMCoordinator()