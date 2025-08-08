#!/usr/bin/env python3
"""
Quick Pipeline Validation - Fast validation of critical pipeline components
Tests basic functionality before running full end-to-end test
"""

import logging
import sys
import time
from datetime import datetime

def setup_logging():
    """Configure logging for validation"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger(__name__)

def main():
    """Quick validation of pipeline components"""
    logger = setup_logging()
    
    logger.info("🚀 QUICK PIPELINE VALIDATION")
    logger.info("=" * 50)
    
    validation_results = {
        'start_time': datetime.now(),
        'tests_passed': 0,
        'tests_failed': 0,
        'issues_found': []
    }
    
    # Test 1: Import all modules
    logger.info("📦 Testing module imports...")
    try:
        from src.core.database_manager_v1 import database_manager
        from src.core.blob_manager_v1 import blob_manager
        from src.pipeline.comprehensive_biomass_processor_v1 import comprehensive_biomass_processor
        from src.pipeline.state_controller_v1 import state_controller
        from src.analyzers.crop_analyzer_v1 import crop_analyzer
        from src.analyzers.forest_analyzer_v1 import forest_analyzer
        from src.analyzers.landcover_analyzer_v1 import landcover_analyzer
        from src.analyzers.vegetation_analyzer_v1 import vegetation_analyzer
        
        logger.info("✅ All modules imported successfully")
        validation_results['tests_passed'] += 1
        
    except Exception as e:
        logger.error(f"❌ Module import failed: {e}")
        validation_results['tests_failed'] += 1
        validation_results['issues_found'].append(f"Module import: {e}")
    
    # Test 2: Database connectivity
    logger.info("🔍 Testing database connectivity...")
    try:
        db_status = database_manager.test_connections()
        failed_dbs = [db for db, status in db_status.items() if not status]
        
        if not failed_dbs:
            logger.info(f"✅ All databases connected: {list(db_status.keys())}")
            validation_results['tests_passed'] += 1
        else:
            logger.error(f"❌ Database connection failed: {failed_dbs}")
            validation_results['tests_failed'] += 1
            validation_results['issues_found'].append(f"Database connectivity: {failed_dbs}")
            
    except Exception as e:
        logger.error(f"❌ Database test failed: {e}")
        validation_results['tests_failed'] += 1
        validation_results['issues_found'].append(f"Database test: {e}")
    
    # Test 3: Rich County data availability
    logger.info("📍 Testing Rich County data availability...")
    try:
        # Test county bounds
        bounds = database_manager.get_county_bounds('49', '033')
        if bounds:
            logger.info(f"✅ Rich County bounds: {bounds}")
            
            # Test parcel availability
            parcels = database_manager.get_county_parcels('49', '033', limit=5)
            if parcels:
                logger.info(f"✅ Found {len(parcels)} test parcels in Rich County")
                validation_results['tests_passed'] += 1
            else:
                logger.error("❌ No parcels found in Rich County")
                validation_results['tests_failed'] += 1
                validation_results['issues_found'].append("No parcels in Rich County")
        else:
            logger.error("❌ Could not get Rich County bounds")
            validation_results['tests_failed'] += 1
            validation_results['issues_found'].append("Rich County bounds unavailable")
            
    except Exception as e:
        logger.error(f"❌ Rich County test failed: {e}")
        validation_results['tests_failed'] += 1
        validation_results['issues_found'].append(f"Rich County test: {e}")
    
    # Test 4: Blob storage connectivity
    logger.info("☁️  Testing blob storage connectivity...")
    try:
        blob_stats = blob_manager.get_cache_stats()
        logger.info(f"✅ Blob storage accessible: {blob_stats}")
        validation_results['tests_passed'] += 1
        
    except Exception as e:
        logger.error(f"❌ Blob storage test failed: {e}")
        validation_results['tests_failed'] += 1
        validation_results['issues_found'].append(f"Blob storage: {e}")
    
    # Test 5: Basic processing components
    logger.info("⚙️  Testing processing components...")
    try:
        # Test processor status
        processor_status = comprehensive_biomass_processor.get_processing_status()
        logger.info("✅ Comprehensive processor accessible")
        
        # Test state controller
        controller_status = state_controller.get_processing_status()
        logger.info("✅ State controller accessible")
        
        validation_results['tests_passed'] += 1
        
    except Exception as e:
        logger.error(f"❌ Processing components test failed: {e}")
        validation_results['tests_failed'] += 1
        validation_results['issues_found'].append(f"Processing components: {e}")
    
    # Final assessment
    validation_results['end_time'] = datetime.now()
    validation_results['duration_seconds'] = (validation_results['end_time'] - validation_results['start_time']).total_seconds()
    
    total_tests = validation_results['tests_passed'] + validation_results['tests_failed']
    success_rate = (validation_results['tests_passed'] / total_tests * 100) if total_tests > 0 else 0
    
    logger.info("=" * 50)
    logger.info("🏁 VALIDATION COMPLETE")
    logger.info(f"📊 Tests Passed: {validation_results['tests_passed']}")
    logger.info(f"📊 Tests Failed: {validation_results['tests_failed']}")
    logger.info(f"📊 Success Rate: {success_rate:.1f}%")
    logger.info(f"⏱️  Duration: {validation_results['duration_seconds']:.1f}s")
    
    if validation_results['issues_found']:
        logger.warning("⚠️  Issues Found:")
        for issue in validation_results['issues_found']:
            logger.warning(f"   • {issue}")
    
    if success_rate >= 80:
        logger.info("✅ BASIC VALIDATION PASSED - Ready for end-to-end test")
        return 0
    else:
        logger.error("❌ BASIC VALIDATION FAILED - Fix issues before proceeding")
        return 1

if __name__ == '__main__':
    try:
        exit_code = main()
        sys.exit(exit_code)
    except Exception as e:
        print(f"💥 Validation crashed: {e}")
        sys.exit(3)