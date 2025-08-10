#!/usr/bin/env python3
"""
V3 QUICK VALIDATION - Fast V3 performance validation with 100 McLean County parcels
Validates that V3 enhanced processing maintains acceptable performance vs V1
"""

import logging
import sys
import time
import json
from datetime import datetime

def setup_logging():
    """Configure logging for V3 quick validation"""
    import os
    os.makedirs('logs', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'logs/V3_QUICK_VALIDATION_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"V3 quick validation logging - output to {log_filename}")
    return logger, log_filename

def main():
    """Quick V3 performance validation with 100 parcels"""
    logger, log_filename = setup_logging()
    
    logger.info("⚡ V3 QUICK VALIDATION - 100 Parcel Performance Test")
    logger.info("=" * 60)
    logger.info("Target: Verify V3 enhanced processing maintains V1 performance")
    logger.info("County: McLean County, Illinois (17113) - 100 parcels")
    logger.info("Baseline: V1 processes ~300 parcels/second")
    logger.info("=" * 60)
    
    test_start = time.time()
    validation_results = {
        'test_start': datetime.now().isoformat(),
        'test_type': 'V3 Quick Performance Validation',
        'test_parcels': 100,
        'target_county': 'McLean County, Illinois (17113)',
        'performance_baseline': '300 parcels/second (V1)',
        'log_file': log_filename
    }
    
    try:
        # Test 1: Module imports (V3 components)
        logger.info("📦 Testing V3 module imports...")
        try:
            from src.core.database_manager_v3 import database_manager
            from src.core.blob_manager_v3 import blob_manager
            from src.pipeline.comprehensive_biomass_processor_v3 import comprehensive_biomass_processor
            from src.analyzers.crop_analyzer_v3 import crop_analyzer
            from src.analyzers.forest_analyzer_v3 import forest_analyzer
            from src.analyzers.vegetation_analyzer_v3 import vegetation_analyzer
            
            logger.info("✅ All V3 modules imported successfully")
            validation_results['v3_imports'] = True
            
        except Exception as e:
            logger.error(f"❌ V3 module import failed: {e}")
            validation_results['v3_imports'] = False
            validation_results['import_error'] = str(e)
            return 1
        
        # Test 2: V3 Database connectivity
        logger.info("🔍 Testing V3 database connectivity...")
        try:
            db_status = database_manager.test_connections()
            failed_dbs = [db for db, status in db_status.items() if not status]
            
            if not failed_dbs:
                logger.info(f"✅ All V3 databases connected: {list(db_status.keys())}")
                validation_results['v3_database_connection'] = True
            else:
                logger.error(f"❌ V3 database connection failed: {failed_dbs}")
                validation_results['v3_database_connection'] = False
                validation_results['failed_databases'] = failed_dbs
                return 1
                
        except Exception as e:
            logger.error(f"❌ V3 database test failed: {e}")
            validation_results['v3_database_connection'] = False
            validation_results['database_error'] = str(e)
            return 1
        
        # Test 3: McLean County data availability
        logger.info("🌽 Testing McLean County data availability...")
        try:
            state_fips = '17'   # Illinois
            county_fips = '113'  # McLean County
            
            # Test county bounds
            bounds = database_manager.get_county_bounds(state_fips, county_fips)
            if bounds:
                logger.info(f"✅ McLean County bounds: {bounds}")
                
                # Test parcel availability
                test_parcels = database_manager.get_county_parcels(state_fips, county_fips, limit=100)
                if test_parcels:
                    logger.info(f"✅ Found {len(test_parcels)} test parcels in McLean County")
                    validation_results['mclean_parcels_available'] = len(test_parcels)
                else:
                    logger.error("❌ No parcels found in McLean County")
                    validation_results['mclean_parcels_available'] = 0
                    return 1
            else:
                logger.error("❌ Could not get McLean County bounds")
                validation_results['mclean_county_bounds'] = False
                return 1
                
        except Exception as e:
            logger.error(f"❌ McLean County test failed: {e}")
            validation_results['mclean_county_error'] = str(e)
            return 1
        
        # Test 4: V3 Enhanced Processing Performance
        logger.info("🚀 Testing V3 enhanced processing performance...")
        logger.info(f"Processing 100 McLean County parcels with V3 enhancements...")
        
        processing_start = time.time()
        
        try:
            from src.pipeline.optimized_county_processor_v3 import optimized_county_processor
            
            result = optimized_county_processor.process_county_optimized(
                state_fips=state_fips,
                county_fips=county_fips,
                max_parcels=100,  # Quick validation with 100 parcels
                batch_size=25     # Smaller batches for quick test
            )
            
            processing_time = time.time() - processing_start
            
            if result.get('success'):
                processing_summary = result.get('processing_summary', {})
                
                parcels_processed = processing_summary.get('parcels_processed', 0)
                processing_errors = processing_summary.get('processing_errors', 0)
                parcels_per_second = processing_summary.get('parcels_per_second', 0)
                
                logger.info("✅ V3 processing completed successfully")
                logger.info(f"   Parcels Processed: {parcels_processed}")
                logger.info(f"   Processing Errors: {processing_errors}")
                logger.info(f"   Processing Time: {processing_time:.2f}s")
                logger.info(f"   Processing Rate: {parcels_per_second:.2f} parcels/sec")
                
                # Quick check of V3 enhanced data
                logger.info("🔍 Validating V3 enhanced data capture...")
                
                with database_manager.get_connection('biomass_output') as conn:
                    cursor = conn.cursor()
                    
                    # Count V3 records
                    cursor.execute("SELECT COUNT(*) as count FROM forestry_analysis_v3 WHERE county_fips = %s", (county_fips,))
                    forestry_count = cursor.fetchone()['count']
                    
                    cursor.execute("SELECT COUNT(*) as count FROM crop_analysis_v3 WHERE county_fips = %s", (county_fips,))
                    crop_count = cursor.fetchone()['count']
                    
                    cursor.execute("SELECT COUNT(DISTINCT parcel_id) as count FROM crop_analysis_v3 WHERE county_fips = %s", (county_fips,))
                    unique_crop_parcels = cursor.fetchone()['count']
                
                avg_crops_per_parcel = crop_count / unique_crop_parcels if unique_crop_parcels > 0 else 0
                
                logger.info(f"   Forestry Records: {forestry_count}")
                logger.info(f"   Crop Records: {crop_count}")
                logger.info(f"   Unique Crop Parcels: {unique_crop_parcels}")
                logger.info(f"   Avg Crops per Parcel: {avg_crops_per_parcel:.2f}")
                
                validation_results.update({
                    'v3_processing_success': True,
                    'parcels_processed': parcels_processed,
                    'processing_errors': processing_errors,
                    'processing_time_seconds': processing_time,
                    'parcels_per_second': parcels_per_second,
                    'forestry_records': forestry_count,
                    'crop_records': crop_count,
                    'unique_crop_parcels': unique_crop_parcels,
                    'avg_crops_per_parcel': avg_crops_per_parcel
                })
                
            else:
                logger.error("❌ V3 processing failed")
                logger.error(f"Error: {result.get('error', 'Unknown error')}")
                validation_results['v3_processing_success'] = False
                validation_results['processing_error'] = result.get('error', 'Unknown error')
                return 1
                
        except Exception as e:
            logger.error(f"❌ V3 processing test failed: {e}")
            validation_results['v3_processing_success'] = False
            validation_results['processing_exception'] = str(e)
            return 1
        
        # Performance Assessment
        logger.info("📊 V3 Performance Assessment...")
        
        performance_criteria = []
        performance_issues = []
        
        # Criterion 1: Processing speed
        if parcels_per_second >= 200:  # Acceptable for V3 enhanced processing
            performance_criteria.append(f"✅ Good processing speed: {parcels_per_second:.2f} parcels/sec")
        elif parcels_per_second >= 100:
            performance_criteria.append(f"⚠️  Acceptable processing speed: {parcels_per_second:.2f} parcels/sec")
        else:
            performance_issues.append(f"❌ Slow processing speed: {parcels_per_second:.2f} parcels/sec")
        
        # Criterion 2: Low error rate
        error_rate = (processing_errors / parcels_processed * 100) if parcels_processed > 0 else 100
        if error_rate <= 5:
            performance_criteria.append(f"✅ Low error rate: {error_rate:.1f}%")
        else:
            performance_issues.append(f"❌ High error rate: {error_rate:.1f}%")
        
        # Criterion 3: Enhanced data capture
        if avg_crops_per_parcel > 1.1:
            performance_criteria.append(f"✅ Multiple crops captured: {avg_crops_per_parcel:.2f} avg")
        else:
            performance_issues.append(f"❌ Limited crop capture: {avg_crops_per_parcel:.2f} avg")
        
        # Criterion 4: Database records created
        if forestry_count > 0 and crop_count > 0:
            performance_criteria.append(f"✅ V3 tables populated: {forestry_count} forestry, {crop_count} crop")
        else:
            performance_issues.append("❌ V3 tables not properly populated")
        
        validation_results['performance_criteria_met'] = performance_criteria
        validation_results['performance_issues'] = performance_issues
        
        # Final assessment
        overall_success = len(performance_issues) == 0
        validation_results['overall_validation_success'] = overall_success
        
        logger.info("\n" + "=" * 60)
        logger.info("🏆 V3 QUICK VALIDATION RESULTS:")
        
        for criterion in performance_criteria:
            logger.info(f"   {criterion}")
        
        if performance_issues:
            logger.warning("\n⚠️  PERFORMANCE ISSUES:")
            for issue in performance_issues:
                logger.warning(f"   {issue}")
        
        logger.info("\n" + "=" * 60)
        if overall_success:
            logger.info("🎉 V3 QUICK VALIDATION PASSED!")
            logger.info("✅ V3 performance is acceptable")
            logger.info("✅ Enhanced data capture working")
            logger.info("🚀 Ready for full McLean County test")
            exit_code = 0
        else:
            logger.error("🚨 V3 QUICK VALIDATION FAILED")
            logger.error("❌ Performance or functionality issues found")
            logger.error("🛠️  Fix issues before full county test")
            exit_code = 1
        
        # Save validation results
        validation_results['test_end'] = datetime.now().isoformat()
        validation_results['total_test_time_seconds'] = time.time() - test_start
        
        results_filename = f"logs/V3_QUICK_VALIDATION_RESULTS_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_filename, 'w') as f:
            json.dump(validation_results, f, indent=2, default=str)
        
        logger.info("=" * 60)
        logger.info(f"📄 Validation results saved to: {results_filename}")
        logger.info(f"📄 Full validation log: {log_filename}")
        logger.info("=" * 60)
        
        return exit_code
        
    except Exception as e:
        logger.error(f"💥 CRITICAL ERROR during V3 quick validation: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        validation_results.update({
            'validation_success': False,
            'critical_error': str(e)
        })
        
        return 2

if __name__ == '__main__':
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n⏹️  V3 quick validation interrupted by user")
        sys.exit(3)