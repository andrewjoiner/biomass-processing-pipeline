#!/usr/bin/env python3
"""
Optimized County Test - Test the new optimized batch processor
Compares performance against the original individual processing approach
"""

import logging
import sys
import time
import json
from datetime import datetime

def setup_logging():
    """Configure logging for optimized county test"""
    import os
    os.makedirs('logs', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'logs/OPTIMIZED_COUNTY_TEST_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Optimized county test logging - output to {log_filename}")
    return logger, log_filename

def main():
    """Test the optimized county processor performance"""
    logger, log_filename = setup_logging()
    
    logger.info("🚀 OPTIMIZED COUNTY PERFORMANCE TEST")
    logger.info("=" * 80)
    logger.info("Testing new batch processing approach vs original")
    logger.info("County: Rich County, Utah (49033)")
    logger.info("=" * 80)
    
    test_start = time.time()
    test_results = {
        'test_start': datetime.now().isoformat(),
        'test_county': 'Rich County, Utah (49033)',
        'log_file': log_filename
    }
    
    try:
        # Import the optimized processor
        from src.pipeline.optimized_county_processor_v1 import optimized_county_processor
        
        state_fips = '49'  # Utah
        county_fips = '033'  # Rich County
        
        # Test with a reasonable subset first to validate approach
        test_limit = 1000  # Start with 1000 parcels
        
        logger.info(f"🎯 Testing with {test_limit} parcels (subset for validation)")
        logger.info("=" * 80)
        
        # Run optimized processing
        processing_start = time.time()
        
        result = optimized_county_processor.process_county_optimized(
            state_fips=state_fips,
            county_fips=county_fips,
            max_parcels=test_limit,
            batch_size=500  # Process in batches of 500
        )
        
        processing_time = time.time() - processing_start
        
        # Analyze results
        if result.get('success'):
            logger.info("🎉 OPTIMIZED PROCESSING SUCCESSFUL!")
            logger.info("=" * 80)
            
            # Extract performance metrics
            processing_summary = result.get('processing_summary', {})
            performance_stats = result.get('performance_stats', {})
            
            parcels_processed = processing_summary.get('parcels_processed', 0)
            parcels_per_second = processing_summary.get('parcels_per_second', 0)
            setup_time = performance_stats.get('setup_time', 0)
            batch_times = performance_stats.get('batch_times', [])
            
            total_biomass = processing_summary.get('total_biomass_tons', 0)
            avg_biomass_per_parcel = processing_summary.get('average_biomass_per_parcel', 0)
            
            # Log detailed results
            logger.info("📊 OPTIMIZED PROCESSING RESULTS:")
            logger.info(f"   Parcels Processed: {parcels_processed:,}")
            logger.info(f"   Total Processing Time: {processing_time:.2f}s")
            logger.info(f"   Setup Time: {setup_time:.2f}s")
            logger.info(f"   Actual Processing Time: {processing_time - setup_time:.2f}s")
            logger.info(f"   Processing Rate: {parcels_per_second:.2f} parcels/sec")
            logger.info(f"   Time per Parcel: {processing_time / parcels_processed:.3f} seconds")
            
            if batch_times:
                avg_batch_time = sum(batch_times) / len(batch_times)
                logger.info(f"   Average Batch Time: {avg_batch_time:.2f}s")
                logger.info(f"   Number of Batches: {len(batch_times)}")
            
            logger.info("\n🌿 BIOMASS RESULTS:")
            logger.info(f"   Total Biomass: {total_biomass:.1f} tons")
            logger.info(f"   Average per Parcel: {avg_biomass_per_parcel:.3f} tons/parcel")
            
            # Performance comparison with targets
            target_time_per_parcel = 0.1  # Target from config
            actual_time_per_parcel = processing_time / parcels_processed if parcels_processed > 0 else float('inf')
            performance_ratio = actual_time_per_parcel / target_time_per_parcel
            
            logger.info("\n🎯 PERFORMANCE ANALYSIS:")
            logger.info(f"   Target: {target_time_per_parcel:.1f}s per parcel")
            logger.info(f"   Actual: {actual_time_per_parcel:.3f}s per parcel")
            logger.info(f"   Performance Ratio: {performance_ratio:.1f}x target")
            
            if performance_ratio <= 1.0:
                logger.info("✅ PERFORMANCE TARGET MET!")
            elif performance_ratio <= 2.0:
                logger.info("🟡 Close to target (within 2x)")
            else:
                logger.info(f"🔴 Needs improvement ({performance_ratio:.1f}x slower than target)")
            
            # Scale estimates
            if parcels_per_second > 0:
                full_county_time_hours = 10766 / parcels_per_second / 3600
                national_time_months = 150_000_000 / parcels_per_second / (3600 * 24 * 30)
                
                logger.info("\n📈 SCALING ESTIMATES:")
                logger.info(f"   Full Rich County (10,766 parcels): {full_county_time_hours:.1f} hours")
                logger.info(f"   National (150M parcels): {national_time_months:.1f} months")
            
            # Success criteria assessment
            success_criteria = []
            
            if parcels_processed >= test_limit * 0.95:
                success_criteria.append("✅ High processing success rate")
            else:
                success_criteria.append("❌ Low processing success rate")
            
            if performance_ratio <= 5.0:  # Within 5x of target is acceptable for initial implementation
                success_criteria.append("✅ Reasonable performance")
            else:
                success_criteria.append("❌ Performance needs improvement")
            
            if total_biomass > 0:
                success_criteria.append("✅ Biomass calculations working")
            else:
                success_criteria.append("❌ No biomass calculated")
            
            if setup_time < processing_time * 0.5:  # Setup should be < 50% of processing time
                success_criteria.append("✅ Efficient setup")
            else:
                success_criteria.append("❌ Setup time too high")
            
            test_results.update({
                'processing_successful': True,
                'parcels_processed': parcels_processed,
                'processing_time_seconds': processing_time,
                'parcels_per_second': parcels_per_second,
                'time_per_parcel_seconds': actual_time_per_parcel,
                'performance_ratio_to_target': performance_ratio,
                'total_biomass_tons': total_biomass,
                'setup_time_seconds': setup_time,
                'success_criteria': success_criteria,
                'full_results': result
            })
            
            logger.info("\n" + "=" * 80)
            logger.info("🏆 SUCCESS CRITERIA ASSESSMENT:")
            for criterion in success_criteria:
                logger.info(f"   {criterion}")
            
            logger.info("\n" + "=" * 80)
            overall_success = all("✅" in criterion for criterion in success_criteria)
            if overall_success:
                logger.info("🎉 OPTIMIZED PROCESSOR VALIDATION SUCCESSFUL!")
                logger.info("✅ All criteria met - ready for full county testing")
                exit_code = 0
            else:
                logger.warning("⚠️ SOME OPTIMIZATION ISSUES FOUND")
                logger.warning("🔧 Continue refinement before full deployment")
                exit_code = 1
                
        else:
            logger.error("💥 OPTIMIZED PROCESSING FAILED!")
            error_message = result.get('error', 'Unknown error')
            logger.error(f"Error: {error_message}")
            
            test_results.update({
                'processing_successful': False,
                'error_message': error_message
            })
            exit_code = 1
        
        # Save test results
        test_results['test_end'] = datetime.now().isoformat()
        test_results['total_test_time_seconds'] = time.time() - test_start
        
        results_filename = f"logs/OPTIMIZED_COUNTY_RESULTS_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_filename, 'w') as f:
            json.dump(test_results, f, indent=2, default=str)
        
        logger.info("=" * 80)
        logger.info(f"📄 Test results saved to: {results_filename}")
        logger.info(f"📄 Full processing log: {log_filename}")
        logger.info("=" * 80)
        
        return exit_code
        
    except Exception as e:
        logger.error(f"💥 CRITICAL ERROR in optimized county test: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        test_results.update({
            'processing_successful': False,
            'critical_error': str(e)
        })
        
        return 2

if __name__ == '__main__':
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n⏹️ Optimized county test interrupted by user")
        sys.exit(3)