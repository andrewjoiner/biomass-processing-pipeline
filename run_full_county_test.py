#!/usr/bin/env python3
"""
FULL COUNTY TEST - Process ENTIRE Rich County, Utah (ALL parcels)
This is the real end-to-end test - process every single parcel in the county
"""

import logging
import sys
import time
import json
from datetime import datetime

def setup_logging():
    """Configure logging for full county test"""
    import os
    os.makedirs('logs', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'logs/FULL_COUNTY_TEST_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"FULL COUNTY TEST logging - output to {log_filename}")
    return logger, log_filename

def main():
    """Process ENTIRE Rich County, Utah - ALL parcels"""
    logger, log_filename = setup_logging()
    
    logger.info("🇺🇸 FULL COUNTY TEST - Rich County, Utah")
    logger.info("=" * 80)
    logger.info("PROCESSING EVERY SINGLE PARCEL IN THE COUNTY")
    logger.info("This is the real test for 150 million parcel readiness")
    logger.info("=" * 80)
    
    test_start = time.time()
    test_results = {
        'test_start': datetime.now().isoformat(),
        'test_county': 'Rich County, Utah (49033) - COMPLETE COUNTY',
        'target_parcels': 'ALL PARCELS IN COUNTY',
        'log_file': log_filename
    }
    
    try:
        # Import the OPTIMIZED processor with Phase 1 improvements
        from src.pipeline.optimized_county_processor_v1 import optimized_county_processor
        from src.core.database_manager_v1 import database_manager
        
        state_fips = '49'  # Utah
        county_fips = '033'  # Rich County
        
        # Get actual parcel count first
        logger.info("📊 Getting total parcel count for Rich County...")
        all_parcels = database_manager.get_county_parcels(state_fips, county_fips, limit=None)
        total_parcel_count = len(all_parcels)
        
        logger.info(f"🎯 TOTAL PARCELS TO PROCESS: {total_parcel_count:,}")
        logger.info(f"🚀 Starting COMPLETE county processing...")
        logger.info("=" * 80)
        
        # Process ENTIRE county with NO limits
        processing_start = time.time()
        
        result = optimized_county_processor.process_county_optimized(
            state_fips=state_fips,
            county_fips=county_fips,
            max_parcels=None,  # NO LIMIT - PROCESS ALL PARCELS
            batch_size=1000   # Use optimized batch size from Phase 1
        )
        
        processing_time = time.time() - processing_start
        
        # Analyze results
        if result.get('success'):
            logger.info("🎉 COMPLETE COUNTY PROCESSING SUCCESSFUL!")
            logger.info("=" * 80)
            
            # Extract key metrics
            processing_summary = result.get('processing_summary', {})
            biomass_totals = result.get('biomass_totals', {})
            data_quality = result.get('data_quality', {})
            
            parcels_processed = processing_summary.get('parcels_processed', 0)
            processing_errors = processing_summary.get('processing_errors', 0)
            parcels_per_second = processing_summary.get('parcels_per_second', 0)
            processing_time_hours = processing_time / 3600
            
            total_biomass = biomass_totals.get('total_biomass_tons', 0)
            forest_biomass = biomass_totals.get('total_forest_biomass_tons', 0)
            crop_yield = biomass_totals.get('total_crop_yield_tons', 0)
            crop_residue = biomass_totals.get('total_crop_residue_tons', 0)
            
            avg_confidence = data_quality.get('average_confidence', 0)
            forest_coverage_rate = data_quality.get('forest_coverage_rate', 0)
            crop_coverage_rate = data_quality.get('crop_coverage_rate', 0)
            
            # Log comprehensive results
            logger.info("📊 COMPLETE PROCESSING RESULTS:")
            logger.info(f"   Total Parcels in County: {total_parcel_count:,}")
            logger.info(f"   Parcels Successfully Processed: {parcels_processed:,}")
            logger.info(f"   Processing Errors: {processing_errors:,}")
            logger.info(f"   Success Rate: {((parcels_processed - processing_errors) / parcels_processed * 100):.1f}%")
            logger.info(f"   Total Processing Time: {processing_time:.1f}s ({processing_time_hours:.2f} hours)")
            logger.info(f"   Average Processing Rate: {parcels_per_second:.2f} parcels/sec")
            logger.info(f"   Estimated Rate for 150M parcels: {150_000_000 / (parcels_per_second * 3600):.0f} hours")
            
            logger.info("\n🌿 BIOMASS PRODUCTION RESULTS:")
            logger.info(f"   Total Biomass Generated: {total_biomass:,.1f} tons")
            logger.info(f"   Forest Biomass: {forest_biomass:,.1f} tons")
            logger.info(f"   Crop Yield: {crop_yield:,.1f} tons") 
            logger.info(f"   Crop Residue: {crop_residue:,.1f} tons")
            logger.info(f"   Average Biomass per Parcel: {total_biomass / parcels_processed:.2f} tons/parcel")
            
            logger.info("\n🎯 DATA QUALITY METRICS:")
            logger.info(f"   Average Confidence Score: {avg_confidence:.3f}")
            logger.info(f"   Parcels with Forest: {forest_coverage_rate:.1%}")
            logger.info(f"   Parcels with Crops: {crop_coverage_rate:.1%}")
            
            # Success criteria for FULL county
            success_criteria_met = []
            success_criteria_failed = []
            
            # Criterion 1: High parcel processing success rate
            success_rate = ((parcels_processed - processing_errors) / parcels_processed * 100)
            if success_rate >= 95:
                success_criteria_met.append(f"✅ Excellent success rate: {success_rate:.1f}%")
            elif success_rate >= 85:
                success_criteria_met.append(f"✅ Good success rate: {success_rate:.1f}%")
            else:
                success_criteria_failed.append(f"❌ Low success rate: {success_rate:.1f}%")
            
            # Criterion 2: Substantial biomass production
            if total_biomass > 0:
                success_criteria_met.append(f"✅ Biomass production: {total_biomass:,.1f} tons")
            else:
                success_criteria_failed.append("❌ No biomass produced")
            
            # Criterion 3: Reasonable processing speed for 150M scale
            parcels_per_hour = parcels_per_second * 3600
            estimated_days_for_150m = 150_000_000 / (parcels_per_hour * 24)
            if estimated_days_for_150m <= 365:  # Less than a year
                success_criteria_met.append(f"✅ Scalable processing rate: {parcels_per_hour:,.0f} parcels/hour")
            else:
                success_criteria_failed.append(f"❌ Too slow for 150M scale: {estimated_days_for_150m:.0f} days estimated")
            
            # Criterion 4: Good confidence scores
            if avg_confidence >= 0.6:
                success_criteria_met.append(f"✅ High confidence: {avg_confidence:.3f}")
            elif avg_confidence >= 0.4:
                success_criteria_met.append(f"✅ Acceptable confidence: {avg_confidence:.3f}")
            else:
                success_criteria_failed.append(f"❌ Low confidence: {avg_confidence:.3f}")
            
            # Criterion 5: Land cover detection working
            if forest_coverage_rate > 0.1 or crop_coverage_rate > 0.1:
                success_criteria_met.append(f"✅ Land cover detection working (Forest: {forest_coverage_rate:.1%}, Crops: {crop_coverage_rate:.1%})")
            else:
                success_criteria_failed.append("❌ Poor land cover detection")
            
            # Criterion 6: Low error rate
            error_rate = (processing_errors / parcels_processed * 100)
            if error_rate <= 5:
                success_criteria_met.append(f"✅ Low error rate: {error_rate:.1f}%")
            else:
                success_criteria_failed.append(f"❌ High error rate: {error_rate:.1f}%")
            
            test_results.update({
                'processing_successful': True,
                'total_parcels_in_county': total_parcel_count,
                'parcels_processed': parcels_processed,
                'processing_errors': processing_errors,
                'success_rate_percent': success_rate,
                'processing_time_seconds': processing_time,
                'processing_time_hours': processing_time_hours,
                'parcels_per_second': parcels_per_second,
                'total_biomass_tons': total_biomass,
                'average_confidence': avg_confidence,
                'estimated_days_for_150m_parcels': estimated_days_for_150m,
                'success_criteria_met': success_criteria_met,
                'success_criteria_failed': success_criteria_failed,
                'full_results': result
            })
            
            # Overall assessment
            overall_success = len(success_criteria_failed) == 0
            test_results['overall_success'] = overall_success
            
            logger.info("\n" + "=" * 80)
            logger.info("🏆 SUCCESS CRITERIA ASSESSMENT:")
            for criterion in success_criteria_met:
                logger.info(f"   {criterion}")
            
            if success_criteria_failed:
                logger.warning("\n⚠️  FAILED CRITERIA:")
                for criterion in success_criteria_failed:
                    logger.warning(f"   {criterion}")
            
            logger.info("\n" + "=" * 80)
            if overall_success:
                logger.info("🎉 PIPELINE FULLY VALIDATED FOR 150M PARCEL DEPLOYMENT!")
                logger.info("✅ Complete county processed successfully")
                logger.info("✅ All success criteria met")
                logger.info("🚀 READY FOR NATIONWIDE VM DEPLOYMENT")
                exit_code = 0
            else:
                logger.error("🚨 PIPELINE VALIDATION ISSUES FOUND")
                logger.error("❌ Some success criteria failed")
                logger.error("🛠️  Address issues before VM deployment")
                exit_code = 1
                
        else:
            logger.error("💥 COMPLETE COUNTY PROCESSING FAILED!")
            error_message = result.get('error', 'Unknown error')
            logger.error(f"Error: {error_message}")
            
            test_results.update({
                'processing_successful': False,
                'error_message': error_message,
                'overall_success': False
            })
            exit_code = 1
        
        # Save detailed results
        test_results['test_end'] = datetime.now().isoformat()
        test_results['total_test_time_seconds'] = time.time() - test_start
        
        results_filename = f"logs/FULL_COUNTY_RESULTS_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_filename, 'w') as f:
            json.dump(test_results, f, indent=2, default=str)
        
        logger.info("=" * 80)
        logger.info(f"📄 Complete results saved to: {results_filename}")
        logger.info(f"📄 Full processing log: {log_filename}")
        logger.info("=" * 80)
        
        return exit_code
        
    except Exception as e:
        logger.error(f"💥 CRITICAL ERROR during full county test: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        test_results.update({
            'processing_successful': False,
            'critical_error': str(e),
            'overall_success': False
        })
        
        return 2

if __name__ == '__main__':
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n⏹️  Full county test interrupted by user")
        sys.exit(3)