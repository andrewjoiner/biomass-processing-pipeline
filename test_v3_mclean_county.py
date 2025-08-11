#!/usr/bin/env python3
"""
V3 MCLEAN COUNTY TEST - Test V3 enhanced data capture with McLean County, Illinois
McLean County chosen for diverse agriculture (corn/soybean) and forestry mix
This validates V3's enhanced crop analysis (multiple crops per parcel) and forestry breakdown
"""

import logging
import sys
import time
import json
from datetime import datetime

def setup_logging():
    """Configure logging for McLean County V3 test"""
    import os
    os.makedirs('logs', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'logs/V3_MCLEAN_COUNTY_TEST_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"V3 McLean County test logging - output to {log_filename}")
    return logger, log_filename

def main():
    """Test V3 enhanced data capture with McLean County, Illinois"""
    logger, log_filename = setup_logging()
    
    logger.info("🌽 V3 MCLEAN COUNTY TEST - Enhanced Data Capture Validation")
    logger.info("=" * 80)
    logger.info("County: McLean County, Illinois (17113)")
    logger.info("Focus: Multiple crops per parcel + forestry species breakdown")
    logger.info("V3 Features: Enhanced crop_analysis_v3 & forestry_analysis_v3 tables")
    logger.info("=" * 80)
    
    test_start = time.time()
    test_results = {
        'test_start': datetime.now().isoformat(),
        'test_county': 'McLean County, Illinois (17113)',
        'test_focus': 'V3 Enhanced Data Capture',
        'log_file': log_filename,
        'v3_features_tested': [
            'Multiple crops per parcel in crop_analysis_v3',
            'Forestry species breakdown in forestry_analysis_v3',
            'Enhanced NDVI and confidence scoring',
            'Biomass component separation (bole/branch/foliage)'
        ]
    }
    
    try:
        # Import V3 processor components
        from src.pipeline.optimized_county_processor_v3 import optimized_county_processor
        from src.core.database_manager_v3 import database_manager
        
        state_fips = '17'   # Illinois
        county_fips = '113'  # McLean County
        
        # Get total parcel count for McLean County
        logger.info("📊 Getting McLean County parcel information...")
        all_parcels = database_manager.get_county_parcels(state_fips, county_fips, limit=None)
        total_parcel_count = len(all_parcels)
        
        logger.info(f"🎯 MCLEAN COUNTY PARCELS: {total_parcel_count:,}")
        logger.info(f"🌾 Expected: Mix of agricultural (corn/soybean) and forested parcels")
        logger.info(f"🚀 Starting V3 enhanced processing...")
        logger.info("=" * 80)
        
        # Process McLean County with V3 enhanced data capture
        processing_start = time.time()
        
        result = optimized_county_processor.process_county_optimized(
            state_fips=state_fips,
            county_fips=county_fips,
            max_parcels=None,  # Process all parcels to validate full V3 capability
            batch_size=50      # V3 optimized batch size for enhanced data writes
        )
        
        processing_time = time.time() - processing_start
        
        # Analyze V3 enhanced results
        if result.get('success'):
            logger.info("🎉 V3 MCLEAN COUNTY PROCESSING SUCCESSFUL!")
            logger.info("=" * 80)
            
            # Extract processing metrics
            processing_summary = result.get('processing_summary', {})
            
            parcels_processed = processing_summary.get('parcels_processed', 0)
            processing_errors = processing_summary.get('processing_errors', 0)
            parcels_per_second = processing_summary.get('parcels_per_second', 0)
            processing_time_minutes = processing_time / 60
            
            # V3 Enhanced data metrics
            total_biomass = processing_summary.get('total_biomass_tons', 0)
            forest_parcels_processed = processing_summary.get('forest_parcels_processed', 0)
            crop_parcels_processed = processing_summary.get('crop_parcels_processed', 0)
            
            # Get V3 database record counts
            logger.info("🔍 Validating V3 enhanced database records...")
            
            # Query V3 tables for data validation
            with database_manager.get_connection('biomass_output') as conn:
                cursor = conn.cursor()
                
                # Count forestry_analysis_v3 records
                cursor.execute("SELECT COUNT(*) as count FROM forestry_analysis_v3 WHERE county_fips = %s", (county_fips,))
                forestry_records = cursor.fetchone()['count']
                
                # Count crop_analysis_v3 records (should be MORE than parcels due to multiple crops)
                cursor.execute("SELECT COUNT(*) as count FROM crop_analysis_v3 WHERE county_fips = %s", (county_fips,))
                crop_records = cursor.fetchone()['count']
                
                # Count unique parcels in crop table
                cursor.execute("SELECT COUNT(DISTINCT parcel_id) as count FROM crop_analysis_v3 WHERE county_fips = %s", (county_fips,))
                unique_crop_parcels = cursor.fetchone()['count']
                
                # Get average crops per parcel
                cursor.execute("""
                    SELECT parcel_id, COUNT(*) as crop_count 
                    FROM crop_analysis_v3 
                    WHERE county_fips = %s 
                    GROUP BY parcel_id 
                    ORDER BY crop_count DESC 
                    LIMIT 10
                """, (county_fips,))
                top_crop_parcels = cursor.fetchall()
                
                # Get sample enhanced forestry data
                cursor.execute("""
                    SELECT parcel_id, dominant_species_name, species_diversity_index,
                           bole_biomass_tons, branch_biomass_tons, foliage_biomass_tons,
                           harvest_probability, ndvi_value
                    FROM forestry_analysis_v3 
                    WHERE county_fips = %s AND total_biomass_tons > 0
                    ORDER BY total_biomass_tons DESC 
                    LIMIT 5
                """, (county_fips,))
                sample_forestry = cursor.fetchall()
                
                # Get sample enhanced crop data
                cursor.execute("""
                    SELECT parcel_id, crop_name, area_percentage, 
                           estimated_yield_tons, ndvi_value, confidence_score
                    FROM crop_analysis_v3 
                    WHERE county_fips = %s 
                    ORDER BY area_percentage DESC 
                    LIMIT 10
                """, (county_fips,))
                sample_crops = cursor.fetchall()
            
            avg_crops_per_parcel = crop_records / unique_crop_parcels if unique_crop_parcels > 0 else 0
            
            # Log comprehensive V3 results
            logger.info("📊 V3 PROCESSING PERFORMANCE:")
            logger.info(f"   Total Parcels in County: {total_parcel_count:,}")
            logger.info(f"   Parcels Successfully Processed: {parcels_processed:,}")
            logger.info(f"   Processing Errors: {processing_errors:,}")
            logger.info(f"   Success Rate: {((parcels_processed - processing_errors) / parcels_processed * 100):.1f}%")
            logger.info(f"   Total Processing Time: {processing_time:.1f}s ({processing_time_minutes:.2f} minutes)")
            logger.info(f"   Processing Rate: {parcels_per_second:.2f} parcels/sec")
            
            logger.info("\n🌿 V3 ENHANCED DATA CAPTURE:")
            logger.info(f"   Forestry Records Created: {forestry_records:,}")
            logger.info(f"   Crop Records Created: {crop_records:,}")
            logger.info(f"   Unique Parcels with Crops: {unique_crop_parcels:,}")
            logger.info(f"   Average Crops per Parcel: {avg_crops_per_parcel:.2f}")
            logger.info(f"   Total Biomass Generated: {total_biomass:,.1f} tons")
            
            logger.info("\n🌽 TOP AGRICULTURAL PARCELS (Multiple Crops):")
            for parcel in top_crop_parcels[:5]:
                logger.info(f"   Parcel {parcel['parcel_id']}: {parcel['crop_count']} different crops")
            
            if sample_forestry:
                logger.info("\n🌲 SAMPLE ENHANCED FORESTRY DATA:")
                for forest in sample_forestry:
                    logger.info(f"   Parcel {forest['parcel_id']}: {forest['dominant_species_name'] or 'Mixed'}")
                    logger.info(f"      Species Diversity: {forest['species_diversity_index'] or 'N/A'}")
                    logger.info(f"      Biomass Components: Bole={forest['bole_biomass_tons'] or 0:.1f}t, Branch={forest['branch_biomass_tons'] or 0:.1f}t, Foliage={forest['foliage_biomass_tons'] or 0:.1f}t")
                    logger.info(f"      Harvest Probability: {forest['harvest_probability'] or 0:.2f}, NDVI: {forest['ndvi_value'] or 'N/A'}")
            
            if sample_crops:
                logger.info("\n🌾 SAMPLE ENHANCED CROP DATA:")
                for crop in sample_crops:
                    logger.info(f"   Parcel {crop['parcel_id']}: {crop['crop_name']}")
                    logger.info(f"      Coverage: {crop['area_percentage']:.1f}%, Yield: {crop['estimated_yield_tons'] or 0:.1f}t")
                    logger.info(f"      NDVI: {crop['ndvi_value'] or 'N/A'}, Confidence: {crop['confidence_score'] or 'N/A'}")
            
            # V3 Success Criteria Assessment
            success_criteria_met = []
            success_criteria_failed = []
            
            # Performance criteria
            success_rate = ((parcels_processed - processing_errors) / parcels_processed * 100)
            if success_rate >= 90:
                success_criteria_met.append(f"✅ High success rate: {success_rate:.1f}%")
            else:
                success_criteria_failed.append(f"❌ Low success rate: {success_rate:.1f}%")
            
            # V3 Enhanced data criteria
            if avg_crops_per_parcel > 1.2:  # Agricultural parcels should have multiple crops
                success_criteria_met.append(f"✅ Multiple crops per parcel: {avg_crops_per_parcel:.2f} avg")
            else:
                success_criteria_failed.append(f"❌ Limited crop diversity: {avg_crops_per_parcel:.2f} avg")
            
            if forestry_records > 0:
                success_criteria_met.append(f"✅ Enhanced forestry data captured: {forestry_records:,} records")
            else:
                success_criteria_failed.append("❌ No enhanced forestry data captured")
            
            if crop_records > parcels_processed:  # Should have more crop records than parcels
                success_criteria_met.append(f"✅ Multiple crop records: {crop_records:,} vs {parcels_processed:,} parcels")
            else:
                success_criteria_failed.append("❌ Not capturing multiple crops per parcel")
            
            # Performance vs V1 criteria
            if parcels_per_second >= 200:  # Acceptable performance for V3 enhanced processing
                success_criteria_met.append(f"✅ Acceptable V3 performance: {parcels_per_second:.2f} parcels/sec")
            else:
                success_criteria_failed.append(f"❌ V3 performance too slow: {parcels_per_second:.2f} parcels/sec")
            
            test_results.update({
                'processing_successful': True,
                'total_parcels': total_parcel_count,
                'parcels_processed': parcels_processed,
                'processing_errors': processing_errors,
                'success_rate_percent': success_rate,
                'processing_time_seconds': processing_time,
                'parcels_per_second': parcels_per_second,
                'forestry_records_created': forestry_records,
                'crop_records_created': crop_records,
                'unique_crop_parcels': unique_crop_parcels,
                'avg_crops_per_parcel': avg_crops_per_parcel,
                'total_biomass_tons': total_biomass,
                'sample_forestry_data': [dict(row) for row in sample_forestry] if sample_forestry else [],
                'sample_crop_data': [dict(row) for row in sample_crops] if sample_crops else [],
                'success_criteria_met': success_criteria_met,
                'success_criteria_failed': success_criteria_failed
            })
            
            # Overall V3 assessment
            overall_success = len(success_criteria_failed) == 0
            test_results['overall_v3_success'] = overall_success
            
            logger.info("\n" + "=" * 80)
            logger.info("🏆 V3 SUCCESS CRITERIA ASSESSMENT:")
            for criterion in success_criteria_met:
                logger.info(f"   {criterion}")
            
            if success_criteria_failed:
                logger.warning("\n⚠️  V3 ISSUES FOUND:")
                for criterion in success_criteria_failed:
                    logger.warning(f"   {criterion}")
            
            logger.info("\n" + "=" * 80)
            if overall_success:
                logger.info("🎉 V3 ENHANCED DATA CAPTURE VALIDATED!")
                logger.info("✅ McLean County processed with enhanced crop/forestry analysis")
                logger.info("✅ Multiple crops per parcel successfully captured")
                logger.info("✅ Enhanced forestry species breakdown working")
                logger.info("🚀 V3 READY FOR PRODUCTION DEPLOYMENT")
                exit_code = 0
            else:
                logger.error("🚨 V3 VALIDATION ISSUES FOUND")
                logger.error("❌ Enhanced data capture has problems")
                logger.error("🛠️  Review V3 implementation before deployment")
                exit_code = 1
                
        else:
            logger.error("💥 V3 MCLEAN COUNTY PROCESSING FAILED!")
            error_message = result.get('error', 'Unknown error')
            logger.error(f"Error: {error_message}")
            
            test_results.update({
                'processing_successful': False,
                'error_message': error_message,
                'overall_v3_success': False
            })
            exit_code = 1
        
        # Save V3 test results
        test_results['test_end'] = datetime.now().isoformat()
        test_results['total_test_time_seconds'] = time.time() - test_start
        
        results_filename = f"logs/V3_MCLEAN_COUNTY_RESULTS_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_filename, 'w') as f:
            json.dump(test_results, f, indent=2, default=str)
        
        logger.info("=" * 80)
        logger.info(f"📄 V3 test results saved to: {results_filename}")
        logger.info(f"📄 Full processing log: {log_filename}")
        logger.info("=" * 80)
        
        return exit_code
        
    except Exception as e:
        logger.error(f"💥 CRITICAL ERROR during V3 McLean County test: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        test_results.update({
            'processing_successful': False,
            'critical_error': str(e),
            'overall_v3_success': False
        })
        
        return 2

if __name__ == '__main__':
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n⏹️  V3 McLean County test interrupted by user")
        sys.exit(3)