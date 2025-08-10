#!/usr/bin/env python3
"""
Quick V3 Test - Test just 10 parcels to verify V3 enhanced data capture is working
"""

import logging
import sys
import time
from datetime import datetime

def setup_logging():
    """Configure logging for quick V3 test"""
    import os
    os.makedirs('logs', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'logs/V3_QUICK_TEST_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"V3 Quick test logging - output to {log_filename}")
    return logger, log_filename

def main():
    """Test V3 enhanced data capture with just 10 parcels"""
    logger, log_filename = setup_logging()
    
    logger.info("🚀 V3 QUICK TEST - 10 Parcels Enhanced Data Validation")
    logger.info("=" * 60)
    
    try:
        # Import V3 processor components
        from src.pipeline.optimized_county_processor_v3 import optimized_county_processor
        from src.core.database_manager_v3 import database_manager
        
        state_fips = '17'   # Illinois
        county_fips = '113'  # McLean County
        
        logger.info(f"🎯 Testing V3 processing with 10 parcels from McLean County")
        logger.info(f"🌿 Expecting enhanced data from V3 analyzers")
        
        # Process just 10 parcels
        processing_start = time.time()
        
        result = optimized_county_processor.process_county_optimized(
            state_fips=state_fips,
            county_fips=county_fips,
            max_parcels=10,     # Just 10 parcels for quick test
            batch_size=5        # Small batches to see results quickly
        )
        
        processing_time = time.time() - processing_start
        
        # Analyze results
        if result.get('success'):
            logger.info("🎉 V3 QUICK TEST SUCCESSFUL!")
            logger.info("=" * 60)
            
            # Extract processing metrics
            processing_summary = result.get('processing_summary', {})
            parcels_processed = processing_summary.get('parcels_processed', 0)
            
            # Check V3 enhanced data
            logger.info("🔍 Checking V3 enhanced database records...")
            
            with database_manager.get_connection('biomass_output') as conn:
                cursor = conn.cursor()
                
                # Count forestry_analysis_v3 records
                cursor.execute("SELECT COUNT(*) as count FROM forestry_analysis_v3 WHERE county_fips = %s", (county_fips,))
                forestry_records = cursor.fetchone()['count']
                
                # Count crop_analysis_v3 records
                cursor.execute("SELECT COUNT(*) as count FROM crop_analysis_v3 WHERE county_fips = %s", (county_fips,))
                crop_records = cursor.fetchone()['count']
                
                # Get sample data if any
                cursor.execute("""
                    SELECT parcel_id, crop_name, area_percentage, estimated_yield_tons 
                    FROM crop_analysis_v3 
                    WHERE county_fips = %s 
                    ORDER BY area_percentage DESC 
                    LIMIT 5
                """, (county_fips,))
                sample_crops = cursor.fetchall()
            
            logger.info(f"📊 RESULTS:")
            logger.info(f"   Parcels Processed: {parcels_processed}")
            logger.info(f"   Processing Time: {processing_time:.1f}s")
            logger.info(f"   Enhanced Forestry Records: {forestry_records}")
            logger.info(f"   Enhanced Crop Records: {crop_records}")
            
            if sample_crops:
                logger.info(f"🌽 Sample Crop Data:")
                for crop in sample_crops:
                    logger.info(f"   {crop['parcel_id']}: {crop['crop_name']} ({crop['area_percentage']:.1f}%)")
            
            # Assessment
            if forestry_records > 0 or crop_records > 0:
                logger.info("✅ SUCCESS: V3 enhanced data is being captured!")
                return 0
            else:
                logger.error("❌ FAILURE: Still getting 0 enhanced records")
                return 1
                
        else:
            logger.error("💥 V3 processing failed!")
            error_message = result.get('error', 'Unknown error')
            logger.error(f"Error: {error_message}")
            return 1
        
    except Exception as e:
        logger.error(f"💥 CRITICAL ERROR: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 2

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)