#!/usr/bin/env python3
"""
V3 Direct Test - Use comprehensive processor directly, skip optimized processor setup
"""

import logging
import sys
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 60)
    logger.info("V3 DIRECT TEST - Using Comprehensive Processor (No Optimized Setup)")
    logger.info("=" * 60)
    
    # Import V3 comprehensive processor directly
    from src.pipeline.comprehensive_biomass_processor_v3 import ComprehensiveBiomassProcessor
    from src.core.database_manager_v3 import database_manager
    
    state_fips = '17'
    county_fips = '113'
    
    # Use comprehensive processor directly
    processor = ComprehensiveBiomassProcessor()
    
    logger.info("🚀 Processing 10 parcels with V3 comprehensive processor directly...")
    
    # Process just 10 parcels
    result = processor.process_county_comprehensive(
        fips_state=state_fips,
        fips_county=county_fips, 
        max_parcels=10,
        batch_size=5,
        enable_parallel=False,  # Disable parallel for simpler debugging
        resume_from_checkpoint=False
    )
    
    logger.info("=" * 60)
    logger.info(f"Processing result: {result.get('success')}")
    
    if result.get('success'):
        summary = result.get('processing_summary', {})
        logger.info(f"Parcels processed: {summary.get('parcels_processed', 0)}")
        
        # Check database immediately
        with database_manager.get_connection('biomass_output') as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM forestry_analysis_v3 WHERE county_fips = %s", (county_fips,))
            forestry_count = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) FROM crop_analysis_v3 WHERE county_fips = %s", (county_fips,))
            crop_count = cursor.fetchone()['count']
            
        logger.info(f"🌲 Forestry records in DB: {forestry_count}")
        logger.info(f"🌽 Crop records in DB: {crop_count}")
        
        if forestry_count == 0 and crop_count == 0:
            logger.error("❌ STILL NO V3 RECORDS SAVED!")
            return 1
        else:
            logger.info(f"✅ SUCCESS! Found {forestry_count + crop_count} V3 enhanced records!")
            return 0
    else:
        logger.error(f"Processing failed: {result.get('error', 'Unknown error')}")
        return 1

if __name__ == '__main__':
    sys.exit(main())