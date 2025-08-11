#!/usr/bin/env python3
"""
V3 Debug Test - Test just 2 parcels with full debugging
"""

import logging
import sys
from datetime import datetime

# Set up debug logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 60)
    logger.info("V3 DEBUG TEST - 2 Parcels Only")
    logger.info("=" * 60)
    
    # Import V3 processor
    from src.pipeline.optimized_county_processor_v3 import optimized_county_processor
    from src.core.database_manager_v3 import database_manager
    
    state_fips = '17'
    county_fips = '113'
    
    # Process just 2 parcels with small batch size
    result = optimized_county_processor.process_county_optimized(
        state_fips=state_fips,
        county_fips=county_fips,
        max_parcels=2,
        batch_size=1  # Process one at a time to see full flow
    )
    
    logger.info("=" * 60)
    logger.info(f"Processing result: {result.get('success')}")
    
    if result.get('success'):
        summary = result.get('processing_summary', {})
        logger.info(f"Parcels processed: {summary.get('parcels_processed', 0)}")
        
        # Check database
        with database_manager.get_connection('biomass_output') as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM forestry_analysis_v3 WHERE county_fips = %s", (county_fips,))
            forestry_count = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) FROM crop_analysis_v3 WHERE county_fips = %s", (county_fips,))
            crop_count = cursor.fetchone()['count']
            
        logger.info(f"Forestry records in DB: {forestry_count}")
        logger.info(f"Crop records in DB: {crop_count}")
        
        if forestry_count == 0 and crop_count == 0:
            logger.error("❌ STILL NO V3 RECORDS!")
        else:
            logger.info(f"✅ SUCCESS! Found {forestry_count + crop_count} V3 records!")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())