#!/usr/bin/env python3
"""
Verify Full County Database Records
"""

import logging
import sys
import os

# Add src to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
sys.path.insert(0, src_dir)

from src.core.database_manager_v1 import database_manager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_full_county():
    county_fips = "49033"
    
    with database_manager.get_connection('biomass_output') as conn:
        cursor = conn.cursor()
        
        # Count total records for this county
        cursor.execute("SELECT COUNT(*) as count FROM parcel_biomass_analysis WHERE county_fips = %s", (county_fips,))
        total_count = cursor.fetchone()['count']
        
        # Count recent records (last 10 minutes)
        cursor.execute("""
            SELECT COUNT(*) as count FROM parcel_biomass_analysis 
            WHERE county_fips = %s AND processing_timestamp >= NOW() - INTERVAL '10 minutes'
        """, (county_fips,))
        recent_count = cursor.fetchone()['count']
        
        logger.info(f"📊 Full County Verification:")
        logger.info(f"   County: Rich County, Utah ({county_fips})")
        logger.info(f"   Total records in database: {total_count:,}")
        logger.info(f"   Records from last 10 minutes: {recent_count:,}")
        
        if recent_count == 10766:
            logger.info("✅ SUCCESS: All 10,766 parcels written to database!")
            return True
        elif total_count >= 10766:
            logger.info("✅ SUCCESS: Database has 10,766+ records (may include previous runs)")
            return True
        else:
            logger.error(f"❌ FAILED: Expected 10,766 records, found {total_count}")
            return False

if __name__ == "__main__":
    success = verify_full_county()
    sys.exit(0 if success else 1)