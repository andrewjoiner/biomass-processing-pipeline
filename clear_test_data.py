#!/usr/bin/env python3
"""
Clear all test data from biomass output database before clean test
"""

import logging
import sys
import os

# Add src to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
sys.path.insert(0, src_dir)

from src.core.database_manager_v1 import database_manager

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clear_rich_county_data():
    county_fips = "49033"
    
    with database_manager.get_connection('biomass_output') as conn:
        cursor = conn.cursor()
        
        # Count existing records
        cursor.execute("SELECT COUNT(*) as count FROM parcel_biomass_analysis WHERE county_fips = %s", (county_fips,))
        before_count = cursor.fetchone()['count']
        
        logger.info(f"📊 Found {before_count:,} existing records for Rich County")
        
        # Clear all Rich County records
        cursor.execute("DELETE FROM parcel_biomass_analysis WHERE county_fips = %s", (county_fips,))
        deleted_count = cursor.rowcount
        conn.commit()
        
        logger.info(f"🗑️ Deleted {deleted_count:,} records")
        
        # Verify cleared
        cursor.execute("SELECT COUNT(*) as count FROM parcel_biomass_analysis WHERE county_fips = %s", (county_fips,))
        after_count = cursor.fetchone()['count']
        
        logger.info(f"✅ Database now has {after_count:,} records for Rich County")
        
        return deleted_count

if __name__ == "__main__":
    deleted = clear_rich_county_data()
    logger.info(f"🎯 Ready for clean test - database cleared of {deleted:,} duplicate records")