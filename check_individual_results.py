#!/usr/bin/env python3
"""
Check individual parcel results in database to see if biomass calculations are actually working
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

def check_individual_parcel_results():
    county_fips = "49033"
    
    with database_manager.get_connection('biomass_output') as conn:
        cursor = conn.cursor()
        
        # Get records with actual biomass values (no time filter)
        cursor.execute("""
            SELECT parcel_id, total_biomass_tons, forest_biomass_tons, crop_yield_tons, 
                   confidence_score, processing_timestamp
            FROM parcel_biomass_analysis 
            WHERE county_fips = %s 
            AND total_biomass_tons > 0
            ORDER BY total_biomass_tons DESC
            LIMIT 10
        """, (county_fips,))
        high_biomass = cursor.fetchall()
        
        # Get latest records (including zero biomass)
        cursor.execute("""
            SELECT parcel_id, total_biomass_tons, forest_biomass_tons, crop_yield_tons, 
                   confidence_score, processing_timestamp
            FROM parcel_biomass_analysis 
            WHERE county_fips = %s 
            ORDER BY processing_timestamp DESC
            LIMIT 10
        """, (county_fips,))
        recent_records = cursor.fetchall()
        
        # Count records by biomass value (all records)
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN total_biomass_tons > 0 THEN 1 END) as nonzero_biomass,
                COALESCE(AVG(total_biomass_tons), 0) as avg_biomass,
                COALESCE(MAX(total_biomass_tons), 0) as max_biomass,
                COALESCE(MIN(total_biomass_tons), 0) as min_biomass
            FROM parcel_biomass_analysis 
            WHERE county_fips = %s
        """, (county_fips,))
        stats = cursor.fetchone()
        
        logger.info(f"📊 Individual Parcel Results Analysis:")
        logger.info(f"   Total recent records: {stats['total_records']:,}")
        logger.info(f"   Records with biomass > 0: {stats['nonzero_biomass']:,}")
        logger.info(f"   Records with zero biomass: {stats['total_records'] - stats['nonzero_biomass']:,}")
        logger.info(f"   Average biomass: {stats['avg_biomass']:.3f} tons")
        logger.info(f"   Max biomass: {stats['max_biomass']:.3f} tons")
        logger.info(f"   Min biomass: {stats['min_biomass']:.3f} tons")
        
        if high_biomass:
            logger.info(f"📈 Top parcels with biomass:")
            for record in high_biomass[:5]:
                logger.info(f"   {record['parcel_id']}: {record['total_biomass_tons']:.3f} tons "
                           f"(forest: {record['forest_biomass_tons']:.3f}, crop: {record['crop_yield_tons']:.3f})")
        else:
            logger.info("❌ NO PARCELS with biomass > 0 found!")
            
        logger.info(f"📋 Sample recent records:")
        for record in recent_records[:5]:
            logger.info(f"   {record['parcel_id']}: {record['total_biomass_tons']:.3f} tons "
                       f"at {record['processing_timestamp']}")
        
        return stats['nonzero_biomass'] > 0

if __name__ == "__main__":
    success = check_individual_parcel_results()
    sys.exit(0 if success else 1)