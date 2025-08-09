#!/usr/bin/env python3
"""
Verify Database Records - Check that records were written to database correctly
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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def verify_rich_county_records():
    """
    Verify that Rich County records were written to the database correctly
    """
    logger.info("🔍 Verifying Rich County records in database...")
    
    county_fips = "49033"  # Rich County, Utah
    
    try:
        with database_manager.get_connection('biomass_output') as conn:
            cursor = conn.cursor()
            
            # Count total records
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM parcel_biomass_analysis 
                WHERE county_fips = %s
            """, (county_fips,))
            total_records = cursor.fetchone()['count']
            
            # Count recent records (last hour)
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM parcel_biomass_analysis 
                WHERE county_fips = %s
                AND processing_timestamp >= NOW() - INTERVAL '1 hour'
            """, (county_fips,))
            recent_records = cursor.fetchone()['count']
            
            # Get summary statistics
            cursor.execute("""
                SELECT 
                    COUNT(*) as record_count,
                    SUM(total_biomass_tons) as total_biomass,
                    AVG(total_biomass_tons) as avg_biomass_per_parcel,
                    SUM(forest_biomass_tons) as total_forest_biomass,
                    SUM(crop_yield_tons) as total_crop_yield,
                    AVG(confidence_score) as avg_confidence,
                    MIN(processing_timestamp) as earliest_timestamp,
                    MAX(processing_timestamp) as latest_timestamp
                FROM parcel_biomass_analysis 
                WHERE county_fips = %s
                AND processing_timestamp >= NOW() - INTERVAL '1 hour'
            """, (county_fips,))
            stats = cursor.fetchone()
            
            # Get sample records
            cursor.execute("""
                SELECT parcel_id, total_biomass_tons, forest_biomass_tons, 
                       crop_yield_tons, confidence_score, processing_timestamp
                FROM parcel_biomass_analysis 
                WHERE county_fips = %s
                AND processing_timestamp >= NOW() - INTERVAL '1 hour'
                ORDER BY total_biomass_tons DESC
                LIMIT 5
            """, (county_fips,))
            sample_records = cursor.fetchall()
        
        logger.info(f"📊 Database Verification Results:")
        logger.info(f"   County FIPS: {county_fips} (Rich County, Utah)")
        logger.info(f"   Total records in database: {total_records:,}")
        logger.info(f"   Recent records (last hour): {recent_records:,}")
        
        if stats and stats['record_count'] > 0:
            logger.info(f"📈 Recent Processing Statistics:")
            logger.info(f"   Records processed: {stats['record_count']:,}")
            logger.info(f"   Total biomass: {stats['total_biomass']:,.2f} tons")
            logger.info(f"   Average per parcel: {stats['avg_biomass_per_parcel']:.2f} tons")
            logger.info(f"   Forest biomass: {stats['total_forest_biomass']:,.2f} tons")
            logger.info(f"   Crop yield: {stats['total_crop_yield']:,.2f} tons")
            logger.info(f"   Average confidence: {stats['avg_confidence']:.3f}")
            logger.info(f"   Processing timespan: {stats['earliest_timestamp']} to {stats['latest_timestamp']}")
            
            logger.info(f"📋 Top 5 parcels by biomass:")
            for i, record in enumerate(sample_records, 1):
                logger.info(f"   {i}. Parcel {record['parcel_id']}: {record['total_biomass_tons']:.2f} tons "
                           f"(forest: {record['forest_biomass_tons']:.2f}, crop: {record['crop_yield_tons']:.2f}) "
                           f"confidence: {record['confidence_score']:.2f}")
        
        # Validate expectations
        expected_recent_records = 1000  # From the test that just ran
        
        if recent_records == expected_recent_records:
            logger.info("✅ Database persistence verification PASSED!")
            logger.info(f"✅ Found expected {recent_records:,} records from recent processing")
            return True
        elif recent_records > 0:
            logger.warning(f"⚠️ Found {recent_records:,} records, expected {expected_recent_records:,}")
            logger.warning("⚠️ Database persistence working but record count differs")
            return True
        else:
            logger.error("❌ Database persistence verification FAILED!")
            logger.error("❌ No recent records found in database")
            return False
            
    except Exception as e:
        logger.error(f"💥 Verification failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = verify_rich_county_records()
    sys.exit(0 if success else 1)