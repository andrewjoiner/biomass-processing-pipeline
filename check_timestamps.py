#!/usr/bin/env python3
"""
Check Database Timestamps - Debug timestamp issues
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

def check_database_timestamps():
    """
    Check timestamps in the database to understand the timing issues
    """
    logger.info("🕐 Checking database timestamps...")
    
    county_fips = "49033"  # Rich County, Utah
    
    try:
        with database_manager.get_connection('biomass_output') as conn:
            cursor = conn.cursor()
            
            # Get current database time
            cursor.execute("SELECT NOW() as current_time")
            db_current_time = cursor.fetchone()['current_time']
            logger.info(f"Database current time: {db_current_time}")
            
            # Get timestamp range of records
            cursor.execute("""
                SELECT 
                    COUNT(*) as count,
                    MIN(processing_timestamp) as earliest,
                    MAX(processing_timestamp) as latest,
                    NOW() - MAX(processing_timestamp) as time_since_latest
                FROM parcel_biomass_analysis 
                WHERE county_fips = %s
            """, (county_fips,))
            timestamp_info = cursor.fetchone()
            
            if timestamp_info['count'] > 0:
                logger.info(f"📊 Timestamp Analysis:")
                logger.info(f"   Record count: {timestamp_info['count']:,}")
                logger.info(f"   Earliest timestamp: {timestamp_info['earliest']}")
                logger.info(f"   Latest timestamp: {timestamp_info['latest']}")
                logger.info(f"   Time since latest: {timestamp_info['time_since_latest']}")
                
                # Check records by time intervals
                intervals = [
                    ("5 minutes", "5 MINUTE"),
                    ("15 minutes", "15 MINUTE"), 
                    ("1 hour", "1 HOUR"),
                    ("6 hours", "6 HOUR"),
                    ("24 hours", "24 HOUR")
                ]
                
                for interval_name, interval_sql in intervals:
                    cursor.execute(f"""
                        SELECT COUNT(*) as count 
                        FROM parcel_biomass_analysis 
                        WHERE county_fips = %s
                        AND processing_timestamp >= NOW() - INTERVAL '{interval_sql}'
                    """, (county_fips,))
                    count = cursor.fetchone()['count']
                    logger.info(f"   Records in last {interval_name}: {count:,}")
                
                # Get sample records with timestamps
                cursor.execute("""
                    SELECT parcel_id, total_biomass_tons, processing_timestamp,
                           NOW() - processing_timestamp as age
                    FROM parcel_biomass_analysis 
                    WHERE county_fips = %s
                    ORDER BY processing_timestamp DESC
                    LIMIT 10
                """, (county_fips,))
                recent_records = cursor.fetchall()
                
                logger.info(f"📋 Most recent records:")
                for record in recent_records[:5]:
                    logger.info(f"   {record['parcel_id']}: {record['total_biomass_tons']:.2f} tons, "
                               f"timestamp: {record['processing_timestamp']}, age: {record['age']}")
                               
            else:
                logger.warning("No records found in database")
                return False
                
        return True
            
    except Exception as e:
        logger.error(f"💥 Check failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = check_database_timestamps()
    sys.exit(0 if success else 1)