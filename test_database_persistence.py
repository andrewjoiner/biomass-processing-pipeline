#!/usr/bin/env python3
"""
Test script to verify database persistence functionality
Tests the optimized processor with a small batch to ensure database writes work
"""

import logging
import sys
import os

# Add src to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
sys.path.insert(0, src_dir)

from src.pipeline.optimized_county_processor_v1 import optimized_county_processor
from src.core.database_manager_v1 import database_manager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_small_batch_database_persistence():
    """
    Test database persistence with a small batch of parcels from Rich County, Utah
    """
    logger.info("🧪 Testing database persistence with small batch...")
    
    # Test parameters
    state_fips = "49"  # Utah
    county_fips = "033"  # Rich County
    max_parcels = 100  # Small test batch
    batch_size = 50   # Small batches for testing
    
    try:
        # Clear any existing test data
        logger.info("🧹 Clearing any existing test data...")
        with database_manager.get_connection('biomass_output') as conn:
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM parcel_biomass_analysis 
                WHERE county_fips = %s
                AND processing_timestamp >= NOW() - INTERVAL '1 hour'
            """, (f"{state_fips}{county_fips}",))
            conn.commit()
            logger.info(f"Cleared {cursor.rowcount} existing test records")
        
        # Count records before processing
        logger.info("📊 Counting existing records...")
        with database_manager.get_connection('biomass_output') as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM parcel_biomass_analysis 
                WHERE county_fips = %s
            """, (f"{state_fips}{county_fips}",))
            before_count = cursor.fetchone()['count']
            logger.info(f"Records before processing: {before_count}")
        
        # Run optimized processing with database persistence
        logger.info(f"🚀 Processing {max_parcels} parcels with database persistence...")
        
        result = optimized_county_processor.process_county_optimized(
            state_fips=state_fips,
            county_fips=county_fips,
            max_parcels=max_parcels,
            batch_size=batch_size
        )
        
        if not result['success']:
            logger.error(f"Processing failed: {result.get('error', 'Unknown error')}")
            return False
        
        # Count records after processing
        logger.info("📊 Counting records after processing...")
        with database_manager.get_connection('biomass_output') as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM parcel_biomass_analysis 
                WHERE county_fips = %s
                AND processing_timestamp >= NOW() - INTERVAL '1 hour'
            """, (f"{state_fips}{county_fips}",))
            after_count = cursor.fetchone()['count']
            logger.info(f"Records after processing: {after_count}")
            
            # Get some sample records for verification
            cursor.execute("""
                SELECT parcel_id, total_biomass_tons, forest_biomass_tons, 
                       crop_yield_tons, confidence_score, processing_timestamp
                FROM parcel_biomass_analysis 
                WHERE county_fips = %s
                AND processing_timestamp >= NOW() - INTERVAL '1 hour'
                LIMIT 5
            """, (f"{state_fips}{county_fips}",))
            sample_records = cursor.fetchall()
        
        # Verify results
        expected_new_records = result['processing_summary']['parcels_processed']
        actual_new_records = after_count - before_count
        
        logger.info(f"📈 Processing Results:")
        logger.info(f"   Parcels processed: {expected_new_records}")
        logger.info(f"   Records in database: {actual_new_records}")
        logger.info(f"   Processing time: {result['total_processing_time']:.2f}s")
        logger.info(f"   Total biomass: {result['processing_summary']['total_biomass_tons']:.2f} tons")
        
        logger.info(f"📋 Sample records:")
        for record in sample_records[:3]:
            logger.info(f"   Parcel {record['parcel_id']}: {record['total_biomass_tons']:.2f} tons "
                       f"(confidence: {record['confidence_score']:.2f})")
        
        # Validation
        success = True
        if actual_new_records != expected_new_records:
            logger.error(f"❌ Record count mismatch: expected {expected_new_records}, got {actual_new_records}")
            success = False
        
        if actual_new_records == 0:
            logger.error("❌ No records were written to database!")
            success = False
            
        if success:
            logger.info("✅ Database persistence test PASSED!")
            logger.info(f"✅ All {expected_new_records} parcels successfully persisted to database")
        else:
            logger.error("❌ Database persistence test FAILED!")
            
        return success
        
    except Exception as e:
        logger.error(f"💥 Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_small_batch_database_persistence()
    sys.exit(0 if success else 1)