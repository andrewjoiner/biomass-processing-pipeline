#!/usr/bin/env python3
"""
Simple Database Write Test - Tests only the database persistence functionality
without running the full optimized processor (which requires Azure setup)
"""

import logging
import sys
import os
from datetime import datetime

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

def test_database_save_biomass_results():
    """
    Test the save_biomass_results functionality directly
    """
    logger.info("🧪 Testing database save_biomass_results functionality...")
    
    # Test parameters
    test_county_fips = "49033"  # Rich County, Utah
    
    try:
        # Create test data in the format expected by save_biomass_results
        test_parcel_results = [
            {
                'parcel_id': 'TEST_001',
                'county_fips': test_county_fips,
                'total_acres': 10.5,
                'centroid_lon': -111.5,
                'centroid_lat': 42.1,
                'processing_timestamp': datetime.now(),
                
                # Allocation factors
                'allocation_factors': {
                    'forest_acres': 5.0,
                    'cropland_acres': 3.0,
                    'other_acres': 2.5
                },
                
                # Biomass results
                'forest_biomass_tons': 25.5,
                'forest_harvestable_tons': 15.0,
                'forest_residue_tons': 2.0,
                'crop_yield_tons': 8.5,
                'crop_residue_tons': 1.5,
                
                # Analysis details
                'landcover_analysis': {'forest_area_acres': 5.0, 'cropland_area_acres': 3.0},
                'forest_analysis': {'total_biomass_tons': 25.5, 'plots_used': 3},
                'crop_analysis': [{'crop_type': 'corn', 'yield_tons': 8.5}],
                
                # Vegetation indices
                'vegetation_indices': {
                    'ndvi': 0.75,
                    'evi': 0.45,
                    'savi': 0.60,
                    'ndwi': 0.30
                },
                
                # Metadata
                'data_sources_used': ['FIA', 'CDL', 'WorldCover'],
                'confidence_score': 0.85
            },
            {
                'parcel_id': 'TEST_002',
                'county_fips': test_county_fips,
                'total_acres': 15.2,
                'centroid_lon': -111.6,
                'centroid_lat': 42.2,
                'processing_timestamp': datetime.now(),
                
                'allocation_factors': {
                    'forest_acres': 8.0,
                    'cropland_acres': 5.0,
                    'other_acres': 2.2
                },
                
                'forest_biomass_tons': 42.3,
                'forest_harvestable_tons': 25.0,
                'forest_residue_tons': 3.2,
                'crop_yield_tons': 12.8,
                'crop_residue_tons': 2.1,
                
                'landcover_analysis': {'forest_area_acres': 8.0, 'cropland_area_acres': 5.0},
                'forest_analysis': {'total_biomass_tons': 42.3, 'plots_used': 5},
                'crop_analysis': [{'crop_type': 'wheat', 'yield_tons': 12.8}],
                
                'vegetation_indices': {
                    'ndvi': 0.82,
                    'evi': 0.51,
                    'savi': 0.67,
                    'ndwi': 0.28
                },
                
                'data_sources_used': ['FIA', 'CDL', 'WorldCover'],
                'confidence_score': 0.90
            }
        ]
        
        # Clear any existing test data
        logger.info("🧹 Clearing existing test data...")
        with database_manager.get_connection('biomass_output') as conn:
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM parcel_biomass_analysis 
                WHERE parcel_id LIKE 'TEST_%'
            """)
            conn.commit()
            logger.info(f"Cleared {cursor.rowcount} existing test records")
        
        # Count records before
        with database_manager.get_connection('biomass_output') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as count FROM parcel_biomass_analysis WHERE parcel_id LIKE 'TEST_%'")
            before_count = cursor.fetchone()['count']
            logger.info(f"Records before test: {before_count}")
        
        # Test the database save functionality
        logger.info("💾 Testing save_biomass_results...")
        success = database_manager.save_biomass_results(test_parcel_results)
        
        if not success:
            logger.error("❌ save_biomass_results returned False")
            return False
        
        # Verify records were written
        with database_manager.get_connection('biomass_output') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as count FROM parcel_biomass_analysis WHERE parcel_id LIKE 'TEST_%'")
            after_count = cursor.fetchone()['count']
            
            # Get the inserted records for verification
            cursor.execute("""
                SELECT parcel_id, total_biomass_tons, forest_biomass_tons, 
                       crop_yield_tons, confidence_score, processing_timestamp
                FROM parcel_biomass_analysis 
                WHERE parcel_id LIKE 'TEST_%'
                ORDER BY parcel_id
            """)
            inserted_records = cursor.fetchall()
        
        logger.info(f"📊 Results:")
        logger.info(f"   Records before: {before_count}")
        logger.info(f"   Records after: {after_count}")
        logger.info(f"   Expected new records: {len(test_parcel_results)}")
        logger.info(f"   Actual new records: {after_count - before_count}")
        
        logger.info(f"📋 Inserted records:")
        for record in inserted_records:
            total_biomass = record['forest_biomass_tons'] + record['crop_yield_tons']
            logger.info(f"   {record['parcel_id']}: {total_biomass:.2f} tons "
                       f"(forest: {record['forest_biomass_tons']:.2f}, crop: {record['crop_yield_tons']:.2f}) "
                       f"confidence: {record['confidence_score']:.2f}")
        
        # Validate
        expected_new_records = len(test_parcel_results)
        actual_new_records = after_count - before_count
        
        if actual_new_records == expected_new_records:
            logger.info("✅ Database write test PASSED!")
            logger.info(f"✅ Successfully wrote {actual_new_records} test records to database")
            
            # Clean up test data
            with database_manager.get_connection('biomass_output') as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM parcel_biomass_analysis WHERE parcel_id LIKE 'TEST_%'")
                conn.commit()
                logger.info(f"🧹 Cleaned up {cursor.rowcount} test records")
            
            return True
        else:
            logger.error(f"❌ Database write test FAILED!")
            logger.error(f"❌ Expected {expected_new_records} records, got {actual_new_records}")
            return False
            
    except Exception as e:
        logger.error(f"💥 Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_database_save_biomass_results()
    sys.exit(0 if success else 1)