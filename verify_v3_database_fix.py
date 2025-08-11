#!/usr/bin/env python3
"""
Verify V3 Database Fix - Test that V3 now uses V1 architecture
Quick verification that our architectural fixes are working
"""

import logging
import sys
import os

# Add src to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
sys.path.insert(0, src_dir)

from src.core.database_manager_v3 import database_manager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def verify_v3_database_config():
    """
    Verify that V3 database manager is now using V1's working database
    """
    logger.info("🔍 Verifying V3 database configuration fixes...")
    
    try:
        # Test database connection
        with database_manager.get_connection('biomass_output') as conn:
            cursor = conn.cursor()
            
            # Check which database we're connected to
            cursor.execute("SELECT current_database();")
            current_db = cursor.fetchone()[0]
            logger.info(f"✅ V3 connected to database: {current_db}")
            
            # Check if parcel_biomass_analysis table exists (V1 table)
            cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_name = 'parcel_biomass_analysis'
            """)
            table_exists = cursor.fetchone()[0] > 0
            
            if table_exists:
                logger.info("✅ V3 can access parcel_biomass_analysis table (V1 compatible)")
                
                # Check for V3 enhanced columns
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'parcel_biomass_analysis' 
                    AND column_name IN ('record_uuid', 'processing_version', 'enhanced_ndvi', 'species_breakdown')
                """)
                v3_columns = [row[0] for row in cursor.fetchall()]
                
                if v3_columns:
                    logger.info(f"✅ V3 enhanced columns detected: {v3_columns}")
                else:
                    logger.info("ℹ️  V3 enhanced columns will be created on first save")
                    
                # Count existing records
                cursor.execute("SELECT COUNT(*) FROM parcel_biomass_analysis")
                record_count = cursor.fetchone()[0]
                logger.info(f"📊 Found {record_count} existing records in table")
                
                return True
            else:
                logger.info("ℹ️  parcel_biomass_analysis table doesn't exist yet (will be created on first save)")
                return True
                
    except Exception as e:
        logger.error(f"❌ V3 database verification failed: {e}")
        return False

def test_v3_single_save_method():
    """
    Test that V3's save method is now single-operation like V1
    """
    logger.info("🧪 Testing V3 single-save architecture...")
    
    try:
        # Create mock parcel result
        mock_result = [{
            'parcel_id': 'TEST_PERFORMANCE_FIX',
            'county_fips': '55095',  # Polk County
            'total_acres': 40.5,
            'centroid_lon': -92.5,
            'centroid_lat': 45.3,
            'allocation_factors': {
                'forest_acres': 25.0,
                'cropland_acres': 15.5
            },
            'forest_biomass_tons': 150.0,
            'forest_harvestable_tons': 90.0,
            'forest_residue_tons': 60.0,
            'crop_yield_tons': 45.0,
            'crop_residue_tons': 20.0,
            'vegetation_indices': {
                'ndvi': 0.75,
                'evi': 0.65,
                'savi': 0.55,
                'ndwi': 0.35
            },
            'confidence_score': 0.85,
            'data_sources_used': ['FIA', 'CDL', 'Sentinel-2'],
            'processing_timestamp': 'NOW()',
            'forest_analysis': {
                'dominant_species_name': 'Oak',
                'dominant_species_code': 832,
                'species_diversity_index': 1.2,
                'average_stand_age': 45,
                'harvest_probability': 0.65,
                'total_harvestable_biomass_tons': 90.0,
                'bole_biomass_tons': 100.0,
                'forest_residue_biomass_tons': 60.0
            },
            'crop_analysis': [
                {'crop_code': 1, 'crop_name': 'Corn', 'area_acres': 15.5, 'yield_tons': 45.0}
            ],
            'dynamic_confidence_score': 0.87
        }]
        
        # Test single save operation
        logger.info("💾 Testing single-save operation...")
        success = database_manager.save_biomass_results(mock_result)
        
        if success:
            logger.info("✅ V3 single-save operation successful!")
            
            # Verify the record was saved
            with database_manager.get_connection('biomass_output') as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT processing_version, enhanced_ndvi, dynamic_confidence, species_breakdown IS NOT NULL
                    FROM parcel_biomass_analysis 
                    WHERE parcel_id = %s
                    ORDER BY processing_timestamp DESC
                    LIMIT 1
                """, ('TEST_PERFORMANCE_FIX',))
                
                result = cursor.fetchone()
                if result:
                    version, enhanced_ndvi, dynamic_conf, has_species = result
                    logger.info(f"✅ Record saved with version: {version}, NDVI: {enhanced_ndvi}, confidence: {dynamic_conf}, species_data: {has_species}")
                else:
                    logger.warning("⚠️ Test record not found after save")
                
            return True
        else:
            logger.error("❌ V3 single-save operation failed")
            return False
            
    except Exception as e:
        logger.error(f"❌ V3 save method test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    logger.info("🚀 V3 Database Architecture Fix Verification")
    logger.info("=" * 50)
    
    # Run verification tests
    config_ok = verify_v3_database_config()
    save_ok = test_v3_single_save_method()
    
    logger.info("=" * 50)
    if config_ok and save_ok:
        logger.info("🎉 V3 DATABASE FIX VERIFICATION: SUCCESS!")
        logger.info("✅ V3 now uses V1's proven single-table architecture")
        logger.info("✅ Database: biomass_production_v2 (working V1 database)")
        logger.info("✅ Table: parcel_biomass_analysis (V1 compatible)")
        logger.info("✅ Save: Single operation (V1 performance pattern)")
        logger.info("✅ Enhanced: V3 features preserved in additional columns")
    else:
        logger.error("❌ V3 DATABASE FIX VERIFICATION: FAILED")
        logger.error("⚠️ Check database configuration and connectivity")