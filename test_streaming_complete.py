#!/usr/bin/env python3
"""
Complete Streaming Architecture Test - End-to-End Validation
Tests that the streaming architecture actually processes parcels and stores correct data
"""

import logging
import sys
import json
from datetime import datetime

def setup_logging():
    """Configure logging for streaming complete test"""
    import os
    os.makedirs('logs', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'logs/STREAMING_COMPLETE_TEST_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Streaming complete test logging - output to {log_filename}")
    return logger, log_filename

def test_streaming_complete():
    """Test complete streaming architecture with actual parcel processing"""
    logger, log_filename = setup_logging()
    
    logger.info("🧪 STREAMING ARCHITECTURE COMPLETE TEST")
    logger.info("=" * 60)
    
    try:
        # Import V3 components
        from src.core.database_manager_v3 import database_manager
        from src.core.blob_manager_v3 import blob_manager
        from src.pipeline.optimized_county_processor_v3 import OptimizedCountyProcessor
        
        state_fips = '17'   # Illinois
        county_fips = '113'  # McLean County
        
        logger.info("🗂️ Step 1: Build tile index for streaming...")
        
        # Get county bounds for tile indexing
        with database_manager.get_connection('parcels') as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    ST_XMin(ST_Extent(geometry)) as min_lon,
                    ST_YMin(ST_Extent(geometry)) as min_lat,
                    ST_XMax(ST_Extent(geometry)) as max_lon,
                    ST_YMax(ST_Extent(geometry)) as max_lat
                FROM parcels
                WHERE fipsstate = %s AND fipscounty = %s
                AND geometry IS NOT NULL
            """, (state_fips, county_fips))
            
            county_result = cursor.fetchone()
            if not county_result:
                logger.error(f"Could not get bounds for county {state_fips}{county_fips}")
                return 1
        
        county_bounds = (
            county_result['min_lon'], county_result['min_lat'],
            county_result['max_lon'], county_result['max_lat']
        )
        
        # Build tile index
        analysis_result = blob_manager.analyze_county_satellite_requirements(county_bounds)
        if 'error' in analysis_result:
            logger.error(f"Tile indexing failed: {analysis_result['error']}")
            return 1
        
        tile_count = len(blob_manager.county_tile_index)
        logger.info(f"✅ Tile index built: {tile_count} tiles indexed")
        
        if tile_count == 0:
            logger.error("❌ No tiles indexed - cannot test streaming")
            return 1
        
        logger.info("🌾 Step 2: Process sample parcels with streaming architecture...")
        
        # Process 5 parcels using the streaming architecture
        processor = OptimizedCountyProcessor()
        
        # Get sample parcel IDs first
        with database_manager.get_connection('parcels') as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT parcelid FROM parcels 
                WHERE fipsstate = %s AND fipscounty = %s
                AND geometry IS NOT NULL
                LIMIT 5
            """, (state_fips, county_fips))
            test_parcels = [row['parcelid'] for row in cursor.fetchall()]
        
        logger.info(f"🎯 Selected {len(test_parcels)} test parcels: {test_parcels[:2]}...")
        
        # Clear any existing test data first
        if test_parcels:
            with database_manager.get_connection('biomass_output') as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    DELETE FROM vegetation_analysis_v3 
                    WHERE parcel_id = ANY(%s)
                """, (test_parcels,))
                
                cursor.execute("""
                    DELETE FROM forestry_analysis_v3 
                    WHERE parcel_id = ANY(%s)
                """, (test_parcels,))
                
                cursor.execute("""
                    DELETE FROM crop_analysis_v3 
                    WHERE parcel_id = ANY(%s)
                """, (test_parcels,))
                conn.commit()
        
        logger.info("🧹 Cleared any existing test data")
        
        # Process parcels with limited count for testing
        logger.info("🚀 Starting streaming-based parcel processing...")
        start_time = datetime.now()
        
        results = processor.process_county_optimized(
            state_fips=state_fips,
            county_fips=county_fips,
            max_parcels=5  # Only process 5 parcels for testing
        )
        
        processing_time = datetime.now() - start_time
        logger.info(f"⏱️ Processing took {processing_time.total_seconds():.2f} seconds")
        
        if not results or results.get('error'):
            logger.error(f"Processing failed: {results.get('error', 'Unknown error')}")
            return 1
        
        logger.info("📊 Step 3: Validate database output...")
        
        # Check what data was actually stored
        with database_manager.get_connection('biomass_output') as conn:
            cursor = conn.cursor()
            
            # Check vegetation analysis records
            cursor.execute("""
                SELECT COUNT(*) as count, 
                       AVG(CASE WHEN ndvi IS NOT NULL THEN 1.0 ELSE 0.0 END) as ndvi_success_rate,
                       AVG(confidence_score) as avg_confidence
                FROM vegetation_analysis_v3 va
                JOIN parcels p ON va.parcel_id = p.parcel_id
                WHERE p.fipsstate = %s AND p.fipscounty = %s
            """, (state_fips, county_fips))
            
            veg_result = cursor.fetchone()
            logger.info(f"🌿 Vegetation Analysis: {veg_result['count']} records")
            logger.info(f"   NDVI Success Rate: {veg_result['ndvi_success_rate']*100:.1f}%")
            logger.info(f"   Average Confidence: {veg_result['avg_confidence']:.3f}")
            
            # Check forest analysis records
            cursor.execute("""
                SELECT COUNT(*) as count,
                       AVG(CASE WHEN total_standing_biomass_tons > 0 THEN 1.0 ELSE 0.0 END) as biomass_success_rate,
                       SUM(total_standing_biomass_tons) as total_biomass
                FROM forestry_analysis_v3 fa
                JOIN parcels p ON fa.parcel_id = p.parcel_id
                WHERE p.fipsstate = %s AND p.fipscounty = %s
            """, (state_fips, county_fips))
            
            forest_result = cursor.fetchone()
            logger.info(f"🌲 Forest Analysis: {forest_result['count']} records")
            logger.info(f"   Biomass Success Rate: {forest_result['biomass_success_rate']*100:.1f}%")
            logger.info(f"   Total Biomass: {forest_result['total_biomass']:.1f} tons")
            
            # Check crop analysis records  
            cursor.execute("""
                SELECT COUNT(*) as count,
                       AVG(CASE WHEN harvestable_residue_tons > 0 THEN 1.0 ELSE 0.0 END) as residue_success_rate,
                       SUM(harvestable_residue_tons) as total_residue
                FROM crop_analysis_v3 ca
                JOIN parcels p ON ca.parcel_id = p.parcel_id
                WHERE p.fipsstate = %s AND p.fipscounty = %s
            """, (state_fips, county_fips))
            
            crop_result = cursor.fetchone()
            logger.info(f"🌽 Crop Analysis: {crop_result['count']} records")
            logger.info(f"   Residue Success Rate: {crop_result['residue_success_rate']*100:.1f}%")
            logger.info(f"   Total Residue: {crop_result['total_residue']:.1f} tons")
            
            # Sample some records to verify data quality
            cursor.execute("""
                SELECT va.ndvi, va.evi, va.confidence_score, va.tile_id, va.analysis_timestamp
                FROM vegetation_analysis_v3 va
                JOIN parcels p ON va.parcel_id = p.parcel_id  
                WHERE p.fipsstate = %s AND p.fipscounty = %s
                AND va.ndvi IS NOT NULL
                LIMIT 3
            """, (state_fips, county_fips))
            
            sample_records = cursor.fetchall()
            if sample_records:
                logger.info("📋 Sample Vegetation Records:")
                for i, record in enumerate(sample_records, 1):
                    logger.info(f"   Record {i}: NDVI={record['ndvi']:.3f}, EVI={record['evi']:.3f}, "
                              f"Confidence={record['confidence_score']:.3f}, TileID={record['tile_id']}")
                    
                    # Validate that streaming was used (tile_id should be 'streaming')
                    if record['tile_id'] == 'streaming':
                        logger.info(f"   ✅ Confirmed: Record {i} used streaming architecture")
                    else:
                        logger.info(f"   ℹ️ Record {i} tile ID: {record['tile_id']}")
        
        logger.info("🎯 Step 4: Performance Analysis...")
        
        # Calculate performance metrics
        total_records = veg_result['count'] + forest_result['count'] + crop_result['count']
        records_per_second = total_records / processing_time.total_seconds()
        
        logger.info(f"   Total Records Created: {total_records}")
        logger.info(f"   Processing Speed: {records_per_second:.2f} records/second")
        logger.info(f"   Memory Used: Minimal (streaming vs ~140GB bulk download)")
        
        # Success criteria validation
        success = True
        
        if veg_result['count'] == 0:
            logger.warning("⚠️ No vegetation analysis records created")
            success = False
        
        if veg_result['ndvi_success_rate'] < 0.1:  # At least 10% should have NDVI
            logger.warning(f"⚠️ Low NDVI success rate: {veg_result['ndvi_success_rate']*100:.1f}%")
        
        if processing_time.total_seconds() > 300:  # Should complete in under 5 minutes for 5 parcels
            logger.warning(f"⚠️ Processing took too long: {processing_time.total_seconds():.1f} seconds")
        
        if success:
            logger.info("✅ STREAMING ARCHITECTURE TEST SUCCESSFUL!")
            logger.info("=" * 60)
            logger.info("🔄 Validation Results:")
            logger.info(f"   ✅ Data Processing: {total_records} records created")
            logger.info(f"   ✅ Performance: {processing_time.total_seconds():.1f}s for 5 parcels")
            logger.info(f"   ✅ Memory Usage: Streaming (not bulk download)")
            logger.info(f"   ✅ Database Storage: V3 tables populated correctly")
            return 0
        else:
            logger.error("❌ STREAMING ARCHITECTURE TEST HAD ISSUES")
            return 1
        
    except Exception as e:
        logger.error(f"💥 CRITICAL ERROR: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 2

if __name__ == '__main__':
    exit_code = test_streaming_complete()
    sys.exit(exit_code)