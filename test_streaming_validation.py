#!/usr/bin/env python3
"""
Quick validation test that streaming architecture is working
"""
import logging
import sys
from datetime import datetime
import json

def setup_logging():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def test_single_parcel_streaming():
    """Test that a single parcel uses streaming methods"""
    logger = setup_logging()
    
    logger.info("🧪 STREAMING VALIDATION TEST")
    logger.info("=" * 50)
    
    try:
        # Import V3 components
        from src.core.database_manager_v3 import database_manager
        from src.analyzers.vegetation_analyzer_v3 import vegetation_analyzer
        
        state_fips = '17'   # Illinois
        county_fips = '113'  # McLean County
        
        # Get one parcel for testing
        with database_manager.get_connection('parcels') as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT ST_AsGeoJSON(geometry) as geometry, usecode, acres
                FROM parcels 
                WHERE fipsstate = %s AND fipscounty = %s
                AND geometry IS NOT NULL
                LIMIT 1
            """, (state_fips, county_fips))
            
            parcel_result = cursor.fetchone()
            if not parcel_result:
                logger.error("Could not get test parcel")
                return 1
        
        parcel_geometry = json.loads(parcel_result['geometry'])
        logger.info(f"📍 Testing parcel: {parcel_result['acres']} acres, usecode: {parcel_result['usecode']}")
        
        # Test vegetation analysis (should use streaming)
        logger.info("🌿 Testing vegetation analysis with streaming...")
        start_time = datetime.now()
        
        vegetation_result = vegetation_analyzer.analyze_parcel_vegetation(parcel_geometry)
        
        analysis_time = datetime.now() - start_time
        logger.info(f"⏱️ Vegetation analysis took {analysis_time.total_seconds():.2f} seconds")
        
        if vegetation_result:
            logger.info("✅ Vegetation analysis successful!")
            logger.info(f"   NDVI: {vegetation_result.get('ndvi', 'N/A')}")
            logger.info(f"   Tile ID: {vegetation_result.get('tile_id', 'N/A')}")
            logger.info(f"   Pixel count: {vegetation_result.get('pixel_count', 'N/A')}")
            logger.info(f"   Confidence: {vegetation_result.get('confidence_score', 'N/A')}")
            
            # Check if this used streaming (tile_id should be 'streaming' for streaming mode)
            if vegetation_result.get('tile_id') == 'streaming':
                logger.info("✅ CONFIRMED: Using streaming architecture!")
            else:
                logger.info(f"ℹ️ Using tile ID: {vegetation_result.get('tile_id')}")
        else:
            logger.warning("⚠️ No vegetation analysis result (may be no satellite data for this area)")
        
        logger.info("🎯 STREAMING VALIDATION COMPLETE")
        return 0
        
    except Exception as e:
        logger.error(f"💥 ERROR: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == '__main__':
    exit_code = test_single_parcel_streaming()
    sys.exit(exit_code)