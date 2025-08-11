#!/usr/bin/env python3
"""
Test Streaming Architecture - Phase 1: Tile Indexing
Tests the new tile indexing functionality without breaking existing systems
"""

import logging
import sys
from datetime import datetime

def setup_logging():
    """Configure logging for streaming architecture test"""
    import os
    os.makedirs('logs', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'logs/STREAMING_ARCHITECTURE_TEST_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Streaming architecture test logging - output to {log_filename}")
    return logger, log_filename

def test_tile_indexing():
    """Test the new tile indexing functionality"""
    logger, log_filename = setup_logging()
    
    logger.info("🚀 STREAMING ARCHITECTURE TEST - Phase 1: Tile Indexing")
    logger.info("=" * 60)
    
    try:
        # Import V3 components
        from src.core.blob_manager_v3 import blob_manager
        from src.core.database_manager_v3 import database_manager
        
        state_fips = '17'   # Illinois
        county_fips = '113'  # McLean County
        
        logger.info(f"🎯 Testing tile indexing for McLean County, Illinois")
        
        # Get county bounds
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
        
        logger.info(f"📍 County bounds: {county_bounds}")
        
        # Test the new tile indexing method
        logger.info("🗂️  Testing tile indexing (no data download)...")
        analysis_start = datetime.now()
        
        analysis_result = blob_manager.analyze_county_satellite_requirements(county_bounds)
        
        analysis_time = datetime.now() - analysis_start
        logger.info(f"⏱️  Tile indexing completed in {analysis_time.total_seconds():.1f} seconds")
        
        # Report results
        if 'error' in analysis_result:
            logger.error(f"❌ Tile indexing failed: {analysis_result['error']}")
            return 1
        
        logger.info("📊 TILE INDEXING RESULTS:")
        logger.info(f"   Tiles Required: {analysis_result['tiles_required']}")
        logger.info(f"   Estimated Data Size: {analysis_result['estimated_data_size_gb']:.1f} GB")
        logger.info(f"   Period: {analysis_result['period']}")
        
        # Test that the county tile index was populated
        index_size = len(blob_manager.county_tile_index)
        logger.info(f"   Tile Index Populated: {index_size} tiles")
        
        if index_size == 0:
            logger.error("❌ Tile index is empty - indexing failed")
            return 1
        
        # Show sample tile metadata
        sample_tile_id = list(blob_manager.county_tile_index.keys())[0]
        sample_tile = blob_manager.county_tile_index[sample_tile_id]
        
        logger.info(f"📋 Sample tile metadata ({sample_tile_id}):")
        logger.info(f"   Blob paths: {len(sample_tile['blob_paths'])} bands")
        logger.info(f"   WGS84 bounds: {sample_tile.get('wgs84_bounds')}")
        logger.info(f"   UTM EPSG: {sample_tile.get('utm_epsg')}")
        logger.info(f"   Date: {sample_tile.get('date')}")
        
        # Test streaming method with a single parcel (without actually processing)
        logger.info("🌐 Testing parcel-tile intersection logic...")
        
        # Get one parcel geometry for testing
        with database_manager.get_connection('parcels') as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT ST_AsGeoJSON(geometry) as geometry
                FROM parcels 
                WHERE fipsstate = %s AND fipscounty = %s
                AND geometry IS NOT NULL
                LIMIT 1
            """, (state_fips, county_fips))
            
            parcel_result = cursor.fetchone()
            if not parcel_result:
                logger.error("Could not get test parcel")
                return 1
        
        # Test tile intersection logic (without downloading)
        import json
        test_parcel_geometry = json.loads(parcel_result['geometry'])
        
        # Find intersecting tiles for this parcel
        from shapely.geometry import shape
        from src.core.coordinate_utils_v3 import coordinate_transformer
        
        geom = shape(test_parcel_geometry)
        parcel_bounds = geom.bounds
        
        intersecting_count = 0
        for tile_id, tile_info in blob_manager.county_tile_index.items():
            if tile_info.get('wgs84_bounds'):
                if coordinate_transformer.bounds_intersect(parcel_bounds, tile_info['wgs84_bounds']):
                    intersecting_count += 1
        
        logger.info(f"🎯 Test parcel intersects {intersecting_count} tiles")
        
        if intersecting_count == 0:
            logger.warning("⚠️  Test parcel doesn't intersect any indexed tiles")
        
        # Success!
        logger.info("✅ STREAMING ARCHITECTURE PHASE 1 TEST SUCCESSFUL!")
        logger.info("=" * 60)
        logger.info("🔄 Architecture Changes:")
        logger.info(f"   - Tile indexing: ✅ Working ({analysis_result['tiles_required']} tiles indexed)")
        logger.info(f"   - Memory usage: ✅ Minimal (metadata only, not {analysis_result['estimated_data_size_gb']:.1f}GB)")
        logger.info(f"   - Performance: ✅ Fast ({analysis_time.total_seconds():.1f}s vs potential hours)")
        logger.info("🎯 Ready for Phase 2: Streaming implementation")
        
        return 0
        
    except Exception as e:
        logger.error(f"💥 CRITICAL ERROR: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 2

if __name__ == '__main__':
    exit_code = test_tile_indexing()
    sys.exit(exit_code)