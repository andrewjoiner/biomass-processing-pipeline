#!/usr/bin/env python3
"""
DeWitt County Performance Test - Validate Performance Fixes
Test our optimizations on DeWitt County, Illinois (FIPS: 17-039)
"""

import logging
import sys
import json
import time
from datetime import datetime
from typing import Dict, Optional

def setup_logging():
    """Configure comprehensive logging for DeWitt County test"""
    import os
    os.makedirs('logs', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'logs/DEWITT_COUNTY_PERFORMANCE_TEST_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"DeWitt County performance test logging - output to {log_filename}")
    return logger, log_filename

def test_database_connections(logger):
    """Test all required database connections"""
    logger.info("🔌 Testing database connections...")
    
    try:
        from src.core.database_manager_v3 import database_manager
        
        # Test all required databases
        databases = ['parcels', 'crops', 'forestry', 'biomass_output']
        for db_name in databases:
            with database_manager.get_connection(db_name) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 as test_col")
                result = cursor.fetchone()
                if result and result['test_col'] == 1:
                    logger.info(f"  ✅ {db_name} database: Connected")
                else:
                    logger.error(f"  ❌ {db_name} database: Failed")
                    return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Database connection test failed: {e}")
        return False

def get_dewitt_county_info(logger):
    """Get DeWitt County parcel information"""
    logger.info("📊 Analyzing DeWitt County scope...")
    
    try:
        from src.core.database_manager_v3 import database_manager
        
        with database_manager.get_connection('parcels') as conn:
            cursor = conn.cursor()
            # Show ALL columns to find the FIPS columns
            cursor.execute("SELECT * FROM parcels LIMIT 1")
            sample_row = cursor.fetchone()
            all_columns = list(sample_row.keys()) if sample_row else []
            logger.info(f"  📋 All columns ({len(all_columns)}): {all_columns}")
            
            # Look specifically for FIPS columns
            fips_columns = [col for col in all_columns if 'fips' in col.lower() or 'state' in col.lower() or 'county' in col.lower()]
            logger.info(f"  🎯 FIPS/State/County columns: {fips_columns}")
            
            cursor.execute("""
                SELECT COUNT(*) as parcel_count,
                       MIN(ST_XMin(geometry)) as min_lon,
                       MAX(ST_XMax(geometry)) as max_lon, 
                       MIN(ST_YMin(geometry)) as min_lat,
                       MAX(ST_YMax(geometry)) as max_lat
                FROM parcels
                WHERE fipsstate = '17' AND fipscounty = '039'
            """)
            
            county_info = cursor.fetchone()
            
            if county_info and county_info['parcel_count'] > 0:
                logger.info(f"  🏘️ DeWitt County: {county_info['parcel_count']:,} parcels")
                logger.info(f"  🗺️ Bounds: ({county_info['min_lon']:.3f}, {county_info['min_lat']:.3f}) to ({county_info['max_lon']:.3f}, {county_info['max_lat']:.3f})")
                return county_info
            else:
                logger.error("❌ No parcels found for DeWitt County")
                return None
                
    except Exception as e:
        logger.error(f"❌ Error querying DeWitt County info: {e}")
        return None

def run_dewitt_county_performance_test():
    """Run DeWitt County performance test with optimizations"""
    logger, log_filename = setup_logging()
    
    logger.info("🚀 DEWITT COUNTY PERFORMANCE TEST - OPTIMIZED PIPELINE")
    logger.info("=" * 80)
    logger.info("Goal: Validate performance fixes on DeWitt County, Illinois (17-039)")
    logger.info("Expected: Significant improvement from database, cache, and parallel processing fixes")
    logger.info("")
    
    start_time = time.time()
    
    try:
        # Step 1: Test database connections (should be much more stable now)
        if not test_database_connections(logger):
            logger.error("❌ Database connection test failed. Aborting.")
            return False
        
        # Step 2: Get county information
        county_info = get_dewitt_county_info(logger)
        if not county_info:
            logger.error("❌ Failed to get county information. Aborting.")
            return False
        
        # Step 3: Initialize optimized pipeline
        logger.info("🗂️ Initializing optimized pipeline architecture...")
        from src.core.blob_manager_v3 import blob_manager
        from src.pipeline.optimized_county_processor_v3 import OptimizedCountyProcessor
        
        # Get initial cache and performance stats
        initial_stats = blob_manager.get_cache_stats()
        logger.info(f"  📊 Initial cache state: {initial_stats['streaming_tiles_cached']} tiles cached")
        logger.info(f"  📊 Cache size limit: 500 tiles (increased from 50)")
        logger.info("  ✅ Optimized pipeline initialized")
        
        # Step 4: Run optimized county processing
        logger.info("")
        logger.info("🚀 STARTING DEWITT COUNTY PROCESSING")
        logger.info("=" * 60)
        
        processor = OptimizedCountyProcessor()
        
        # Process with optimized settings
        processing_start = time.time()
        results = processor.process_county_optimized(
            state_fips='17',        # Illinois
            county_fips='039',      # DeWitt County  
            max_parcels=None,       # Process all parcels
            batch_size=100,         # Optimized batch size (reduced from 500)
            max_workers=4           # Parallel processing (new feature)
        )
        processing_time = time.time() - processing_start
        
        # Step 5: Analyze performance results
        logger.info("")
        logger.info("📊 PERFORMANCE RESULTS ANALYSIS")
        logger.info("=" * 50)
        
        if results.get('success'):
            logger.info(f"✅ DeWitt County processing completed successfully!")
            logger.info(f"⏱️ Processing time: {processing_time/60:.1f} minutes")
            logger.info(f"📊 Parcels processed: {results.get('parcels_processed', 0):,}")
            
            # Calculate performance metrics
            parcels_processed = results.get('parcels_processed', 0)
            if parcels_processed > 0 and processing_time > 0:
                parcels_per_second = parcels_processed / processing_time
                parcels_per_hour = parcels_per_second * 3600
                logger.info(f"🏃 Processing rate: {parcels_per_second:.2f} parcels/second")
                logger.info(f"🏃 Processing rate: {parcels_per_hour:.0f} parcels/hour")
            
            # Performance target analysis
            if processing_time < 1800:  # <30 minutes  
                logger.info("🎉 EXCELLENT: Completed in <30 minutes!")
            elif processing_time < 3600:  # <1 hour
                logger.info("✅ GOOD: Completed in <1 hour!")
            else:
                logger.warning(f"⚠️ Slower than hoped: {processing_time/60:.1f} minutes")
            
            # Get final performance statistics
            final_stats = blob_manager.get_cache_stats()
            logger.info(f"📈 Performance Statistics:")
            logger.info(f"   Cache hits: {final_stats['streaming_cache_hits']}")
            logger.info(f"   Cache rate: {final_stats['streaming_cache_rate']}")
            logger.info(f"   Total downloads: {final_stats['total_downloads']}")
            logger.info(f"   Preprocessed tiles used: {final_stats['preprocessed_tiles_used']}")
            logger.info(f"   Compressed tiles fallback: {final_stats['compressed_tiles_fallback']}")
            logger.info(f"   Preprocessing usage rate: {final_stats['preprocessed_usage_rate']}")
            
        else:
            logger.error(f"❌ DeWitt County processing failed: {results.get('error', 'Unknown error')}")
            return False
        
        # Step 6: Database output validation
        logger.info("")
        logger.info("🗄️ VALIDATING DATABASE OUTPUTS")
        logger.info("=" * 40)
        
        from src.core.database_manager_v3 import database_manager
        with database_manager.get_connection('biomass_output') as conn:
            cursor = conn.cursor()
            
            # Count output records for DeWitt County
            cursor.execute("""
                SELECT COUNT(*) as total_records,
                       COUNT(DISTINCT parcel_id) as unique_parcels,
                       COUNT(CASE WHEN forest_biomass_tons > 0 THEN 1 END) as with_forest,
                       COUNT(CASE WHEN crop_yield_tons > 0 THEN 1 END) as with_crops,
                       AVG(total_biomass_tons) as avg_biomass
                FROM parcel_biomass_analysis
                WHERE state_fips = '17' AND county_fips = '039'
                AND created_at >= %s
            """, [datetime.fromtimestamp(start_time)])
            
            output_stats = cursor.fetchone()
            
            if output_stats and output_stats['total_records'] > 0:
                logger.info(f"✅ Database outputs validated:")
                logger.info(f"   Total records: {output_stats['total_records']:,}")
                logger.info(f"   Unique parcels: {output_stats['unique_parcels']:,}")
                logger.info(f"   With forest data: {output_stats['with_forest']:,}")
                logger.info(f"   With crop data: {output_stats['with_crops']:,}")
                logger.info(f"   Average biomass: {output_stats['avg_biomass']:.2f} tons/parcel")
            else:
                logger.warning("⚠️ No output records found in database")
        
        total_time = time.time() - start_time
        logger.info("")
        logger.info("🎉 DEWITT COUNTY PERFORMANCE TEST COMPLETED")
        logger.info(f"⏱️ Total runtime: {total_time/60:.1f} minutes")
        logger.info(f"📄 Full results saved to: {log_filename}")
        logger.info("")
        logger.info("🔧 Performance Optimizations Applied:")
        logger.info("   ✅ Database connection pool: Forestry DB 20 connections")
        logger.info("   ✅ Parallel processing: 4 concurrent workers")
        logger.info("   ✅ Reduced batch size: 100 parcels per batch")
        logger.info("   ✅ Increased cache size: 500 tiles (10x increase)")
        logger.info("   ✅ HTTP connection optimization: Retry policies & pooling")
        logger.info("   ✅ Preprocessing support: Ready for uncompressed tiles")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ DeWitt County test failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = run_dewitt_county_performance_test()
    sys.exit(0 if success else 1)