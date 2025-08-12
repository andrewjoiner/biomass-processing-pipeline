#!/usr/bin/env python3
"""
McLean County Production Test - Complete Pipeline with Streaming Architecture
Target: Complete county processing in <1 hour with full analysis outputs
"""

import logging
import sys
import json
import time
from datetime import datetime
from typing import Dict, Optional

def setup_logging():
    """Configure comprehensive logging for production test"""
    import os
    os.makedirs('logs', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'logs/MCLEAN_COUNTY_PRODUCTION_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"McLean County production test logging - output to {log_filename}")
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

def verify_biomass_output_table(logger):
    """Verify biomass_output table structure"""
    logger.info("🗄️ Verifying biomass_output table structure...")
    
    try:
        from src.core.database_manager_v3 import database_manager
        
        with database_manager.get_connection('biomass_output') as conn:
            cursor = conn.cursor()
            
            # Check if table exists and get structure
            cursor.execute("""
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'parcel_biomass_analysis'
                ORDER BY ordinal_position
            """)
            
            columns = cursor.fetchall()
            if columns:
                logger.info(f"  ✅ parcel_biomass_analysis table found with {len(columns)} columns")
                for col in columns[:5]:  # Show first 5 columns
                    logger.info(f"    - {col[0]} ({col[1]})")
                return True
            else:
                logger.error("  ❌ parcel_biomass_analysis table not found")
                return False
                
    except Exception as e:
        logger.error(f"❌ Table verification failed: {e}")
        return False

def run_mclean_county_production_test():
    """Run complete McLean County production test"""
    logger, log_filename = setup_logging()
    
    logger.info("🚀 MCLEAN COUNTY PRODUCTION TEST - COMPLETE PIPELINE")
    logger.info("=" * 80)
    logger.info("Target: Complete county processing in <1 hour with streaming architecture")
    logger.info("Output: All analysis data saved to biomass_output.parcel_biomass_analysis")
    logger.info("")
    
    start_time = time.time()
    
    try:
        # Step 1: Test database connections
        if not test_database_connections(logger):
            logger.error("❌ Database connection test failed. Aborting.")
            return False
        
        # Step 2: Verify output table structure
        if not verify_biomass_output_table(logger):
            logger.error("❌ Output table verification failed. Aborting.")
            return False
        
        # Step 3: Initialize streaming architecture
        logger.info("🗂️ Initializing streaming architecture...")
        from src.core.blob_manager_v3 import blob_manager
        from src.pipeline.optimized_county_processor_v3 import OptimizedCountyProcessor
        
        # Get streaming cache stats
        initial_stats = blob_manager.get_cache_stats()
        logger.info(f"  📊 Initial cache state: {initial_stats['streaming_tiles_cached']} tiles cached")
        logger.info("  ✅ Streaming architecture initialized")
        
        # Step 4: Get McLean County parcel count
        logger.info("📊 Analyzing McLean County scope...")
        from src.core.database_manager_v3 import database_manager
        
        with database_manager.get_connection('parcels') as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) as parcel_count,
                       MIN(ST_XMin(geometry)) as min_lon,
                       MAX(ST_XMax(geometry)) as max_lon, 
                       MIN(ST_YMin(geometry)) as min_lat,
                       MAX(ST_YMax(geometry)) as max_lat
                FROM parcels
                WHERE statefips = '17' AND countyfips = '113'
            """)
            
            county_info = cursor.fetchone()
            if county_info and county_info['parcel_count'] > 0:
                logger.info(f"  🏘️ McLean County: {county_info['parcel_count']:,} parcels")
                logger.info(f"  🗺️ Bounds: ({county_info['min_lon']:.3f}, {county_info['min_lat']:.3f}) to ({county_info['max_lon']:.3f}, {county_info['max_lat']:.3f})")
            else:
                logger.error("❌ No parcels found for McLean County")
                return False
        
        # Step 5: Run optimized county processing with time limit
        logger.info("")
        logger.info("🚀 STARTING MCLEAN COUNTY PROCESSING")
        logger.info("=" * 60)
        
        processor = OptimizedCountyProcessor()
        
        # Process with reasonable batch size for streaming
        processing_start = time.time()
        results = processor.process_county_optimized(
            state_fips='17',      # Illinois
            county_fips='113',    # McLean County  
            max_parcels=None,     # Process all parcels
            batch_size=500        # Optimized batch size for streaming
        )
        processing_time = time.time() - processing_start
        
        # Step 6: Analyze results
        logger.info("")
        logger.info("📊 PROCESSING RESULTS ANALYSIS")
        logger.info("=" * 50)
        
        if results.get('success'):
            logger.info(f"✅ County processing completed successfully!")
            logger.info(f"⏱️ Processing time: {processing_time/60:.1f} minutes")
            logger.info(f"📊 Parcels processed: {results.get('parcels_processed', 0):,}")
            
            # Check performance targets
            if processing_time < 3600:  # <1 hour
                if processing_time < 1800:  # <30 minutes  
                    logger.info("🎉 EXCELLENT: Completed in <30 minutes!")
                else:
                    logger.info("✅ GOOD: Completed in <1 hour target!")
            else:
                logger.warning(f"⚠️ Exceeded 1 hour target by {(processing_time-3600)/60:.1f} minutes")
            
            # Get final cache statistics
            final_stats = blob_manager.get_cache_stats()
            logger.info(f"📈 Cache performance:")
            logger.info(f"   Streaming cache hits: {final_stats['streaming_cache_hits']}")
            logger.info(f"   Streaming cache rate: {final_stats['streaming_cache_rate']}")
            logger.info(f"   Total downloads: {final_stats['total_downloads']}")
        else:
            logger.error(f"❌ County processing failed: {results.get('error', 'Unknown error')}")
            return False
        
        # Step 7: Validate database outputs
        logger.info("")
        logger.info("🗄️ VALIDATING DATABASE OUTPUTS")
        logger.info("=" * 40)
        
        with database_manager.get_connection('biomass_output') as conn:
            cursor = conn.cursor()
            
            # Count output records
            cursor.execute("""
                SELECT COUNT(*) as total_records,
                       COUNT(DISTINCT parcel_id) as unique_parcels,
                       COUNT(CASE WHEN crop_analysis IS NOT NULL THEN 1 END) as with_crops,
                       COUNT(CASE WHEN forest_analysis IS NOT NULL THEN 1 END) as with_forestry,
                       COUNT(CASE WHEN vegetation_analysis IS NOT NULL THEN 1 END) as with_vegetation
                FROM parcel_biomass_analysis
                WHERE state_fips = '17' AND county_fips = '113'
                AND created_at >= %s
            """, [datetime.fromtimestamp(start_time)])
            
            output_stats = cursor.fetchone()
            
            if output_stats and output_stats['total_records'] > 0:
                logger.info(f"✅ Database outputs validated:")
                logger.info(f"   Total records: {output_stats['total_records']:,}")
                logger.info(f"   Unique parcels: {output_stats['unique_parcels']:,}")
                logger.info(f"   With crop analysis: {output_stats['with_crops']:,}")
                logger.info(f"   With forest analysis: {output_stats['with_forestry']:,}")  
                logger.info(f"   With vegetation analysis: {output_stats['with_vegetation']:,}")
            else:
                logger.error("❌ No output records found in database")
                return False
        
        total_time = time.time() - start_time
        logger.info("")
        logger.info("🎉 MCLEAN COUNTY PRODUCTION TEST COMPLETED SUCCESSFULLY")
        logger.info(f"⏱️ Total runtime: {total_time/60:.1f} minutes")
        logger.info(f"📄 Full results saved to: {log_filename}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Production test failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = run_mclean_county_production_test()
    sys.exit(0 if success else 1)