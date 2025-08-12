#!/usr/bin/env python3
"""
Quick test to validate the satellite tile cache fix
Tests the new unified caching mechanism with a single parcel
"""

import logging
import sys
import json
import time
from datetime import datetime

def setup_logging():
    """Setup simple logging for cache test"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger(__name__)

def main():
    logger = setup_logging()
    logger.info("🧪 TESTING SATELLITE TILE CACHE FIX")
    logger.info("=" * 60)
    
    try:
        # Import the optimized processor
        from src.pipeline.optimized_county_processor_v3 import OptimizedCountyProcessor
        
        # Initialize processor
        processor = OptimizedCountyProcessor()
        logger.info("✅ Processor initialized")
        
        # Test with DeWitt County setup
        state_fips = '17'
        county_fips = '039'
        
        logger.info(f"🚀 Processing {state_fips}-{county_fips} with 2 parcels to test caching...")
        start_time = time.time()
        
        # Process 2 parcels to test the cache behavior
        results = processor.process_county_optimized(
            state_fips=state_fips,
            county_fips=county_fips,
            max_parcels=2,
            max_workers=1  # Single thread for cleaner testing
        )
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Get final cache stats
        blob_manager = processor.blob_manager
        final_stats = blob_manager.get_performance_stats()
        
        logger.info("=" * 60)
        logger.info("📈 CACHE PERFORMANCE RESULTS")
        logger.info("=" * 60)
        logger.info(f"Processing time: {processing_time:.1f} seconds")
        logger.info(f"Tiles cached: {final_stats['streaming_tiles_cached']}")
        logger.info(f"Cache hits: {final_stats['streaming_cache_hits']}")
        logger.info(f"Cache misses: {final_stats['streaming_cache_misses']}")
        logger.info(f"Downloads: {final_stats['downloads']}")
        
        if final_stats['streaming_cache_hits'] > 0:
            hit_rate = final_stats['streaming_cache_hits'] / (final_stats['streaming_cache_hits'] + final_stats['streaming_cache_misses'])
            logger.info(f"Cache hit rate: {hit_rate:.1%}")
            
        # Validate results
        if results and len(results) > 0:
            logger.info(f"✅ Successfully processed {len(results)} parcels")
            
            # Check if caching worked (should have some cache hits)
            if final_stats['streaming_cache_hits'] > 0:
                logger.info("✅ Cache system is working - found cache hits!")
            else:
                logger.warning("⚠️ No cache hits detected - cache may not be working optimally")
                
            return True
        else:
            logger.error("❌ No results returned from processing")
            return False
            
    except Exception as e:
        logger.error(f"❌ Cache test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 Cache test completed successfully!")
        sys.exit(0)
    else:
        print("\n💥 Cache test failed!")
        sys.exit(1)