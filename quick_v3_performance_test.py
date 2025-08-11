#!/usr/bin/env python3
"""
Quick V3 Performance Test - Test 1 batch only to check if fixes worked
"""

import logging
import sys
import os
import time

# Add src to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, 'src')
sys.path.insert(0, src_dir)

from src.pipeline.optimized_county_processor_v3 import optimized_county_processor_v3

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def quick_v3_test():
    """
    Quick test: Process just 1 batch (50 parcels) to see if performance fixes worked
    """
    logger.info("🚀 QUICK V3 PERFORMANCE TEST: 1 batch only")
    logger.info("=" * 50)
    
    start_time = time.time()
    
    try:
        # Process just 50 parcels from Polk County
        result = optimized_county_processor_v3.process_county(
            state_fips="55",
            county_fips="095", 
            max_parcels=50,  # Just 1 batch
            batch_size=50,
            save_detailed_records=True,
            database_only=True
        )
        
        end_time = time.time()
        total_time = end_time - start_time
        
        if result and result.get('success'):
            parcels_processed = result.get('parcels_processed', 0)
            parcels_per_second = parcels_processed / total_time if total_time > 0 else 0
            
            logger.info("🎉 V3 QUICK TEST RESULTS:")
            logger.info(f"✅ Processed: {parcels_processed} parcels")
            logger.info(f"⏱️ Time: {total_time:.1f} seconds")
            logger.info(f"🚀 Speed: {parcels_per_second:.1f} parcels/second")
            logger.info("=" * 50)
            
            if parcels_per_second > 5:
                logger.info("🎉 PERFORMANCE IMPROVEMENT DETECTED!")
                logger.info(f"Previous V3 speed: ~1.5 parcels/second")
                logger.info(f"Current V3 speed: {parcels_per_second:.1f} parcels/second") 
                logger.info(f"Improvement: {parcels_per_second/1.5:.1f}x faster")
                
                if parcels_per_second > 50:
                    logger.info("🔥 MASSIVE PERFORMANCE GAIN - V3 FIXES WORKED!")
                elif parcels_per_second > 20:
                    logger.info("🎯 SIGNIFICANT PERFORMANCE GAIN - Good progress!")
                elif parcels_per_second > 10:
                    logger.info("✅ MODERATE PERFORMANCE GAIN - On the right track!")
                else:
                    logger.info("📈 SMALL PERFORMANCE GAIN - Still needs work")
            else:
                logger.warning("⚠️ Still slow - may need more optimization")
                
        else:
            logger.error("❌ Test failed - check logs for errors")
            
    except Exception as e:
        logger.error(f"❌ Quick test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    quick_v3_test()