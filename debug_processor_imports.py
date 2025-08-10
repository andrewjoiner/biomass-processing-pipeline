#!/usr/bin/env python3
"""
Debug Processor Import Pattern
Test the exact import pattern used in optimized_county_processor_v3
"""

import logging
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_processor_imports():
    """Test the exact import pattern from optimized_county_processor_v3"""
    
    try:
        logger.info("🧪 Testing Processor Import Pattern")
        logger.info("=" * 50)
        
        # Test the exact imports from optimized_county_processor_v3
        logger.info("Importing database_manager...")
        from src.core.database_manager_v3 import database_manager
        logger.info(f"✅ database_manager: {type(database_manager)}")
        
        logger.info("Importing blob_manager...")
        from src.core.blob_manager_v3 import blob_manager
        logger.info(f"✅ blob_manager: {type(blob_manager)}")
        
        # Check if both instances have their expected methods
        logger.info("Testing database_manager methods...")
        db_has_method = hasattr(database_manager, 'get_county_bounds')
        logger.info(f"✅ database_manager has get_county_bounds: {db_has_method}")
        
        logger.info("Testing blob_manager methods...")
        blob_has_method = hasattr(blob_manager, 'get_required_tiles_for_parcels')
        logger.info(f"✅ blob_manager has get_required_tiles_for_parcels: {blob_has_method}")
        
        # Create a simple OptimizedCountyProcessorV3 instance to test initialization
        logger.info("Creating OptimizedCountyProcessorV3 instance...")
        
        class TestProcessor:
            def __init__(self):
                self.db_manager = database_manager
                self.blob_manager = blob_manager
                
        test_processor = TestProcessor()
        logger.info(f"✅ TestProcessor created")
        logger.info(f"✅ test_processor.db_manager: {type(test_processor.db_manager)}")
        logger.info(f"✅ test_processor.blob_manager: {type(test_processor.blob_manager)}")
        
        # Test the specific method call that's failing
        logger.info("Testing get_required_tiles_for_parcels call...")
        test_geometries = []
        result = test_processor.blob_manager.get_required_tiles_for_parcels(test_geometries)
        logger.info(f"✅ get_required_tiles_for_parcels result: {result}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error in processor imports: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = test_processor_imports()
    
    logger.info("=" * 50)
    if success:
        logger.info("✅ Processor import pattern: SUCCESS")
    else:
        logger.info("❌ Processor import pattern: FAILED")