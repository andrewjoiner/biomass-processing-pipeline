#!/usr/bin/env python3
"""
Debug BlobManagerV3 Instance and Methods
Quick test to verify the global blob_manager instance and its methods
"""

import logging
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_blob_manager_instance():
    """Test if the blob_manager global instance is properly initialized"""
    
    try:
        logger.info("🧪 Testing BlobManagerV3 Global Instance")
        logger.info("=" * 50)
        
        # Import the global instance
        from src.core.blob_manager_v3 import blob_manager
        
        logger.info(f"✅ Global blob_manager imported: {type(blob_manager)}")
        
        # Check if it has the required method
        has_method = hasattr(blob_manager, 'get_required_tiles_for_parcels')
        logger.info(f"✅ Has get_required_tiles_for_parcels method: {has_method}")
        
        if has_method:
            # Get method info
            method = getattr(blob_manager, 'get_required_tiles_for_parcels')
            logger.info(f"✅ Method type: {type(method)}")
            logger.info(f"✅ Method callable: {callable(method)}")
        else:
            # List available methods
            methods = [attr for attr in dir(blob_manager) if not attr.startswith('_')]
            logger.error(f"❌ Available methods: {methods}")
        
        # Test a simple method call (if Azure config allows)
        try:
            stats = blob_manager.get_cache_stats()
            logger.info(f"✅ get_cache_stats() works: {list(stats.keys())}")
        except Exception as e:
            logger.warning(f"⚠️  get_cache_stats() failed: {e}")
        
        # Test the problematic method with empty data
        try:
            test_geometries = []  # Empty list should not cause errors
            result = blob_manager.get_required_tiles_for_parcels(test_geometries)
            logger.info(f"✅ get_required_tiles_for_parcels() with empty list: {result}")
            return True
        except Exception as e:
            logger.error(f"❌ get_required_tiles_for_parcels() failed: {e}")
            return False
        
    except ImportError as e:
        logger.error(f"❌ Import error: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    success = test_blob_manager_instance()
    
    logger.info("=" * 50)
    if success:
        logger.info("✅ BlobManagerV3 instance test: SUCCESS")
    else:
        logger.info("❌ BlobManagerV3 instance test: FAILED")