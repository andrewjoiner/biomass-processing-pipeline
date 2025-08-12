#!/usr/bin/env python3
"""
Debug tile bounds calculation to understand why we're getting 0 or 144 tiles
"""

import sys
sys.path.append('src')

from core.coordinate_utils_v3 import CoordinateTransformer

def debug_tile_bounds():
    """Debug what tile bounds are being calculated"""
    
    # DeWitt County bounds
    county_bounds = (-89.14866970243132, 40.048844739300705, -88.58895840990046, 40.283138654684116)
    print(f"DeWitt County bounds: {county_bounds}")
    
    transformer = CoordinateTransformer()
    
    # Test a few example tiles that should be in Illinois
    test_tiles = ['15TUL', '15TUM', '16TCK', '16TDK', '16TDL']
    
    for tile_id in test_tiles:
        try:
            tile_bounds = transformer.get_sentinel2_tile_bounds(tile_id)
            print(f"\nTile {tile_id}:")
            print(f"  Calculated bounds: {tile_bounds}")
            
            if tile_bounds:
                intersects = transformer.bounds_intersect(county_bounds, tile_bounds)
                print(f"  Intersects DeWitt County: {intersects}")
            else:
                print(f"  Failed to calculate bounds")
                
        except Exception as e:
            print(f"  Error: {e}")

if __name__ == "__main__":
    debug_tile_bounds()