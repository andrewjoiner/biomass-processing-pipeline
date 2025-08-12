#!/usr/bin/env python3
"""
Create spatial indexes for CDL and forestry tables to fix performance bottlenecks
"""

import sys
sys.path.append('src')

def create_spatial_indexes():
    """Create spatial indexes on geometry columns"""
    
    try:
        from src.core.database_manager_v3 import database_manager
    except ImportError:
        print("Could not import database_manager")
        return

    print("=== Creating Spatial Indexes ===\n")
    
    # Index creation commands
    index_commands = [
        {
            'database': 'crops',
            'name': 'CDL Spatial Index',
            'sql': """
                CREATE INDEX IF NOT EXISTS idx_us_cdl_data_geometry_gist 
                ON cdl.us_cdl_data USING GIST (geometry);
            """,
            'description': 'Spatial index on cdl.us_cdl_data.geometry for fast ST_Intersects queries'
        }
    ]
    
    # We'll add forestry index if needed after checking what tables exist
    
    for index_info in index_commands:
        db_name = index_info['database']
        index_name = index_info['name']
        sql = index_info['sql']
        description = index_info['description']
        
        print(f"Creating {index_name} on {db_name} database...")
        print(f"Purpose: {description}")
        
        try:
            with database_manager.get_connection(db_name) as conn:
                cursor = conn.cursor()
                
                print(f"Executing: {sql.strip()}")
                cursor.execute(sql)
                conn.commit()
                
                print(f"✅ {index_name} created successfully!")
                
        except Exception as e:
            print(f"❌ Error creating {index_name}: {e}")
            # Check if index already exists
            if "already exists" in str(e).lower():
                print("   Index may already exist - this is OK")
            else:
                print(f"   Unexpected error: {e}")
        
        print()
    
    print("=== Index Creation Complete ===")
    print("CDL spatial index building should now be much faster!")
    print("Re-run the DeWitt County test to see the improvement.")

if __name__ == "__main__":
    create_spatial_indexes()