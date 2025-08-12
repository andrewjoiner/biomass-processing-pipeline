#!/usr/bin/env python3
"""
Check CDL table indexes and performance characteristics
"""

import sys
sys.path.append('src')

def check_cdl_indexes():
    """Check what indexes exist on the CDL and forestry tables"""
    
    try:
        from src.core.database_manager_v3 import database_manager
    except ImportError:
        print("Could not import database_manager")
        return
    
    try:
        with database_manager.get_connection('crops') as conn:
            cursor = conn.cursor()
            
            print("=== CDL & Forestry Table Index Analysis ===\n")
            
            # First, let's find what tables exist in forestry database
            print("=== Finding Forestry Tables ===")
            with database_manager.get_connection('forestry') as forestry_conn:
                forestry_cursor = forestry_conn.cursor()
                forestry_cursor.execute("""
                    SELECT table_schema, table_name 
                    FROM information_schema.tables 
                    WHERE table_type = 'BASE TABLE' 
                    AND table_schema NOT IN ('information_schema', 'pg_catalog')
                    ORDER BY table_schema, table_name;
                """)
                forestry_tables = forestry_cursor.fetchall()
                print("Forestry database tables:")
                for schema, table in forestry_tables:
                    print(f"  • {schema}.{table}")
                print()
            
            # Check both CDL and forestry tables
            tables_to_check = [
                ('crops', 'cdl', 'us_cdl_data')
            ]
            
            # Add forestry tables that likely contain geometry
            for schema, table in forestry_tables:
                if 'fia' in table.lower() or 'plot' in table.lower() or 'forest' in table.lower():
                    tables_to_check.append(('forestry', schema, table))
            
            for db_name, schema, table in tables_to_check:
                print(f"--- {db_name.upper()} DATABASE: {schema}.{table} ---")
                
                # Switch to appropriate database connection
                if db_name == 'forestry':
                    conn.close()
                    conn = database_manager.get_connection('forestry').__enter__()
                    cursor = conn.cursor()
                
                # Check if table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = %s 
                        AND table_name = %s
                    ) as table_exists;
                """, (schema, table))
                
                table_exists = cursor.fetchone()['table_exists']
                print(f"Table exists: {'✅ YES' if table_exists else '❌ NO'}")
                
                if not table_exists:
                    print("Skipping index check - table doesn't exist\n")
                    continue
                
                # Check what indexes exist
                cursor.execute("""
                    SELECT 
                        indexname, 
                        indexdef
                    FROM pg_indexes 
                    WHERE tablename = %s 
                    AND schemaname = %s
                    ORDER BY indexname;
                """, (table, schema))
                
                indexes = cursor.fetchall()
                print(f"Found {len(indexes)} indexes:")
                if indexes:
                    for idx_name, idx_def in indexes:
                        print(f"  • {idx_name}")
                        if 'gist' in idx_def.lower() and 'geometry' in idx_def.lower():
                            print(f"    {idx_def} ⭐ SPATIAL")
                        else:
                            print(f"    {idx_def}")
                        print()
                else:
                    print("  ❌ NO INDEXES FOUND!")
                
                # Check if spatial index exists specifically
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_indexes 
                        WHERE tablename = %s 
                        AND schemaname = %s
                        AND (indexdef ILIKE '%%gist%%geometry%%' OR indexdef ILIKE '%%gist%%geom%%')
                    ) as has_spatial_index;
                """, (table, schema))
                
                has_spatial = cursor.fetchone()['has_spatial_index']
                print(f"Spatial Index Status: {'✅ YES' if has_spatial else '❌ NO - NEEDS SPATIAL INDEX!'}")
                
                if not has_spatial:
                    print(f"🚨 Missing spatial index on {schema}.{table}.geometry!")
                    print(f"   This causes slow ST_Intersects queries.")
                
                print()
                
    except Exception as e:
        print(f"Error checking indexes: {e}")

if __name__ == "__main__":
    check_cdl_indexes()