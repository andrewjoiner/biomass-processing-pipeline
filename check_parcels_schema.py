#!/usr/bin/env python3
"""Check parcels table schema to get correct column names"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from core.database_manager_v3 import database_manager

def check_schema():
    try:
        with database_manager.get_connection('parcels') as conn:
            cursor = conn.cursor()
            
            # Get column names and types
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'parcels' 
                ORDER BY ordinal_position
            """)
            
            print("📊 Parcels table columns:")
            for row in cursor.fetchall():
                print(f"  {row['column_name']:20} | {row['data_type']}")
                
            print("\n🔍 Sample data:")
            cursor.execute("SELECT * FROM parcels LIMIT 2")
            rows = cursor.fetchall()
            if rows:
                for key in rows[0].keys():
                    print(f"  {key:20} | {rows[0][key]}")
                    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_schema()