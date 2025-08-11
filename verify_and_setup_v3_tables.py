#!/usr/bin/env python3
"""
Verify V3 tables exist and create them if needed
"""

import logging
import psycopg2
from src.config.database_config_v3 import get_database_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_and_create_v3_database():
    """Check if biomass_v3 database exists and create if needed"""
    config = get_database_config()
    base_config = config['parcels'].copy()
    base_config['database'] = 'postgres'
    
    try:
        conn = psycopg2.connect(**base_config)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'biomass_v3'")
        if cursor.fetchone():
            logger.info("✅ biomass_v3 database exists")
        else:
            logger.info("Creating biomass_v3 database...")
            cursor.execute("CREATE DATABASE biomass_v3")
            logger.info("✅ Created biomass_v3 database")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Database check/creation failed: {e}")
        return False

def check_and_create_v3_tables():
    """Check if V3 tables exist and create if needed"""
    config = get_database_config()
    biomass_config = config['biomass_output']
    
    try:
        conn = psycopg2.connect(**biomass_config)
        cursor = conn.cursor()
        
        # Check if forestry_analysis_v3 table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'forestry_analysis_v3'
            )
        """)
        forestry_exists = cursor.fetchone()[0]
        
        if not forestry_exists:
            logger.info("Creating forestry_analysis_v3 table...")
            cursor.execute("""
                CREATE TABLE forestry_analysis_v3 (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    parcel_id TEXT NOT NULL,
                    county_fips TEXT NOT NULL,
                    processing_timestamp TIMESTAMP DEFAULT NOW(),
                    
                    -- Basic metrics
                    total_biomass_tons NUMERIC(12,3),
                    forest_area_acres NUMERIC(10,3),
                    forest_percentage NUMERIC(5,2),
                    
                    -- Enhanced forest characteristics
                    stand_age_average NUMERIC(5,1),
                    forest_type_classification TEXT,
                    harvest_probability NUMERIC(3,2),
                    last_treatment_years INTEGER,
                    
                    -- Tree characteristics
                    tree_count_estimate INTEGER,
                    average_dbh_inches NUMERIC(6,2),
                    average_height_feet NUMERIC(7,2),
                    
                    -- Biomass breakdown 
                    standing_biomass_tons NUMERIC(12,3),
                    harvestable_biomass_tons NUMERIC(12,3),
                    residue_biomass_tons NUMERIC(12,3),
                    
                    -- FIA analysis metadata
                    fia_plot_count INTEGER,
                    fia_tree_count INTEGER,
                    data_sources TEXT,
                    
                    -- Quality metrics
                    ndvi_value NUMERIC(6,4),
                    confidence_score NUMERIC(4,3)
                );
                
                CREATE INDEX idx_forestry_v3_parcel ON forestry_analysis_v3(parcel_id);
                CREATE INDEX idx_forestry_v3_county ON forestry_analysis_v3(county_fips);
            """)
            conn.commit()
            logger.info("✅ Created forestry_analysis_v3 table")
        else:
            logger.info("✅ forestry_analysis_v3 table exists")
        
        # Check if crop_analysis_v3 table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'crop_analysis_v3'
            )
        """)
        crop_exists = cursor.fetchone()[0]
        
        if not crop_exists:
            logger.info("Creating crop_analysis_v3 table...")
            cursor.execute("""
                CREATE TABLE crop_analysis_v3 (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    parcel_id TEXT NOT NULL,
                    county_fips TEXT NOT NULL,
                    processing_timestamp TIMESTAMP DEFAULT NOW(),
                    
                    -- Crop identification
                    crop_code INTEGER NOT NULL,
                    crop_name TEXT NOT NULL,
                    is_dominant_crop BOOLEAN DEFAULT FALSE,
                    crop_category TEXT,
                    
                    -- Area analysis
                    area_acres NUMERIC(10,3),
                    area_percentage NUMERIC(5,2),
                    coverage_percent NUMERIC(5,2),
                    
                    -- Yield analysis
                    estimated_yield_tons NUMERIC(10,3),
                    estimated_yield_tons_per_acre NUMERIC(10,3),
                    estimated_residue_tons_dry NUMERIC(10,3),
                    estimated_residue_tons_wet NUMERIC(10,3),
                    harvestable_residue_tons NUMERIC(10,3),
                    residue_ratio NUMERIC(4,2),
                    moisture_content NUMERIC(4,3),
                    harvestable_residue_percent NUMERIC(5,2),
                    
                    -- Quality metrics
                    ndvi_value NUMERIC(6,4),
                    confidence_score NUMERIC(4,3)
                );
                
                CREATE INDEX idx_crop_v3_parcel ON crop_analysis_v3(parcel_id);
                CREATE INDEX idx_crop_v3_county ON crop_analysis_v3(county_fips);
                CREATE INDEX idx_crop_v3_code ON crop_analysis_v3(crop_code);
            """)
            conn.commit()
            logger.info("✅ Created crop_analysis_v3 table")
        else:
            logger.info("✅ crop_analysis_v3 table exists")
        
        # Count records in tables
        cursor.execute("SELECT COUNT(*) FROM forestry_analysis_v3")
        forestry_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM crop_analysis_v3")
        crop_count = cursor.fetchone()[0]
        
        logger.info(f"📊 Current records: {forestry_count} forestry, {crop_count} crop")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Table check/creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    logger.info("🔍 Verifying V3 database and tables...")
    
    # Check/create database
    if check_and_create_v3_database():
        # Check/create tables
        if check_and_create_v3_tables():
            logger.info("✅ V3 database and tables are ready!")
        else:
            logger.error("❌ Failed to setup V3 tables")
    else:
        logger.error("❌ Failed to setup V3 database")