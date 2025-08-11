#!/usr/bin/env python3
"""
Setup V3 Database Schema
Creates the biomass_v3 database with enhanced forestry and crop tables
"""

import logging
import psycopg2
from src.config.database_config_v3 import get_database_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_v3_database():
    """Create the biomass_v3 database"""
    config = get_database_config()
    
    # Connect to postgres database to create biomass_v3
    base_config = config['parcels'].copy()
    base_config['database'] = 'postgres'
    
    try:
        conn = psycopg2.connect(**base_config)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'biomass_v3'")
        if cursor.fetchone():
            logger.info("biomass_v3 database already exists")
        else:
            cursor.execute("CREATE DATABASE biomass_v3")
            logger.info("✅ Created biomass_v3 database")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to create biomass_v3 database: {e}")
        return False

def create_v3_tables():
    """Create the enhanced V3 tables"""
    config = get_database_config()
    biomass_config = config['biomass_output']
    
    try:
        conn = psycopg2.connect(**biomass_config)
        cursor = conn.cursor()
        
        # Create forestry_analysis_v3 table
        forestry_sql = """
        CREATE TABLE IF NOT EXISTS forestry_analysis_v3 (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            parcel_id TEXT NOT NULL,
            county_fips TEXT NOT NULL,
            processing_timestamp TIMESTAMP DEFAULT NOW(),
            
            -- Basic metrics (from V1)
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
        """
        
        cursor.execute(forestry_sql)
        logger.info("✅ Created forestry_analysis_v3 table")
        
        # Create crop_analysis_v3 table
        crop_sql = """
        CREATE TABLE IF NOT EXISTS crop_analysis_v3 (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            parcel_id TEXT NOT NULL,
            county_fips TEXT NOT NULL,
            processing_timestamp TIMESTAMP DEFAULT NOW(),
            
            -- Crop identification (ALL CDL codes)
            crop_code INTEGER NOT NULL,
            crop_name TEXT NOT NULL,
            is_dominant_crop BOOLEAN DEFAULT FALSE,
            crop_category TEXT,
            
            -- Area analysis 
            area_acres NUMERIC(10,3),
            area_percentage NUMERIC(5,2),
            coverage_percent NUMERIC(5,2),
            
            -- Yield analysis
            yield_tons NUMERIC(10,3),
            yield_tons_per_acre NUMERIC(8,3),
            
            -- Residue analysis
            residue_tons_dry NUMERIC(10,3),
            residue_tons_wet NUMERIC(10,3),
            harvestable_residue_tons NUMERIC(10,3),
            residue_ratio NUMERIC(4,2),
            moisture_content NUMERIC(5,3),
            harvestable_residue_percent NUMERIC(5,2),
            
            -- Quality metrics
            ndvi_value NUMERIC(6,4),
            confidence_score NUMERIC(4,3)
        );
        """
        
        cursor.execute(crop_sql)
        logger.info("✅ Created crop_analysis_v3 table")
        
        # Create indexes for performance
        index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_forestry_v3_parcel ON forestry_analysis_v3(parcel_id);",
            "CREATE INDEX IF NOT EXISTS idx_forestry_v3_county ON forestry_analysis_v3(county_fips);",
            "CREATE INDEX IF NOT EXISTS idx_forestry_v3_timestamp ON forestry_analysis_v3(processing_timestamp);",
            
            "CREATE INDEX IF NOT EXISTS idx_crop_v3_parcel ON crop_analysis_v3(parcel_id);",
            "CREATE INDEX IF NOT EXISTS idx_crop_v3_county ON crop_analysis_v3(county_fips);",
            "CREATE INDEX IF NOT EXISTS idx_crop_v3_code ON crop_analysis_v3(crop_code);",
            "CREATE INDEX IF NOT EXISTS idx_crop_v3_timestamp ON crop_analysis_v3(processing_timestamp);",
            "CREATE INDEX IF NOT EXISTS idx_crop_v3_dominant ON crop_analysis_v3(is_dominant_crop);",
        ]
        
        for idx_sql in index_queries:
            cursor.execute(idx_sql)
        
        logger.info("✅ Created performance indexes")
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Failed to create V3 tables: {e}")
        return False

def main():
    """Setup complete V3 database schema"""
    logger.info("🚀 Setting up V3 database schema...")
    
    # Step 1: Create database
    if not create_v3_database():
        logger.error("❌ Failed to create database")
        return False
    
    # Step 2: Create tables
    if not create_v3_tables():
        logger.error("❌ Failed to create tables")
        return False
    
    logger.info("✅ V3 database schema setup complete!")
    return True

if __name__ == "__main__":
    main()