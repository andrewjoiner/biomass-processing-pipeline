#!/usr/bin/env python3
"""
Database Configuration v1 - PostgreSQL Connection Settings
Clean configuration management for biomass processing pipeline
"""

import os
from typing import Dict

def get_database_config() -> Dict[str, Dict[str, str]]:
    """
    Get database configuration from environment variables
    Based on actual database schema discovered: postgres DB with forestry/cdl schemas
    
    Returns:
        Dictionary containing database configurations for parcels, crops, and forestry
    """
    base_config = {
        'host': os.getenv('POSTGRES_HOST', 'parcel-postgis-staging.postgres.database.azure.com'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'sslmode': os.getenv('POSTGRES_SSL_MODE', 'require'),
        'user': os.getenv('POSTGRES_USER', 'postgresadmin'),
        'password': os.getenv('POSTGRES_PASSWORD', 'P@ssw0rd1234')
    }
    
    return {
        # All source data is in postgres database but different schemas
        'parcels': {
            **base_config,
            'database': os.getenv('PARCELS_DB', 'postgres')  # parcels table in public schema
        },
        'crops': {
            **base_config,
            'database': os.getenv('CROPS_DB', 'postgres')    # CDL data in cdl schema  
        },
        'forestry': {
            **base_config,
            'database': os.getenv('FORESTRY_DB', 'postgres') # FIA data in forestry schema
        },
        'biomass_output': {
            **base_config,
            'database': os.getenv('BIOMASS_OUTPUT_DB', 'biomass_production_v2')  # Output database
        }
    }

def get_database_queries() -> Dict[str, str]:
    """
    Optimized SQL queries for biomass processing
    
    Returns:
        Dictionary of SQL query templates
    """
    return {
        # Parcel queries
        'get_county_parcels': """
            SELECT 
                parcelid,
                ST_AsGeoJSON(geometry) as geometry,
                ST_AsText(geometry) as postgis_geometry,
                ST_X(ST_Centroid(geometry)) as centroid_lon,
                ST_Y(ST_Centroid(geometry)) as centroid_lat
            FROM parcels
            WHERE fipsstate = %s AND fipscounty = %s
            AND geometry IS NOT NULL
            AND ST_Area(geography(geometry)) > %s
            ORDER BY parcelid
            LIMIT %s
        """,
        
        'get_county_bounds': """
            SELECT 
                ST_XMin(ST_Extent(geometry)) as min_lon,
                ST_YMin(ST_Extent(geometry)) as min_lat,
                ST_XMax(ST_Extent(geometry)) as max_lon,
                ST_YMax(ST_Extent(geometry)) as max_lat
            FROM parcels
            WHERE fipsstate = %s AND fipscounty = %s
            AND geometry IS NOT NULL
        """,
        
        # CDL crop queries (cdl schema) - Fixed parameter count mismatch
        'get_cdl_intersections': """
            SELECT 
                crop_code,
                ST_Area(ST_Intersection(ST_MakeValid(geometry), ST_GeomFromText(%s, 4326))) as intersection_area_m2,
                ST_Area(ST_GeomFromText(%s, 4326)) as parcel_area_m2,
                (ST_Area(ST_Intersection(ST_MakeValid(geometry), ST_GeomFromText(%s, 4326))) / 
                 NULLIF(ST_Area(ST_GeomFromText(%s, 4326)), 0) * 100) as coverage_percent
            FROM cdl.us_cdl_data
            WHERE ST_Intersects(ST_MakeValid(geometry), ST_GeomFromText(%s, 4326))
            AND crop_code NOT IN (111, 112, 121, 122, 123, 124, 131)
        """,
        
        'get_county_cdl_bulk': """
            SELECT 
                crop_code,
                ST_Area(geometry) as intersection_area_m2
            FROM cdl.us_cdl_data 
            WHERE crop_code NOT IN (111, 112, 121, 122, 123, 124, 131)
            AND ST_Intersects(geometry, ST_MakeEnvelope(%s, %s, %s, %s, 4326))
        """,
        
        # FIA forest queries (forestry schema) - Fixed SRID mismatch
        'get_nearby_fia_plots': """
            SELECT 
                p.cn as plot_cn, p.lat, p.lon, p.statecd, p.countycd,
                p.plot as plot_id, p.invyr as inventory_year,
                ST_Distance(ST_Centroid(ST_GeomFromText(%s, 4326)), ST_SetSRID(ST_Point(p.lon, p.lat), 4326)) as distance_degrees
            FROM forestry.plot_local p
            WHERE ST_DWithin(ST_Centroid(ST_GeomFromText(%s, 4326)), ST_SetSRID(ST_Point(p.lon, p.lat), 4326), %s)
            AND p.lat IS NOT NULL AND p.lon IS NOT NULL
            ORDER BY distance_degrees
            LIMIT 50
        """,
        
        # FIA tree biomass query - using available columns only
        'get_fia_trees_for_plots': """
            SELECT 
                t.plt_cn, t.cn as tree_cn, t.spcd as species_code,
                t.drybio_ag, t.drybio_bole, t.drybio_stump, 
                t.drybio_branch, t.drybio_foliage,
                t.drybio_stem, t.drybio_sawlog, t.drybio_bg,
                t.dia as diameter, t.ht as height, t.statuscd
            FROM forestry.tree_local t
            WHERE t.plt_cn = ANY(%s)
            AND t.drybio_ag IS NOT NULL AND t.drybio_ag > 0
            AND t.statuscd = 1
        """,
        
        # Enhanced parcel query with spatial optimization
        'get_county_parcels_optimized': """
            SELECT 
                parcelid,
                ST_AsGeoJSON(geometry) as geometry,
                ST_AsText(geometry) as postgis_geometry,
                ST_X(ST_Centroid(geometry)) as centroid_lon,
                ST_Y(ST_Centroid(geometry)) as centroid_lat,
                ST_XMin(geometry) as min_lon,
                ST_YMin(geometry) as min_lat,
                ST_XMax(geometry) as max_lon,
                ST_YMax(geometry) as max_lat
            FROM parcels
            WHERE fipsstate = %s AND fipscounty = %s
            AND geometry IS NOT NULL
            AND ST_Area(geography(geometry)) > %s
            ORDER BY parcelid
            LIMIT %s
        """
    }

# CDL Crop Code Mapping
CDL_CODES = {
    1: 'Corn', 2: 'Cotton', 3: 'Rice', 4: 'Sorghum', 5: 'Soybeans',
    6: 'Sunflower', 10: 'Peanuts', 11: 'Tobacco', 12: 'Sweet_Corn',
    13: 'Pop_Orn_Corn', 14: 'Mint', 21: 'Barley', 22: 'Durum_Wheat',
    23: 'Spring_Wheat', 24: 'Winter_Wheat', 25: 'Other_Small_Grains',
    26: 'Dbl_Crop_WinWht_Soybeans', 27: 'Rye', 28: 'Oats', 29: 'Millet',
    30: 'Speltz', 31: 'Canola', 32: 'Flaxseed', 33: 'Safflower',
    34: 'Rape_Seed', 35: 'Mustard', 36: 'Alfalfa', 37: 'Other_Hay_Non_Alfalfa',
    38: 'Camelina', 39: 'Buckwheat', 41: 'Sugarbeets', 42: 'Dry_Beans',
    43: 'Potatoes', 44: 'Other_Crops', 45: 'Sugarcane', 46: 'Sweet_Potatoes',
    47: 'Misc_Vegs_Fruits', 48: 'Watermelons', 49: 'Onions', 50: 'Cucumbers',
    51: 'Chick_Peas', 52: 'Lentils', 53: 'Peas', 54: 'Tomatoes',
    55: 'Caneberries', 56: 'Hops', 57: 'Herbs', 58: 'Clover_Wildflowers',
    59: 'Sod_Grass_Seed', 60: 'Switchgrass', 61: 'Fallow_Idle_Cropland',
    63: 'Forest', 64: 'Shrubland', 65: 'Barren', 81: 'Clouds_No_Data',
    82: 'Developed', 83: 'Water', 87: 'Wetlands', 88: 'Nonag_Undefined',
    111: 'Open_Water', 112: 'Perennial_Ice_Snow', 121: 'Developed_Open_Space',
    122: 'Developed_Low_Intensity', 123: 'Developed_Medium_Intensity',
    124: 'Developed_High_Intensity', 131: 'Barren_Land', 141: 'Deciduous_Forest',
    142: 'Evergreen_Forest', 143: 'Mixed_Forest', 152: 'Shrubland',
    176: 'Grassland_Pasture', 190: 'Woody_Wetlands', 195: 'Herbaceous_Wetlands'
}

# Urban/Non-Agricultural codes to filter out
URBAN_CODES = {111, 112, 121, 122, 123, 124, 131}

# WorldCover Land Cover Classes  
WORLDCOVER_CLASSES = {
    10: 'Tree_Cover', 20: 'Shrubland', 30: 'Grassland', 40: 'Cropland',
    50: 'Built_Up', 60: 'Bare_Sparse_Vegetation', 70: 'Snow_Ice',
    80: 'Permanent_Water', 90: 'Herbaceous_Wetland', 95: 'Mangroves',
    100: 'Moss_Lichen'
}

# Enhanced Crop Biomass Calculations - Yield + Residue Ratios
CROP_BIOMASS_DATA = {
    # Format: crop_code: {yield_tons_per_acre, residue_ratio, moisture_content, harvestable_residue_percent}
    1: {'name': 'Corn', 'yield_tons_per_acre': 4.2, 'residue_ratio': 1.2, 'moisture': 0.15, 'harvestable_residue': 0.40},
    5: {'name': 'Soybeans', 'yield_tons_per_acre': 1.6, 'residue_ratio': 1.5, 'moisture': 0.12, 'harvestable_residue': 0.25},
    21: {'name': 'Barley', 'yield_tons_per_acre': 2.1, 'residue_ratio': 1.3, 'moisture': 0.14, 'harvestable_residue': 0.50},
    22: {'name': 'Durum_Wheat', 'yield_tons_per_acre': 1.8, 'residue_ratio': 1.8, 'moisture': 0.14, 'harvestable_residue': 0.60},
    23: {'name': 'Spring_Wheat', 'yield_tons_per_acre': 1.9, 'residue_ratio': 1.8, 'moisture': 0.14, 'harvestable_residue': 0.60},
    24: {'name': 'Winter_Wheat', 'yield_tons_per_acre': 2.2, 'residue_ratio': 1.8, 'moisture': 0.14, 'harvestable_residue': 0.60},
    27: {'name': 'Rye', 'yield_tons_per_acre': 1.7, 'residue_ratio': 1.9, 'moisture': 0.14, 'harvestable_residue': 0.55},
    28: {'name': 'Oats', 'yield_tons_per_acre': 2.3, 'residue_ratio': 1.4, 'moisture': 0.14, 'harvestable_residue': 0.45},
    36: {'name': 'Alfalfa', 'yield_tons_per_acre': 3.8, 'residue_ratio': 0.1, 'moisture': 0.20, 'harvestable_residue': 0.80},
    37: {'name': 'Other_Hay', 'yield_tons_per_acre': 2.5, 'residue_ratio': 0.1, 'moisture': 0.20, 'harvestable_residue': 0.75},
    41: {'name': 'Sugarbeets', 'yield_tons_per_acre': 28.5, 'residue_ratio': 0.8, 'moisture': 0.75, 'harvestable_residue': 0.30},
    4: {'name': 'Sorghum', 'yield_tons_per_acre': 3.1, 'residue_ratio': 1.3, 'moisture': 0.15, 'harvestable_residue': 0.45},
    3: {'name': 'Rice', 'yield_tons_per_acre': 3.8, 'residue_ratio': 1.5, 'moisture': 0.20, 'harvestable_residue': 0.35},
    2: {'name': 'Cotton', 'yield_tons_per_acre': 0.8, 'residue_ratio': 3.2, 'moisture': 0.10, 'harvestable_residue': 0.60},
    # Default values for unlisted crops
    'default': {'name': 'Other_Crop', 'yield_tons_per_acre': 2.0, 'residue_ratio': 1.0, 'moisture': 0.15, 'harvestable_residue': 0.40}
}

# FIA Forest Type Biomass Characteristics (tons per acre)
FOREST_BIOMASS_TYPES = {
    'conifer_dominant': {'standing_biomass': 125.0, 'harvestable_ratio': 0.70, 'residue_ratio': 0.35},
    'deciduous_dominant': {'standing_biomass': 95.0, 'harvestable_ratio': 0.65, 'residue_ratio': 0.40}, 
    'mixed_forest': {'standing_biomass': 110.0, 'harvestable_ratio': 0.68, 'residue_ratio': 0.37},
    'default_forest': {'standing_biomass': 100.0, 'harvestable_ratio': 0.65, 'residue_ratio': 0.38}
}