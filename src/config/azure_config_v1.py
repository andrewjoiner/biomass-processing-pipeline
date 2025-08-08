#!/usr/bin/env python3
"""
Azure Configuration v1 - Blob Storage Settings
Clean configuration management for Azure blob access
"""

import os
from typing import Dict
from dotenv import load_dotenv

# Load environment variables when this module is imported
load_dotenv()

def get_azure_config() -> Dict[str, str]:
    """
    Get Azure blob storage configuration from environment variables
    
    Returns:
        Dictionary containing Azure storage configuration
    """
    return {
        'account_name': os.getenv('AZURE_STORAGE_ACCOUNT', 'cdlstorage2024'),
        'account_url': os.getenv('AZURE_STORAGE_URL', 'https://cdlstorage2024.blob.core.windows.net'),
        'account_key': os.getenv('AZURE_STORAGE_KEY'),
        'containers': {
            'sentinel2': os.getenv('SENTINEL2_CONTAINER', 'sentinel2-data'),
            'worldcover': os.getenv('WORLDCOVER_CONTAINER', 'worldcover-data'),
            'results': os.getenv('RESULTS_CONTAINER', 'parcel-analysis-results')
        }
    }

def get_sentinel2_config() -> Dict:
    """
    Configuration for Sentinel-2 satellite imagery access
    
    Returns:
        Dictionary with Sentinel-2 specific settings
    """
    return {
        'periods': {
            'june': 'sentinel2_june',
            'august': 'sentinel2_august', 
            'october': 'sentinel2_october'
        },
        'bands': ['B02', 'B03', 'B04', 'B08'],  # Blue, Green, Red, NIR
        'band_names': {
            'B02': 'blue',
            'B03': 'green', 
            'B04': 'red',
            'B08': 'nir'
        },
        'default_period': os.getenv('SENTINEL2_PERIOD', 'august'),
        'tile_size_meters': 109800,  # Sentinel-2 tiles are ~110km x 110km
        'pixel_size_meters': 10,
        'crs': 'UTM'  # UTM projection varies by tile
    }

def get_worldcover_config() -> Dict:
    """
    Configuration for ESA WorldCover land use data access
    
    Returns:
        Dictionary with WorldCover specific settings
    """
    return {
        'container_path': 'worldcover_2021',
        'tile_pattern': 'ESA_WorldCover_10m_2021_v200_{lat}{lon}.tif',
        'tile_size_degrees': 3.0,  # 3° x 3° tiles
        'pixel_size_meters': 10,
        'crs': 'EPSG:4326',  # WGS84
        'forest_class': 10,  # Tree cover class
        'classes': {
            10: 'Tree_Cover',
            20: 'Shrubland', 
            30: 'Grassland',
            40: 'Cropland',
            50: 'Built_Up',
            60: 'Bare_Sparse_Vegetation',
            70: 'Snow_Ice',
            80: 'Permanent_Water',
            90: 'Herbaceous_Wetland',
            95: 'Mangroves',
            100: 'Moss_Lichen'
        }
    }

def get_blob_paths() -> Dict[str, str]:
    """
    Generate blob path templates for different data types
    
    Returns:
        Dictionary of blob path templates
    """
    return {
        # Sentinel-2 path template: period/tile_id_date_band.tif
        'sentinel2': '{period}/{tile_id}_{date}_{band}.tif',
        
        # WorldCover path template: worldcover_2021/tile_name.tif  
        'worldcover': 'worldcover_2021/ESA_WorldCover_10m_2021_v200_{lat}{lon}.tif',
        
        # Results path template: state/county/file
        'results': 'biomass-inventory/state={state}/county={county}/{filename}'
    }

def get_tile_naming_conventions() -> Dict:
    """
    Tile naming conventions for different satellite systems
    
    Returns:
        Dictionary with naming pattern information
    """
    return {
        'sentinel2': {
            'tile_id_pattern': r'(\d{2}[A-Z]{3})',  # e.g., '15TUL'
            'date_pattern': r'(\d{8})',  # YYYYMMDD
            'band_pattern': r'(B0[2348])',  # B02, B03, B04, B08
            'mgrs_zones': {
                'iowa': ['15TUL', '15TUM', '15TUN', '15TUE', '15TUF', '15TUG'],
                'california': ['10S', '11S', '10T', '11T'],
                'texas': ['14R', '14S', '15R', '15S']
            }
        },
        'worldcover': {
            'lat_pattern': r'([NS]\d{2})',  # N39, S30, etc.
            'lon_pattern': r'([EW]\d{3})',  # W096, E120, etc.
            'grid_resolution': 3.0,  # 3 degree tiles
            'coordinate_format': '{ns}{lat:02d}{ew}{lon:03d}'
        }
    }