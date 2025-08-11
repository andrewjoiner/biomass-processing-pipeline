#!/usr/bin/env python3
"""
Processing Configuration v1 - Processing Parameters and Settings
Clean configuration management for biomass processing parameters
"""

import os
from typing import Dict, List

def get_processing_config() -> Dict:
    """
    Get processing configuration from environment variables
    
    Returns:
        Dictionary containing processing parameters
    """
    return {
        'batch_size': int(os.getenv('BATCH_SIZE', '1000')),
        'max_memory_mb': int(os.getenv('MAX_MEMORY_MB', '8192')),
        'max_workers': int(os.getenv('MAX_WORKERS', '4')),
        'save_frequency': int(os.getenv('SAVE_FREQUENCY', '1000')),
        'sentinel2_period': os.getenv('SENTINEL2_PERIOD', 'august'),
        'fia_search_radius_degrees': float(os.getenv('FIA_SEARCH_RADIUS_DEGREES', '0.1')),
        'min_parcel_area_acres': float(os.getenv('MIN_PARCEL_AREA_ACRES', '0.1')),
        'confidence_threshold': float(os.getenv('CONFIDENCE_THRESHOLD', '0.5')),
        'timeout_seconds': int(os.getenv('PROCESSING_TIMEOUT_SECONDS', '300'))
    }

def get_test_config() -> Dict:
    """
    Get test configuration settings
    
    Returns:
        Dictionary containing test parameters
    """
    return {
        'test_county_fips': os.getenv('TEST_COUNTY_FIPS', '19055'),  # Delaware County, IA
        'test_parcel_limit': int(os.getenv('TEST_PARCEL_LIMIT', '100')),
        'output_dir': os.getenv('OUTPUT_DIR', 'results'),
        'log_level': os.getenv('LOG_LEVEL', 'INFO')
    }

def get_output_schema() -> List[str]:
    """
    Get standardized output schema for biomass inventory records
    
    Returns:
        List of column names for output CSV files
    """
    return [
        'parcel_id',                    # Unique parcel identifier
        'state',                        # State abbreviation  
        'county',                       # County name
        'fips_code',                    # 5-digit FIPS code
        'processing_date',              # Date of analysis
        'parcel_total_acres',           # Total parcel area
        'geometry_centroid_lat',        # Parcel centroid latitude
        'geometry_centroid_lon',        # Parcel centroid longitude
        'biomass_type',                 # Type: 'crop', 'forest', 'other'
        'source_code',                  # CDL code or WorldCover class
        'source_name',                  # Human-readable source name
        'area_acres',                   # Area of this biomass type
        'coverage_percent',             # Percentage of parcel covered
        'ndvi',                         # Normalized Difference Vegetation Index
        'evi',                          # Enhanced Vegetation Index
        'savi',                         # Soil Adjusted Vegetation Index
        'ndwi',                         # Normalized Difference Water Index
        'acquisition_date',             # Date of satellite imagery
        'confidence_score'              # Analysis confidence (0-1)
    ]

def get_performance_targets() -> Dict:
    """
    Get performance targets for processing pipeline
    
    Returns:
        Dictionary with performance expectations
    """
    return {
        'parcel_processing_time_seconds': 0.1,     # Target processing time per parcel
        'county_setup_time_minutes': 5,            # Target tile download time per county
        'error_rate_percent': 1.0,                 # Maximum acceptable error rate
        'memory_usage_mb_per_parcel': 8,           # Memory usage target
        'parcels_per_hour': 36000,                 # Target throughput (0.1s per parcel)
        'national_processing_months': 6             # Target time for all 150M parcels
    }

def get_state_processing_order() -> Dict[str, List[str]]:
    """
    Get prioritized order for state processing
    
    Returns:
        Dictionary with processing phases and state lists
    """
    return {
        'phase_1_corn_belt': ['IA', 'IL', 'IN', 'OH'],      # High agricultural value
        'phase_2_major_ag': ['CA', 'TX', 'KS', 'NE'],       # Major agricultural states
        'phase_3_forest_states': ['OR', 'WA', 'GA', 'AL'],  # High forestry value
        'phase_4_mixed': ['WI', 'MI', 'NY', 'PA', 'FL', 'NC', 'SC'],  # Mixed ag/forest
        'phase_5_remaining': [
            'AK', 'AZ', 'AR', 'CO', 'CT', 'DE', 'HI', 'ID', 'KY', 
            'LA', 'ME', 'MD', 'MA', 'MN', 'MS', 'MO', 'MT', 'NV', 
            'NH', 'NJ', 'NM', 'ND', 'OK', 'RI', 'SD', 'TN', 'UT', 
            'VT', 'VA', 'WV', 'WY'
        ]
    }

def get_vegetation_index_thresholds() -> Dict[str, Dict[str, float]]:
    """
    Get vegetation index thresholds for quality assessment
    
    Returns:
        Dictionary with vegetation index ranges and thresholds
    """
    return {
        'ndvi': {
            'min_valid': -1.0,
            'max_valid': 1.0,
            'healthy_vegetation_min': 0.3,
            'dense_vegetation_min': 0.7,
            'water_max': 0.1
        },
        'evi': {
            'min_valid': -1.0,
            'max_valid': 1.0,
            'healthy_vegetation_min': 0.2,
            'dense_vegetation_min': 0.6
        },
        'savi': {
            'min_valid': -1.0,
            'max_valid': 1.0,
            'healthy_vegetation_min': 0.2,
            'dense_vegetation_min': 0.6
        },
        'ndwi': {
            'min_valid': -1.0,
            'max_valid': 1.0,
            'water_min': 0.3,
            'wet_vegetation_min': 0.1
        }
    }

def get_confidence_scoring_weights() -> Dict[str, float]:
    """
    Get weights for confidence score calculation
    
    Returns:
        Dictionary with component weights for confidence scoring
    """
    return {
        'pixel_count_weight': 0.3,          # More pixels = higher confidence
        'vegetation_correlation_weight': 0.3, # Expected vegetation matches observed
        'data_quality_weight': 0.2,         # Cloud cover, sensor quality
        'spatial_coverage_weight': 0.2      # How much of parcel is covered by analysis
    }