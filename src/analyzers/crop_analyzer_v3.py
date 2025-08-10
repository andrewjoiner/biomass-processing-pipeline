#!/usr/bin/env python3
"""
Crop Analyzer v1 - CDL Spatial Analysis for Crop Biomass
Clean implementation of crop type analysis using USDA CDL polygon data
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np

from ..config.database_config_v3 import CDL_CODES, URBAN_CODES, CROP_BIOMASS_DATA
from ..config.processing_config_v3 import get_confidence_scoring_weights
from ..core.database_manager_v3 import database_manager

logger = logging.getLogger(__name__)

class CropAnalyzer:
    """
    Crop analyzer using USDA CDL (Cropland Data Layer) polygon intersections
    Analyzes crop types and coverage within parcels for biomass estimation
    """
    
    def __init__(self):
        self.db_manager = database_manager
        self.confidence_weights = get_confidence_scoring_weights()
        
        # Crop categories for biomass analysis
        self.crop_categories = {
            'major_grains': [1, 5, 21, 22, 23, 24, 27, 28],  # Corn, soybeans, wheat, etc.
            'specialty_crops': [41, 42, 43, 44, 45, 46, 47, 48, 49, 50],  # Vegetables, fruits
            'forage_crops': [36, 37, 58, 59, 60],  # Alfalfa, hay, grass seed
            'other_crops': [2, 3, 4, 6, 10, 11, 12, 13, 14, 31, 32, 33, 34, 35, 38, 39, 51, 52, 53]
        }
    
    def analyze_parcel_crops(self, parcel_postgis_geometry: str, 
                           vegetation_indices: Optional[Dict] = None) -> Optional[List[Dict]]:
        """
        Analyze crop composition for a single parcel
        
        Args:
            parcel_postgis_geometry: PostGIS geometry string
            vegetation_indices: Optional vegetation indices for validation
            
        Returns:
            List of crop analysis dictionaries or None if failed
        """
        try:
            # Get CDL intersections for parcel
            intersections = self.db_manager.get_cdl_intersections_single(parcel_postgis_geometry)
            
            if not intersections:
                logger.debug("No CDL intersections found for parcel")
                return None
            
            # Filter out urban/non-agricultural codes
            agricultural_intersections = [
                intersection for intersection in intersections 
                if intersection['crop_code'] not in URBAN_CODES
            ]
            
            if not agricultural_intersections:
                logger.debug("No agricultural crops found for parcel")
                return None
            
            # Process each crop intersection
            crop_records = []
            total_agricultural_area = sum(
                intersection['intersection_area_m2'] 
                for intersection in agricultural_intersections
            )
            
            for intersection in agricultural_intersections:
                crop_record = self._create_crop_record(
                    intersection, 
                    total_agricultural_area,
                    vegetation_indices
                )
                
                if crop_record:
                    crop_records.append(crop_record)
            
            # Sort by coverage area (largest first)
            crop_records.sort(key=lambda x: x['area_acres'], reverse=True)
            
            logger.debug(f"Found {len(crop_records)} crop types for parcel")
            return crop_records
            
        except Exception as e:
            logger.error(f"Error analyzing crops for parcel: {e}")
            return None
    
    def analyze_county_crops_bulk(self, fips_state: str, fips_county: str,
                                 parcel_list: Optional[List[Dict]] = None,
                                 vegetation_data: Optional[Dict] = None) -> Dict[str, List[Dict]]:
        """
        Bulk analyze crops for entire county (OPTIMIZATION)
        
        Args:
            fips_state: 2-digit state FIPS code
            fips_county: 3-digit county FIPS code
            vegetation_data: Optional dict mapping parcel_id to vegetation indices
            
        Returns:
            Dictionary mapping parcel_id to list of crop records
        """
        try:
            # Get bulk CDL intersections for county (pass parcel list to avoid re-querying)
            intersections_by_parcel = self.db_manager.get_cdl_intersections_bulk(
                fips_state, fips_county, parcel_list
            )
            
            crop_analysis_by_parcel = {}
            
            for parcel_id, intersections in intersections_by_parcel.items():
                # Filter agricultural intersections
                agricultural_intersections = [
                    intersection for intersection in intersections 
                    if intersection['crop_code'] not in URBAN_CODES
                ]
                
                if not agricultural_intersections:
                    continue
                
                # Get vegetation data for this parcel if available
                parcel_vegetation = vegetation_data.get(parcel_id) if vegetation_data else None
                
                # Process crop intersections
                crop_records = []
                total_agricultural_area = sum(
                    intersection['intersection_area_m2'] 
                    for intersection in agricultural_intersections
                )
                
                for intersection in agricultural_intersections:
                    crop_record = self._create_crop_record(
                        intersection,
                        total_agricultural_area,
                        parcel_vegetation
                    )
                    
                    if crop_record:
                        crop_records.append(crop_record)
                
                if crop_records:
                    # Sort by area (largest first)
                    crop_records.sort(key=lambda x: x['area_acres'], reverse=True)
                    crop_analysis_by_parcel[parcel_id] = crop_records
            
            logger.info(f"Bulk analyzed crops for {len(crop_analysis_by_parcel)} parcels")
            return crop_analysis_by_parcel
            
        except Exception as e:
            logger.error(f"Error in bulk crop analysis: {e}")
            return {}
    
    def _create_crop_record(self, intersection: Dict, total_agricultural_area: float,
                          vegetation_indices: Optional[Dict] = None) -> Optional[Dict]:
        """
        Create a crop analysis record from CDL intersection data
        
        Args:
            intersection: CDL intersection data
            total_agricultural_area: Total agricultural area in parcel
            vegetation_indices: Optional vegetation indices for validation
            
        Returns:
            Crop record dictionary or None
        """
        try:
            crop_code = intersection['crop_code']
            crop_name = intersection['crop_name']
            area_m2 = intersection['intersection_area_m2']
            area_acres = area_m2 * 0.000247105  # m² to acres
            coverage_percent = intersection['coverage_percent']
            
            # Skip very small intersections (< 0.01 acres)
            if area_acres < 0.01:
                return None
            
            # Get comprehensive crop biomass data
            crop_data = CROP_BIOMASS_DATA.get(crop_code, CROP_BIOMASS_DATA['default'])
            
            # Calculate crop yield (total production)
            yield_tons_per_acre = crop_data['yield_tons_per_acre']
            total_yield_tons = area_acres * yield_tons_per_acre
            
            # Calculate crop residue biomass
            residue_ratio = crop_data['residue_ratio']
            total_residue_tons_wet = total_yield_tons * residue_ratio
            moisture_content = crop_data['moisture']
            total_residue_tons_dry = total_residue_tons_wet * (1 - moisture_content)
            
            # Calculate harvestable residue (accounts for field accessibility and collection efficiency)
            harvestable_residue_percent = crop_data['harvestable_residue']
            harvestable_residue_tons = total_residue_tons_dry * harvestable_residue_percent
            
            # Determine crop category
            crop_category = self._get_crop_category(crop_code)
            
            # Calculate confidence score
            confidence_score = self._calculate_crop_confidence(
                intersection, vegetation_indices, area_acres
            )
            
            # Create comprehensive crop record
            crop_record = {
                'biomass_type': 'crop',
                'source_code': crop_code,
                'source_name': crop_name,
                'crop_category': crop_category,
                'area_acres': round(area_acres, 3),
                'coverage_percent': round(coverage_percent, 2),
                
                # Crop production (total yield)
                'yield_tons': round(total_yield_tons, 2),
                'yield_tons_per_acre': round(yield_tons_per_acre, 2),
                
                # Crop residue biomass (wet)
                'residue_tons_wet': round(total_residue_tons_wet, 2),
                'residue_ratio': round(residue_ratio, 2),
                
                # Crop residue biomass (dry)
                'residue_tons_dry': round(total_residue_tons_dry, 2),
                'moisture_content': round(moisture_content, 3),
                
                # Harvestable residue (collectible biomass)
                'harvestable_residue_tons': round(harvestable_residue_tons, 2),
                'harvestable_residue_percent': round(harvestable_residue_percent, 2),
                
                # Analysis metadata
                'confidence_score': round(confidence_score, 3),
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            # Add vegetation correlation if available
            if vegetation_indices:
                crop_record.update(self._assess_vegetation_correlation(
                    crop_code, vegetation_indices
                ))
            
            return crop_record
            
        except Exception as e:
            logger.warning(f"Failed to create crop record: {e}")
            return None
    
    def _get_crop_category(self, crop_code: int) -> str:
        """
        Determine crop category for biomass analysis
        
        Args:
            crop_code: CDL crop code
            
        Returns:
            Crop category string
        """
        for category, codes in self.crop_categories.items():
            if crop_code in codes:
                return category
        return 'other_crops'
    
    def _calculate_crop_confidence(self, intersection: Dict, 
                                 vegetation_indices: Optional[Dict],
                                 area_acres: float) -> float:
        """
        Calculate confidence score for crop analysis
        
        Args:
            intersection: CDL intersection data
            vegetation_indices: Optional vegetation indices
            area_acres: Area in acres
            
        Returns:
            Confidence score between 0 and 1
        """
        confidence_factors = []
        
        # Factor 1: Area coverage (larger areas = higher confidence)
        if area_acres >= 1.0:
            confidence_factors.append(0.9)
        elif area_acres >= 0.5:
            confidence_factors.append(0.8)
        elif area_acres >= 0.1:
            confidence_factors.append(0.7)
        else:
            confidence_factors.append(0.5)
        
        # Factor 2: Coverage percentage (higher percentage = higher confidence)
        coverage_percent = intersection['coverage_percent']
        if coverage_percent >= 80:
            confidence_factors.append(0.9)
        elif coverage_percent >= 50:
            confidence_factors.append(0.8)
        elif coverage_percent >= 20:
            confidence_factors.append(0.7)
        else:
            confidence_factors.append(0.6)
        
        # Factor 3: Vegetation correlation (if available)
        if vegetation_indices:
            veg_correlation = self._assess_vegetation_correlation(
                intersection['crop_code'], vegetation_indices
            )
            confidence_factors.append(veg_correlation.get('correlation_confidence', 0.7))
        
        # Factor 4: Crop type reliability (some crops are more reliably detected)
        crop_code = intersection['crop_code']
        if crop_code in [1, 5, 24, 36]:  # Major crops with good CDL accuracy
            confidence_factors.append(0.9)
        elif crop_code in self.crop_categories['major_grains']:
            confidence_factors.append(0.8)
        else:
            confidence_factors.append(0.7)
        
        return float(np.mean(confidence_factors))
    
    def _assess_vegetation_correlation(self, crop_code: int, 
                                     vegetation_indices: Dict) -> Dict:
        """
        Assess correlation between crop type and observed vegetation indices
        
        Args:
            crop_code: CDL crop code
            vegetation_indices: Calculated vegetation indices
            
        Returns:
            Dictionary with correlation assessment
        """
        ndvi = vegetation_indices.get('ndvi', np.nan)
        
        # Expected NDVI ranges for different crop types (simplified)
        expected_ndvi_ranges = {
            1: (0.4, 0.8),   # Corn - moderate to high NDVI
            5: (0.3, 0.7),   # Soybeans - moderate NDVI
            24: (0.2, 0.6),  # Winter wheat - low to moderate NDVI
            36: (0.5, 0.9),  # Alfalfa - high NDVI
            61: (0.1, 0.3),  # Fallow - low NDVI
        }
        
        correlation_assessment = {
            'expected_ndvi_range': expected_ndvi_ranges.get(crop_code, (0.2, 0.8)),
            'observed_ndvi': ndvi,
            'correlation_confidence': 0.7  # Default
        }
        
        if not np.isnan(ndvi):
            expected_min, expected_max = expected_ndvi_ranges.get(crop_code, (0.2, 0.8))
            
            if expected_min <= ndvi <= expected_max:
                correlation_assessment['correlation_status'] = 'good'
                correlation_assessment['correlation_confidence'] = 0.9
            elif abs(ndvi - (expected_min + expected_max) / 2) < 0.2:
                correlation_assessment['correlation_status'] = 'acceptable'
                correlation_assessment['correlation_confidence'] = 0.7
            else:
                correlation_assessment['correlation_status'] = 'poor'
                correlation_assessment['correlation_confidence'] = 0.4
        else:
            correlation_assessment['correlation_status'] = 'no_data'
            correlation_assessment['correlation_confidence'] = 0.5
        
        return correlation_assessment
    
    def get_crop_summary(self, crop_records: List[Dict]) -> Dict:
        """
        Generate summary statistics for crop analysis
        
        Args:
            crop_records: List of crop record dictionaries
            
        Returns:
            Summary statistics dictionary
        """
        if not crop_records:
            return {
                'total_crops': 0,
                'total_agricultural_acres': 0.0,
                'total_biomass_tons': 0.0,
                'dominant_crop': None
            }
        
        total_acres = sum(record['area_acres'] for record in crop_records)
        total_yield_tons = sum(record['yield_tons'] for record in crop_records)
        total_residue_wet_tons = sum(record['residue_tons_wet'] for record in crop_records)
        total_residue_dry_tons = sum(record['residue_tons_dry'] for record in crop_records)
        total_harvestable_residue_tons = sum(record['harvestable_residue_tons'] for record in crop_records)
        
        # Find dominant crop (largest area)
        dominant_crop = max(crop_records, key=lambda x: x['area_acres'])
        
        # Group by category
        category_stats = {}
        for record in crop_records:
            category = record['crop_category']
            if category not in category_stats:
                category_stats[category] = {
                    'acres': 0.0,
                    'yield_tons': 0.0,
                    'residue_dry_tons': 0.0,
                    'harvestable_residue_tons': 0.0,
                    'crop_count': 0
                }
            category_stats[category]['acres'] += record['area_acres']
            category_stats[category]['yield_tons'] += record['yield_tons']
            category_stats[category]['residue_dry_tons'] += record['residue_tons_dry']
            category_stats[category]['harvestable_residue_tons'] += record['harvestable_residue_tons']
            category_stats[category]['crop_count'] += 1
        
        return {
            'total_crops': len(crop_records),
            'total_agricultural_acres': round(total_acres, 2),
            
            # Crop production totals
            'total_yield_tons': round(total_yield_tons, 2),
            'average_yield_per_acre': round(total_yield_tons / total_acres, 2) if total_acres > 0 else 0,
            
            # Crop residue totals
            'total_residue_wet_tons': round(total_residue_wet_tons, 2),
            'total_residue_dry_tons': round(total_residue_dry_tons, 2),
            'total_harvestable_residue_tons': round(total_harvestable_residue_tons, 2),
            'average_residue_per_acre': round(total_residue_dry_tons / total_acres, 2) if total_acres > 0 else 0,
            
            # Dominant crop
            'dominant_crop': {
                'name': dominant_crop['source_name'],
                'code': dominant_crop['source_code'],
                'acres': dominant_crop['area_acres'],
                'coverage_percent': dominant_crop['coverage_percent'],
                'yield_tons': dominant_crop['yield_tons'],
                'harvestable_residue_tons': dominant_crop['harvestable_residue_tons']
            },
            'category_breakdown': category_stats
        }
    
    def validate_crop_analysis(self, crop_records: List[Dict]) -> Dict:
        """
        Validate crop analysis results
        
        Args:
            crop_records: List of crop record dictionaries
            
        Returns:
            Validation results dictionary
        """
        validation = {
            'valid': True,
            'warnings': [],
            'errors': []
        }
        
        if not crop_records:
            validation['warnings'].append("No crops detected in parcel")
            return validation
        
        # Check for reasonable total coverage
        total_coverage = sum(record['coverage_percent'] for record in crop_records)
        if total_coverage > 105:  # Allow 5% overlap tolerance
            validation['warnings'].append(f"Total crop coverage {total_coverage:.1f}% exceeds 100%")
        
        # Check confidence scores
        low_confidence_crops = [
            record for record in crop_records 
            if record['confidence_score'] < 0.5
        ]
        if low_confidence_crops:
            validation['warnings'].append(f"{len(low_confidence_crops)} crops have low confidence scores")
        
        # Check for very small areas
        tiny_crops = [record for record in crop_records if record['area_acres'] < 0.05]
        if len(tiny_crops) > len(crop_records) / 2:
            validation['warnings'].append("Many very small crop areas detected - may indicate fragmentation")
        
        return validation


# Global crop analyzer instance
crop_analyzer = CropAnalyzer()