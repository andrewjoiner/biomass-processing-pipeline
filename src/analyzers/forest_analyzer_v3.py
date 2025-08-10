#!/usr/bin/env python3
"""
Forest Analyzer v1 - WorldCover + FIA Analysis for Forest Biomass
Clean implementation combining ESA WorldCover land use data with FIA forest inventory
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np

from ..config.database_config_v3 import WORLDCOVER_CLASSES, FOREST_BIOMASS_TYPES
from ..config.processing_config_v3 import get_processing_config, get_confidence_scoring_weights
from ..core.database_manager_v3 import database_manager
from ..core.blob_manager_v3 import blob_manager

logger = logging.getLogger(__name__)

class ForestAnalyzer:
    """
    Forest analyzer combining WorldCover land use classification with FIA forest inventory data
    Analyzes forest coverage and biomass potential for parcels
    """
    
    def __init__(self):
        self.db_manager = database_manager
        self.blob_manager = blob_manager
        self.processing_config = get_processing_config()
        self.confidence_weights = get_confidence_scoring_weights()
        
        # Regional biomass estimates (tons per acre) when FIA data unavailable
        self.regional_biomass_estimates = {
            'deciduous_forest': 85.0,      # Tons per acre above-ground biomass
            'evergreen_forest': 120.0,     # Tons per acre above-ground biomass  
            'mixed_forest': 100.0,         # Tons per acre above-ground biomass
            'default_forest': 90.0         # Default estimate
        }
        
        # Biomass component ratios (based on FIA data patterns)
        self.biomass_ratios = {
            'bole_ratio': 0.65,           # Bole (trunk) biomass as fraction of total
            'branch_ratio': 0.20,         # Branch biomass as fraction of total
            'foliage_ratio': 0.08,        # Foliage biomass as fraction of total
            'stump_ratio': 0.07,          # Stump biomass as fraction of total
            'residue_ratio': 0.35         # Non-bole biomass for waste estimation
        }
    
    def analyze_parcel_forest(self, parcel_geometry: Dict, parcel_postgis_geometry: str,
                            parcel_acres: float, vegetation_indices: Optional[Dict] = None) -> Optional[Dict]:
        """
        Analyze forest coverage and biomass for a single parcel
        
        Args:
            parcel_geometry: GeoJSON geometry dictionary
            parcel_postgis_geometry: PostGIS geometry string for database queries
            parcel_acres: Total parcel area in acres
            vegetation_indices: Optional vegetation indices for validation
            
        Returns:
            Forest analysis dictionary or None if no forest found
        """
        try:
            # Step 1: Get forest coverage from WorldCover
            worldcover_data = self.blob_manager.get_worldcover_data_for_parcel(parcel_geometry)
            
            if not worldcover_data:
                logger.debug("No WorldCover data available for parcel")
                return None
            
            forest_area_acres = worldcover_data.get('forest_area_acres', 0.0)
            forest_percentage = worldcover_data.get('forest_percentage', 0.0)
            
            # Skip parcels with minimal forest coverage
            if forest_area_acres < 0.1 or forest_percentage < 5.0:
                logger.debug(f"Insufficient forest coverage: {forest_area_acres} acres ({forest_percentage}%)")
                return None
            
            # Step 2: Get nearby FIA plots and tree biomass data
            fia_plots = self.db_manager.get_nearby_fia_plots(
                parcel_postgis_geometry, 
                self.processing_config['fia_search_radius_degrees']
            )
            
            # Get detailed tree biomass data if FIA plots found
            fia_trees_data = None
            if fia_plots:
                plot_cns = [plot['plot_cn'] for plot in fia_plots]
                fia_trees_data = self.db_manager.get_fia_trees_for_plots(plot_cns)
            
            # Step 3: Calculate comprehensive biomass estimates
            if fia_trees_data:
                biomass_data = self._calculate_comprehensive_fia_biomass(fia_plots, fia_trees_data, forest_area_acres)
                data_source = 'WorldCover+FIA_Trees'
            elif fia_plots:
                biomass_data = self._estimate_from_fia_plots(fia_plots, forest_area_acres)
                data_source = 'WorldCover+FIA_Plots'
            else:
                biomass_data = self._estimate_regional_biomass(forest_area_acres, vegetation_indices)
                data_source = 'WorldCover+Regional'
            
            # Step 4: Create comprehensive forest record with standing + harvestable biomass
            forest_record = {
                'biomass_type': 'forest',
                'source_code': 10,  # WorldCover tree cover class
                'source_name': 'Tree_Cover',
                'area_acres': round(forest_area_acres, 3),
                'coverage_percent': round(forest_percentage, 2),
                
                # Standing biomass (total ecosystem biomass)
                'total_standing_biomass_tons': round(biomass_data.get('total_standing_biomass_tons', biomass_data.get('total_biomass_tons', 0)), 2),
                'standing_biomass_tons_per_acre': round(biomass_data.get('standing_biomass_tons_per_acre', biomass_data.get('biomass_tons_per_acre', 0)), 2),
                
                # Harvestable biomass (merchantable portions)
                'total_harvestable_biomass_tons': round(biomass_data.get('total_harvestable_biomass_tons', biomass_data.get('bole_biomass_tons', 0)), 2),  
                'harvestable_biomass_tons_per_acre': round(biomass_data.get('harvestable_biomass_tons_per_acre', 0), 2),
                
                # Forest residue biomass (tops, branches, non-merchantable)
                'forest_residue_biomass_tons': round(biomass_data.get('forest_residue_biomass_tons', biomass_data.get('residue_biomass_tons', 0)), 2),
                'residue_biomass_tons_per_acre': round(biomass_data.get('residue_biomass_tons_per_acre', 0), 2),
                
                # Tree-level data
                'tree_count_estimate': biomass_data.get('tree_count_estimate', 0),
                'average_dbh_inches': round(biomass_data.get('average_dbh_inches', 0), 1),
                'average_height_feet': round(biomass_data.get('average_height_feet', 0), 1),
                
                # Forest management data
                'stand_age_avg': biomass_data.get('stand_age_avg', 0),
                'forest_type_dominant': biomass_data.get('forest_type_dominant', 'Mixed Forest'),
                'harvest_probability': round(biomass_data.get('harvest_probability', 0.2), 2),
                'last_treatment_years': biomass_data.get('last_treatment_years', 0),
                
                # Analysis metadata
                'confidence_score': self._calculate_forest_confidence(
                    worldcover_data, fia_plots, vegetation_indices, forest_area_acres
                ),
                'data_sources': data_source,
                'fia_plot_count': len(fia_plots) if fia_plots else 0,
                'fia_tree_count': len(fia_trees_data) if fia_trees_data else 0,
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            # Add vegetation correlation if available
            if vegetation_indices:
                forest_record.update(self._assess_forest_vegetation_correlation(vegetation_indices))
            
            # Use the correct key name depending on which function provided biomass_data
            total_biomass_key = 'total_biomass_tons' if 'total_biomass_tons' in biomass_data else 'total_standing_biomass_tons'
            logger.debug(f"Forest analysis: {forest_area_acres:.2f} acres, "
                        f"{biomass_data.get(total_biomass_key, 0):.1f} tons biomass")
            
            return forest_record
            
        except Exception as e:
            logger.error(f"Error analyzing forest for parcel: {e}")
            return None
    
    def _calculate_comprehensive_fia_biomass(self, fia_plots: List[Dict], fia_trees_data: List[Dict],
                                           forest_area_acres: float) -> Dict:
        """
        Calculate comprehensive biomass estimates using actual FIA tree-level data
        Separates standing biomass, harvestable biomass, and forest residue
        
        Args:
            fia_plots: List of FIA plot records
            fia_trees_data: List of FIA tree records with DRYBIO_* measurements
            forest_area_acres: Forest area in acres
            
        Returns:
            Dictionary with comprehensive biomass estimates
        """
        if not fia_trees_data:
            return self._get_default_biomass_estimates(forest_area_acres)
        
        # Group trees by plot for distance weighting
        trees_by_plot = {}
        plot_distances = {plot['plot_cn']: plot['distance_degrees'] for plot in fia_plots}
        
        for tree in fia_trees_data:
            plt_cn = tree['plt_cn']
            if plt_cn not in trees_by_plot:
                trees_by_plot[plt_cn] = []
            trees_by_plot[plt_cn].append(tree)
        
        # Calculate distance-weighted biomass per acre
        total_weight = 0
        weighted_standing_biomass = 0
        weighted_harvestable_biomass = 0  
        weighted_residue_biomass = 0
        tree_count = 0
        total_dbh = 0
        total_height = 0
        
        for plt_cn, trees in trees_by_plot.items():
            distance = plot_distances.get(plt_cn, 1.0)
            weight = 1.0 / (distance + 0.01)  # Inverse distance weighting
            
            plot_standing_biomass = 0
            plot_harvestable_biomass = 0
            plot_residue_biomass = 0
            plot_tree_count = len(trees)
            
            for tree in trees:
                # Standing biomass: total above-ground + below-ground
                tree_standing = (tree.get('drybio_ag', 0) or 0) + (tree.get('drybio_bg', 0) or 0)
                
                # Harvestable biomass: merchantable bole + sawlog + portion of stem
                tree_harvestable = (
                    (tree.get('drybio_bole', 0) or 0) + 
                    (tree.get('drybio_sawlog', 0) or 0) +
                    ((tree.get('drybio_stem', 0) or 0) * 0.8)  # 80% of stem is harvestable
                )
                
                # Residue biomass: branches, foliage, stump + non-harvestable portions
                # Using actual database columns only
                tree_residue = (
                    (tree.get('drybio_branch', 0) or 0) +
                    (tree.get('drybio_foliage', 0) or 0) + 
                    (tree.get('drybio_stump', 0) or 0) +
                    ((tree.get('drybio_stem', 0) or 0) * 0.2)  # 20% of stem as residue
                )
                
                plot_standing_biomass += tree_standing
                plot_harvestable_biomass += tree_harvestable
                plot_residue_biomass += tree_residue
                
                # Collect tree characteristics for averages
                if tree.get('dia'):
                    total_dbh += tree['dia'] * weight
                if tree.get('ht'):
                    total_height += tree['ht'] * weight
                tree_count += weight
            
            # Weight by distance to parcel
            weighted_standing_biomass += plot_standing_biomass * weight
            weighted_harvestable_biomass += plot_harvestable_biomass * weight
            weighted_residue_biomass += plot_residue_biomass * weight
            total_weight += weight
        
        # Calculate per-acre averages
        standing_biomass_per_acre = weighted_standing_biomass / total_weight if total_weight > 0 else 0
        harvestable_biomass_per_acre = weighted_harvestable_biomass / total_weight if total_weight > 0 else 0
        residue_biomass_per_acre = weighted_residue_biomass / total_weight if total_weight > 0 else 0
        
        # Scale to parcel forest area
        total_standing_biomass_tons = standing_biomass_per_acre * forest_area_acres
        total_harvestable_biomass_tons = harvestable_biomass_per_acre * forest_area_acres
        forest_residue_biomass_tons = residue_biomass_per_acre * forest_area_acres
        
        # Calculate forest characteristics from plots
        stand_age_avg = self._calculate_weighted_stand_age([plot for plot in fia_plots if plot['plot_cn'] in trees_by_plot])
        forest_type_dominant = self._determine_dominant_forest_type([plot for plot in fia_plots if plot['plot_cn'] in trees_by_plot])
        harvest_probability = self._calculate_harvest_probability([plot for plot in fia_plots if plot['plot_cn'] in trees_by_plot])
        
        return {
            'total_standing_biomass_tons': total_standing_biomass_tons,
            'standing_biomass_tons_per_acre': standing_biomass_per_acre,
            'total_harvestable_biomass_tons': total_harvestable_biomass_tons,
            'harvestable_biomass_tons_per_acre': harvestable_biomass_per_acre,
            'forest_residue_biomass_tons': forest_residue_biomass_tons,
            'residue_biomass_tons_per_acre': residue_biomass_per_acre,
            'tree_count_estimate': int(tree_count * forest_area_acres / len(fia_plots)),
            'average_dbh_inches': total_dbh / tree_count if tree_count > 0 else 0,
            'average_height_feet': total_height / tree_count if tree_count > 0 else 0,
            'stand_age_avg': stand_age_avg,
            'forest_type_dominant': forest_type_dominant, 
            'harvest_probability': harvest_probability,
            'last_treatment_years': self._get_last_treatment_years([plot for plot in fia_plots if plot['plot_cn'] in trees_by_plot]),
            'confidence_score': self._calculate_fia_confidence_score(len(trees_by_plot), total_weight),
            'estimation_method': 'FIA_Tree_Level_Analysis'
        }
    
    def _estimate_from_fia_plots(self, fia_plots: List[Dict], forest_area_acres: float) -> Dict:
        """
        Estimate biomass from FIA plots when tree data is not available
        Uses plot-level characteristics and regional estimates
        """
        # Use regional estimates adjusted by plot characteristics
        return self._estimate_regional_biomass(forest_area_acres, None)
    
    def _calculate_fia_biomass_estimates(self, fia_plots: List[Dict], 
                                       forest_area_acres: float) -> Dict:
        """
        Calculate detailed biomass estimates from FIA plot data following FIA MPC methodology
        Uses actual DRYBIO_* measurements for comprehensive forest biomass analysis
        
        Args:
            fia_plots: List of FIA plot dictionaries with DRYBIO_* measurements
            forest_area_acres: Forest area in acres
            
        Returns:
            Dictionary with detailed FIA-based biomass estimates
        """
        if not fia_plots:
            return self._get_default_biomass_estimates(forest_area_acres)
        
        # Extract detailed biomass components from FIA plots
        plot_biomass_data = []
        stand_characteristics = []
        
        for plot in fia_plots:
            biomass = plot.get('biomass', {})
            
            # Extract all FIA biomass components (already in tons per acre from database)
            drybio_ag = biomass.get('drybio_ag', 0) or 0        # Total above-ground biomass
            drybio_bole = biomass.get('drybio_bole', 0) or 0    # Merchantable bole
            drybio_stump = biomass.get('drybio_stump', 0) or 0  # Stump
            drybio_branch = biomass.get('drybio_branch', 0) or 0 # Branches
            drybio_foliage = biomass.get('drybio_foliage', 0) or 0 # Foliage
            
            if drybio_ag > 0:  # Valid plot with biomass data
                plot_data = {
                    'drybio_ag': drybio_ag,
                    'drybio_bole': drybio_bole, 
                    'drybio_stump': drybio_stump,
                    'drybio_branch': drybio_branch,
                    'drybio_foliage': drybio_foliage,
                    'distance': plot.get('distance_degrees', 1.0)
                }
                plot_biomass_data.append(plot_data)
                
                # Extract stand characteristics for forest management analysis
                stand_data = {
                    'stand_age': plot.get('stand_age', 0) or 0,
                    'forest_type_code': plot.get('forest_type_code', 'Unknown'),
                    'treatment_code_1': plot.get('treatment_code_1', 0),
                    'treatment_code_2': plot.get('treatment_code_2', 0), 
                    'treatment_code_3': plot.get('treatment_code_3', 0),
                    'treatment_year_1': plot.get('treatment_year_1', 0),
                    'treatment_year_2': plot.get('treatment_year_2', 0),
                    'treatment_year_3': plot.get('treatment_year_3', 0),
                    'ownership_group': plot.get('ownership_group', 0),
                    'distance': plot.get('distance_degrees', 1.0)
                }
                stand_characteristics.append(stand_data)
        
        if not plot_biomass_data:
            logger.debug("No valid FIA biomass data found, using regional estimates")
            return self._get_default_biomass_estimates(forest_area_acres)
        
        # Calculate distance-weighted biomass components
        total_weight = 0
        weighted_components = {
            'drybio_ag': 0, 'drybio_bole': 0,
            'drybio_stump': 0, 'drybio_branch': 0, 'drybio_foliage': 0
        }
        
        for plot_data in plot_biomass_data:
            # Inverse distance weighting
            weight = 1.0 / (plot_data['distance'] + 0.01)
            total_weight += weight
            
            for component in weighted_components:
                weighted_components[component] += plot_data[component] * weight
        
        # Calculate weighted averages (tons per acre)
        avg_biomass_per_acre = {
            component: weighted_components[component] / total_weight
            for component in weighted_components
        }
        
        # Scale biomass components to parcel forest area
        total_biomass_tons = avg_biomass_per_acre['drybio_ag'] * forest_area_acres
        bole_biomass_tons = avg_biomass_per_acre['drybio_bole'] * forest_area_acres
        
        # Calculate residue biomass (branches + stumps - excluding foliage for harvesting)
        residue_per_acre = (avg_biomass_per_acre['drybio_branch'] + 
                           avg_biomass_per_acre['drybio_stump'])
        residue_biomass_tons = residue_per_acre * forest_area_acres
        
        # Calculate stand characteristics and forest management metrics
        stand_age_avg = self._calculate_weighted_stand_age(stand_characteristics)
        forest_type_dominant = self._determine_dominant_forest_type(stand_characteristics)
        harvest_probability = self._calculate_harvest_probability(stand_characteristics)
        last_treatment_years = self._get_last_treatment_years(stand_characteristics)
        confidence_score = self._calculate_fia_confidence_score(len(plot_biomass_data), total_weight)
        
        return {
            'total_biomass_tons': total_biomass_tons,
            'bole_biomass_tons': bole_biomass_tons,
            'residue_biomass_tons': residue_biomass_tons,
            'stand_age_avg': stand_age_avg,
            'forest_type_dominant': forest_type_dominant,
            'harvest_probability': harvest_probability,
            'last_treatment_years': last_treatment_years,
            'biomass_tons_per_acre': avg_biomass_per_acre['drybio_ag'],
            'confidence_score': confidence_score,
            'fia_plots_used': len(plot_biomass_data),
            'estimation_method': 'FIA_Plot_Interpolation'
        }
    
    def _estimate_regional_biomass(self, forest_area_acres: float, 
                                 vegetation_indices: Optional[Dict] = None) -> Dict:
        """
        Estimate comprehensive biomass using regional averages when FIA data unavailable
        Provides standing, harvestable, and residue biomass estimates
        
        Args:
            forest_area_acres: Forest area in acres
            vegetation_indices: Optional vegetation indices for estimation refinement
            
        Returns:
            Dictionary with comprehensive biomass estimates
        """
        # Use default forest biomass characteristics
        forest_type = FOREST_BIOMASS_TYPES['default_forest']
        base_standing_biomass_per_acre = forest_type['standing_biomass']
        
        # Adjust based on vegetation indices if available
        if vegetation_indices:
            ndvi = vegetation_indices.get('ndvi', np.nan)
            if not np.isnan(ndvi):
                if ndvi >= 0.7:  # Dense vegetation
                    multiplier = 1.2
                elif ndvi >= 0.5:  # Moderate vegetation
                    multiplier = 1.0
                elif ndvi >= 0.3:  # Sparse vegetation
                    multiplier = 0.8
                else:  # Very sparse vegetation
                    multiplier = 0.6
                
                base_standing_biomass_per_acre *= multiplier
        
        # Calculate comprehensive biomass components
        standing_biomass_per_acre = base_standing_biomass_per_acre
        harvestable_biomass_per_acre = standing_biomass_per_acre * forest_type['harvestable_ratio']
        residue_biomass_per_acre = standing_biomass_per_acre * forest_type['residue_ratio']
        
        # Scale to parcel forest area
        total_standing_biomass_tons = standing_biomass_per_acre * forest_area_acres
        total_harvestable_biomass_tons = harvestable_biomass_per_acre * forest_area_acres
        forest_residue_biomass_tons = residue_biomass_per_acre * forest_area_acres
        
        # Estimate tree characteristics
        avg_biomass_per_tree = 0.8  # tons (estimated average)
        tree_count_estimate = int(total_standing_biomass_tons / avg_biomass_per_tree)
        
        return {
            'total_standing_biomass_tons': total_standing_biomass_tons,
            'standing_biomass_tons_per_acre': standing_biomass_per_acre,
            'total_harvestable_biomass_tons': total_harvestable_biomass_tons,
            'harvestable_biomass_tons_per_acre': harvestable_biomass_per_acre,
            'forest_residue_biomass_tons': forest_residue_biomass_tons,
            'residue_biomass_tons_per_acre': residue_biomass_per_acre,
            'tree_count_estimate': tree_count_estimate,
            'average_dbh_inches': 12.0,  # Regional average
            'average_height_feet': 65.0,  # Regional average
            'stand_age_avg': 45,  # Regional average
            'forest_type_dominant': 'Mixed Forest',
            'harvest_probability': 0.25,  # Regional average
            'last_treatment_years': 0,
            'confidence_score': 0.4,  # Lower confidence for regional estimates
            'estimation_method': 'Regional_Average'
        }
    
    def _get_default_biomass_estimates(self, forest_area_acres: float) -> Dict:
        """Get default biomass estimates when no data is available"""
        return self._estimate_regional_biomass(forest_area_acres)
    
    def _calculate_forest_confidence(self, worldcover_data: Dict, fia_plots: List[Dict],
                                   vegetation_indices: Optional[Dict], forest_area_acres: float) -> float:
        """
        Calculate confidence score for forest analysis
        
        Args:
            worldcover_data: WorldCover analysis results
            fia_plots: List of nearby FIA plots
            vegetation_indices: Optional vegetation indices
            forest_area_acres: Forest area in acres
            
        Returns:
            Confidence score between 0 and 1
        """
        confidence_factors = []
        
        # Factor 1: Forest area size (larger areas = higher confidence)
        if forest_area_acres >= 5.0:
            confidence_factors.append(0.9)
        elif forest_area_acres >= 1.0:
            confidence_factors.append(0.8)
        elif forest_area_acres >= 0.5:
            confidence_factors.append(0.7)
        else:
            confidence_factors.append(0.6)
        
        # Factor 2: WorldCover pixel count
        pixel_count = worldcover_data.get('total_pixels', 0)
        if pixel_count >= 100:
            confidence_factors.append(0.9)
        elif pixel_count >= 50:
            confidence_factors.append(0.8)
        elif pixel_count >= 20:
            confidence_factors.append(0.7)
        else:
            confidence_factors.append(0.6)
        
        # Factor 3: FIA plot availability
        if len(fia_plots) >= 5:
            confidence_factors.append(0.9)
        elif len(fia_plots) >= 2:
            confidence_factors.append(0.8)
        elif len(fia_plots) >= 1:
            confidence_factors.append(0.7)
        else:
            confidence_factors.append(0.5)  # Regional estimates only
        
        # Factor 4: Vegetation indices correlation
        if vegetation_indices:
            ndvi = vegetation_indices.get('ndvi', np.nan)
            if not np.isnan(ndvi):
                if 0.5 <= ndvi <= 0.9:  # Expected range for forests
                    confidence_factors.append(0.9)
                elif 0.3 <= ndvi <= 0.95:  # Acceptable range
                    confidence_factors.append(0.7)
                else:
                    confidence_factors.append(0.5)  # Unusual NDVI for forest
            else:
                confidence_factors.append(0.6)  # No vegetation data
        else:
            confidence_factors.append(0.6)
        
        return float(np.mean(confidence_factors))
    
    def _assess_forest_vegetation_correlation(self, vegetation_indices: Dict) -> Dict:
        """
        Assess correlation between forest classification and vegetation indices
        
        Args:
            vegetation_indices: Calculated vegetation indices
            
        Returns:
            Dictionary with correlation assessment
        """
        ndvi = vegetation_indices.get('ndvi', np.nan)
        evi = vegetation_indices.get('evi', np.nan)
        
        correlation_assessment = {
            'expected_ndvi_range': (0.5, 0.9),
            'expected_evi_range': (0.3, 0.7),
            'observed_ndvi': ndvi,
            'observed_evi': evi
        }
        
        # Assess NDVI correlation
        if not np.isnan(ndvi):
            if 0.6 <= ndvi <= 0.85:  # Optimal forest NDVI
                ndvi_correlation = 'excellent'
                ndvi_confidence = 0.95
            elif 0.5 <= ndvi <= 0.9:   # Good forest NDVI
                ndvi_correlation = 'good'
                ndvi_confidence = 0.85
            elif 0.3 <= ndvi <= 0.95:  # Acceptable range
                ndvi_correlation = 'acceptable'
                ndvi_confidence = 0.7
            else:
                ndvi_correlation = 'poor'
                ndvi_confidence = 0.4
        else:
            ndvi_correlation = 'no_data'
            ndvi_confidence = 0.5
        
        correlation_assessment.update({
            'ndvi_correlation': ndvi_correlation,
            'ndvi_confidence': ndvi_confidence,
            'overall_vegetation_correlation': ndvi_confidence  # Simplified to NDVI for now
        })
        
        return correlation_assessment
    
    def get_forest_summary(self, forest_record: Dict) -> str:
        """
        Generate human-readable summary of forest analysis
        
        Args:
            forest_record: Forest analysis record
            
        Returns:
            Human-readable summary string
        """
        area_acres = forest_record['area_acres']
        total_biomass = forest_record['total_biomass_tons']
        residue_biomass = forest_record['residue_biomass_tons']
        confidence = forest_record['confidence_score']
        data_sources = forest_record['data_sources']
        
        return (f"Forest: {area_acres:.1f} acres, {total_biomass:.1f} tons total biomass "
               f"({residue_biomass:.1f} tons waste biomass), {confidence:.1%} confidence "
               f"({data_sources})")
    
    def validate_forest_analysis(self, forest_record: Dict) -> Dict:
        """
        Validate forest analysis results
        
        Args:
            forest_record: Forest analysis record
            
        Returns:
            Validation results dictionary
        """
        validation = {
            'valid': True,
            'warnings': [],
            'errors': []
        }
        
        # Check biomass reasonableness
        biomass_per_acre = forest_record['biomass_tons_per_acre']
        if biomass_per_acre < 10:
            validation['warnings'].append(f"Low biomass density: {biomass_per_acre:.1f} tons/acre")
        elif biomass_per_acre > 200:
            validation['warnings'].append(f"High biomass density: {biomass_per_acre:.1f} tons/acre")
        
        # Check confidence score
        confidence = forest_record['confidence_score']
        if confidence < 0.5:
            validation['warnings'].append(f"Low confidence score: {confidence:.2f}")
        
        # Check area reasonableness
        area_acres = forest_record['area_acres']
        if area_acres < 0.1:
            validation['warnings'].append(f"Very small forest area: {area_acres:.3f} acres")
        
        return validation
    
    def _calculate_weighted_stand_age(self, stand_characteristics: List[Dict]) -> float:
        """Calculate distance-weighted average stand age from FIA plots"""
        if not stand_characteristics:
            return 0.0
        
        total_weight = 0
        weighted_age_sum = 0
        
        for stand in stand_characteristics:
            age = stand.get('stand_age', 0)
            distance = stand.get('distance', 1.0)
            weight = 1.0 / (distance + 0.01)
            
            if age > 0:
                weighted_age_sum += age * weight
                total_weight += weight
        
        return weighted_age_sum / total_weight if total_weight > 0 else 0.0
    
    def _determine_dominant_forest_type(self, stand_characteristics: List[Dict]) -> str:
        """Determine dominant forest type from FIA plot data"""
        if not stand_characteristics:
            return 'Unknown'
        
        # Count forest types weighted by inverse distance
        type_weights = {}
        for stand in stand_characteristics:
            forest_type = stand.get('forest_type_code', 'Unknown')
            distance = stand.get('distance', 1.0)
            weight = 1.0 / (distance + 0.01)
            
            if forest_type not in type_weights:
                type_weights[forest_type] = 0
            type_weights[forest_type] += weight
        
        if not type_weights:
            return 'Unknown'
        
        # Return most weighted forest type
        dominant_type_code = max(type_weights.items(), key=lambda x: x[1])[0]
        
        # Convert FIA forest type codes to readable names (common types)
        forest_type_names = {
            'Unknown': 'Mixed Forest',
            100: 'White/Red/Jack Pine Group',
            200: 'Spruce/Fir Group', 
            300: 'Longleaf/Slash Pine Group',
            400: 'Loblolly/Shortleaf Pine Group',
            500: 'Oak/Pine Group',
            600: 'Oak/Hickory Group',
            700: 'Oak/Gum/Cypress Group',
            800: 'Elm/Ash/Cottonwood Group',
            900: 'Maple/Beech/Birch Group'
        }
        
        return forest_type_names.get(dominant_type_code, f'Forest Type {dominant_type_code}')
    
    def _calculate_harvest_probability(self, stand_characteristics: List[Dict]) -> float:
        """Calculate harvest probability based on FIA treatment codes and ownership"""
        if not stand_characteristics:
            return 0.0
        
        total_weight = 0
        weighted_probability_sum = 0
        
        for stand in stand_characteristics:
            distance = stand.get('distance', 1.0)
            weight = 1.0 / (distance + 0.01)
            
            # Base probability factors
            probability = 0.1  # Base 10% probability
            
            # Adjust based on ownership (private more likely to harvest)
            ownership = stand.get('ownership_group', 0)
            if ownership in [40, 41, 42, 43, 44, 45]:  # Private ownership codes
                probability += 0.2
            elif ownership in [10, 11, 12]:  # National Forest
                probability += 0.05
            
            # Adjust based on recent treatments (indicates active management)
            recent_treatments = [
                stand.get('treatment_year_1', 0),
                stand.get('treatment_year_2', 0),
                stand.get('treatment_year_3', 0)
            ]
            
            current_year = 2024  # Update this as needed
            for treatment_year in recent_treatments:
                if treatment_year and treatment_year > 0:
                    years_since = current_year - treatment_year
                    if years_since < 10:  # Recent treatment
                        probability += 0.15
                    elif years_since < 20:  # Moderate treatment
                        probability += 0.05
            
            # Cap probability at 0.8 (80%)
            probability = min(probability, 0.8)
            
            weighted_probability_sum += probability * weight
            total_weight += weight
        
        return weighted_probability_sum / total_weight if total_weight > 0 else 0.1
    
    def _get_last_treatment_years(self, stand_characteristics: List[Dict]) -> int:
        """Get years since last treatment from FIA data"""
        if not stand_characteristics:
            return 0
        
        current_year = 2024  # Update this as needed
        most_recent_treatment = 0
        
        for stand in stand_characteristics:
            treatment_years = [
                stand.get('treatment_year_1', 0) or 0,
                stand.get('treatment_year_2', 0) or 0, 
                stand.get('treatment_year_3', 0) or 0
            ]
            
            for year in treatment_years:
                if year > most_recent_treatment:
                    most_recent_treatment = year
        
        if most_recent_treatment > 0:
            return current_year - most_recent_treatment
        else:
            return 0  # No treatment recorded
    
    def _calculate_fia_confidence_score(self, plot_count: int, total_weight: float) -> float:
        """Calculate confidence score based on FIA plot density and proximity"""
        if plot_count == 0:
            return 0.3  # Low confidence with no FIA data
        
        # Base confidence increases with more plots
        base_confidence = min(0.7, 0.4 + (plot_count * 0.1))
        
        # Adjust based on average distance (higher weight = closer plots)
        avg_weight = total_weight / plot_count if plot_count > 0 else 0
        if avg_weight > 10:  # Very close plots
            base_confidence += 0.2
        elif avg_weight > 5:  # Moderately close plots
            base_confidence += 0.1
        
        return min(base_confidence, 0.95)  # Cap at 95% confidence


# Global forest analyzer instance  
forest_analyzer = ForestAnalyzer()