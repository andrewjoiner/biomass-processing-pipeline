#!/usr/bin/env python3
"""
Vegetation Analyzer v1 - Sentinel-2 Processing for Vegetation Indices
Clean implementation of NDVI, EVI, SAVI, NDWI calculations from satellite imagery
"""

import logging
from datetime import datetime
from typing import Dict, Optional

import numpy as np

from ..config.processing_config_v3 import get_vegetation_index_thresholds
from ..core.blob_manager_v3 import blob_manager

logger = logging.getLogger(__name__)

class VegetationAnalyzer:
    """
    Vegetation index analyzer using Sentinel-2 satellite imagery
    Calculates NDVI, EVI, SAVI, and NDWI for biomass analysis
    """
    
    def __init__(self):
        self.thresholds = get_vegetation_index_thresholds()
        self.blob_manager = blob_manager
    
    def analyze_parcel_vegetation(self, parcel_geometry: Dict) -> Optional[Dict]:
        """
        Analyze vegetation indices for a single parcel
        
        Args:
            parcel_geometry: GeoJSON geometry dictionary
            
        Returns:
            Dictionary with vegetation indices and metadata or None if failed
        """
        try:
            # Get Sentinel-2 data for parcel
            sentinel2_data = self.blob_manager.get_sentinel2_data_for_parcel(parcel_geometry)
            
            if not sentinel2_data:
                logger.debug("No Sentinel-2 data available for parcel")
                return None
            
            # Extract band data
            bands = sentinel2_data['bands']
            
            # Validate all required bands are present
            required_bands = ['B02', 'B03', 'B04', 'B08']  # Blue, Green, Red, NIR
            if not all(band in bands for band in required_bands):
                logger.warning(f"Missing required bands. Available: {list(bands.keys())}")
                return None
            
            # Extract band arrays
            blue = bands['B02']['data'].astype(np.float32)
            green = bands['B03']['data'].astype(np.float32)
            red = bands['B04']['data'].astype(np.float32)
            nir = bands['B08']['data'].astype(np.float32)
            
            # Validate band data
            if not self._validate_band_data(blue, green, red, nir):
                logger.warning("Invalid band data - empty or all nodata values")
                return None
            
            # Calculate vegetation indices
            vegetation_indices = self._calculate_vegetation_indices(blue, green, red, nir)
            
            # Add metadata
            vegetation_indices.update({
                'tile_id': sentinel2_data['tile_id'],
                'acquisition_date': sentinel2_data['acquisition_date'],
                'pixel_count': int(np.sum(~np.isnan(vegetation_indices['ndvi']))),
                'analysis_timestamp': datetime.now().isoformat()
            })
            
            # Calculate confidence score
            vegetation_indices['confidence_score'] = self._calculate_confidence_score(vegetation_indices)
            
            logger.debug(f"Calculated vegetation indices for {vegetation_indices['pixel_count']} pixels")
            return vegetation_indices
            
        except Exception as e:
            logger.error(f"Error analyzing vegetation for parcel: {e}")
            return None
    
    def _validate_band_data(self, blue: np.ndarray, green: np.ndarray, 
                           red: np.ndarray, nir: np.ndarray) -> bool:
        """
        Validate that band data is usable for vegetation analysis
        
        Args:
            blue, green, red, nir: Band arrays
            
        Returns:
            True if data is valid for analysis
        """
        # Check if arrays have same shape
        shapes = [arr.shape for arr in [blue, green, red, nir]]
        if not all(shape == shapes[0] for shape in shapes):
            return False
        
        # Check if arrays have any valid (non-nan, non-zero) data
        for band_name, band_data in [('blue', blue), ('green', green), ('red', red), ('nir', nir)]:
            valid_pixels = np.isfinite(band_data) & (band_data > 0)
            if np.sum(valid_pixels) == 0:
                logger.debug(f"No valid pixels in {band_name} band")
                return False
        
        return True
    
    def _calculate_vegetation_indices(self, blue: np.ndarray, green: np.ndarray,
                                    red: np.ndarray, nir: np.ndarray) -> Dict:
        """
        Calculate all vegetation indices from Sentinel-2 bands
        
        Args:
            blue, green, red, nir: Sentinel-2 band arrays
            
        Returns:
            Dictionary with calculated vegetation indices
        """
        # Create mask for valid pixels (non-zero, finite values)
        valid_mask = (
            np.isfinite(blue) & np.isfinite(green) & 
            np.isfinite(red) & np.isfinite(nir) &
            (blue > 0) & (green > 0) & (red > 0) & (nir > 0)
        )
        
        # Initialize arrays with NaN
        ndvi = np.full_like(red, np.nan, dtype=np.float32)
        evi = np.full_like(red, np.nan, dtype=np.float32)
        savi = np.full_like(red, np.nan, dtype=np.float32)
        ndwi = np.full_like(red, np.nan, dtype=np.float32)
        
        # Calculate indices only for valid pixels
        if np.any(valid_mask):
            # NDVI: (NIR - Red) / (NIR + Red)
            ndvi_denom = nir + red
            ndvi_valid = valid_mask & (ndvi_denom != 0)
            ndvi[ndvi_valid] = (nir[ndvi_valid] - red[ndvi_valid]) / ndvi_denom[ndvi_valid]
            
            # EVI: 2.5 * (NIR - Red) / (NIR + 6*Red - 7.5*Blue + 1)
            evi_denom = nir + 6*red - 7.5*blue + 1
            evi_valid = valid_mask & (evi_denom != 0)
            evi[evi_valid] = 2.5 * (nir[evi_valid] - red[evi_valid]) / evi_denom[evi_valid]
            
            # SAVI: 1.5 * (NIR - Red) / (NIR + Red + 0.5)
            savi_denom = nir + red + 0.5
            savi_valid = valid_mask & (savi_denom != 0)
            savi[savi_valid] = 1.5 * (nir[savi_valid] - red[savi_valid]) / savi_denom[savi_valid]
            
            # NDWI: (Green - NIR) / (Green + NIR)
            ndwi_denom = green + nir
            ndwi_valid = valid_mask & (ndwi_denom != 0)
            ndwi[ndwi_valid] = (green[ndwi_valid] - nir[ndwi_valid]) / ndwi_denom[ndwi_valid]
        
        # Calculate mean values (ignoring NaN)
        return {
            'ndvi': float(np.nanmean(ndvi)) if np.any(~np.isnan(ndvi)) else np.nan,
            'evi': float(np.nanmean(evi)) if np.any(~np.isnan(evi)) else np.nan,
            'savi': float(np.nanmean(savi)) if np.any(~np.isnan(savi)) else np.nan,
            'ndwi': float(np.nanmean(ndwi)) if np.any(~np.isnan(ndwi)) else np.nan,
            'ndvi_std': float(np.nanstd(ndvi)) if np.any(~np.isnan(ndvi)) else np.nan,
            'evi_std': float(np.nanstd(evi)) if np.any(~np.isnan(evi)) else np.nan,
            'savi_std': float(np.nanstd(savi)) if np.any(~np.isnan(savi)) else np.nan,
            'ndwi_std': float(np.nanstd(ndwi)) if np.any(~np.isnan(ndwi)) else np.nan
        }
    
    def _calculate_confidence_score(self, vegetation_indices: Dict) -> float:
        """
        Calculate confidence score for vegetation analysis
        
        Args:
            vegetation_indices: Dictionary with calculated indices
            
        Returns:
            Confidence score between 0 and 1
        """
        confidence_factors = []
        
        # Factor 1: Pixel count (more pixels = higher confidence)
        pixel_count = vegetation_indices.get('pixel_count', 0)
        if pixel_count > 0:
            # Normalize pixel count (assume 100+ pixels is good coverage)
            pixel_confidence = min(pixel_count / 100.0, 1.0)
            confidence_factors.append(pixel_confidence)
        
        # Factor 2: NDVI validity (reasonable range)
        ndvi = vegetation_indices.get('ndvi')
        if not np.isnan(ndvi):
            ndvi_thresholds = self.thresholds['ndvi']
            if ndvi_thresholds['min_valid'] <= ndvi <= ndvi_thresholds['max_valid']:
                # Higher confidence for vegetation-like NDVI values
                if ndvi >= ndvi_thresholds['healthy_vegetation_min']:
                    confidence_factors.append(0.9)
                elif ndvi >= 0:
                    confidence_factors.append(0.7)
                else:
                    confidence_factors.append(0.5)  # Negative NDVI (water, soil)
            else:
                confidence_factors.append(0.3)  # Out of valid range
        
        # Factor 3: Standard deviation (lower std = more uniform, higher confidence)
        ndvi_std = vegetation_indices.get('ndvi_std')
        if not np.isnan(ndvi_std):
            # Normalize std (assume 0.1 is reasonable variation)
            std_confidence = max(0.3, 1.0 - min(ndvi_std / 0.1, 1.0))
            confidence_factors.append(std_confidence)
        
        # Factor 4: Index consistency (all indices should be reasonable)
        index_values = [
            vegetation_indices.get('ndvi'),
            vegetation_indices.get('evi'),
            vegetation_indices.get('savi'),
            vegetation_indices.get('ndwi')
        ]
        
        valid_indices = [v for v in index_values if not np.isnan(v)]
        if len(valid_indices) >= 3:  # At least 3 indices calculated
            confidence_factors.append(0.8)
        elif len(valid_indices) >= 2:
            confidence_factors.append(0.6)
        else:
            confidence_factors.append(0.3)
        
        # Calculate overall confidence as weighted mean
        if confidence_factors:
            return float(np.mean(confidence_factors))
        else:
            return 0.0
    
    def validate_vegetation_indices(self, vegetation_indices: Dict) -> Dict:
        """
        Validate vegetation indices against expected ranges
        
        Args:
            vegetation_indices: Dictionary with calculated indices
            
        Returns:
            Dictionary with validation results
        """
        validation = {
            'valid': True,
            'warnings': [],
            'errors': []
        }
        
        # Validate NDVI
        ndvi = vegetation_indices.get('ndvi')
        if not np.isnan(ndvi):
            ndvi_thresholds = self.thresholds['ndvi']
            if not (ndvi_thresholds['min_valid'] <= ndvi <= ndvi_thresholds['max_valid']):
                validation['errors'].append(f"NDVI {ndvi:.3f} outside valid range")
                validation['valid'] = False
            elif ndvi < -0.5:
                validation['warnings'].append(f"Very low NDVI {ndvi:.3f} - may indicate water or bare soil")
        else:
            validation['errors'].append("NDVI could not be calculated")
            validation['valid'] = False
        
        # Validate pixel count
        pixel_count = vegetation_indices.get('pixel_count', 0)
        if pixel_count < 10:
            validation['warnings'].append(f"Low pixel count {pixel_count} - may affect accuracy")
        
        # Validate acquisition date
        acquisition_date = vegetation_indices.get('acquisition_date')
        if not acquisition_date:
            validation['warnings'].append("No acquisition date available")
        
        return validation
    
    def get_vegetation_summary(self, vegetation_indices: Dict) -> str:
        """
        Generate human-readable summary of vegetation analysis
        
        Args:
            vegetation_indices: Dictionary with calculated indices
            
        Returns:
            Human-readable summary string
        """
        ndvi = vegetation_indices.get('ndvi', np.nan)
        pixel_count = vegetation_indices.get('pixel_count', 0)
        confidence = vegetation_indices.get('confidence_score', 0.0)
        
        if not np.isnan(ndvi):
            if ndvi >= self.thresholds['ndvi']['dense_vegetation_min']:
                vegetation_type = "Dense vegetation"
            elif ndvi >= self.thresholds['ndvi']['healthy_vegetation_min']:
                vegetation_type = "Healthy vegetation"
            elif ndvi >= 0.1:
                vegetation_type = "Sparse vegetation"
            elif ndvi >= 0:
                vegetation_type = "Bare soil/minimal vegetation"
            else:
                vegetation_type = "Water/non-vegetated"
            
            return (f"{vegetation_type} (NDVI: {ndvi:.3f}, "
                   f"{pixel_count} pixels, {confidence:.1%} confidence)")
        else:
            return f"No vegetation data available ({pixel_count} pixels)"


# Global vegetation analyzer instance
vegetation_analyzer = VegetationAnalyzer()