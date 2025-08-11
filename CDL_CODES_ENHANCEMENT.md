# CDL Codes Enhancement Plan

**Created**: January 10, 2025  
**Priority**: AFTER V3 is working  
**Estimated Time**: 4-6 hours

## Current State Analysis

### What V1/V3 Currently Does:
- **Processes**: ALL CDL crop codes found in parcels (not filtered)
- **Has yield data for**: Only 14 major crops
- **Uses defaults for**: All other ~240+ CDL codes
- **Saves to database**: Only dominant crop (V1) or all crops (V3)

### Current Limitations:
1. **CROP_BIOMASS_DATA** only contains 14 crops with accurate yield data:
   - Corn, Soybeans, Barley, Wheat varieties, Rye, Oats
   - Alfalfa, Other Hay, Sugarbeets, Sorghum, Rice, Cotton
2. **Default values** (2.0 tons/acre) used for all other crops
3. **Missing specialty crops**: Vegetables, fruits, nuts, specialty grains
4. **No regional variations**: Same yield estimates nationwide

---

## Enhancement Objectives

### Primary Goals:
1. ✅ Expand yield data from 14 to 100+ crops
2. ✅ Add regional yield variations
3. ✅ Include specialty and minor crops
4. ✅ Improve confidence scoring based on data quality
5. ✅ Add crop rotation patterns

---

## Phase 1: Research & Data Collection (2 hours)

### 1.1 USDA NASS Data Sources
- [ ] Download USDA NASS QuickStats yield data
  - URL: https://quickstats.nass.usda.gov/
- [ ] Extract state-level yield averages for all CDL crops
- [ ] Create mapping of CDL codes to NASS commodity codes
- [ ] Document data vintage and update frequency

### 1.2 Compile Comprehensive Crop List
- [ ] Review all 255 CDL codes
- [ ] Identify agricultural vs non-agricultural codes
- [ ] Group into categories:
  - [ ] Major grains (15-20 crops)
  - [ ] Specialty crops (30-40 crops)
  - [ ] Forage/hay crops (10-15 crops)
  - [ ] Vegetables (20-30 crops)
  - [ ] Fruits/nuts (15-20 crops)
  - [ ] Cover crops (5-10 crops)
  - [ ] Other agricultural (10-15 crops)

### 1.3 Residue Ratio Research
- [ ] Find crop residue coefficients for each crop
- [ ] Document moisture content variations
- [ ] Identify harvestable residue percentages
- [ ] Create confidence tiers for data quality

---

## Phase 2: Data Structure Enhancement (1 hour)

### 2.1 Enhanced CROP_BIOMASS_DATA Structure
```python
CROP_BIOMASS_DATA_V2 = {
    1: {  # Corn
        'name': 'Corn',
        'category': 'major_grain',
        'yield_national': 4.2,  # National average
        'yield_regional': {      # Regional variations
            'midwest': 4.8,
            'south': 3.5,
            'west': 5.1,
            'northeast': 3.9
        },
        'residue_ratio': 1.2,
        'moisture': 0.15,
        'harvestable_residue': 0.40,
        'data_quality': 'high',  # high/medium/low/estimated
        'nass_commodity_code': '111991',
        'last_updated': '2024-01-01'
    },
    # ... 100+ more crops
}
```

### 2.2 Create Crop Categories Mapping
- [ ] Define crop groupings for analysis
- [ ] Create biomass potential tiers
- [ ] Add processing difficulty ratings
- [ ] Include market value indicators

### 2.3 Regional Mapping
- [ ] Create state-to-region mapping
- [ ] Define climate zones for yield variations
- [ ] Account for irrigation vs dryland differences

---

## Phase 3: Implementation (2 hours)

### 3.1 Update database_config_v3.py
- [ ] Replace limited CROP_BIOMASS_DATA with comprehensive version
- [ ] Add regional yield lookup functions
- [ ] Implement data quality tracking
- [ ] Add fallback logic for missing data

### 3.2 Update crop_analyzer_v3.py
- [ ] Use regional yields based on location
- [ ] Apply data quality confidence adjustments
- [ ] Track which crops use actual vs estimated data
- [ ] Add crop category analysis

### 3.3 Database Schema Updates
- [ ] Add columns to crop_analysis_v3:
  ```sql
  ALTER TABLE crop_analysis_v3 ADD COLUMN yield_data_source TEXT;
  ALTER TABLE crop_analysis_v3 ADD COLUMN yield_confidence TEXT;
  ALTER TABLE crop_analysis_v3 ADD COLUMN regional_adjustment NUMERIC(4,2);
  ALTER TABLE crop_analysis_v3 ADD COLUMN crop_category TEXT;
  ```

---

## Phase 4: Testing & Validation (1 hour)

### 4.1 Unit Tests
- [ ] Test yield lookups for all CDL codes
- [ ] Verify regional adjustments work correctly
- [ ] Test fallback to defaults when data missing
- [ ] Validate confidence scoring

### 4.2 Integration Tests
- [ ] Process test county with diverse crops
- [ ] Compare results: old vs new yield estimates
- [ ] Verify performance not impacted
- [ ] Check database storage of new fields

### 4.3 Validation Against Known Data
- [ ] Compare with USDA county-level production
- [ ] Validate against state agricultural reports
- [ ] Cross-check with academic literature

---

## Comprehensive CDL Codes to Add

### High Priority Crops (Next 30 to add):
```
Code | Crop Name | Typical Yield | Residue Ratio
-----|-----------|---------------|---------------
61   | Fallow    | 0.0          | 0.0
66   | Cherries  | 3.5          | 0.3
67   | Peaches   | 8.0          | 0.4
68   | Apples    | 15.0         | 0.5
69   | Grapes    | 4.0          | 0.8
70   | Pecans    | 0.9          | 1.2
71   | Almonds   | 1.0          | 1.5
72   | Walnuts   | 2.0          | 1.8
74   | Pistachios| 1.5          | 1.0
75   | Triticale | 2.5          | 1.7
76   | Carrots   | 20.0         | 0.2
77   | Asparagus | 2.5          | 0.5
204  | Pistachios| 1.5          | 1.0
205  | Triticale | 2.5          | 1.7
206  | Carrots   | 20.0         | 0.2
207  | Asparagus | 2.5          | 0.5
208  | Garlic    | 8.0          | 0.3
209  | Cantaloupes| 12.0        | 0.4
210  | Prunes    | 4.0          | 0.5
211  | Olives    | 2.5          | 0.8
212  | Oranges   | 12.0         | 0.6
213  | Honeydew  | 15.0         | 0.4
214  | Broccoli  | 6.0          | 0.8
216  | Peppers   | 10.0         | 0.5
217  | Pomegranates| 8.0        | 0.6
218  | Nectarines| 9.0          | 0.4
219  | Greens    | 8.0          | 0.3
220  | Plums     | 6.0          | 0.4
221  | Strawberries| 20.0       | 0.2
222  | Squash    | 15.0         | 0.5
```

### Medium Priority (Next 40 to add):
- Tree fruits (15 codes)
- Nuts (10 codes)  
- Vegetables (25 codes)
- Berries (5 codes)

### Low Priority (Remaining ~150 codes):
- Double crops
- Cover crops
- Rare specialty crops
- Regional specific crops

---

## Expected Outcomes

### Data Quality Improvements:
- **Before**: 14 crops with real data, ~240 with defaults
- **After**: 100+ crops with real data, <50 with defaults
- **Accuracy**: 3-5x improvement in yield estimates

### Performance Impact:
- **Processing Speed**: No change (lookup is O(1))
- **Memory Usage**: +2MB for expanded lookup table
- **Database Storage**: +4 columns per crop record

### Business Value:
- More accurate biomass estimates
- Better regional analysis
- Improved investor confidence
- Enhanced agricultural insights

---

## Implementation Checklist

### Pre-Implementation:
- [ ] V3 must be working with current CDL codes
- [ ] Performance benchmarks established
- [ ] Database backup created

### Implementation:
- [ ] Research and collect yield data
- [ ] Create enhanced data structure
- [ ] Update configuration files
- [ ] Modify analyzer logic
- [ ] Update database schema
- [ ] Test thoroughly

### Post-Implementation:
- [ ] Document all data sources
- [ ] Create update procedures
- [ ] Train team on new capabilities
- [ ] Monitor accuracy improvements

---

## Risk Mitigation

### Data Quality Risks:
- Use tiered confidence levels
- Document all data sources
- Provide fallback values
- Flag estimated vs actual data

### Performance Risks:
- Keep lookups in memory
- Use efficient data structures
- Test with large counties
- Monitor processing speeds

### Maintenance Risks:
- Document update procedures
- Automate data refreshes
- Version control yield data
- Track historical changes

---

## Notes

- **DO NOT** implement until V3 is stable
- **DO** prioritize crops by acreage/importance  
- **DO** maintain backward compatibility
- **DO** document all data sources
- **DO** test regional variations thoroughly

This enhancement will significantly improve the accuracy of crop biomass estimates while maintaining system performance.