# V3 Migration Plan - From Working V1 to Enhanced V3

**Created**: January 10, 2025  
**Status**: Ready to Execute  
**Performance Target**: Maintain V1's 300 parcels/second (6.6 minutes for 10,766 parcels)

## Critical Success Factors
- ✅ V1 already calculates most V3 data - just not saving it
- ✅ No complex new calculations needed - extract existing values  
- ✅ Performance must match V1 (300 parcels/second minimum)
- ✅ Single transaction per batch (not 3 separate saves like failed V3)

---

## Phase 1: Clean Rollback to V1 (30 minutes)

### 1.1 Git Reset and Cleanup
- [x] Checkout commit `f2cd23c` (last working V1 before V3 attempts)
  ```bash
  git checkout f2cd23c -b v3-correct-implementation
  ```
- [x] Delete all existing V3 files from src directory
  ```bash
  find src -name "*_v3.py" -delete
  ```
- [x] Remove V3 test files
  ```bash
  rm -f test_v3_*.py setup_v3_*.py debug_*_v3.py verify_v3_*.py
  ```
- [x] Remove V3 log files
  ```bash
  rm -f v3_test_*.log
  ```

### 1.2 Verify V1 Functionality
- [x] Run quick validation test with 100 parcels
  ```bash
  python run_quick_validation.py
  ```
- [x] Verify performance is ~300 parcels/second after setup
- [x] Check database records are written to `biomass_production_v2`
- [x] Document baseline performance metrics

---

## Phase 2: Create Correct V3 Structure (2-3 hours)

### 2.1 Copy V1 Files to V3 (Preserve Logic Exactly)
- [x] Copy analyzer files
  ```bash
  cp src/analyzers/forest_analyzer_v1.py src/analyzers/forest_analyzer_v3.py
  cp src/analyzers/crop_analyzer_v1.py src/analyzers/crop_analyzer_v3.py
  cp src/analyzers/landcover_analyzer_v1.py src/analyzers/landcover_analyzer_v3.py
  cp src/analyzers/vegetation_analyzer_v1.py src/analyzers/vegetation_analyzer_v3.py
  ```
- [x] Copy core files
  ```bash
  cp src/core/database_manager_v1.py src/core/database_manager_v3.py
  cp src/core/blob_manager_v1.py src/core/blob_manager_v3.py
  cp src/core/coordinate_utils_v1.py src/core/coordinate_utils_v3.py
  ```
- [x] Copy pipeline files
  ```bash
  cp src/pipeline/comprehensive_biomass_processor_v1.py src/pipeline/comprehensive_biomass_processor_v3.py
  cp src/pipeline/county_processor_v1.py src/pipeline/county_processor_v3.py
  cp src/pipeline/optimized_county_processor_v1.py src/pipeline/optimized_county_processor_v3.py
  ```
- [x] Copy config files
  ```bash
  cp src/config/database_config_v1.py src/config/database_config_v3.py
  cp src/config/processing_config_v1.py src/config/processing_config_v3.py
  cp src/config/azure_config_v1.py src/config/azure_config_v3.py
  ```

### 2.2 Update Imports (V1 → V3)
- [x] Update all V3 files to import from V3 modules
  ```python
  # Change: from ..config.database_config_v1 import ...
  # To:     from ..config.database_config_v3 import ...
  ```
- [x] Test imports work correctly
  ```bash
  python -c "from src.pipeline.comprehensive_biomass_processor_v3 import ComprehensiveBiomassProcessor"
  ```

### 2.3 Enhanced Data Extraction (From Existing Calculations)

#### Forest Analyzer V3 Enhancements
- [x] Return species breakdown (already calculated in `_calculate_comprehensive_fia_biomass`)
- [x] Return stand age (already calculated in `_calculate_weighted_stand_age`)
- [x] Return forest type (already calculated in `_determine_dominant_forest_type`)
- [x] Return biomass components (bole, branch, foliage - already calculated)
- [x] Return harvest probability (already calculated in `_calculate_harvest_probability`)
- [x] Return real NDVI from vegetation indices (already passed in)

#### Crop Analyzer V3 Enhancements  
- [x] Return ALL crop intersections (not just dominant)
- [x] Keep multiple crop records per parcel
- [x] Include area_acres and area_percentage for each crop
- [x] Include CDL confidence scores (already calculated)
- [x] Return real NDVI from vegetation indices (already passed in)

#### Vegetation Analyzer V3 Enhancements
- [x] Calculate and return actual NDVI/EVI/SAVI values (not empty)
- [x] Return dynamic confidence scores (not hardcoded 0.8)

**DISCOVERY**: All analyzers are already calculating and returning enhanced data! V1 saves this as JSON blobs in forest_analysis and crop_analysis columns. V3 needs to extract this data to separate relational tables.

---

## Phase 3: Database Schema Enhancement (1 hour)

### 3.1 Create V3 Database
- [x] Connect to Azure PostgreSQL server
- [x] Create biomass_v3 database
  ```sql
  CREATE DATABASE biomass_v3;
  ```
- [x] Grant permissions to postgresadmin user

### 3.2 Create Enhanced Tables

#### Forestry Analysis Table
- [x] Create forestry_analysis_v3 table
  ```sql
  CREATE TABLE forestry_analysis_v3 (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      parcel_id TEXT NOT NULL,
      county_fips TEXT NOT NULL,
      processing_timestamp TIMESTAMP DEFAULT NOW(),
      
      -- Basic metrics (from V1)
      total_biomass_tons NUMERIC(12,3),
      forest_area_acres NUMERIC(10,3),
      
      -- Enhanced metrics (already calculated in V1)
      dominant_species_code INTEGER,
      dominant_species_name TEXT,
      species_diversity_index NUMERIC(4,2),
      stand_age_average NUMERIC(5,1),
      forest_type_classification TEXT,
      
      -- Biomass breakdown (already calculated)
      bole_biomass_tons NUMERIC(12,3),
      branch_biomass_tons NUMERIC(12,3),
      foliage_biomass_tons NUMERIC(12,3),
      residue_biomass_tons NUMERIC(12,3),
      
      -- Harvest analysis (already calculated)
      harvest_probability NUMERIC(3,2),
      fia_plots_used INTEGER,
      average_plot_distance_km NUMERIC(6,2),
      
      -- Quality metrics
      ndvi_value NUMERIC(6,4),
      confidence_score NUMERIC(4,3)
  );
  ```
- [ ] Create indexes for forestry table
  ```sql
  CREATE INDEX idx_forestry_v3_parcel ON forestry_analysis_v3(parcel_id);
  CREATE INDEX idx_forestry_v3_county ON forestry_analysis_v3(county_fips);
  CREATE INDEX idx_forestry_v3_timestamp ON forestry_analysis_v3(processing_timestamp);
  ```

#### Crop Analysis Table
- [x] Create crop_analysis_v3 table
  ```sql
  CREATE TABLE crop_analysis_v3 (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      parcel_id TEXT NOT NULL,
      county_fips TEXT NOT NULL,
      processing_timestamp TIMESTAMP DEFAULT NOW(),
      
      -- Crop identification
      crop_code INTEGER NOT NULL,
      crop_name TEXT NOT NULL,
      is_dominant_crop BOOLEAN DEFAULT FALSE,
      
      -- Area analysis
      area_acres NUMERIC(10,3),
      area_percentage NUMERIC(5,2),
      coverage_confidence NUMERIC(4,3),
      
      -- Yield analysis
      estimated_yield_tons NUMERIC(10,3),
      estimated_residue_tons NUMERIC(10,3),
      
      -- Quality metrics
      ndvi_value NUMERIC(6,4),
      confidence_score NUMERIC(4,3)
  );
  ```
- [ ] Create indexes for crop table
  ```sql
  CREATE INDEX idx_crop_v3_parcel ON crop_analysis_v3(parcel_id);
  CREATE INDEX idx_crop_v3_county ON crop_analysis_v3(county_fips);
  CREATE INDEX idx_crop_v3_code ON crop_analysis_v3(crop_code);
  CREATE INDEX idx_crop_v3_timestamp ON crop_analysis_v3(processing_timestamp);
  ```

### 3.3 Update Database Configuration
- [x] Update database_config_v3.py to point to biomass_v3
  ```python
  'biomass_output': {
      **base_config,
      'database': os.getenv('BIOMASS_OUTPUT_DB', 'biomass_v3')
  }
  ```

### 3.4 Update Database Manager Save Methods
- [x] Implement `save_forestry_analysis_v3()` method
- [x] Implement `save_crop_analysis_v3()` method
- [x] Use single transaction for both tables per batch
- [x] Implement bulk insert for multiple crop rows

---

## Phase 4: Testing & Validation (1 hour)

### 4.1 Performance Testing
- [ ] Test with 100 parcels
  ```bash
  python test_v3_quick_validation.py
  ```
- [ ] Verify processing speed matches V1 (~300 parcels/second)
- [ ] Check memory usage is similar to V1

### 4.2 Full County Test
- [ ] Test with Rich County, Utah (10,766 parcels)
  ```bash
  python test_v3_rich_county.py
  ```
- [ ] Must complete in ~6.6 minutes (±10%)
- [ ] Verify all parcels processed successfully

### 4.3 Data Quality Validation
- [ ] Verify forestry_analysis_v3 records have:
  - [ ] Species breakdown data
  - [ ] Stand age values
  - [ ] Forest type classification
  - [ ] Biomass component breakdown
  - [ ] Real NDVI values (not NULL)
  - [ ] Dynamic confidence scores (not 0.8)

- [ ] Verify crop_analysis_v3 records have:
  - [ ] Multiple rows per parcel (where multiple crops exist)
  - [ ] ALL crop types (not just dominant)
  - [ ] Area percentages for each crop
  - [ ] Real NDVI values (not NULL)
  - [ ] CDL confidence scores

### 4.4 Database Verification
- [ ] Check record counts
  ```sql
  SELECT COUNT(*) FROM forestry_analysis_v3;
  SELECT COUNT(*) FROM crop_analysis_v3;
  SELECT COUNT(DISTINCT parcel_id) FROM crop_analysis_v3;
  ```
- [ ] Verify foreign key relationships
- [ ] Check for data integrity issues

---

## Phase 5: Documentation & Cleanup (30 minutes)

### 5.1 Documentation
- [ ] Create V3_IMPLEMENTATION_SUCCESS.md
- [ ] Update README.md with V3 capabilities
- [ ] Document performance benchmarks
- [ ] Create deployment guide

### 5.2 Git Cleanup
- [ ] Commit V3 implementation
  ```bash
  git add src/*_v3.py
  git commit -m "feat: Implement correct V3 with enhanced data capture"
  ```
- [ ] Create PR for review
- [ ] Archive failed V3 documentation for reference

### 5.3 Final Verification
- [ ] Run complete test suite
- [ ] Verify CI/CD pipeline passes
- [ ] Document any remaining issues

---

## Success Metrics

| Metric | V1 Baseline | V3 Target | Acceptable Range |
|--------|------------|-----------|------------------|
| Processing Speed | 300 parcels/sec | 300 parcels/sec | 270-330 parcels/sec |
| Rich County Time | 6.6 minutes | 6.6 minutes | 6-8 minutes |
| Memory Usage | ~2GB | ~2GB | 1.8-2.5GB |
| Database Writes | 1 per batch | 2 per batch | Max 2 per batch |
| Data Completeness | Dominant crop only | All crops | 100% crop coverage |
| Forest Detail | Basic totals | Full breakdown | All components |
| NDVI Values | NULL | Real values | >80% populated |

---

## Risk Mitigation

### If Performance Degrades:
1. Profile code to identify bottlenecks
2. Ensure batch sizes remain at 50 parcels
3. Check database connection pooling
4. Verify no additional tile downloads

### If Data Quality Issues:
1. Compare V3 output with V1 for same parcels
2. Verify all calculations use same formulas
3. Check for NULL handling in new fields
4. Validate against known test cases

### If Database Issues:
1. Ensure proper indexes are created
2. Check connection pool settings
3. Verify bulk insert performance
4. Monitor transaction sizes

---

## Notes

- **DO NOT** change core processing logic from V1
- **DO NOT** add new complex calculations
- **DO NOT** create 3 separate database saves
- **DO** extract data already being calculated
- **DO** maintain single transaction per batch
- **DO** test performance after each change

This plan ensures V3 implementation succeeds by preserving V1's proven performance while enhancing data capture capabilities.