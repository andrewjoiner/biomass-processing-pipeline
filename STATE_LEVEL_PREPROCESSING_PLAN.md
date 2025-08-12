# State-Level Preprocessing Pipeline Plan
**Project**: US-Wide Biomass Processing Pipeline Performance Fix  
**Created**: August 12, 2025  
**Last Updated**: August 12, 2025  
**Status**: 🚀 Implementation Ready

---

## 🎯 Executive Summary

### Current Critical Issues
- [x] **Performance Failure Identified**: McLean County test took 3+ hours for <1% of parcels (target: <1 hour complete)
- [x] **Root Cause #1**: Streaming disabled for compressed Sentinel-2 tiles, causing 267x performance degradation
- [x] **Root Cause #2**: Database connection failures under load (forestry DB dropping connections)
- [x] **Root Cause #3**: Inefficient batch processing (500 parcels too large, no parallelization)
- [x] **Root Cause #4**: Cache thrashing (36GB needed vs 50 tile limit causing repeated downloads)
- [x] **Storage Impact**: Downloading 250MB tiles instead of 2.4MB pixel windows (100x+ excess data transfer)
- [x] **Production Blocker**: Multiple compounding issues make current implementation unusable

### Solution Strategy
**State-Level Preprocessing**: Temporarily preprocess compressed tiles to uncompressed format for one state at a time, enable streaming processing, then cleanup - repeat for all 50 states.

**Key Benefits**:
- ✅ 500x+ performance improvement (streaming enabled)
- ✅ 98% storage efficiency (temporary preprocessing only)
- ✅ 51 preprocessing operations vs 3000+ counties
- ✅ Complete US processing in 2-3 months vs 30+ years

---

## 📋 Implementation Phases

### Phase 1: Core Infrastructure Development
**Timeline**: Week 1  
**Goal**: Build state-level preprocessing pipeline components

#### State Tile Indexing Service
- [ ] **Create `src/preprocessing/state_tile_indexer.py`**
  - [ ] Implement `get_state_tiles(state_fips)` function
  - [ ] Add tile deduplication across counties
  - [ ] Include tile usage frequency analysis
  - [ ] Add progress tracking and logging
- [ ] **Database Integration**
  - [ ] Create state_tile_index table for caching results
  - [ ] Add tile priority scoring (by county overlap)
  - [ ] Implement resume capability for large states
- [ ] **Testing & Validation**
  - [ ] Test with Illinois (17) - should find ~800-1200 unique tiles
  - [ ] Validate against McLean County's 144 tiles (should be subset)
  - [ ] Performance test: complete state indexing in <30 minutes

#### Tile Preprocessing Pipeline
- [ ] **Create `src/preprocessing/tile_preprocessor.py`**
  - [ ] Implement GDAL-based tile conversion to uncompressed COGs
  - [ ] Add parallel processing support (4-8 concurrent conversions)
  - [ ] Include hash verification (preprocessed vs original integrity)
  - [ ] Add progress tracking with ETA calculations
- [ ] **Storage Management**
  - [ ] Create temporary storage structure: `/tmp/preprocessing/{state_fips}/`
  - [ ] Implement disk space monitoring (abort if <100GB free)
  - [ ] Add cleanup verification before next state
- [ ] **Error Handling**
  - [ ] Resumable processing from interruptions
  - [ ] Corrupted tile detection and reprocessing
  - [ ] Network failure retry logic with exponential backoff

#### Integration with County Processor
- [ ] **Modify `src/core/blob_manager_v3.py`**
  - [ ] Add preprocessed tile detection logic
  - [ ] Prioritize uncompressed tiles when available
  - [ ] Maintain fallback to compressed originals
  - [ ] Update streaming support check (line 1174)
  - [ ] **Fix cache thrashing**: Increase cache from 50 to 500+ tiles
  - [ ] **Optimize cache eviction**: Implement smarter LRU based on tile usage patterns
- [ ] **Update `src/pipeline/optimized_county_processor_v3.py`**  
  - [ ] Add preprocessing status checking
  - [ ] Integrate with tile cleanup after county completion
  - [ ] Include preprocessed tile performance metrics
  - [ ] **Reduce batch size**: Change from 500 to 50-100 parcels per batch
  - [ ] **Add parallel processing**: Process multiple parcels concurrently within batches

#### Database Connection Fixes  
- [ ] **Modify `src/core/database_manager_v3.py`**
  - [ ] **Increase forestry DB connection pool**: From default to 20+ connections
  - [ ] **Add connection retry logic**: Exponential backoff for failed connections
  - [ ] **Implement connection health checks**: Periodic validation and reconnection
  - [ ] **Add connection timeout handling**: Graceful recovery from dropped connections
- [ ] **Connection Pool Configuration**
  - [ ] Set `max_connections=25` for forestry database
  - [ ] Set `connection_timeout=30` seconds
  - [ ] Set `retry_attempts=4` with exponential backoff
  - [ ] Add connection pool monitoring and alerting

### Phase 2: Production Pipeline Implementation  
**Timeline**: Week 2  
**Goal**: Robust, production-ready state processing pipeline

#### Preprocessing Optimization
- [ ] **Performance Enhancements**
  - [ ] Implement concurrent tile downloads (8-12 parallel)
  - [ ] Add SSD caching for frequently accessed tiles
  - [ ] Optimize GDAL conversion parameters for speed
  - [ ] Include GPU acceleration if available (GDAL CUDA support)
- [ ] **HTTP Connection Optimization**
  - [ ] **Add HTTP connection pooling**: Reuse connections for Azure blob downloads
  - [ ] **Enable HTTP/2 if supported**: Check Azure blob storage HTTP/2 support
  - [ ] **Optimize chunk size**: Test 1MB vs 4MB vs 8MB chunks for downloads
  - [ ] **Add connection keep-alive**: Maintain persistent connections during batch processing
- [ ] **Quality Assurance**
  - [ ] Automated tile validation (CRS, bounds, data integrity)
  - [ ] Streaming compatibility verification
  - [ ] Performance regression testing
- [ ] **Monitoring & Alerting**
  - [ ] Real-time processing metrics
  - [ ] Storage utilization alerts
  - [ ] Failure notification system
  - [ ] **Download performance tracking**: Monitor bytes/second per connection

#### State Processing Orchestrator
- [ ] **Create `src/pipeline/state_processor.py`**
  - [ ] End-to-end state processing workflow
  - [ ] County processing order optimization (largest counties first)
  - [ ] Progress checkpointing after each county
  - [ ] Automated cleanup after state completion
- [ ] **Database Persistence**
  - [ ] State processing status tracking
  - [ ] County completion verification
  - [ ] Performance metrics collection
  - [ ] Error logging and recovery status

#### Cleanup & Storage Management
- [ ] **Create `src/preprocessing/tile_cleanup.py`**
  - [ ] Verify all counties completed successfully before cleanup
  - [ ] Selective cleanup (keep tiles for failed counties)
  - [ ] Storage space reclamation verification
  - [ ] Emergency cleanup procedures
- [ ] **Storage Monitoring**
  - [ ] Real-time disk usage tracking
  - [ ] Automatic cleanup triggers
  - [ ] Storage cost optimization reporting

### Phase 3: Illinois Deployment & Validation
**Timeline**: Week 3  
**Goal**: Complete Illinois state processing with performance validation

#### Pre-Production Testing
- [ ] **McLean County Validation Test**
  - [ ] Run preprocessing for McLean County's 144 tiles
  - [ ] Verify streaming performance improvement
  - [ ] Confirm <30 minute processing time for full county
  - [ ] Validate output database completeness
- [ ] **Illinois Scope Analysis**  
  - [ ] Complete tile indexing for all Illinois counties
  - [ ] Estimate preprocessing time and storage requirements
  - [ ] Create Illinois processing plan and timeline

#### Illinois Production Run
- [ ] **Preprocessing Phase**
  - [ ] Execute Illinois tile preprocessing (~800-1200 tiles)
  - [ ] Monitor storage usage and performance metrics
  - [ ] Validate streaming compatibility for all tiles
  - [ ] Target completion: <6 hours for entire state preprocessing
- [ ] **County Processing Phase**
  - [ ] Process all Illinois counties with streaming enabled
  - [ ] Track per-county processing times (target: <30 minutes each)
  - [ ] Monitor database outputs and data quality
  - [ ] Target completion: <2 hours for all counties
- [ ] **Cleanup & Validation**
  - [ ] Execute cleanup of preprocessed tiles
  - [ ] Verify storage reclamation (600GB → 0GB preprocessed data)
  - [ ] Validate all Illinois counties in output database
  - [ ] Performance analysis and documentation

#### Performance Validation
- [ ] **Metrics Collection**
  - [ ] Document preprocessing time vs county processing time ratio
  - [ ] Measure storage efficiency gains
  - [ ] Track streaming vs fallback ratio (target: >95% streaming)
  - [ ] Calculate total time for state completion
- [ ] **Optimization Opportunities**
  - [ ] Identify bottlenecks in preprocessing pipeline
  - [ ] Optimize county processing order
  - [ ] Fine-tune parallel processing parameters
  - [ ] Document lessons learned for other states

### Phase 4: National Rollout
**Timeline**: Weeks 4-16  
**Goal**: Process all remaining 49 states with optimized pipeline

#### Rollout Strategy
- [ ] **State Prioritization**
  - [ ] Order states by total parcel count (largest impact first)
  - [ ] Consider geographic clustering for tile overlap optimization
  - [ ] Account for available processing windows
- [ ] **Batch Processing Implementation**
  - [ ] Queue management for state processing
  - [ ] Resource allocation optimization
  - [ ] Parallel state processing where storage permits

#### State-by-State Tracking

##### High-Priority States (Week 4-6)
- [ ] **Texas (48)** - Largest state by area
  - [ ] Tile indexing completed
  - [ ] Preprocessing completed (~2000-3000 tiles estimated)  
  - [ ] County processing completed
  - [ ] Cleanup completed
  - [ ] Performance metrics documented
- [ ] **California (06)** - High parcel density
  - [ ] Tile indexing completed
  - [ ] Preprocessing completed
  - [ ] County processing completed  
  - [ ] Cleanup completed
  - [ ] Performance metrics documented
- [ ] **Florida (12)** - High agricultural value
  - [ ] Tile indexing completed
  - [ ] Preprocessing completed
  - [ ] County processing completed
  - [ ] Cleanup completed
  - [ ] Performance metrics documented

##### Medium-Priority States (Week 7-10)
- [ ] **New York (36)**
- [ ] **Pennsylvania (42)**  
- [ ] **Ohio (39)**
- [ ] **Michigan (26)**
- [ ] **Wisconsin (55)**
- [ ] **Minnesota (27)**
- [ ] **Iowa (19)**
- [ ] **Nebraska (31)**

##### Remaining States (Week 11-16)
- [ ] **Northeast States** (8 states)
- [ ] **Southeast States** (6 states)  
- [ ] **Mountain West States** (8 states)
- [ ] **Pacific States** (3 states)
- [ ] **Alaska (02) & Hawaii (15)** (special handling needed)

---

## 🔧 Technical Specifications

### Storage Requirements by State Size

#### Large States (>2000 tiles)
- **Examples**: Texas, California, Alaska
- **Compressed tiles**: 2000 × 250MB = 500GB
- **Uncompressed temp**: 2000 × 750MB = 1.5TB
- **Peak storage**: 2TB during preprocessing

#### Medium States (800-2000 tiles)  
- **Examples**: Illinois, Florida, New York
- **Compressed tiles**: 1200 × 250MB = 300GB
- **Uncompressed temp**: 1200 × 750MB = 900GB
- **Peak storage**: 1.2TB during preprocessing

#### Small States (<800 tiles)
- **Examples**: Rhode Island, Delaware, Connecticut
- **Compressed tiles**: 400 × 250MB = 100GB  
- **Uncompressed temp**: 400 × 750MB = 300GB
- **Peak storage**: 400GB during preprocessing

### Performance Targets

#### Preprocessing Phase (Per State)
- **Small states**: <2 hours
- **Medium states**: <4 hours  
- **Large states**: <8 hours
- **Parallel tile processing**: 4-8 concurrent conversions
- **Disk I/O optimization**: SSD recommended

#### County Processing Phase (With Streaming)
- **Small counties (<10K parcels)**: <15 minutes
- **Medium counties (10K-50K parcels)**: <30 minutes
- **Large counties (>50K parcels)**: <60 minutes
- **Streaming success rate**: >95%
- **Data transfer reduction**: >100x improvement

### Command Examples

#### State Tile Indexing
```bash
python src/preprocessing/state_tile_indexer.py --state-fips=17 --output-dir=/tmp/preprocessing/
```

#### Tile Preprocessing  
```bash
python src/preprocessing/tile_preprocessor.py --state-fips=17 --parallel-jobs=8 --storage-dir=/tmp/preprocessing/17/
```

#### State Processing
```bash
python src/pipeline/state_processor.py --state-fips=17 --batch-size=100 --enable-streaming
```

#### Cleanup
```bash
python src/preprocessing/tile_cleanup.py --state-fips=17 --verify-completion --force-cleanup
```

---

## ⚠️ Risk Mitigation

### Storage Management Risks
- [ ] **Disk space monitoring implemented**
- [ ] **Emergency cleanup procedures documented**  
- [ ] **Storage alerts configured**
- [ ] **Cleanup verification mandatory**

### Processing Reliability Risks
- [ ] **Checkpointing after each county**
- [ ] **Resume capability from interruptions**
- [ ] **State validation before cleanup**
- [ ] **Rollback procedures documented**

### Data Integrity Risks  
- [ ] **Hash verification for all preprocessed tiles**
- [ ] **Streaming validation before county processing**
- [ ] **Never delete compressed originals**
- [ ] **Database backup before state processing**

---

## 📊 Success Criteria

### Phase 1 Success Criteria
- [ ] State tile indexing completes in <30 minutes for Illinois
- [ ] Tile preprocessing converts 144 McLean tiles in <2 hours
- [ ] Streaming successfully enabled for uncompressed tiles
- [ ] County processor integration works without errors

### Phase 2 Success Criteria  
- [ ] Full Illinois preprocessing completes in <6 hours
- [ ] All Illinois counties process in <2 hours total
- [ ] Storage cleanup successfully reclaims >95% of temporary space
- [ ] Zero data loss or corruption incidents

### Phase 3 Success Criteria
- [ ] McLean County processes in <30 minutes with streaming
- [ ] Database outputs match quality of original implementation
- [ ] Performance metrics show >500x improvement over baseline
- [ ] Illinois completion serves as template for other states

### Phase 4 Success Criteria
- [ ] All 50 states processed successfully  
- [ ] Total US processing time <3 months
- [ ] No persistent storage increase (temporary preprocessing only)
- [ ] Complete biomass database for all US parcels

---

## 📝 Notes & Discoveries

### Current Status Notes
- **Latest Update**: Initial plan created, ready for implementation
- **Blockers**: None identified
- **Next Action**: Begin Phase 1 development

### Implementation Discoveries
- [ ] **McLean County test results**: [Document findings here]
- [ ] **Tile compression analysis**: [Document compression types found]
- [ ] **Performance bottlenecks**: [Document any unexpected issues]  
- [ ] **Optimization opportunities**: [Document improvements discovered]

### State-Specific Notes
- [ ] **Illinois**: [Document state-specific findings]
- [ ] **Texas**: [Document large state processing notes]
- [ ] **Alaska/Hawaii**: [Document special cases needed]

---

## 🔗 Related Files

### Core Implementation Files
- `src/core/blob_manager_v3.py` - Main streaming implementation
- `src/pipeline/optimized_county_processor_v3.py` - County processing
- `CRITICAL_PERFORMANCE_FAILURE_REPORT.md` - Original problem analysis

### New Files to Create  
- `src/preprocessing/state_tile_indexer.py`
- `src/preprocessing/tile_preprocessor.py`  
- `src/preprocessing/tile_cleanup.py`
- `src/pipeline/state_processor.py`

### Testing & Validation
- `test_mclean_county_production.py` - Current test script
- `test_streaming_complete.py` - Streaming validation test

---

**🎯 READY FOR IMPLEMENTATION**  
This plan provides complete roadmap for fixing the biomass processing pipeline performance issues through state-level preprocessing. All phases are defined with clear checkboxes for progress tracking across sessions.