# Phase 2: On-Demand Tile Loading with LRU Cache

## Objective
Eliminate setup time bottleneck (430s → 30s) by implementing intelligent on-demand tile loading with memory-efficient caching.

## Current Bottleneck Analysis
Phase 1 testing revealed setup time breakdown:
- **CDL spatial indexing**: ~300s (building spatial index for 83 records)
- **FIA spatial indexing**: ~120s (loading 893 plots + 4858 trees)  
- **Tile analysis**: ~10s (calculating 102 required tiles)

**Root Cause**: Building complete spatial indices upfront instead of loading data on-demand.

## Implementation Plan

### Task 1: Create LRU Tile Cache System
**File**: `src/core/tile_cache_manager_v1.py`

```python
class TileCacheManager:
    def __init__(self, max_memory_mb: int = 100):
        """LRU cache for satellite tiles with memory management"""
        self.max_memory_mb = max_memory_mb
        self.sentinel2_cache = {}  # tile_id -> tile_data
        self.worldcover_cache = {}  # tile_id -> tile_data
        self.access_order = {}     # tile_id -> timestamp
        self.memory_usage = 0      # bytes
    
    def get_tile(self, tile_id: str, tile_type: str) -> Optional[Dict]:
        """Get tile from cache or return None if not cached"""
        
    def cache_tile(self, tile_id: str, tile_type: str, tile_data: Dict):
        """Cache tile with LRU eviction if needed"""
        
    def evict_lru_tiles(self, required_memory: int):
        """Evict least recently used tiles to free memory"""
```

**Success Criteria**:
- [ ] Configurable memory limit (default 100MB)
- [ ] Automatic LRU eviction when memory limit reached
- [ ] Separate tracking for Sentinel-2 vs WorldCover tiles
- [ ] Memory usage monitoring and reporting

### Task 2: Implement On-Demand Tile Loading
**File**: `src/core/blob_manager_v1.py` (modify existing)

```python
def load_tile_on_demand(self, tile_id: str, tile_type: str) -> Optional[Dict]:
    """Load single tile only when needed"""
    # 1. Check tile cache first
    # 2. Download if not cached
    # 3. Cache with LRU management
    # 4. Return tile data

def get_tiles_for_parcel_batch(self, parcels: List[Dict]) -> Dict[str, Dict]:
    """Get only the tiles needed for current parcel batch"""
    # 1. Calculate required tiles for this batch
    # 2. Load missing tiles on-demand
    # 3. Return tile data for batch processing
```

**Success Criteria**:
- [ ] Downloads tiles only when actually needed
- [ ] Integrates with LRU cache system
- [ ] Handles download failures gracefully
- [ ] Minimizes redundant tile downloads

### Task 3: Replace Bulk Spatial Indexing
**File**: `src/pipeline/optimized_county_processor_v1.py` (modify existing)

Remove these expensive operations:
- `_build_spatial_indices()` - delete entirely
- CDL spatial index pre-building
- FIA spatial index pre-building  

Replace with:
```python
def _setup_county_data_lightweight(self, state_fips: str, county_fips: str):
    """Lightweight setup - no bulk indexing"""
    # 1. Load parcel geometries only
    # 2. Calculate county bounds
    # 3. Initialize tile cache manager
    # 4. Skip spatial index building
```

**Success Criteria**:
- [ ] Setup time reduced from 430s to <30s
- [ ] No pre-built spatial indices
- [ ] On-demand data loading during processing
- [ ] Maintain processing accuracy

### Task 4: Batch-Aware Processing
**File**: `src/pipeline/optimized_county_processor_v1.py` (modify existing)

```python
def _process_parcel_batch_ondemand(self, batch_gdf: gpd.GeoDataFrame):
    """Process batch with on-demand data loading"""
    # 1. Determine tiles needed for this batch
    # 2. Load tiles on-demand via cache manager
    # 3. Query CDL/FIA data only for batch extent
    # 4. Process parcels with loaded data
    # 5. Clear unnecessary cached data
```

**Success Criteria**:
- [ ] Loads data only for current batch
- [ ] Efficient spatial queries (bbox-based)
- [ ] Memory usage remains constant
- [ ] Processing speed maintained

### Task 5: On-Demand CDL/FIA Querying
**File**: `src/core/database_manager_v1.py` (modify existing)

Replace bulk spatial indexing with efficient bbox queries:
```python
def get_cdl_for_bbox(self, bbox: Tuple[float, float, float, float]) -> gpd.GeoDataFrame:
    """Get CDL data only for specific bounding box"""
    
def get_fia_for_bbox(self, bbox: Tuple[float, float, float, float]) -> Dict:
    """Get FIA plots and trees only for specific bounding box"""
```

**Success Criteria**:
- [ ] No upfront spatial index building
- [ ] Fast bbox-based database queries
- [ ] Results cached per batch
- [ ] Proper PostGIS index utilization

## Performance Targets

### Setup Time Reduction
- **Current**: 430s setup time
- **Target**: <30s setup time  
- **Improvement**: 14x faster setup

### Memory Usage
- **Current**: Unbounded (grows with county size)
- **Target**: <100MB constant memory usage
- **Benefit**: Scales to largest counties

### Processing Speed
- **Current**: 500+ parcels/sec (maintain)
- **Target**: 500+ parcels/sec (no regression)
- **Cache hit rate**: >90% for batch processing

### Overall Performance
- **Phase 1**: 18+ hours → 1.3 hours (13x improvement)
- **Phase 2**: 1.3 hours → 20 minutes (54x total improvement)

## Testing Plan

### Unit Tests
- [ ] LRU cache eviction logic
- [ ] On-demand tile loading
- [ ] Memory usage monitoring
- [ ] Bbox-based database queries

### Integration Tests
- [ ] End-to-end processing with Phase 2 optimizations
- [ ] Memory usage validation (stays <100MB)
- [ ] Cache hit rate measurement
- [ ] Processing accuracy validation

### Performance Tests
- [ ] Setup time measurement (<30s target)
- [ ] Memory usage under load
- [ ] Processing speed maintenance
- [ ] Large county scalability (10K+ parcels)

## Implementation Schedule

### Week 1: Cache Infrastructure
- [ ] Task 1: Implement TileCacheManager class
- [ ] Task 2: Add on-demand tile loading to blob_manager
- [ ] Unit tests for caching logic

### Week 2: Processing Integration  
- [ ] Task 3: Remove bulk spatial indexing
- [ ] Task 4: Implement batch-aware processing
- [ ] Integration testing

### Week 3: Database Optimization
- [ ] Task 5: Implement on-demand CDL/FIA querying
- [ ] Performance testing and optimization
- [ ] End-to-end validation

### Week 4: Testing & Documentation
- [ ] Comprehensive performance testing
- [ ] Memory usage validation
- [ ] Documentation updates
- [ ] Production readiness assessment

## Risk Mitigation

### Cache Hit Rate Risk
- **Risk**: Low cache hit rate increases tile downloads
- **Mitigation**: Batch processing by spatial locality
- **Fallback**: Increase cache size if needed

### Database Query Performance Risk  
- **Risk**: Bbox queries slower than pre-built indices
- **Mitigation**: Ensure proper PostGIS indices exist
- **Fallback**: Implement spatial query optimization

### Memory Management Risk
- **Risk**: Memory leaks in tile cache
- **Mitigation**: Comprehensive memory usage monitoring
- **Fallback**: Aggressive LRU eviction settings

## Success Metrics

### Phase 2 Complete When:
- [ ] Setup time <30 seconds
- [ ] Memory usage <100MB constant
- [ ] Processing speed maintained (500+ parcels/sec)
- [ ] Cache hit rate >90%
- [ ] No accuracy regression vs Phase 1
- [ ] Full county processing <30 minutes
- [ ] Scales to any county size

## Expected Impact

### Performance Improvement Stack
```
Original Pipeline:     18+ hours per county
├─ Phase 1:           1.3 hours per county (13x improvement)
└─ Phase 2:           20 minutes per county (54x total improvement)
```

### National Scale Impact
```
150M Parcels Processing:
├─ Original:          25+ years
├─ Phase 1:           25 months  
├─ Phase 2:           2 months
└─ Phase 2 + 50 VMs:  1.2 days
```

Phase 2 optimizations will make real-time national biomass processing feasible.