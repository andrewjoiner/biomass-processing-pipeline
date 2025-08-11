# V3 Satellite Data Streaming Implementation Plan

**Branch**: `streaming-satellite-data`  
**Date**: August 11, 2025  
**Goal**: Replace 14-hour tile download with on-demand streaming for McLean County processing  

## Problem Statement

Current V3 approach downloads entire Sentinel-2 tiles (250MB × 4 bands × 144 tiles = ~144GB) before processing, causing:
- 14+ hour download times with timeouts
- Massive memory usage (GB of cached tiles)
- Processing only tiny parcel areas from huge tiles

## Solution: Streaming Architecture

Stream only the small areas needed for each parcel (1-2KB vs 250MB per tile) using Azure blob range requests.

---

## Implementation Checklist

### Phase 1: Core Streaming Infrastructure
- [x] **1.1** Implement `analyze_county_satellite_requirements()` method in blob_manager_v3.py
  - [x] Create lightweight tile index (144 tiles for McLean County in 12.8s)
  - [x] Build blob paths for all required bands without downloading
  - [x] Store tile metadata (bounds, UTM zones, dates) in county_tile_index
- [x] **1.2** Implement `get_sentinel2_data_for_parcel_streaming()` method
  - [x] Find intersecting tiles from county index for each parcel
  - [x] Stream data on-demand using _stream_parcel_window_from_tile()
  - [x] Handle multi-tile parcels with proper data merging
- [x] **1.3** Test streaming architecture with McLean County
  - [x] Successfully indexed 144 tiles (140.6GB if downloaded) 
  - [x] Verified parcel-tile intersection logic works correctly
  - [x] Memory usage: minimal metadata vs 140.6GB bulk download
  
**Status: ✅ COMPLETED** - Tile indexing and streaming methods implemented and tested

### Phase 2: Remove Bulk Download Logic  
- [ ] **2.1** Modify optimized_county_processor_v3.py
  - [ ] Remove `download_sentinel2_county_tiles()` call from setup
  - [ ] Replace with tile requirement analysis only
  - [ ] Keep pre-loading of CDL and FIA data (these are smaller)
- [ ] **2.2** Update vegetation analyzer integration
  - [ ] Modify vegetation_analyzer_v3.py to accept streaming data source
  - [ ] Ensure NDVI, EVI, SAVI, NDWI calculations work with windowed data
  - [ ] Handle edge cases for parcels spanning multiple tiles
- [ ] **2.3** Preserve V3 enhanced data capture
  - [ ] Ensure forest_analyzer_v3.py continues generating enhanced records
  - [ ] Ensure crop_analyzer_v3.py creates multiple crop records per parcel
  - [ ] Verify database_manager_v3.py saves all enhanced data correctly

### Phase 3: Memory and Performance Optimization
- [ ] **3.1** Remove in-memory tile cache
  - [ ] Clear sentinel2_cache and worldcover_cache from blob_manager_v3.py
  - [ ] Free memory immediately after each parcel is processed
  - [ ] Add memory usage monitoring and reporting
- [ ] **3.2** Add concurrent parcel processing (optional)
  - [ ] Stream satellite data for multiple parcels in parallel
  - [ ] Maintain thread safety for Azure blob client
  - [ ] Balance concurrency with Azure rate limits
- [ ] **3.3** Error handling and resilience
  - [ ] Retry failed range requests with exponential backoff
  - [ ] Handle network timeouts gracefully
  - [ ] Fall back to full tile download if range requests fail

### Phase 4: Validation and Testing
- [ ] **4.1** Full McLean County test
  - [ ] Process complete county (not 10-parcel subset) with streaming
  - [ ] Target completion time: 10-15 minutes (similar to V1 Rich County)
  - [ ] Verify all enhanced V3 records are captured correctly
- [ ] **4.2** Data quality validation
  - [ ] Compare vegetation indices from streaming vs cached approaches
  - [ ] Ensure forestry_analysis_v3 contains enhanced species data
  - [ ] Ensure crop_analysis_v3 contains multiple crops per agricultural parcel
- [ ] **4.3** Performance benchmarking
  - [ ] Measure total data transfer (target: <1GB vs 144GB)
  - [ ] Measure processing time per parcel (target: <1 second)
  - [ ] Measure memory usage (target: <1GB vs 144GB)

---

## Technical Specifications

### Streaming Data Flow
```
For each parcel:
1. Calculate which Sentinel-2 tiles intersect parcel bounds
2. For each intersecting tile:
   a. Calculate pixel window covering parcel area  
   b. Calculate byte range for that pixel window in GeoTIFF
   c. Stream only those bytes from Azure blob
   d. Process vegetation indices immediately
3. Save enhanced V3 results to database
4. Clear parcel data from memory
```

### Key Implementation Details

#### Azure Blob Range Requests
```python
# Instead of downloading entire 250MB tile:
blob_client.download_blob().readall()

# Download only parcel's pixel window (~1-2KB):
blob_client.download_blob(offset=byte_start, length=byte_length).readall()
```

#### Pixel Window Calculation
```python
def get_pixel_window(parcel_bounds, tile_transform):
    """Convert parcel WGS84 bounds to tile pixel coordinates"""
    min_col, min_row = ~tile_transform * (parcel_bounds[0], parcel_bounds[3])
    max_col, max_row = ~tile_transform * (parcel_bounds[2], parcel_bounds[1]) 
    return (int(min_col), int(min_row), int(max_col-min_col), int(max_row-min_row))
```

---

## Success Criteria

- [ ] **Performance**: Complete McLean County processing in 10-15 minutes
- [ ] **Data Transfer**: <1GB total vs current 144GB requirement  
- [ ] **Memory Usage**: <1GB peak vs current 144GB requirement
- [ ] **Data Quality**: Enhanced V3 records with real vegetation indices
- [ ] **Reliability**: No timeouts or memory errors during processing

---

## Rollback Plan

If streaming implementation fails:
1. Keep current bulk download approach
2. Add parallel tile downloads and proper timeout handling
3. Accept longer processing times but ensure stability

---

## Notes

- Maintain all V3 enhanced data capture functionality
- Use real Sentinel-2 data (no placeholders or stubs)
- Preserve compatibility with existing V3 analyzers
- Test with full county datasets, not partial samples

---

**Implementation Status**: 🟡 In Progress  
**Next Action**: Implement core streaming infrastructure in blob_manager_v3.py