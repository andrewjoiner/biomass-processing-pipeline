# Streaming Satellite Data Architecture - Root Cause Analysis

**Date**: January 11, 2025  
**Branch**: `streaming-satellite-data`  
**Test Results**: 2+ hours for 5 parcels (FAILED - expected ~30 seconds)  
**Status**: ❌ CRITICAL FAILURE - Streaming not actually implemented

---

## Executive Summary

The streaming satellite data architecture promised to reduce processing time from 14+ hours to 10-15 minutes for McLean County by avoiding the download of 140GB of satellite tiles. However, testing revealed the system took **over 2 hours to process just 5 parcels**, projecting to **20+ days for the full county**. This document provides an exhaustive analysis of why the streaming implementation failed.

**Key Finding**: The "streaming" implementation is actually downloading ENTIRE 250MB tiles and then clipping them, making it SLOWER than the original bulk download approach due to inefficient sequential processing.

---

## Test Configuration & Results

### Test Parameters
- **County**: McLean County, Illinois (FIPS: 17113)
- **Total Parcels Available**: 70,153
- **Test Sample Size**: 5 parcels
- **Expected Tiles**: 144 Sentinel-2 tiles (~140.6GB if all downloaded)
- **Bands Processed**: 4 (B02-Blue, B03-Green, B04-Red, B08-NIR)

### Actual Test Results
- **Start Time**: 10:51:39 AM
- **Kill Time**: 12:56:09 PM (process manually terminated)
- **Duration**: 2 hours 5 minutes (still incomplete)
- **Parcels Processed**: Unknown (likely 0-1)
- **Tile Access Attempts**: 6,480+ 
- **Data Written to Database**: 0 records
- **Memory Usage**: 153MB (reasonable, but processing was glacial)

### Performance Comparison
| Metric | Expected (Streaming) | Actual | Original V1 |
|--------|---------------------|---------|-------------|
| 5 parcels | ~30 seconds | 2+ hours (incomplete) | ~1 second |
| Full county (70k parcels) | 10-15 minutes | 20+ days (projected) | 14 hours |
| Data transfer | <1GB | ~140GB | 140GB |
| Tile accesses per parcel | 1-4 | 144 × 4 = 576 | N/A (bulk cached) |

---

## Critical Problems Identified

### 1. No Actual Streaming Implementation (MOST CRITICAL)

#### Evidence
```python
# From blob_manager_v3.py, lines 538-555
def _stream_parcel_window_from_tile(self, blob_path: str, parcel_geometry: Dict, 
                                  tile_info: Dict) -> Optional[Dict]:
    """
    Stream only the pixel window needed for a parcel from a Sentinel-2 tile
    PHASE 1: Uses full tile download and clipping (fallback approach)
    TODO PHASE 2: Implement true pixel window range requests
    """
    try:
        # PHASE 1 IMPLEMENTATION: Download full tile and clip
        # This maintains compatibility while we test the architecture
        blob_data = self.download_blob_to_memory(
            self.config['containers']['sentinel2'], 
            blob_path
        )
```

#### What's Happening
- Method name suggests "streaming" but implementation downloads ENTIRE tile
- `download_blob_to_memory()` calls `blob_client.download_blob().readall()`
- Each tile is 250MB, downloaded completely before clipping
- This is WORSE than bulk download because of no caching between parcels

#### Expected Implementation (Never Built)
```python
# What SHOULD have been implemented
def _stream_parcel_window_from_tile(self, blob_path, parcel_geometry, tile_info):
    # 1. Parse GeoTIFF header to get pixel layout (first 1KB)
    header = blob_client.download_blob(offset=0, length=1024).readall()
    
    # 2. Calculate pixel window for parcel
    pixel_window = calculate_pixel_window(parcel_geometry, tile_info['transform'])
    
    # 3. Calculate byte range for pixel window
    byte_offset, byte_length = tiff_calculate_byte_range(header, pixel_window)
    
    # 4. Download ONLY the needed bytes (1-10KB instead of 250MB)
    window_data = blob_client.download_blob(
        offset=byte_offset, 
        length=byte_length
    ).readall()
```

---

### 2. Incorrect Tile Intersection Logic

#### Evidence
```
# From streaming_test_output.log
2025-08-11 10:51:50,648 - INFO - Found 144 intersecting tiles
2025-08-11 10:52:04,539 - INFO - Found 144 intersecting tiles  
2025-08-11 10:52:10,755 - INFO - Found 144 intersecting tiles
```

Every single parcel thinks it intersects with ALL 144 tiles in the county!

#### The Problem
```python
# From blob_manager_v3.py, lines 466-469
for tile_id, tile_info in self.county_tile_index.items():
    if tile_info.get('wgs84_bounds'):
        if coordinate_transformer.bounds_intersect(parcel_bounds, tile_info['wgs84_bounds']):
            intersecting_tile_ids.append(tile_id)
```

#### Why It's Failing
1. **Coordinate System Mismatch**: Sentinel-2 tiles are in UTM, parcels in WGS84
2. **Bounds Calculation Error**: The tile bounds transformation is likely too broad
3. **No Spatial Filtering**: Should use R-tree or similar spatial index
4. **Missing Validation**: No logging to verify intersection logic

#### Expected Behavior
- Small parcels (0.1-100 acres) should intersect 1-4 tiles maximum
- Most parcels fit entirely within a single 100km × 100km tile
- Only parcels on tile boundaries need multiple tiles

---

### 3. Sequential Processing Without Caching

#### Evidence
```
# Timestamps from log show ~30-40 seconds per tile attempt:
11:01:41 - ERROR - Failed to stream from 15TTF_20240831_B02.tif
11:02:12 - ERROR - Failed to stream from 15TTF_20240831_B03.tif  (31 seconds)
11:02:51 - ERROR - Failed to stream from 15TTF_20240831_B04.tif  (39 seconds)
11:03:27 - ERROR - Failed to stream from 15TTF_20240831_B08.tif  (36 seconds)
```

#### Processing Pattern Issues
1. **Sequential Downloads**: Each tile × band processed one at a time
2. **No Parallelization**: Could process 4 bands simultaneously
3. **No Caching**: Re-downloading same tiles for adjacent parcels
4. **No Grouping**: Should process all parcels in a tile together

#### Impact Calculation
- 5 parcels × 144 tiles × 4 bands = 2,880 download attempts
- At 35 seconds average per attempt = 28 hours total
- Even successful downloads take 30+ seconds each

---

### 4. Missing GeoTIFF Range Request Implementation

#### What's Needed But Missing
1. **GeoTIFF Header Parser**: Read IFD (Image File Directory) to find pixel data location
2. **Pixel-to-Byte Mapping**: Calculate byte offsets for pixel windows
3. **Tile/Strip Organization**: Handle tiled vs stripped TIFFs differently
4. **Compression Handling**: Deal with compressed blocks (LZW, DEFLATE)

#### Azure Blob Storage Supports Range Requests
```python
# Azure SDK supports this, but not being used:
blob_client.download_blob(
    offset=1024,      # Start byte
    length=4096       # Number of bytes
)
```

#### Current Implementation Downloads Everything
```python
# What's actually being used:
blob_client.download_blob().readall()  # Downloads entire 250MB file
```

---

## Secondary Issues

### 5. Misleading Success Messages

#### False Positives in Logs
```
✅ Tile index built: 144 tiles indexed
✅ County satellite analysis complete: 144 tiles, ~140.6GB if downloaded
📡 Sentinel-2 data will be streamed on-demand for each parcel
```

These messages suggest streaming is working when it's not actually implemented.

### 6. Database Configuration Issues (Now Fixed)

#### Original Problem
- Configuration pointed to `biomass_production_v2` instead of `biomass_v3`
- Fixed by updating `.env` file
- V3 tables exist and have proper permissions
- This was a minor issue compared to streaming failure

### 7. Excessive Error Logging

#### Pattern Observed
```
ERROR - Failed to stream window: Input shapes do not overlap raster
```

This error appears thousands of times because system tries to clip parcels from tiles they don't actually intersect.

---

## Root Cause Timeline

### Development History
1. **Original V1**: Downloads all tiles upfront, caches in memory (14 hours)
2. **V3 Plan**: Stream only needed pixels on-demand (target: 15 minutes)
3. **V3 Phase 1**: Implemented tile indexing (✅ works)
4. **V3 Phase 2**: Should implement pixel streaming (❌ never built)
5. **Current State**: Using Phase 1 fallback that's worse than original

### Why Phase 2 Was Never Implemented
- **Complexity**: GeoTIFF range requests require deep format knowledge
- **Testing Gap**: Success of tile indexing created false confidence
- **Time Pressure**: Fallback mode allowed "working" system
- **Missing Expertise**: GeoTIFF internal structure not well understood

---

## Performance Impact Analysis

### Current Processing Flow (Per Parcel)
1. Check intersection with 144 tiles (should be 1-4)
2. For each "intersecting" tile (144):
   - For each band (4):
     - Download entire 250MB tile
     - Load into memory
     - Clip to parcel bounds
     - Usually fails (parcel doesn't actually intersect)
3. Total: 576 download attempts per parcel

### Data Transfer Calculation
- **Per Parcel**: 144 tiles × 4 bands × 250MB = 144GB
- **5 Test Parcels**: 720GB attempted downloads
- **Full County**: 70,000 parcels × 144GB = 10 Petabytes!

### Time Calculation
- **Per Download**: ~35 seconds (includes network, processing)
- **Per Parcel**: 576 attempts × 35 seconds = 5.6 hours
- **Full County**: 70,000 parcels × 5.6 hours = 44 years

---

## Why Testing Didn't Catch This

### Test Gaps
1. **Tile Index Test**: Only verified 144 tiles found (✅ passed)
2. **No Data Transfer Test**: Never measured actual bytes downloaded
3. **No Speed Test**: Didn't validate 10-15 minute target
4. **Small Sample Confusion**: 5 parcels seemed reasonable for testing
5. **Incomplete Execution**: Tests timed out, masking full scope of problem

### Misleading Indicators
- Memory usage stayed low (streaming promise seemed kept)
- Tile index built quickly (12 seconds)
- Architecture diagram looked correct
- Code structure suggested streaming was implemented

---

## Solutions Required

### Immediate (Fix Intersection Logic)
1. **Spatial Index**: Implement R-tree for tile-parcel matching
2. **Bounds Validation**: Log and verify actual intersections
3. **Coordinate Fix**: Ensure proper UTM-WGS84 transformations
4. **Result**: Reduce 144 tiles to 1-4 per parcel

### Short-term (Implement Caching)
1. **Tile Cache**: Keep downloaded tiles in memory for reuse
2. **Parcel Grouping**: Process parcels by tile locality
3. **Parallel Downloads**: Use threading for concurrent band fetching
4. **Result**: 10x speedup even without true streaming

### Long-term (True Pixel Streaming)
1. **GeoTIFF Parser**: Implement header parsing for byte ranges
2. **Range Requests**: Use Azure blob storage offset/length parameters
3. **Pixel Windows**: Calculate exact byte ranges for parcel bounds
4. **Compression Support**: Handle various TIFF compression formats
5. **Result**: 1000x speedup, <1GB data transfer

---

## Validation Requirements

### Before Declaring Success
1. **Speed Test**: Full county must complete in <20 minutes
2. **Data Transfer Test**: Measure actual bytes downloaded (<1GB target)
3. **Memory Test**: Peak memory usage <1GB
4. **Accuracy Test**: Verify NDVI calculations match V1 results
5. **Scale Test**: Process 10%, 50%, 100% of county parcels

### Monitoring Metrics
- Bytes downloaded per parcel
- Time per parcel (target: <0.01 seconds)
- Tiles accessed per parcel (target: 1-4)
- Cache hit rate (target: >90% for adjacent parcels)
- Database write success rate (target: 100%)

---

## Lessons Learned

### Technical Lessons
1. **Naming ≠ Implementation**: "stream" in method name doesn't mean streaming
2. **TODO Comments Matter**: "TODO PHASE 2" indicates incomplete work
3. **Fallbacks Can Be Worse**: Compatibility mode performed worse than original
4. **Coordinate Systems Are Hard**: UTM-WGS84 conversion errors compound
5. **GeoTIFF Is Complex**: Byte range calculation requires deep format knowledge

### Process Lessons
1. **Test End-to-End Early**: Don't just test components
2. **Measure Actual Performance**: Not just functional correctness
3. **Validate Claims**: "140GB avoided" needs measurement
4. **Time Full Runs**: 5 parcels ≠ good enough for county-scale
5. **Read the Code**: Comments often reveal true state

---

## Conclusion

The streaming satellite data architecture is fundamentally broken because **it's not actually streaming**. Instead of downloading only the needed pixels (1-10KB per parcel), it's downloading entire 250MB tiles and then clipping them. Combined with incorrect intersection logic that attempts to process ALL 144 tiles for EVERY parcel, the system is approximately **10,000x slower** than designed.

The fix requires:
1. **Immediate**: Fix tile intersection logic (144 → 1-4 tiles)
2. **Critical**: Implement actual pixel-level streaming
3. **Important**: Add caching and parallelization

Without these fixes, the V3 "streaming" architecture is unusable for production, taking weeks to process what V1 does in hours.

---

**Document Status**: Complete  
**Next Action**: Implement fixes in priority order  
**Estimated Fix Time**: 2-3 days for full implementation  
**Risk Level**: 🔴 CRITICAL - System completely non-functional at scale