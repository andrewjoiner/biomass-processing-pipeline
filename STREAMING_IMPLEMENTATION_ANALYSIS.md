# Streaming Implementation Analysis - Why It's Not Working

**Date**: January 11, 2025  
**Branch**: `streaming-satellite-data`  
**Critical Finding**: The streaming architecture is fundamentally incomplete, NOT a preprocessing limitation

---

## Executive Summary

The V3 "streaming" pipeline is **not actually streaming data**. Instead, it's downloading entire 250MB compressed tiles for each parcel, which is actually **worse than the V1 bulk download approach** because V1 at least cached tiles in memory. This is NOT a preprocessing limitation - it's an incomplete implementation.

---

## The Core Problem: No True Streaming Implementation

### What the Code Claims vs Reality

**The Method Name** (`_stream_parcel_window_from_tile`) suggests streaming, but the implementation reveals:

```python
# From blob_manager_v3.py, lines ~1560-1580
def _stream_parcel_window_from_tile(self, blob_path: str, parcel_geometry: Dict, 
                                  tile_info: Dict) -> Optional[Dict]:
    """
    Stream only the pixel window needed for a parcel from a Sentinel-2 tile
    PHASE 3: True pixel streaming with GeoTIFF range requests
    """
    
    # ... code checks for preprocessed tiles first ...
    
    # Fallback: Download full tile and cache it (Phase 2 approach)
    logger.info(f"📦 Falling back to full tile download and caching for {blob_path}")
    blob_data = self.download_blob_to_memory(
        self.config['containers']['sentinel2'], 
        blob_path
    )
```

**The Reality**: It ALWAYS falls back to `download_blob_to_memory()` which downloads the ENTIRE tile.

---

## Why Preprocessing Won't Solve This

### The Preprocessing Misconception

The STATE_LEVEL_PREPROCESSING_PLAN suggests preprocessing tiles from compressed to uncompressed format would enable streaming. However:

1. **The code still downloads entire tiles** whether compressed or uncompressed
2. **Uncompressed tiles are 3x LARGER** (750MB vs 250MB), making downloads slower
3. **The fundamental issue is the download approach**, not compression

### What the Code Actually Does with Preprocessed Tiles

```python
# Lines ~1550-1560
if preprocessed_path:
    logger.info(f"🎯 Using preprocessed uncompressed tile: {preprocessed_path}")
    
    # Download preprocessed tile (should enable streaming)
    blob_data = self.download_blob_to_memory(  # <-- STILL DOWNLOADS ENTIRE TILE!
        self.preprocessed_container, 
        preprocessed_path
    )
```

Even with preprocessed tiles, it still calls `download_blob_to_memory()` - downloading the entire 750MB uncompressed tile!

---

## The Missing Implementation: True Pixel Streaming

### What Should Be Implemented (But Isn't)

True streaming requires:

1. **Parse GeoTIFF header** (first 8KB) to understand tile structure
2. **Calculate byte ranges** for the specific pixel window needed
3. **Use Azure Blob Storage range requests** to download only those bytes

```python
# What SHOULD be implemented:
def stream_pixel_window(blob_client, parcel_bounds, tile_info):
    # 1. Download just the header (8KB)
    header_data = blob_client.download_blob(offset=0, length=8192).readall()
    
    # 2. Parse header to find pixel data location
    pixel_layout = parse_geotiff_header(header_data)
    
    # 3. Calculate which pixels we need
    pixel_window = calculate_pixel_intersection(parcel_bounds, tile_info['transform'])
    
    # 4. Calculate byte ranges for those pixels
    byte_ranges = calculate_byte_ranges(pixel_layout, pixel_window)
    
    # 5. Download ONLY those bytes (typically 1-50KB for a parcel)
    pixel_data = blob_client.download_blob(
        offset=byte_ranges['start'],
        length=byte_ranges['length']
    ).readall()
    
    return decode_pixels(pixel_data)
```

### Why This Wasn't Implemented

Looking at the code, there's an attempt at GeoTIFF parsing (`_parse_geotiff_header()` at line 1189), but:

1. **It's never called** in the actual streaming path
2. **It only parses basic header info**, not pixel byte ranges
3. **The complex tile/strip organization** of GeoTIFF isn't handled
4. **Compression within tiles** (LZW, DEFLATE) isn't addressed

---

## Performance Impact Analysis

### Current Performance (No Streaming)
- **Per parcel**: Downloads 4 bands × 250MB = 1GB
- **Actual data needed**: ~10KB per parcel
- **Waste ratio**: 99.999% unnecessary data transfer
- **Processing speed**: ~4.7 parcels/minute (observed)
- **DeWitt County (10,297 parcels)**: ~36 hours

### With True Streaming
- **Per parcel**: Downloads 4 bands × 10KB = 40KB  
- **Speed improvement**: ~1000x faster downloads
- **Processing speed**: 300+ parcels/second (achievable)
- **DeWitt County**: ~30 seconds

### With Preprocessing (Still No Streaming)
- **Per parcel**: Downloads 4 bands × 750MB = 3GB (WORSE!)
- **Processing speed**: ~1.5 parcels/minute (3x slower)
- **DeWitt County**: ~108 hours

---

## Why Caching Also Fails

The code has a `streaming_tile_cache` but it's poorly implemented:

1. **Cache key includes band** - Same tile downloaded 4 times (once per band)
2. **No cache sharing between parcels** - Each parcel re-downloads
3. **LRU eviction too aggressive** - Cache thrashes with 500 tile limit

```python
# Current cache check (ineffective)
cache_key = f"{blob_path}_{band}"  # Different key per band!
if cache_key in self.streaming_tile_cache:
    # This rarely hits because of band-specific keys
```

---

## The Solution Path

### Option 1: Implement True Streaming (Complex)
- Requires deep GeoTIFF format knowledge
- Must handle tile/strip organization
- Need to decode compression blocks
- Estimated effort: 1-2 weeks

### Option 2: Fix Caching (Simple, Immediate)
- Download each tile ONCE, cache for all parcels
- Share cache across bands
- Increase cache size for county processing
- Estimated effort: 2-4 hours

### Option 3: Revert to V1 Bulk Download (Proven)
- Download all tiles upfront
- Cache in memory
- Process parcels from cache
- Already works, just needs integration

---

## Conclusion

**The streaming failure is NOT a preprocessing limitation.** It's a fundamental implementation gap. The code never actually implements pixel-level streaming - it always downloads entire tiles. Preprocessing to uncompressed format would actually make performance WORSE (3x larger downloads).

The most practical immediate solution is to fix the caching mechanism so tiles are downloaded once and reused across all parcels. True pixel-level streaming, while ideal, requires significant additional implementation work.

---

## Key Code Locations

- **Fake streaming method**: `blob_manager_v3.py:1540-1620` (`_stream_parcel_window_from_tile`)
- **Full download method**: `blob_manager_v3.py:380-410` (`download_blob_to_memory`)
- **Broken cache**: `blob_manager_v3.py:1580-1600` (cache check logic)
- **Unused GeoTIFF parser**: `blob_manager_v3.py:1189-1250` (`_parse_geotiff_header`)

---

**Bottom Line**: The V3 architecture promises streaming but delivers something worse than V1's bulk download. The fix requires either implementing true streaming (complex) or proper caching (simple).