#!/usr/bin/env python3
"""
Database Manager v1 - Optimized PostgreSQL Interface
Clean, high-performance database access for biomass processing pipeline
"""

import json
import logging
import time
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

from ..config.database_config_v3 import (
    get_database_config, 
    get_database_queries,
    CDL_CODES,
    URBAN_CODES,
    WORLDCOVER_CLASSES
)
from ..config.processing_config_v3 import get_processing_config

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    High-performance PostgreSQL database manager with connection pooling
    and optimized queries for biomass processing
    """
    
    def __init__(self):
        self.config = get_database_config()
        self.queries = get_database_queries()
        self.processing_config = get_processing_config()
        
        # Initialize connection pools for each database
        self.pools = {}
        self._initialize_connection_pools()
        
    def _initialize_connection_pools(self):
        """Initialize threaded connection pools for each database with optimized sizes per database"""
        
        for db_name, db_config in self.config.items():
            try:
                # Different pool sizes based on database usage patterns
                if db_name == 'forestry':
                    # Forestry database gets most connections due to heavy FIA queries
                    pool_config = {
                        'minconn': 2,
                        'maxconn': 20,  # Increased for forestry DB performance
                        'cursor_factory': psycopg2.extras.RealDictCursor,
                        'connect_timeout': 30
                    }
                    logger.info(f"Using high-capacity pool for {db_name} database (20 connections)")
                elif db_name == 'biomass_output':
                    # Output database gets moderate connections for batch inserts
                    pool_config = {
                        'minconn': 1,
                        'maxconn': 8,
                        'cursor_factory': psycopg2.extras.RealDictCursor,
                        'connect_timeout': 30
                    }
                    logger.info(f"Using medium-capacity pool for {db_name} database (8 connections)")
                else:
                    # Parcels and crops databases use standard pool
                    pool_config = {
                        'minconn': 1,
                        'maxconn': 5,
                        'cursor_factory': psycopg2.extras.RealDictCursor,
                        'connect_timeout': 30
                    }
                    logger.info(f"Using standard pool for {db_name} database (5 connections)")
                
                self.pools[db_name] = ThreadedConnectionPool(
                    **pool_config,
                    **db_config
                )
                logger.info(f"Database {db_name} connection pool: OK")
            except Exception as e:
                logger.error(f"Failed to initialize {db_name} database pool: {e}")
                raise
    
    @contextmanager
    def get_connection(self, database: str, timeout: int = 60, retries: int = None):
        """
        Context manager for database connections with automatic cleanup, retry logic, and timeout
        
        Args:
            database: Database name ('parcels', 'crops', 'forestry', 'biomass_output')
            timeout: Connection timeout in seconds
            retries: Number of retry attempts (None for auto-selection based on database)
            
        Yields:
            Database connection
        """
        conn = None
        last_exception = None
        
        # Auto-select retry count based on database reliability needs
        if retries is None:
            retries = 5 if database == 'forestry' else 3  # More retries for problematic forestry DB
        
        for attempt in range(retries + 1):
            try:
                conn = self.pools[database].getconn()
                if conn.closed != 0:
                    # Connection is closed, put it back and get a new one
                    self.pools[database].putconn(conn, close=True)
                    conn = self.pools[database].getconn()
                yield conn
                return
            except psycopg2.OperationalError as e:
                last_exception = e
                if conn:
                    try:
                        conn.rollback()
                        self.pools[database].putconn(conn, close=True)
                    except:
                        pass
                    conn = None
                
                if attempt < retries:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Database connection failed on {database}, attempt {attempt + 1}/{retries + 1}, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Database connection failed on {database} after {retries + 1} attempts: {e}")
                    raise
            except Exception as e:
                if conn:
                    try:
                        conn.rollback()
                    except:
                        pass
                last_exception = e
                logger.error(f"Database error on {database}: {e}")
                raise
            finally:
                if conn and conn.closed == 0:
                    try:
                        self.pools[database].putconn(conn)
                    except Exception as e:
                        logger.warning(f"Error returning connection to {database} pool: {e}")
                    conn = None
    
    def get_county_bounds(self, fips_state: str, fips_county: str) -> Optional[Tuple[float, float, float, float]]:
        """
        Get spatial bounds for a county
        
        Args:
            fips_state: 2-digit state FIPS code
            fips_county: 3-digit county FIPS code
            
        Returns:
            Tuple of (min_lon, min_lat, max_lon, max_lat) or None if not found
        """
        with self.get_connection('parcels') as conn:
            cursor = conn.cursor()
            cursor.execute(self.queries['get_county_bounds'], (fips_state, fips_county))
            result = cursor.fetchone()
            
            if result and all(result):
                return (result['min_lon'], result['min_lat'], result['max_lon'], result['max_lat'])
            return None
    
    def get_county_parcels(self, fips_state: str, fips_county: str, 
                          min_acres: Optional[float] = None, max_acres: Optional[float] = None,
                          limit: Optional[int] = None) -> List[Dict]:
        """
        Get parcels for a specific county with optimized query
        
        Args:
            fips_state: 2-digit state FIPS code
            fips_county: 3-digit county FIPS code
            min_acres: Minimum parcel size in acres
            max_acres: Maximum parcel size in acres  
            limit: Maximum number of parcels to return
            
        Returns:
            List of parcel dictionaries with geometry and metadata
        """
        # Convert minimum acres to square meters for ST_Area(geography(geometry))
        min_acres_val = min_acres or self.processing_config['min_parcel_area_acres']
        min_area_m2 = min_acres_val * 4047  # acres to square meters (0.1 acres = 404.7 m²)
        # No artificial limit - process all parcels in county
        # limit = limit or 50000  # Removed: artificial limit not appropriate for production
        
        with self.get_connection('parcels') as conn:
            cursor = conn.cursor()
            
            # Build query dynamically to handle optional limit
            base_query = self.queries['get_county_parcels_optimized']
            params = (fips_state, fips_county, min_area_m2)
            
            if limit is not None:
                query = base_query + " LIMIT %s"
                params = params + (limit,)
            else:
                query = base_query
            
            cursor.execute(query, params)
            
            parcels = []
            for row in cursor.fetchall():
                geometry_dict = json.loads(row['geometry'])
                # Calculate acres from geometry instead of using unreliable database field
                from ..utils.geometry_utils_v1 import calculate_geometry_area_acres
                calculated_acres = calculate_geometry_area_acres(geometry_dict)
                
                parcel = {
                    'parcelid': row['parcelid'],  # Keep consistent with processor expectations
                    'parcel_id': row['parcelid'], # Also provide this for backward compatibility
                    'geometry': geometry_dict,
                    'postgis_geometry': row['postgis_geometry'],
                    'acres': calculated_acres,
                    'centroid_lon': float(row['centroid_lon']),
                    'centroid_lat': float(row['centroid_lat'])
                }
                parcels.append(parcel)
            
            logger.info(f"Loaded {len(parcels)} parcels for county {fips_state}{fips_county}")
            return parcels
    
    def get_county_parcels_batch(self, fips_state: str, fips_county: str, 
                               offset: int, limit: int,
                               min_acres: Optional[float] = None, 
                               max_acres: Optional[float] = None) -> List[Dict]:
        """
        Get a batch of parcels for a county with OFFSET support for efficient batch processing
        
        Args:
            fips_state: 2-digit state FIPS code
            fips_county: 3-digit county FIPS code
            offset: Number of parcels to skip
            limit: Number of parcels to return
            min_acres: Minimum parcel size in acres
            max_acres: Maximum parcel size in acres  
            
        Returns:
            List of parcel dictionaries with geometry and metadata
        """
        # Convert minimum acres to square meters for ST_Area(geography(geometry))
        min_acres_val = min_acres or self.processing_config['min_parcel_area_acres']
        min_area_m2 = min_acres_val * 4047  # acres to square meters
        
        # Create query with OFFSET support
        query = """
            SELECT 
                parcelid,
                ST_AsGeoJSON(geometry) as geometry,
                ST_AsText(geometry) as postgis_geometry,
                ST_X(ST_Centroid(geometry)) as centroid_lon,
                ST_Y(ST_Centroid(geometry)) as centroid_lat,
                ST_XMin(geometry) as min_lon,
                ST_YMin(geometry) as min_lat,
                ST_XMax(geometry) as max_lon,
                ST_YMax(geometry) as max_lat
            FROM parcels
            WHERE fipsstate = %s AND fipscounty = %s
            AND geometry IS NOT NULL
            AND ST_Area(geography(geometry)) > %s
            ORDER BY parcelid
            LIMIT %s OFFSET %s
        """
        
        with self.get_connection('parcels') as conn:
            cursor = conn.cursor()
            cursor.execute(query, (fips_state, fips_county, min_area_m2, limit, offset))
            
            parcels = []
            for row in cursor.fetchall():
                try:
                    geometry_dict = json.loads(row['geometry'])
                    # Calculate acres from geometry instead of using unreliable database field
                    from ..utils.geometry_utils_v1 import calculate_geometry_area_acres
                    calculated_acres = calculate_geometry_area_acres(geometry_dict)
                    
                    parcel = {
                        'parcelid': row['parcelid'],
                        'parcel_id': row['parcelid'],
                        'geometry': geometry_dict,
                        'postgis_geometry': row['postgis_geometry'],
                        'acres': calculated_acres,
                        'centroid_lon': float(row['centroid_lon']),
                        'centroid_lat': float(row['centroid_lat'])
                    }
                    parcels.append(parcel)
                except Exception as e:
                    logger.warning(f"Error processing parcel row: {e}")
                    continue
            
            logger.debug(f"Loaded batch: {len(parcels)} parcels (offset: {offset}, limit: {limit})")
            return parcels
    
    def get_cdl_intersections_single(self, parcel_postgis_geometry: str) -> List[Dict]:
        """
        Get CDL crop intersections for a single parcel
        
        Args:
            parcel_postgis_geometry: PostGIS geometry string
            
        Returns:
            List of crop intersection dictionaries
        """
        with self.get_connection('crops') as conn:
            cursor = conn.cursor()
            cursor.execute(
                self.queries['get_cdl_intersections'],
                (parcel_postgis_geometry, parcel_postgis_geometry, parcel_postgis_geometry, 
                 parcel_postgis_geometry, parcel_postgis_geometry)
            )
            
            intersections = []
            result_rows = cursor.fetchall()
            
            if not result_rows:
                logger.debug(f"No CDL intersections found for parcel")
                return intersections
            
            for row in result_rows:
                try:
                    # Validate row structure
                    if not isinstance(row, dict):
                        logger.warning(f"Unexpected row type in CDL query result: {type(row)}")
                        continue
                    
                    # Check required fields exist
                    required_fields = ['crop_code', 'intersection_area_m2', 'parcel_area_m2', 'coverage_percent']
                    if not all(field in row for field in required_fields):
                        logger.warning(f"Missing required fields in CDL result: {row.keys()}")
                        continue
                    
                    # Skip zero or null intersections
                    if not row['intersection_area_m2'] or row['intersection_area_m2'] <= 0:
                        continue
                    
                    # Skip null or invalid parcel areas
                    if not row['parcel_area_m2'] or row['parcel_area_m2'] <= 0:
                        logger.warning(f"Invalid parcel area in CDL result: {row['parcel_area_m2']}")
                        continue
                    
                    crop_code = row['crop_code']
                    intersection = {
                        'crop_code': crop_code,
                        'crop_name': CDL_CODES.get(crop_code, f'Unknown_{crop_code}'),
                        'intersection_area_m2': float(row['intersection_area_m2']),
                        'parcel_area_m2': float(row['parcel_area_m2']),
                        'coverage_percent': float(row['coverage_percent']) if row['coverage_percent'] else 0.0
                    }
                    intersections.append(intersection)
                    
                except (KeyError, ValueError, TypeError) as e:
                    logger.warning(f"Error processing CDL intersection row: {e}, row: {row}")
                    continue
            
            logger.debug(f"Found {len(intersections)} valid CDL intersections")
            return intersections
    
    def get_cdl_intersections_bulk(self, fips_state: str, fips_county: str, 
                                   parcel_list: Optional[List[Dict]] = None) -> Dict[str, List[Dict]]:
        """
        Get CDL crop intersections for county parcels in bulk (OPTIMIZATION)
        Since parcels and CDL data are in different databases, we do individual parcel analysis
        
        Args:
            fips_state: 2-digit state FIPS code
            fips_county: 3-digit county FIPS code
            parcel_list: Optional pre-loaded parcel list to avoid re-querying
            
        Returns:
            Dictionary mapping parcel_id to list of crop intersections
        """
        # Use provided parcels or get them fresh
        if parcel_list:
            parcels = parcel_list
        else:
            parcels = self.get_county_parcels(fips_state, fips_county, limit=None)
        
        intersections_by_parcel = {}
        
        # Process each parcel individually since we can't do cross-database joins
        for parcel in parcels:
            try:
                intersections = self.get_cdl_intersections_single(parcel['postgis_geometry'])
                if intersections:
                    intersections_by_parcel[parcel['parcel_id']] = intersections
            except Exception as e:
                logger.warning(f"Failed to get CDL intersections for parcel {parcel['parcel_id']}: {e}")
                continue
                
        logger.info(f"Bulk loaded CDL intersections for {len(intersections_by_parcel)} parcels")
        return intersections_by_parcel
    
    def get_nearby_fia_plots(self, parcel_postgis_geometry: str, 
                           search_radius_degrees: Optional[float] = None) -> List[Dict]:
        """
        Get nearby FIA forest inventory plots for biomass estimation (updated for forestry schema)
        
        Args:
            parcel_postgis_geometry: PostGIS geometry string
            search_radius_degrees: Search radius in degrees (default from config)
            
        Returns:
            List of nearby FIA plot dictionaries
        """
        radius = search_radius_degrees or self.processing_config.get('fia_search_radius_degrees', 0.1)
        
        try:
            with self.get_connection('forestry') as conn:
                cursor = conn.cursor()
                cursor.execute(
                    self.queries['get_nearby_fia_plots'],
                    (parcel_postgis_geometry, parcel_postgis_geometry, radius)
                )
                
                plots = []
                for row in cursor.fetchall():
                    plot = dict(row)  # RealDictCursor gives us dict-like access
                    plots.append(plot)
                
                logger.debug(f"Found {len(plots)} FIA plots within {radius} degrees")
                return plots
                
        except Exception as e:
            logger.error(f"Error getting nearby FIA plots: {e}")
            return []
    
    def get_fia_trees_for_plots(self, plot_cns: List[str]) -> List[Dict]:
        """
        Get FIA tree biomass data for specific plots
        
        Args:
            plot_cns: List of plot CN identifiers
            
        Returns:
            List of FIA tree records with biomass data
        """
        try:
            if not plot_cns:
                return []
            
            with self.get_connection('forestry') as conn:
                cursor = conn.cursor()
                cursor.execute(self.queries['get_fia_trees_for_plots'], (plot_cns,))
                trees = cursor.fetchall()
                
                logger.debug(f"Found {len(trees)} FIA trees for {len(plot_cns)} plots")
                return [dict(tree) for tree in trees]
                
        except Exception as e:
            logger.error(f"Error getting FIA trees for plots: {e}")
            return []
    
    def test_connections(self) -> Dict[str, bool]:
        """
        Test all database connections
        
        Returns:
            Dictionary mapping database names to connection status
        """
        results = {}
        
        for db_name in self.config.keys():
            try:
                with self.get_connection(db_name) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    results[db_name] = True
                    logger.info(f"Database {db_name} connection: OK")
            except Exception as e:
                results[db_name] = False
                logger.error(f"Database {db_name} connection failed: {e}")
        
        return results
    
    def get_database_stats(self) -> Dict[str, Dict]:
        """
        Get database statistics for monitoring
        
        Returns:
            Dictionary with database statistics
        """
        stats = {}
        
        # Parcels database stats
        try:
            with self.get_connection('parcels') as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) as parcel_count FROM parcels WHERE geometry IS NOT NULL")
                parcel_count = cursor.fetchone()['parcel_count']
                stats['parcels'] = {'total_parcels': parcel_count}
        except Exception as e:
            logger.error(f"Failed to get parcels stats: {e}")
            stats['parcels'] = {'error': str(e)}
        
        # Crops database stats  
        try:
            with self.get_connection('crops') as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) as cdl_count FROM us_cdl_data")
                cdl_count = cursor.fetchone()['cdl_count']
                stats['crops'] = {'total_cdl_polygons': cdl_count}
        except Exception as e:
            logger.error(f"Failed to get crops stats: {e}")
            stats['crops'] = {'error': str(e)}
        
        # Forestry database stats
        try:
            with self.get_connection('forestry') as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) as plot_count FROM plot")
                plot_count = cursor.fetchone()['plot_count']
                cursor.execute("SELECT COUNT(*) as tree_count FROM tree")
                tree_count = cursor.fetchone()['tree_count']
                stats['forestry'] = {
                    'total_fia_plots': plot_count,
                    'total_trees': tree_count
                }
        except Exception as e:
            logger.error(f"Failed to get forestry stats: {e}")
            stats['forestry'] = {'error': str(e)}
        
        return stats
    
    def create_checkpoint(self, fips_state: str, fips_county: str, batch_num: int, 
                         offset: int, total_processed: int, total_errors: int) -> bool:
        """
        Create a processing checkpoint for resume capability
        
        Args:
            fips_state: State FIPS code
            fips_county: County FIPS code
            batch_num: Current batch number
            offset: Current parcel offset
            total_processed: Total parcels processed successfully
            total_errors: Total errors encountered
            
        Returns:
            True if checkpoint created successfully
        """
        try:
            with self.get_connection('biomass_output') as conn:
                cursor = conn.cursor()
                
                # Create checkpoint table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS processing_checkpoints (
                        id SERIAL PRIMARY KEY,
                        county_fips TEXT NOT NULL,
                        batch_num INTEGER NOT NULL,
                        parcel_offset INTEGER NOT NULL,
                        parcels_processed INTEGER NOT NULL,
                        errors_count INTEGER NOT NULL,
                        checkpoint_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
                        status TEXT DEFAULT 'in_progress',
                        UNIQUE(county_fips)
                    )
                """)
                
                # Upsert checkpoint
                cursor.execute("""
                    INSERT INTO processing_checkpoints 
                    (county_fips, batch_num, parcel_offset, parcels_processed, errors_count)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (county_fips) DO UPDATE SET
                        batch_num = EXCLUDED.batch_num,
                        parcel_offset = EXCLUDED.parcel_offset,
                        parcels_processed = EXCLUDED.parcels_processed,
                        errors_count = EXCLUDED.errors_count,
                        checkpoint_timestamp = NOW(),
                        status = 'in_progress'
                """, (f"{fips_state}{fips_county}", batch_num, offset, total_processed, total_errors))
                
                conn.commit()
                logger.debug(f"Created checkpoint for county {fips_state}{fips_county} at batch {batch_num}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to create checkpoint: {e}")
            return False
    
    def get_checkpoint(self, fips_state: str, fips_county: str) -> Optional[Dict]:
        """
        Get existing checkpoint for county processing
        
        Args:
            fips_state: State FIPS code
            fips_county: County FIPS code
            
        Returns:
            Checkpoint data dictionary or None
        """
        try:
            with self.get_connection('biomass_output') as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT batch_num, parcel_offset, parcels_processed, errors_count,
                           checkpoint_timestamp, status
                    FROM processing_checkpoints
                    WHERE county_fips = %s AND status = 'in_progress'
                """, (f"{fips_state}{fips_county}",))
                
                result = cursor.fetchone()
                if result:
                    return dict(result)
                return None
                
        except Exception as e:
            logger.error(f"Failed to get checkpoint: {e}")
            return None
    
    def complete_county_processing(self, fips_state: str, fips_county: str) -> bool:
        """
        Mark county processing as complete
        
        Args:
            fips_state: State FIPS code
            fips_county: County FIPS code
            
        Returns:
            True if marked complete successfully
        """
        try:
            with self.get_connection('biomass_output') as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE processing_checkpoints 
                    SET status = 'completed', checkpoint_timestamp = NOW()
                    WHERE county_fips = %s
                """, (f"{fips_state}{fips_county}",))
                
                conn.commit()
                logger.info(f"Marked county {fips_state}{fips_county} as completed")
                return True
                
        except Exception as e:
            logger.error(f"Failed to mark county complete: {e}")
            return False
    
    def save_biomass_results(self, parcel_results: List[Dict]) -> bool:
        """
        Save biomass analysis results to the output database
        
        Args:
            parcel_results: List of comprehensive parcel analysis results
            
        Returns:
            True if successful, False otherwise
        """
        if not parcel_results:
            logger.warning("No results to save")
            return True
        
        try:
            # Connect to output database (biomass_production_v2)
            with self.get_connection('biomass_output') as conn:
                cursor = conn.cursor()
                
                # Create table if it doesn't exist
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS parcel_biomass_analysis (
                    id SERIAL PRIMARY KEY,
                    parcel_id TEXT NOT NULL,
                    county_fips TEXT NOT NULL,
                    total_acres NUMERIC(10,3),
                    centroid_lon NUMERIC(12,8),
                    centroid_lat NUMERIC(12,8),
                    
                    -- Land cover breakdown
                    forest_acres NUMERIC(10,3),
                    cropland_acres NUMERIC(10,3),
                    other_acres NUMERIC(10,3),
                    forest_percentage NUMERIC(5,2),
                    cropland_percentage NUMERIC(5,2),
                    
                    -- Biomass totals
                    forest_biomass_tons NUMERIC(12,3),
                    forest_harvestable_tons NUMERIC(12,3),
                    forest_residue_tons NUMERIC(12,3),
                    crop_yield_tons NUMERIC(12,3),
                    crop_residue_tons NUMERIC(12,3),
                    total_biomass_tons NUMERIC(12,3),
                    
                    -- Vegetation indices
                    ndvi NUMERIC(6,4),
                    evi NUMERIC(6,4),
                    savi NUMERIC(6,4),
                    ndwi NUMERIC(6,4),
                    
                    -- Analysis metadata
                    confidence_score NUMERIC(4,3),
                    data_sources TEXT[],
                    processing_timestamp TIMESTAMP,
                    
                    -- Detailed analysis (JSON)
                    landcover_analysis JSONB,
                    forest_analysis JSONB,
                    crop_analysis JSONB,
                    
                    -- Indexes for efficient querying
                    CONSTRAINT unique_parcel_analysis UNIQUE (parcel_id, processing_timestamp)
                );
                
                -- Create indexes if they don't exist
                CREATE INDEX IF NOT EXISTS idx_parcel_biomass_county ON parcel_biomass_analysis (county_fips);
                CREATE INDEX IF NOT EXISTS idx_parcel_biomass_timestamp ON parcel_biomass_analysis (processing_timestamp);
                CREATE INDEX IF NOT EXISTS idx_parcel_biomass_location ON parcel_biomass_analysis (centroid_lat, centroid_lon);
                """
                
                cursor.execute(create_table_sql)
                conn.commit()
                
                # Prepare batch insert
                insert_sql = """
                INSERT INTO parcel_biomass_analysis (
                    parcel_id, county_fips, total_acres, centroid_lon, centroid_lat,
                    forest_acres, cropland_acres, other_acres, forest_percentage, cropland_percentage,
                    forest_biomass_tons, forest_harvestable_tons, forest_residue_tons,
                    crop_yield_tons, crop_residue_tons, total_biomass_tons,
                    ndvi, evi, savi, ndwi, confidence_score, data_sources,
                    processing_timestamp, landcover_analysis, forest_analysis, crop_analysis
                ) VALUES %s
                ON CONFLICT (parcel_id, processing_timestamp) DO UPDATE SET
                    total_acres = EXCLUDED.total_acres,
                    forest_biomass_tons = EXCLUDED.forest_biomass_tons,
                    crop_yield_tons = EXCLUDED.crop_yield_tons,
                    total_biomass_tons = EXCLUDED.total_biomass_tons,
                    confidence_score = EXCLUDED.confidence_score
                """
                
                # Prepare data for batch insert
                batch_data = []
                for result in parcel_results:
                    # Extract allocation factors
                    allocation = result.get('allocation_factors', {})
                    forest_acres = allocation.get('forest_acres', 0)
                    cropland_acres = allocation.get('cropland_acres', 0) 
                    total_acres = result.get('total_acres', 0)
                    other_acres = max(0, total_acres - forest_acres - cropland_acres)
                    
                    # Calculate percentages
                    forest_pct = (forest_acres / total_acres * 100) if total_acres > 0 else 0
                    cropland_pct = (cropland_acres / total_acres * 100) if total_acres > 0 else 0
                    
                    # Extract vegetation indices
                    veg_indices = result.get('vegetation_indices', {})
                    
                    # Calculate total biomass
                    total_biomass = (
                        result.get('forest_biomass_tons', 0) +
                        result.get('crop_yield_tons', 0) +
                        result.get('crop_residue_tons', 0)
                    )
                    
                    batch_data.append((
                        result['parcel_id'],
                        result['county_fips'],
                        result.get('total_acres', 0),
                        result.get('centroid_lon', 0),
                        result.get('centroid_lat', 0),
                        forest_acres,
                        cropland_acres,
                        other_acres,
                        round(forest_pct, 2),
                        round(cropland_pct, 2),
                        result.get('forest_biomass_tons', 0),
                        result.get('forest_harvestable_tons', 0),
                        result.get('forest_residue_tons', 0),
                        result.get('crop_yield_tons', 0),
                        result.get('crop_residue_tons', 0),
                        round(total_biomass, 3),
                        veg_indices.get('ndvi') if veg_indices else None,
                        veg_indices.get('evi') if veg_indices else None,
                        veg_indices.get('savi') if veg_indices else None,
                        veg_indices.get('ndwi') if veg_indices else None,
                        result.get('confidence_score', 0.5),
                        result.get('data_sources_used', []),
                        result.get('processing_timestamp'),
                        json.dumps(result.get('landcover_analysis')),
                        json.dumps(result.get('forest_analysis')),
                        json.dumps(result.get('crop_analysis'))
                    ))
                
                # Execute batch insert using execute_values for better performance
                psycopg2.extras.execute_values(
                    cursor, insert_sql, batch_data,
                    template=None, page_size=1000
                )
                
                conn.commit()
                
                logger.info(f"Successfully saved {len(parcel_results)} biomass analysis results to database")
                
                # Also save V3 enhanced results to separate tables
                success_v3 = self.save_v3_enhanced_results(parcel_results)
                if not success_v3:
                    logger.warning("V3 enhanced results save failed, but V1 results saved successfully")
                
                return True
                
        except Exception as e:
            logger.error(f"Failed to save biomass results to database: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def save_v3_enhanced_results(self, parcel_results: List[Dict]) -> bool:
        """
        Save enhanced V3 results to forestry and crop analysis tables
        Extracts detailed data from analyzer outputs for relational storage
        
        Args:
            parcel_results: List of comprehensive parcel analysis results
            
        Returns:
            True if successful, False otherwise
        """
        if not parcel_results:
            logger.warning("No V3 results to save")
            return True
        
        try:
            # Connect to V3 output database
            with self.get_connection('biomass_output') as conn:
                cursor = conn.cursor()
                
                # Prepare forestry and crop data for batch insert
                forestry_records = []
                crop_records = []
                
                for result in parcel_results:
                    parcel_id = result['parcel_id']
                    county_fips = result['county_fips']
                    processing_timestamp = result.get('processing_timestamp')
                    vegetation_indices = result.get('vegetation_indices', {})
                    
                    # Process forest analysis data
                    forest_analysis = result.get('forest_analysis')
                    logger.info(f"🌲 Parcel {parcel_id} forest_analysis type: {type(forest_analysis)}, value: {forest_analysis is not None}")
                    if forest_analysis and isinstance(forest_analysis, list):
                        logger.info(f"🌲 Parcel {parcel_id} has {len(forest_analysis)} forest records")
                        for forest_record in forest_analysis:
                            if forest_record and forest_record.get('biomass_type') == 'forest':
                                logger.info(f"✅ Processing forest record for {parcel_id}: {forest_record.get('area_acres')} acres, biomass_type={forest_record.get('biomass_type')}")
                                forestry_record = self._extract_forestry_record(
                                    parcel_id, county_fips, processing_timestamp, 
                                    forest_record, vegetation_indices
                                )
                                if forestry_record:
                                    forestry_records.append(forestry_record)
                                    logger.info(f"✅ Added forest record to save list for {parcel_id}")
                            else:
                                logger.warning(f"❌ Forest record missing biomass_type='forest': {forest_record}")
                    elif forest_analysis:
                        logger.warning(f"❌ Parcel {parcel_id} forest_analysis is not a list: {type(forest_analysis)}")
                    
                    # Process crop analysis data  
                    crop_analysis = result.get('crop_analysis')
                    logger.info(f"🌾 Parcel {parcel_id} crop_analysis type: {type(crop_analysis)}, value: {crop_analysis is not None}")
                    if crop_analysis and isinstance(crop_analysis, list):
                        logger.info(f"🌾 Parcel {parcel_id} has {len(crop_analysis)} crop records")
                        dominant_crop = None
                        max_area = 0
                        
                        # Find dominant crop
                        for crop_record in crop_analysis:
                            if crop_record and crop_record.get('biomass_type') == 'crop':
                                area = crop_record.get('area_acres', 0)
                                if area > max_area:
                                    max_area = area
                                    dominant_crop = crop_record
                            else:
                                logger.warning(f"❌ Crop record missing biomass_type='crop': {crop_record}")
                        
                        # Process all crop records
                        for crop_record in crop_analysis:
                            if crop_record and crop_record.get('biomass_type') == 'crop':
                                logger.info(f"✅ Processing crop record for {parcel_id}: {crop_record.get('source_name')}, {crop_record.get('area_acres')} acres, biomass_type={crop_record.get('biomass_type')}")
                                is_dominant = (crop_record == dominant_crop)
                                crop_v3_record = self._extract_crop_record(
                                    parcel_id, county_fips, processing_timestamp,
                                    crop_record, vegetation_indices, is_dominant
                                )
                                if crop_v3_record:
                                    crop_records.append(crop_v3_record)
                                    logger.info(f"✅ Added crop record to save list for {parcel_id}: {crop_record.get('source_name')}")
                    elif crop_analysis:
                        logger.warning(f"❌ Parcel {parcel_id} crop_analysis is not a list: {type(crop_analysis)}")
                
                # Bulk insert forestry records
                if forestry_records:
                    self._bulk_insert_forestry_records(cursor, forestry_records)
                    logger.debug(f"Saved {len(forestry_records)} forestry records")
                
                # Bulk insert crop records
                if crop_records:
                    self._bulk_insert_crop_records(cursor, crop_records)
                    logger.debug(f"Saved {len(crop_records)} crop records")
                
                conn.commit()
                logger.info(f"Successfully saved V3 enhanced results: {len(forestry_records)} forestry, {len(crop_records)} crop records")
                return True
                
        except Exception as e:
            logger.error(f"Failed to save V3 enhanced results: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _extract_forestry_record(self, parcel_id: str, county_fips: str, 
                               processing_timestamp, forest_record: Dict, 
                               vegetation_indices: Dict) -> Optional[Tuple]:
        """Extract forestry record data for database insert"""
        try:
            return (
                parcel_id,
                county_fips,
                processing_timestamp,
                forest_record.get('total_standing_biomass_tons'),
                forest_record.get('area_acres'),
                forest_record.get('coverage_percent'),
                forest_record.get('stand_age_avg'),
                forest_record.get('forest_type_dominant'),
                forest_record.get('harvest_probability'),
                forest_record.get('last_treatment_years'),
                forest_record.get('tree_count_estimate'),
                forest_record.get('average_dbh_inches'),
                forest_record.get('average_height_feet'),
                forest_record.get('total_standing_biomass_tons'),
                forest_record.get('total_harvestable_biomass_tons'),
                forest_record.get('forest_residue_biomass_tons'),
                forest_record.get('fia_plot_count'),
                forest_record.get('fia_tree_count'),
                forest_record.get('data_sources'),
                vegetation_indices.get('ndvi'),
                forest_record.get('confidence_score')
            )
        except Exception as e:
            logger.warning(f"Failed to extract forestry record: {e}")
            return None
    
    def _extract_crop_record(self, parcel_id: str, county_fips: str,
                           processing_timestamp, crop_record: Dict,
                           vegetation_indices: Dict, is_dominant: bool) -> Optional[Tuple]:
        """Extract crop record data for database insert"""
        try:
            return (
                parcel_id,
                county_fips,
                processing_timestamp,
                crop_record.get('source_code'),
                crop_record.get('source_name'),
                is_dominant,
                crop_record.get('crop_category'),
                crop_record.get('area_acres'),
                crop_record.get('coverage_percent'),
                crop_record.get('coverage_percent'),  # area_percentage
                crop_record.get('yield_tons'),
                crop_record.get('yield_tons_per_acre'),
                crop_record.get('residue_tons_dry'),
                crop_record.get('residue_tons_wet'),
                crop_record.get('harvestable_residue_tons'),
                crop_record.get('residue_ratio'),
                crop_record.get('moisture_content'),
                crop_record.get('harvestable_residue_percent'),
                vegetation_indices.get('ndvi'),
                crop_record.get('confidence_score')
            )
        except Exception as e:
            logger.warning(f"Failed to extract crop record: {e}")
            return None
    
    def _bulk_insert_forestry_records(self, cursor, forestry_records: List[Tuple]):
        """Bulk insert forestry records"""
        insert_sql = """
        INSERT INTO forestry_analysis_v3 (
            parcel_id, county_fips, processing_timestamp,
            total_biomass_tons, forest_area_acres, forest_percentage,
            stand_age_average, forest_type_classification, harvest_probability,
            last_treatment_years, tree_count_estimate, average_dbh_inches,
            average_height_feet, standing_biomass_tons, harvestable_biomass_tons,
            residue_biomass_tons, fia_plot_count, fia_tree_count, data_sources,
            ndvi_value, confidence_score
        ) VALUES %s
        """
        psycopg2.extras.execute_values(cursor, insert_sql, forestry_records, page_size=1000)
    
    def _bulk_insert_crop_records(self, cursor, crop_records: List[Tuple]):
        """Bulk insert crop records"""  
        insert_sql = """
        INSERT INTO crop_analysis_v3 (
            parcel_id, county_fips, processing_timestamp,
            crop_code, crop_name, is_dominant_crop, crop_category,
            area_acres, coverage_percent, area_percentage, yield_tons,
            yield_tons_per_acre, residue_tons_dry, residue_tons_wet,
            harvestable_residue_tons, residue_ratio, moisture_content,
            harvestable_residue_percent, ndvi_value, confidence_score
        ) VALUES %s
        """
        psycopg2.extras.execute_values(cursor, insert_sql, crop_records, page_size=1000)
    
    def close_all_pools(self):
        """Close all connection pools"""
        for db_name, pool in self.pools.items():
            try:
                pool.closeall()
                logger.info(f"Closed connection pool for {db_name}")
            except Exception as e:
                logger.error(f"Error closing {db_name} pool: {e}")


# Global database manager instance
database_manager = DatabaseManager()