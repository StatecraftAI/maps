#!/usr/bin/env python3
"""
Supabase/PostGIS Integration Module

This module provides functions to upload optimized GeoDataFrames to Supabase's
PostgreSQL database with PostGIS extension. It integrates seamlessly with our
existing voter and household analysis pipelines.

Key Features:
- Secure credential management with environment variables
- Robust error handling and connection validation
- Automatic spatial indexing and optimization
- Data type optimization for PostGIS
- Configurable upload strategies (replace, append, update)
- Integration with existing logging infrastructure

Usage:
    from supabase_integration import SupabaseUploader
    
    uploader = SupabaseUploader()
    success = uploader.upload_geodataframe(
        gdf, 
        table_name="voter_hexagons",
        description="Hexagonal voter density aggregation"
    )
"""

import os
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
from urllib.parse import quote_plus

import geopandas as gpd
import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError

# Import our configuration system
import sys
sys.path.append(str(Path(__file__).parent.parent))
from ops import Config


class SupabaseUploader:
    """
    Handles uploading GeoDataFrames to Supabase PostGIS database.
    
    Provides robust connection management, error handling, and optimization
    for spatial data uploads to PostgreSQL with PostGIS extension.
    """
    
    def __init__(self, config: Optional[Config] = None):
        """
        Initialize Supabase uploader with configuration.
        
        Args:
            config: Optional Config instance. If None, creates new instance.
        """
        self.config = config or Config()
        self.engine = None
        self._connection_validated = False
        
        # Get credentials from environment or config
        self.credentials = self._load_credentials()
        
    def _load_credentials(self) -> Dict[str, str]:
        """
        Load Supabase credentials from environment variables or config.
        
        Returns:
            Dictionary with database connection parameters
        """
        logger.debug("📋 Loading Supabase credentials...")
        
        # Try environment variables first (most secure)
        credentials = {
            'host': os.getenv('SUPABASE_DB_HOST'),
            'user': os.getenv('SUPABASE_DB_USER', 'postgres'),
            'password': os.getenv('SUPABASE_DB_PASSWORD'),
            'database': os.getenv('SUPABASE_DB_NAME', 'postgres'),
            'port': int(os.getenv('SUPABASE_DB_PORT', '5432'))
        }
        
        # Fall back to config file if environment variables not set
        if not credentials['host'] or not credentials['password']:
            try:
                supabase_config = self.config.get('supabase', {})
                credentials.update({
                    'host': supabase_config.get('db_host', credentials['host']),
                    'password': supabase_config.get('db_password', credentials['password']),
                    'user': supabase_config.get('db_user', credentials['user']),
                    'database': supabase_config.get('db_name', credentials['database']),
                    'port': supabase_config.get('db_port', credentials['port'])
                })
            except Exception as e:
                logger.debug(f"   ⚠️ Could not load from config: {e}")
        
        # Validate required credentials
        missing = [k for k, v in credentials.items() if not v and k in ['host', 'password']]
        if missing:
            logger.error("❌ Missing required Supabase credentials:")
            logger.error(f"   Missing: {missing}")
            logger.error("   Set environment variables: SUPABASE_DB_HOST, SUPABASE_DB_PASSWORD")
            logger.error("   Or add to config.yaml under 'supabase' section")
            raise ValueError(f"Missing required credentials: {missing}")
        
        # Mask password in logs
        log_creds = credentials.copy()
        log_creds['password'] = '***' if credentials['password'] else None
        logger.debug(f"   ✅ Loaded credentials: {log_creds}")
        
        return credentials
    
    def _create_connection(self) -> bool:
        """
        Create SQLAlchemy engine for database connection.
        
        Returns:
            Success status
        """
        try:
            logger.debug("🔌 Creating database connection...")
            
            # URL-encode password to handle special characters
            password_encoded = quote_plus(self.credentials['password'])
            
            connection_string = (
                f"postgresql://{self.credentials['user']}:{password_encoded}@"
                f"{self.credentials['host']}:{self.credentials['port']}/{self.credentials['database']}"
            )
            
            # Create engine with connection pooling and optimization
            self.engine = create_engine(
                connection_string,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=3600,
                echo=False  # Set to True for SQL debugging
            )
            
            logger.debug("   ✅ Database engine created")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create database connection: {e}")
            self.engine = None
            return False
    
    def validate_connection(self) -> bool:
        """
        Validate database connection and PostGIS availability.
        
        Returns:
            Connection validation status
        """
        if self._connection_validated:
            return True
            
        logger.info("🔍 Validating Supabase/PostGIS connection...")
        
        try:
            # Create connection if needed
            if not self.engine and not self._create_connection():
                return False
            
            # Test basic connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version();"))
                version = result.scalar()
                logger.debug(f"   📊 PostgreSQL version: {version}")
                
                # Check PostGIS extension
                result = conn.execute(text(
                    "SELECT extname, extversion FROM pg_extension WHERE extname = 'postgis';"
                ))
                postgis_info = result.fetchone()
                
                if postgis_info:
                    logger.success(f"   🗺️ PostGIS version: {postgis_info[1]}")
                else:
                    logger.warning("   ⚠️ PostGIS extension not found")
                    logger.info("      💡 Run: CREATE EXTENSION postgis; (if you have admin access)")
                
                # Test spatial functionality
                try:
                    conn.execute(text("SELECT ST_Point(0, 0);"))
                    logger.debug("   ✅ Spatial functions available")
                except Exception as e:
                    logger.warning(f"   ⚠️ Spatial functions test failed: {e}")
            
            self._connection_validated = True
            logger.success("✅ Supabase/PostGIS connection validated")
            return True
            
        except Exception as e:
            logger.error(f"❌ Connection validation failed: {e}")
            logger.debug("   💡 Check credentials and network connectivity")
            return False
    
    def optimize_geodataframe_for_postgis(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Optimize GeoDataFrame for PostGIS upload.
        
        Args:
            gdf: Input GeoDataFrame
            
        Returns:
            Optimized GeoDataFrame
        """
        logger.debug("🔧 Optimizing GeoDataFrame for PostGIS...")
        
        gdf_opt = gdf.copy()
        
        # Ensure proper CRS (PostGIS expects EPSG:4326 for web usage)
        if gdf_opt.crs is None:
            logger.warning("   ⚠️ No CRS set, assuming EPSG:4326")
            gdf_opt = gdf_opt.set_crs("EPSG:4326")
        elif gdf_opt.crs.to_epsg() != 4326:
            logger.debug(f"   🔄 Reprojecting from {gdf_opt.crs} to EPSG:4326")
            gdf_opt = gdf_opt.to_crs("EPSG:4326")
        
        # Clean up field names for PostgreSQL compatibility
        column_mapping = {}
        for col in gdf_opt.columns:
            if col == 'geometry':
                continue
            # PostgreSQL prefers lowercase, underscores
            clean_col = col.lower().replace(' ', '_').replace('-', '_')
            # Remove special characters
            clean_col = ''.join(c for c in clean_col if c.isalnum() or c == '_')
            # Ensure doesn't start with number
            if clean_col and clean_col[0].isdigit():
                clean_col = f"field_{clean_col}"
            
            if clean_col != col:
                column_mapping[col] = clean_col
        
        if column_mapping:
            gdf_opt = gdf_opt.rename(columns=column_mapping)
            logger.debug(f"   📝 Renamed {len(column_mapping)} columns for PostgreSQL compatibility")
        
        # Optimize data types
        for col in gdf_opt.columns:
            if col == 'geometry':
                continue
                
            dtype = gdf_opt[col].dtype
            
            # Convert object columns to appropriate types
            if dtype == 'object':
                # Try to convert to numeric first
                numeric_series = pd.to_numeric(gdf_opt[col], errors='coerce')
                if not numeric_series.isna().all():
                    # Check if it's integer-like
                    if numeric_series.dropna().apply(lambda x: x.is_integer()).all():
                        gdf_opt[col] = numeric_series.astype('Int64')  # Nullable integer
                    else:
                        gdf_opt[col] = numeric_series
                else:
                    # Keep as string but ensure it's clean
                    gdf_opt[col] = gdf_opt[col].astype(str).replace('nan', None)
        
        # Validate geometry
        invalid_geom = ~gdf_opt.geometry.is_valid
        if invalid_geom.any():
            logger.warning(f"   ⚠️ Found {invalid_geom.sum()} invalid geometries, fixing...")
            gdf_opt.geometry = gdf_opt.geometry.buffer(0)
        
        logger.debug(f"   ✅ Optimized {len(gdf_opt)} features with {len(gdf_opt.columns)} fields")
        return gdf_opt
    
    def upload_geodataframe(
        self,
        gdf: gpd.GeoDataFrame,
        table_name: str,
        description: str = "",
        if_exists: str = 'replace',
        create_indexes: bool = True
    ) -> bool:
        """
        Upload GeoDataFrame to Supabase PostGIS table.
        
        Args:
            gdf: GeoDataFrame to upload
            table_name: Target table name
            description: Table description for metadata
            if_exists: How to behave if table exists ('replace', 'append', 'fail')
            create_indexes: Whether to create spatial indexes
            
        Returns:
            Upload success status
        """
        logger.info(f"📤 Uploading to Supabase table: {table_name}")
        
        try:
            # Validate connection
            if not self.validate_connection():
                return False
            
            # Optimize GeoDataFrame
            gdf_optimized = self.optimize_geodataframe_for_postgis(gdf)
            
            # Upload to PostGIS
            logger.debug("   💾 Uploading to PostGIS...")
            start_time = time.time()
            
            gdf_optimized.to_postgis(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=False,
                chunksize=1000  # Upload in chunks for large datasets
            )
            
            elapsed = time.time() - start_time
            logger.success(f"   ✅ Uploaded {len(gdf_optimized):,} features in {elapsed:.1f}s")
            
            # Create spatial indexes for performance
            if create_indexes:
                self._create_spatial_indexes(table_name)
            
            # Add table metadata
            self._add_table_metadata(table_name, description, gdf_optimized)
            
            logger.success(f"🎉 Successfully uploaded to Supabase: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Upload failed for table {table_name}: {e}")
            logger.trace("Detailed upload error:")
            import traceback
            logger.trace(traceback.format_exc())
            return False
    
    def _create_spatial_indexes(self, table_name: str) -> None:
        """
        Create spatial indexes on geometry column for performance.
        
        Args:
            table_name: Table to index
        """
        try:
            logger.debug(f"   📊 Creating spatial indexes on {table_name}...")
            
            with self.engine.connect() as conn:
                # Create spatial index on geometry column
                index_name = f"idx_{table_name}_geom"
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS {index_name} 
                    ON {table_name} USING GIST (geometry);
                """))
                
                # Analyze table for query optimization
                conn.execute(text(f"ANALYZE {table_name};"))
                conn.commit()
                
            logger.debug(f"   ✅ Spatial indexes created for {table_name}")
            
        except Exception as e:
            logger.warning(f"   ⚠️ Could not create spatial indexes: {e}")
    
    def _add_table_metadata(self, table_name: str, description: str, gdf: gpd.GeoDataFrame) -> None:
        """
        Add metadata comments to table and columns.
        
        Args:
            table_name: Table name
            description: Table description
            gdf: GeoDataFrame for field analysis
        """
        try:
            logger.debug(f"   📝 Adding metadata to {table_name}...")
            
            with self.engine.connect() as conn:
                # Add table comment
                if description:
                    conn.execute(text(f"""
                        COMMENT ON TABLE {table_name} IS '{description}';
                    """))
                
                # Add column comments for key fields
                for col in gdf.columns:
                    if col == 'geometry':
                        conn.execute(text(f"""
                            COMMENT ON COLUMN {table_name}.geometry IS 'Spatial geometry (EPSG:4326)';
                        """))
                    elif 'voter' in col.lower():
                        conn.execute(text(f"""
                            COMMENT ON COLUMN {table_name}.{col} IS 'Voter-related metric';
                        """))
                    elif 'household' in col.lower():
                        conn.execute(text(f"""
                            COMMENT ON COLUMN {table_name}.{col} IS 'Household demographic data';
                        """))
                    elif 'pct' in col.lower() or 'percent' in col.lower():
                        conn.execute(text(f"""
                            COMMENT ON COLUMN {table_name}.{col} IS 'Percentage value (0-100)';
                        """))
                
                conn.commit()
                
            logger.debug(f"   ✅ Metadata added to {table_name}")
            
        except Exception as e:
            logger.debug(f"   ⚠️ Could not add metadata: {e}")
    
    def list_tables(self) -> List[str]:
        """
        List all tables in the database.
        
        Returns:
            List of table names
        """
        try:
            if not self.validate_connection():
                return []
            
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            return tables
            
        except Exception as e:
            logger.error(f"❌ Could not list tables: {e}")
            return []
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in database.
        
        Args:
            table_name: Table name to check
            
        Returns:
            True if table exists
        """
        try:
            tables = self.list_tables()
            return table_name in tables
        except Exception:
            return False
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a table.
        
        Args:
            table_name: Table name
            
        Returns:
            Dictionary with table information or None
        """
        try:
            if not self.validate_connection():
                return None
            
            with self.engine.connect() as conn:
                # Get row count
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};"))
                row_count = result.scalar()
                
                # Get spatial extent if geometry column exists
                try:
                    result = conn.execute(text(f"""
                        SELECT 
                            ST_XMin(ST_Extent(geometry)) as min_x,
                            ST_YMin(ST_Extent(geometry)) as min_y,
                            ST_XMax(ST_Extent(geometry)) as max_x,
                            ST_YMax(ST_Extent(geometry)) as max_y
                        FROM {table_name}
                        WHERE geometry IS NOT NULL;
                    """))
                    extent = result.fetchone()
                    spatial_info = {
                        'bounds': [extent[0], extent[1], extent[2], extent[3]] if extent[0] else None
                    }
                except Exception:
                    spatial_info = {'bounds': None}
                
                return {
                    'table_name': table_name,
                    'row_count': row_count,
                    **spatial_info
                }
                
        except Exception as e:
            logger.error(f"❌ Could not get table info for {table_name}: {e}")
            return None


def upload_to_supabase(
    gdf: gpd.GeoDataFrame,
    table_name: str,
    description: str = "",
    config: Optional[Config] = None
) -> bool:
    """
    Convenience function to upload GeoDataFrame to Supabase.
    
    Args:
        gdf: GeoDataFrame to upload
        table_name: Target table name
        description: Table description
        config: Optional Config instance
        
    Returns:
        Upload success status
    """
    try:
        uploader = SupabaseUploader(config)
        return uploader.upload_geodataframe(gdf, table_name, description)
    except Exception as e:
        logger.error(f"❌ Supabase upload failed: {e}")
        return False 