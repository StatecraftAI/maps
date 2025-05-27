"""
process_geojson_universal.py

Universal GeoJSON Processing and Upload Engine for StatecraftAI Maps

This script provides a comprehensive, robust pipeline for processing any GeoJSON file:
1. Loading and validation with multiple format support
2. Geometry validation and repair
3. CRS standardization and projection handling
4. Data cleaning and optimization
5. Property validation and type conversion
6. Spatial filtering and clipping
7. Performance optimization for web mapping
8. Supabase PostGIS upload with metadata

Key Features:
- Handles any GeoJSON format (FeatureCollection, Feature, Geometry)
- Robust error handling and recovery
- Automatic geometry repair and validation
- Smart property type detection and optimization
- Configurable spatial filtering (bounding box, polygon clipping)
- Web-optimized output with precision control
- Comprehensive logging and reporting
- Supabase integration with automatic table creation
- Metadata preservation and enhancement

Usage:
    python process_geojson_universal.py input.geojson --table schools --description "School locations"
    python process_geojson_universal.py input.geojson --config config.yaml --clip-to-pps
    python process_geojson_universal.py input.geojson --optimize-web --precision 6

Dependencies:
- geopandas, pandas, shapely, loguru
- Supabase integration: sqlalchemy, psycopg2-binary
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import geopandas as gpd
import pandas as pd
from loguru import logger
from shapely.geometry import Point, Polygon, MultiPolygon, LineString, MultiLineString
from shapely.validation import make_valid
import numpy as np

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from ops import Config

# Import optimization functions from existing scripts
try:
    from process_election_results import (
        clean_numeric,
        optimize_geojson_properties,
        validate_and_reproject_to_wgs84,
    )
    logger.debug("✅ Imported optimization functions from process_election_results")
except ImportError as e:
    logger.warning(f"⚠️ Could not import from process_election_results: {e}")

# Import Supabase integration
try:
    from ops.repositories import SpatialQueryManager
    from ops.supabase_integration import SupabaseDatabase, SupabaseUploader
    logger.debug("✅ Imported Supabase integration")
    SUPABASE_AVAILABLE = True
except ImportError as e:
    logger.debug(f"📊 Supabase integration not available: {e}")
    SUPABASE_AVAILABLE = False

    # Fallback implementations
    class SupabaseUploader:
        def __init__(self, config):
            pass
        def upload_geodataframe(self, *args, **kwargs):
            return False

    class SupabaseDatabase:
        def __init__(self, config):
            pass

    class SpatialQueryManager:
        def __init__(self, db):
            pass


class GeoJSONProcessor:
    """
    Universal GeoJSON processor with comprehensive validation, cleaning, and optimization.
    """
    
    def __init__(self, config: Config, options: Dict[str, Any] = None):
        """
        Initialize the processor with configuration and options.
        
        Args:
            config: Configuration instance
            options: Processing options dictionary
        """
        self.config = config
        self.options = options or {}
        
        # Processing statistics
        self.stats = {
            'input_features': 0,
            'output_features': 0,
            'invalid_geometries_fixed': 0,
            'properties_optimized': 0,
            'crs_transformations': 0,
            'spatial_filters_applied': 0,
            'processing_time': 0
        }
        
        # Default processing options
        self.default_options = {
            'target_crs': 'EPSG:4326',
            'precision': 6,
            'optimize_properties': True,
            'fix_geometries': True,
            'remove_invalid': False,
            'simplify_tolerance': None,
            'clip_to_bounds': None,
            'clip_to_pps': False,
            'web_optimize': True,
            'validate_topology': True,
            'preserve_metadata': True
        }
        
        # Merge options
        self.processing_options = {**self.default_options, **self.options}
        
    def load_geojson(self, input_path: Union[str, Path]) -> Optional[gpd.GeoDataFrame]:
        """
        Load GeoJSON from file with robust error handling and format detection.
        
        Args:
            input_path: Path to input GeoJSON file
            
        Returns:
            GeoDataFrame or None if loading failed
        """
        input_path = Path(input_path)
        logger.info(f"🗺️ Loading GeoJSON from {input_path}")
        
        if not input_path.exists():
            logger.critical(f"❌ Input file not found: {input_path}")
            return None
            
        try:
            # Try loading with geopandas first (most robust)
            gdf = gpd.read_file(input_path)
            logger.success(f"  ✅ Loaded {len(gdf):,} features using geopandas")
            
        except Exception as gpd_error:
            logger.warning(f"  ⚠️ Geopandas failed: {gpd_error}")
            logger.info("  🔄 Trying manual JSON parsing...")
            
            try:
                # Fallback to manual JSON parsing
                with open(input_path, 'r', encoding='utf-8') as f:
                    geojson_data = json.load(f)
                
                gdf = self._parse_geojson_manually(geojson_data)
                if gdf is not None:
                    logger.success(f"  ✅ Loaded {len(gdf):,} features using manual parsing")
                else:
                    return None
                    
            except Exception as json_error:
                logger.critical(f"❌ Failed to load GeoJSON: {json_error}")
                return None
        
        # Basic validation
        if len(gdf) == 0:
            logger.warning("⚠️ Loaded GeoJSON contains no features")
            return gdf
            
        # Store original feature count
        self.stats['input_features'] = len(gdf)
        
        # Validate geometry column
        if gdf.geometry.isna().all():
            logger.critical("❌ No valid geometries found in GeoJSON")
            return None
            
        logger.info(f"  📊 Input summary:")
        logger.info(f"     Features: {len(gdf):,}")
        logger.info(f"     Geometry types: {gdf.geometry.type.value_counts().to_dict()}")
        logger.info(f"     CRS: {gdf.crs}")
        logger.info(f"     Columns: {list(gdf.columns)}")
        
        return gdf
    
    def _parse_geojson_manually(self, geojson_data: Dict) -> Optional[gpd.GeoDataFrame]:
        """
        Manually parse GeoJSON data when geopandas fails.
        
        Args:
            geojson_data: Parsed GeoJSON dictionary
            
        Returns:
            GeoDataFrame or None if parsing failed
        """
        try:
            # Handle different GeoJSON structures
            if geojson_data.get('type') == 'FeatureCollection':
                features = geojson_data.get('features', [])
            elif geojson_data.get('type') == 'Feature':
                features = [geojson_data]
            else:
                logger.error("❌ Unsupported GeoJSON structure")
                return None
            
            if not features:
                logger.warning("⚠️ No features found in GeoJSON")
                return gpd.GeoDataFrame()
            
            # Convert to GeoDataFrame
            gdf = gpd.GeoDataFrame.from_features(features)
            
            # Set CRS if specified in GeoJSON
            if 'crs' in geojson_data:
                crs_info = geojson_data['crs']
                if crs_info.get('type') == 'name':
                    crs_name = crs_info.get('properties', {}).get('name')
                    if crs_name:
                        try:
                            gdf = gdf.set_crs(crs_name)
                        except Exception:
                            logger.warning(f"⚠️ Could not set CRS from GeoJSON: {crs_name}")
            
            return gdf
            
        except Exception as e:
            logger.error(f"❌ Manual GeoJSON parsing failed: {e}")
            return None
    
    def validate_and_fix_geometries(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Validate and fix geometry issues with comprehensive error handling.
        
        Args:
            gdf: Input GeoDataFrame
            
        Returns:
            GeoDataFrame with fixed geometries
        """
        logger.info("🔧 Validating and fixing geometries...")
        
        if not self.processing_options['fix_geometries']:
            logger.info("  ⏭️ Geometry fixing disabled, skipping...")
            return gdf
        
        original_count = len(gdf)
        fixed_count = 0
        
        # Check for null geometries
        null_geom_mask = gdf.geometry.isna()
        null_count = null_geom_mask.sum()
        
        if null_count > 0:
            logger.warning(f"  ⚠️ Found {null_count} null geometries")
            if self.processing_options['remove_invalid']:
                gdf = gdf[~null_geom_mask].copy()
                logger.info(f"  🗑️ Removed {null_count} features with null geometries")
            else:
                logger.info("  💡 Keeping null geometries (remove_invalid=False)")
        
        # Check for invalid geometries
        if len(gdf) > 0:
            try:
                invalid_mask = ~gdf.geometry.is_valid
                invalid_count = invalid_mask.sum()
                
                if invalid_count > 0:
                    logger.warning(f"  ⚠️ Found {invalid_count} invalid geometries")
                    
                    # Attempt to fix invalid geometries
                    logger.info("  🔨 Attempting to fix invalid geometries...")
                    
                    for idx in gdf[invalid_mask].index:
                        try:
                            original_geom = gdf.loc[idx, 'geometry']
                            fixed_geom = make_valid(original_geom)
                            
                            # Validate the fix
                            if fixed_geom.is_valid and not fixed_geom.is_empty:
                                gdf.loc[idx, 'geometry'] = fixed_geom
                                fixed_count += 1
                            elif self.processing_options['remove_invalid']:
                                gdf = gdf.drop(idx)
                                logger.debug(f"    🗑️ Removed unfixable geometry at index {idx}")
                            else:
                                logger.debug(f"    ⚠️ Could not fix geometry at index {idx}")
                                
                        except Exception as e:
                            logger.debug(f"    ❌ Error fixing geometry at index {idx}: {e}")
                            if self.processing_options['remove_invalid']:
                                gdf = gdf.drop(idx)
                    
                    logger.success(f"  ✅ Fixed {fixed_count} invalid geometries")
                    
            except Exception as e:
                logger.warning(f"  ⚠️ Geometry validation failed: {e}")
        
        # Reset index after potential row removal
        gdf = gdf.reset_index(drop=True)
        
        # Update statistics
        self.stats['invalid_geometries_fixed'] = fixed_count
        
        final_count = len(gdf)
        if final_count != original_count:
            logger.info(f"  📊 Feature count: {original_count:,} → {final_count:,}")
        
        return gdf
    
    def standardize_crs(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Standardize coordinate reference system with validation.
        
        Args:
            gdf: Input GeoDataFrame
            
        Returns:
            GeoDataFrame with standardized CRS
        """
        target_crs = self.processing_options['target_crs']
        logger.info(f"🌐 Standardizing CRS to {target_crs}...")
        
        # Handle missing CRS
        if gdf.crs is None:
            logger.warning("  ⚠️ No CRS defined, assuming WGS84 (EPSG:4326)")
            gdf = gdf.set_crs('EPSG:4326')
        
        # Transform if needed
        current_crs = gdf.crs.to_string() if gdf.crs else None
        
        if current_crs != target_crs:
            logger.info(f"  🔄 Transforming from {current_crs} to {target_crs}")
            try:
                gdf = gdf.to_crs(target_crs)
                self.stats['crs_transformations'] += 1
                logger.success(f"  ✅ CRS transformation completed")
            except Exception as e:
                logger.error(f"  ❌ CRS transformation failed: {e}")
                logger.info("  💡 Continuing with original CRS")
        else:
            logger.info(f"  ✅ Already in target CRS: {target_crs}")
        
        return gdf
    
    def optimize_properties(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Optimize property data types and clean values.
        
        Args:
            gdf: Input GeoDataFrame
            
        Returns:
            GeoDataFrame with optimized properties
        """
        if not self.processing_options['optimize_properties']:
            logger.info("⏭️ Property optimization disabled, skipping...")
            return gdf
        
        logger.info("🔧 Optimizing properties...")
        
        original_memory = gdf.memory_usage(deep=True).sum()
        optimized_cols = 0
        
        for col in gdf.columns:
            if col == 'geometry':
                continue
                
            try:
                original_dtype = gdf[col].dtype
                series = gdf[col]
                
                # Skip if already optimized
                if pd.api.types.is_numeric_dtype(series) and series.dtype in ['int8', 'int16', 'float32']:
                    continue
                
                # Optimize based on data characteristics
                optimized_series = self._optimize_column(col, series)
                
                if optimized_series is not None and optimized_series.dtype != original_dtype:
                    gdf[col] = optimized_series
                    optimized_cols += 1
                    logger.debug(f"  🔧 {col}: {original_dtype} → {optimized_series.dtype}")
                    
            except Exception as e:
                logger.debug(f"  ⚠️ Could not optimize column {col}: {e}")
        
        # Calculate memory savings
        final_memory = gdf.memory_usage(deep=True).sum()
        memory_saved = original_memory - final_memory
        memory_reduction = (memory_saved / original_memory) * 100 if original_memory > 0 else 0
        
        logger.success(f"  ✅ Optimized {optimized_cols} columns")
        logger.info(f"  💾 Memory usage: {original_memory:,} → {final_memory:,} bytes ({memory_reduction:.1f}% reduction)")
        
        self.stats['properties_optimized'] = optimized_cols
        
        return gdf
    
    def _optimize_column(self, col_name: str, series: pd.Series) -> Optional[pd.Series]:
        """
        Optimize a single column based on its data characteristics.
        
        Args:
            col_name: Column name
            series: Pandas series to optimize
            
        Returns:
            Optimized series or None if no optimization possible
        """
        # Handle missing values
        if series.isna().all():
            return series.astype('object')
        
        # Try numeric conversion
        if series.dtype == 'object':
            # Try to convert to numeric
            numeric_series = pd.to_numeric(series, errors='coerce')
            
            if not numeric_series.isna().all():
                # Determine best numeric type
                if numeric_series.dtype == 'float64':
                    # Check if can be integer
                    if numeric_series.dropna().apply(lambda x: x.is_integer()).all():
                        # Convert to integer
                        int_series = numeric_series.astype('Int64')  # Nullable integer
                        
                        # Optimize integer size
                        min_val = int_series.min()
                        max_val = int_series.max()
                        
                        if pd.isna(min_val) or pd.isna(max_val):
                            return int_series
                        
                        if -128 <= min_val <= 127 and -128 <= max_val <= 127:
                            return int_series.astype('int8')
                        elif -32768 <= min_val <= 32767 and -32768 <= max_val <= 32767:
                            return int_series.astype('int16')
                        elif -2147483648 <= min_val <= 2147483647 and -2147483648 <= max_val <= 2147483647:
                            return int_series.astype('int32')
                        else:
                            return int_series
                    else:
                        # Keep as float but optimize precision
                        return numeric_series.astype('float32')
                
                return numeric_series
        
        # Optimize string columns
        elif series.dtype == 'object':
            # Check if categorical would be more efficient
            unique_ratio = series.nunique() / len(series)
            if unique_ratio < 0.5 and series.nunique() < 1000:
                return series.astype('category')
        
        return None
    
    def apply_spatial_filters(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Apply spatial filtering (clipping, bounding box, etc.).
        
        Args:
            gdf: Input GeoDataFrame
            
        Returns:
            Spatially filtered GeoDataFrame
        """
        logger.info("🎯 Applying spatial filters...")
        
        original_count = len(gdf)
        
        # Clip to PPS district if requested
        if self.processing_options['clip_to_pps']:
            gdf = self._clip_to_pps_district(gdf)
            if gdf is None:
                return gpd.GeoDataFrame()
        
        # Apply bounding box filter if specified
        bounds = self.processing_options.get('clip_to_bounds')
        if bounds:
            gdf = self._clip_to_bounds(gdf, bounds)
        
        # Update statistics
        final_count = len(gdf)
        if final_count != original_count:
            self.stats['spatial_filters_applied'] += 1
            logger.info(f"  📊 Spatial filtering: {original_count:,} → {final_count:,} features")
        else:
            logger.info("  ✅ No spatial filtering applied")
        
        return gdf
    
    def _clip_to_pps_district(self, gdf: gpd.GeoDataFrame) -> Optional[gpd.GeoDataFrame]:
        """
        Clip features to PPS district boundaries.
        
        Args:
            gdf: Input GeoDataFrame
            
        Returns:
            Clipped GeoDataFrame or None if failed
        """
        logger.info("  🏫 Clipping to PPS district boundaries...")
        
        try:
            pps_path = self.config.get_input_path("pps_boundary_geojson")
            
            if not pps_path.exists():
                logger.warning(f"  ⚠️ PPS boundary file not found: {pps_path}")
                return gdf
            
            # Load PPS boundaries
            pps_gdf = gpd.read_file(pps_path)
            
            # Ensure same CRS
            if gdf.crs != pps_gdf.crs:
                pps_gdf = pps_gdf.to_crs(gdf.crs)
            
            # Create union of PPS boundaries
            pps_union = pps_gdf.geometry.unary_union
            
            # Clip features
            clipped_gdf = gdf[gdf.geometry.intersects(pps_union)].copy()
            
            logger.success(f"    ✅ Clipped to {len(clipped_gdf):,} features within PPS district")
            return clipped_gdf
            
        except Exception as e:
            logger.error(f"  ❌ PPS clipping failed: {e}")
            return gdf
    
    def _clip_to_bounds(self, gdf: gpd.GeoDataFrame, bounds: List[float]) -> gpd.GeoDataFrame:
        """
        Clip features to bounding box.
        
        Args:
            gdf: Input GeoDataFrame
            bounds: [minx, miny, maxx, maxy]
            
        Returns:
            Clipped GeoDataFrame
        """
        logger.info(f"  📦 Clipping to bounds: {bounds}")
        
        try:
            minx, miny, maxx, maxy = bounds
            bbox = Polygon([(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)])
            
            # Create bounding box GeoDataFrame
            bbox_gdf = gpd.GeoDataFrame([1], geometry=[bbox], crs=gdf.crs)
            
            # Clip features
            clipped_gdf = gdf[gdf.geometry.intersects(bbox)].copy()
            
            logger.success(f"    ✅ Clipped to {len(clipped_gdf):,} features within bounds")
            return clipped_gdf
            
        except Exception as e:
            logger.error(f"  ❌ Bounds clipping failed: {e}")
            return gdf
    
    def web_optimize(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Optimize GeoDataFrame for web mapping performance.
        
        Args:
            gdf: Input GeoDataFrame
            
        Returns:
            Web-optimized GeoDataFrame
        """
        if not self.processing_options['web_optimize']:
            logger.info("⏭️ Web optimization disabled, skipping...")
            return gdf
        
        logger.info("🌐 Optimizing for web mapping...")
        
        # Simplify geometries if tolerance specified
        simplify_tolerance = self.processing_options.get('simplify_tolerance')
        if simplify_tolerance:
            logger.info(f"  🔧 Simplifying geometries (tolerance: {simplify_tolerance})")
            try:
                gdf.geometry = gdf.geometry.simplify(simplify_tolerance, preserve_topology=True)
                logger.success("    ✅ Geometry simplification completed")
            except Exception as e:
                logger.warning(f"    ⚠️ Geometry simplification failed: {e}")
        
        # Round coordinates to specified precision
        precision = self.processing_options['precision']
        if precision < 15:  # Only round if precision is specified
            logger.info(f"  🔢 Rounding coordinates to {precision} decimal places")
            try:
                # This is handled by the GeoJSON export with precision parameter
                logger.success("    ✅ Coordinate precision will be applied on export")
            except Exception as e:
                logger.warning(f"    ⚠️ Coordinate rounding failed: {e}")
        
        return gdf
    
    def generate_metadata(self, gdf: gpd.GeoDataFrame, table_name: str, description: str = None) -> Dict[str, Any]:
        """
        Generate comprehensive metadata for the processed GeoDataFrame.
        
        Args:
            gdf: Processed GeoDataFrame
            table_name: Target table name
            description: Optional description
            
        Returns:
            Metadata dictionary
        """
        logger.info("📋 Generating metadata...")
        
        # Calculate bounds
        bounds = gdf.total_bounds if len(gdf) > 0 else [0, 0, 0, 0]
        
        # Analyze geometry types
        geom_types = gdf.geometry.type.value_counts().to_dict() if len(gdf) > 0 else {}
        
        # Analyze properties
        property_info = {}
        for col in gdf.columns:
            if col == 'geometry':
                continue
            
            series = gdf[col]
            property_info[col] = {
                'dtype': str(series.dtype),
                'null_count': int(series.isna().sum()),
                'unique_count': int(series.nunique()),
                'sample_values': series.dropna().head(3).tolist() if len(series.dropna()) > 0 else []
            }
        
        metadata = {
            'table_name': table_name,
            'description': description or f"Processed geospatial data: {table_name}",
            'feature_count': len(gdf),
            'geometry_types': geom_types,
            'crs': str(gdf.crs) if gdf.crs else None,
            'bounds': {
                'minx': float(bounds[0]),
                'miny': float(bounds[1]),
                'maxx': float(bounds[2]),
                'maxy': float(bounds[3])
            },
            'properties': property_info,
            'processing_stats': self.stats.copy(),
            'processing_options': self.processing_options.copy(),
            'created_at': pd.Timestamp.now().isoformat()
        }
        
        return metadata
    
    def export_geojson(self, gdf: gpd.GeoDataFrame, output_path: Union[str, Path]) -> bool:
        """
        Export optimized GeoJSON with precision control.
        
        Args:
            gdf: GeoDataFrame to export
            output_path: Output file path
            
        Returns:
            True if export successful, False otherwise
        """
        output_path = Path(output_path)
        logger.info(f"💾 Exporting optimized GeoJSON to {output_path}")
        
        try:
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Export with precision control
            precision = self.processing_options['precision']
            
            gdf.to_file(
                output_path,
                driver='GeoJSON',
                coordinate_precision=precision
            )
            
            # Verify export
            file_size = output_path.stat().st_size
            logger.success(f"  ✅ Exported {len(gdf):,} features ({file_size:,} bytes)")
            
            return True
            
        except Exception as e:
            logger.error(f"  ❌ Export failed: {e}")
            return False
    
    def upload_to_supabase(self, gdf: gpd.GeoDataFrame, table_name: str, description: str = None) -> bool:
        """
        Upload processed GeoDataFrame to Supabase PostGIS.
        
        Args:
            gdf: GeoDataFrame to upload
            table_name: Target table name
            description: Optional table description
            
        Returns:
            True if upload successful, False otherwise
        """
        if not SUPABASE_AVAILABLE:
            logger.warning("📊 Supabase integration not available - skipping upload")
            return False
        
        logger.info(f"🚀 Uploading to Supabase table: {table_name}")
        
        try:
            uploader = SupabaseUploader(self.config)
            
            # Generate description if not provided
            if not description:
                description = f"Processed geospatial data: {table_name} ({len(gdf):,} features)"
            
            # Upload
            success = uploader.upload_geodataframe(
                gdf,
                table_name=table_name,
                description=description
            )
            
            if success:
                logger.success(f"  ✅ Successfully uploaded to table: {table_name}")
                
                # Verify upload
                try:
                    db = SupabaseDatabase(self.config)
                    query_manager = SpatialQueryManager(db)
                    
                    if query_manager.table_exists(table_name):
                        sample_records = query_manager.get_sample_records(table_name, limit=3)
                        logger.info(f"  📊 Verified upload: {len(sample_records)} sample records")
                    else:
                        logger.warning("  ⚠️ Table verification failed")
                        
                except Exception as verification_error:
                    logger.warning(f"  ⚠️ Upload verification failed: {verification_error}")
                
                return True
            else:
                logger.error(f"  ❌ Upload failed for table: {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"  ❌ Supabase upload error: {e}")
            return False
    
    def process(self, input_path: Union[str, Path], table_name: str = None, 
                description: str = None, output_path: Union[str, Path] = None) -> Tuple[Optional[gpd.GeoDataFrame], Dict[str, Any]]:
        """
        Main processing pipeline that orchestrates all steps.
        
        Args:
            input_path: Path to input GeoJSON file
            table_name: Target Supabase table name
            description: Optional description
            output_path: Optional output file path
            
        Returns:
            Tuple of (processed GeoDataFrame, metadata)
        """
        start_time = time.time()
        logger.info("🚀 Starting universal GeoJSON processing pipeline")
        logger.info("=" * 70)
        
        # 1. Load GeoJSON
        gdf = self.load_geojson(input_path)
        if gdf is None:
            return None, {}
        
        # 2. Validate and fix geometries
        gdf = self.validate_and_fix_geometries(gdf)
        if len(gdf) == 0:
            logger.warning("⚠️ No valid features remaining after geometry validation")
            return gdf, {}
        
        # 3. Standardize CRS
        gdf = self.standardize_crs(gdf)
        
        # 4. Apply spatial filters
        gdf = self.apply_spatial_filters(gdf)
        if len(gdf) == 0:
            logger.warning("⚠️ No features remaining after spatial filtering")
            return gdf, {}
        
        # 5. Optimize properties
        gdf = self.optimize_properties(gdf)
        
        # 6. Web optimization
        gdf = self.web_optimize(gdf)
        
        # 7. Generate metadata
        metadata = self.generate_metadata(gdf, table_name or "processed_data", description)
        
        # 8. Export to file if requested
        if output_path:
            export_success = self.export_geojson(gdf, output_path)
            metadata['export_success'] = export_success
        
        # 9. Upload to Supabase if table name provided
        if table_name:
            upload_success = self.upload_to_supabase(gdf, table_name, description)
            metadata['upload_success'] = upload_success
        
        # Update final statistics
        self.stats['output_features'] = len(gdf)
        self.stats['processing_time'] = time.time() - start_time
        
        logger.info("🎉 Processing pipeline completed!")
        logger.info(f"  📊 Features: {self.stats['input_features']:,} → {self.stats['output_features']:,}")
        logger.info(f"  ⏱️ Processing time: {self.stats['processing_time']:.2f} seconds")
        
        return gdf, metadata


def main():
    """Main CLI interface for the universal GeoJSON processor."""
    parser = argparse.ArgumentParser(
        description="Universal GeoJSON Processing and Upload Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic processing and upload
  python process_geojson_universal.py schools.geojson --table pps_schools --description "PPS school locations"
  
  # Process with PPS clipping and web optimization
  python process_geojson_universal.py data.geojson --table my_data --clip-to-pps --web-optimize
  
  # Export to file only (no upload)
  python process_geojson_universal.py input.geojson --output optimized.geojson --precision 4
  
  # Advanced processing with custom options
  python process_geojson_universal.py input.geojson --table my_table --simplify 0.001 --bounds -122.8 45.4 -122.4 45.7
        """
    )
    
    # Required arguments
    parser.add_argument('input_file', help='Input GeoJSON file path')
    
    # Optional arguments
    parser.add_argument('--table', '-t', help='Supabase table name for upload')
    parser.add_argument('--description', '-d', help='Table/dataset description')
    parser.add_argument('--output', '-o', help='Output GeoJSON file path')
    parser.add_argument('--config', '-c', help='Configuration file path')
    
    # Processing options
    parser.add_argument('--precision', type=int, default=6, help='Coordinate precision (decimal places)')
    parser.add_argument('--clip-to-pps', action='store_true', help='Clip features to PPS district boundaries')
    parser.add_argument('--bounds', nargs=4, type=float, metavar=('MINX', 'MINY', 'MAXX', 'MAXY'),
                       help='Bounding box for spatial filtering')
    parser.add_argument('--simplify', type=float, help='Geometry simplification tolerance')
    parser.add_argument('--web-optimize', action='store_true', help='Enable web optimization')
    parser.add_argument('--no-fix-geometries', action='store_true', help='Disable geometry fixing')
    parser.add_argument('--remove-invalid', action='store_true', help='Remove invalid geometries instead of fixing')
    parser.add_argument('--target-crs', default='EPSG:4326', help='Target coordinate reference system')
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        if args.config:
            config = Config(args.config)
        else:
            config = Config()
        logger.info(f"📋 Using configuration: {config.config_path}")
    except Exception as e:
        logger.critical(f"❌ Configuration error: {e}")
        sys.exit(1)
    
    # Build processing options
    options = {
        'precision': args.precision,
        'clip_to_pps': args.clip_to_pps,
        'web_optimize': args.web_optimize,
        'fix_geometries': not args.no_fix_geometries,
        'remove_invalid': args.remove_invalid,
        'target_crs': args.target_crs
    }
    
    if args.bounds:
        options['clip_to_bounds'] = args.bounds
    
    if args.simplify:
        options['simplify_tolerance'] = args.simplify
    
    # Initialize processor
    processor = GeoJSONProcessor(config, options)
    
    # Process the file
    try:
        gdf, metadata = processor.process(
            input_path=args.input_file,
            table_name=args.table,
            description=args.description,
            output_path=args.output
        )
        
        if gdf is not None:
            logger.success("🎉 Processing completed successfully!")
            
            # Print summary
            print("\n" + "="*50)
            print("PROCESSING SUMMARY")
            print("="*50)
            print(f"Input features:     {metadata.get('processing_stats', {}).get('input_features', 0):,}")
            print(f"Output features:    {metadata.get('feature_count', 0):,}")
            print(f"Processing time:    {metadata.get('processing_stats', {}).get('processing_time', 0):.2f}s")
            print(f"Geometry types:     {metadata.get('geometry_types', {})}")
            print(f"CRS:               {metadata.get('crs', 'Unknown')}")
            
            if args.table:
                upload_success = metadata.get('upload_success', False)
                print(f"Supabase upload:   {'✅ Success' if upload_success else '❌ Failed'}")
            
            if args.output:
                export_success = metadata.get('export_success', False)
                print(f"File export:       {'✅ Success' if export_success else '❌ Failed'}")
        else:
            logger.error("❌ Processing failed")
            sys.exit(1)
            
    except Exception as e:
        logger.critical(f"❌ Processing error: {e}")
        import traceback
        logger.trace(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main() 