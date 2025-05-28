"""
spatial_utils.py

Universal Spatial Processing and Analysis Engine for StatecraftAI Maps

This module provides a comprehensive, robust pipeline for spatial data processing:
1. Loading and validation with multiple format support
2. Geometry validation and repair  
3. CRS standardization and projection handling
4. Data cleaning and optimization
5. Property validation and type conversion
6. Spatial filtering and clipping
7. Spatial aggregation and analysis
8. Performance optimization for web mapping
9. Supabase PostGIS upload with metadata

Key Features:
- Handles any geospatial format (GeoJSON, Shapefile, etc.)
- Robust error handling and recovery
- Automatic geometry repair and validation
- Smart property type detection and optimization
- Configurable spatial filtering (bounding box, polygon clipping)
- Spatial joins and aggregations
- Web-optimized output with precision control
- Comprehensive logging and reporting
- Supabase integration with automatic table creation
- Metadata preservation and enhancement

Usage as Module:
    from processing.spatial_utils import SpatialProcessor, clean_numeric, validate_and_reproject_to_wgs84
    
    processor = SpatialProcessor(config)
    gdf = processor.load_geojson('data.geojson')
    gdf = validate_and_reproject_to_wgs84(gdf, config)

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
from shapely.geometry import Point, Polygon, MultiPolygon, LineString, MultiLineString, box
from shapely.validation import make_valid
import numpy as np

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from ops import Config

# Remove circular imports - spatial functions will be defined in this module
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


class SpatialProcessor:
    """
    Universal Spatial Processor with comprehensive validation, cleaning, and optimization.
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
    processor = SpatialProcessor(config, options)
    
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

# =============================================================================
# Specialized Processing Functions (from other scripts)
# =============================================================================

def load_and_process_acs_data(config: Config) -> Optional[pd.DataFrame]:
    """
    Load and process ACS household data from JSON with robust validation.
    Moved from process_census_households.py
    """
    acs_path = config.get_input_path("acs_households_json")
    logger.info(f"📊 Loading ACS household data from {acs_path}")

    if not acs_path.exists():
        logger.critical(f"❌ ACS JSON file not found: {acs_path}")
        return None

    try:
        with open(acs_path) as f:
            data_array = json.load(f)

        if not isinstance(data_array, list) or len(data_array) < 2:
            logger.critical("❌ Invalid ACS JSON structure - expected array with header and data")
            return None

        header = data_array[0]
        records = data_array[1:]
        df = pd.DataFrame(records, columns=header)
        logger.success(f"  ✅ Loaded {len(df):,} ACS records")

        # Process and standardize ACS field names
        df = df.rename(columns={
            "B11001_001E": "total_households",
            "B11001_002E": "households_no_minors",
        })

        # Validate required columns
        required_cols = ["total_households", "households_no_minors"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.critical(f"❌ Missing required ACS columns: {missing_cols}")
            return None

        # Convert to numeric
        df["total_households"] = clean_numeric(df["total_households"]).astype(int)
        df["households_no_minors"] = clean_numeric(df["households_no_minors"]).astype(int)

        # Create standardized GEOID
        geo_cols = ["state", "county", "tract", "block group"]
        missing_geo_cols = [col for col in geo_cols if col not in df.columns]
        if missing_geo_cols:
            logger.critical(f"❌ Missing geographic identifier columns: {missing_geo_cols}")
            return None

        df["GEOID"] = (
            df["state"].astype(str) + df["county"].astype(str) + 
            df["tract"].astype(str) + df["block group"].astype(str)
        )

        # Calculate percentage
        df["pct_households_no_minors"] = df.apply(
            lambda row: round(100 * row["households_no_minors"] / row["total_households"], 1)
            if row["total_households"] > 0 else 0.0, axis=1
        )

        return df

    except Exception as e:
        logger.critical(f"❌ Error loading ACS data: {e}")
        return None


def load_and_validate_voter_data(config: Config) -> Optional[pd.DataFrame]:
    """
    Load and validate voter registration data.
    Moved from process_voters_file.py
    """
    voters_path = config.get_input_path("voters_csv")
    logger.info(f"📊 Loading voter data from {voters_path}")

    if not voters_path.exists():
        logger.critical(f"❌ Voter file not found: {voters_path}")
        return None

    try:
        df = pd.read_csv(voters_path, low_memory=False)
        logger.success(f"  ✅ Loaded {len(df):,} voter records")

        # Validate required columns
        required_cols = ["Latitude", "Longitude"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.critical(f"❌ Missing required columns: {missing_cols}")
            return None

        # Clean coordinates
        df["Latitude"] = clean_numeric(df["Latitude"])
        df["Longitude"] = clean_numeric(df["Longitude"])

        # Remove invalid coordinates
        valid_coords = (
            (df["Latitude"].between(45.0, 46.0)) & 
            (df["Longitude"].between(-123.5, -122.0))
        )
        df = df[valid_coords].copy()
        logger.info(f"  ✅ Filtered to {len(df):,} voters with valid coordinates")

        return df

    except Exception as e:
        logger.critical(f"❌ Error loading voter data: {e}")
        return None


def create_voter_geodataframe(voters_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Convert voter DataFrame to GeoDataFrame with Point geometries.
    Moved from process_voters_file.py
    """
    from shapely.geometry import Point
    
    logger.info("🗺️ Creating voter GeoDataFrame...")
    
    # Create Point geometries
    geometry = [Point(lon, lat) for lon, lat in zip(voters_df["Longitude"], voters_df["Latitude"])]
    
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(voters_df, geometry=geometry, crs="EPSG:4326")
    
    logger.success(f"  ✅ Created GeoDataFrame with {len(gdf):,} voter points")
    return gdf


def create_hexagonal_aggregation(voters_gdf: gpd.GeoDataFrame, config: Config, resolution: int = 8) -> gpd.GeoDataFrame:
    """
    Create hexagonal aggregation of voter data using H3.
    Moved from process_voters_file.py
    """
    try:
        import h3
    except ImportError:
        logger.error("❌ H3 library not available. Install with: pip install h3")
        return None

    logger.info(f"🔷 Creating H3 hexagonal aggregation (resolution {resolution})...")

    # Get H3 indices for each voter
    h3_indices = []
    for _, voter in voters_gdf.iterrows():
        lat, lon = voter.geometry.y, voter.geometry.x
        h3_index = h3.geo_to_h3(lat, lon, resolution)
        h3_indices.append(h3_index)

    voters_gdf["h3_index"] = h3_indices

    # Aggregate by hexagon
    hex_stats = voters_gdf.groupby("h3_index").agg({
        "VOTER_ID": "count"
    }).rename(columns={"VOTER_ID": "voter_count"}).reset_index()

    # Create hexagon geometries
    from shapely.geometry import Polygon
    
    hex_geometries = []
    for h3_index in hex_stats["h3_index"]:
        hex_boundary = h3.h3_to_geo_boundary(h3_index, geo_json=True)
        hex_geom = Polygon(hex_boundary)
        hex_geometries.append(hex_geom)

    # Create final GeoDataFrame
    hex_gdf = gpd.GeoDataFrame(hex_stats, geometry=hex_geometries, crs="EPSG:4326")
    
    logger.success(f"  ✅ Created {len(hex_gdf):,} hexagons with voter data")
    return hex_gdf


def merge_acs_with_geometries(acs_df: pd.DataFrame, bg_gdf: gpd.GeoDataFrame) -> Optional[gpd.GeoDataFrame]:
    """
    Merge ACS data with block group geometries.
    Moved from process_census_households.py
    """
    logger.info("🔗 Merging ACS data with block group geometries...")

    try:
        # Perform merge
        gdf = bg_gdf.merge(
            acs_df[["GEOID", "total_households", "households_no_minors", "pct_households_no_minors"]],
            on="GEOID", how="left"
        )

        # Handle missing values
        fill_cols = ["total_households", "households_no_minors", "pct_households_no_minors"]
        gdf[fill_cols] = gdf[fill_cols].fillna(0)

        # Ensure proper data types
        gdf["total_households"] = gdf["total_households"].astype(int)
        gdf["households_no_minors"] = gdf["households_no_minors"].astype(int)
        gdf["pct_households_no_minors"] = gdf["pct_households_no_minors"].round(1)

        # Calculate area and density
        gdf_proj = gdf.to_crs("EPSG:3857")
        gdf["area_km2"] = (gdf_proj.geometry.area / 1e6).round(3)
        gdf["household_density"] = (gdf["total_households"] / gdf["area_km2"]).round(1)
        gdf["household_density"] = gdf["household_density"].replace([float("inf"), -float("inf")], 0)

        logger.success(f"  ✅ Merged data for {len(gdf):,} block groups")
        return gdf

    except Exception as e:
        logger.critical(f"❌ Error merging ACS data: {e}")
        return None


def load_block_group_boundaries(config: Config) -> Optional[gpd.GeoDataFrame]:
    """
    Load and validate block group geometries.
    Moved from process_voters_file.py and process_census_households.py
    """
    bg_path = config.get_input_path("census_blocks_geojson")
    logger.info(f"🗺️ Loading block group geometries from {bg_path}")

    if not bg_path.exists():
        logger.critical(f"❌ Block groups file not found: {bg_path}")
        return None

    try:
        gdf = gpd.read_file(bg_path)
        logger.success(f"  ✅ Loaded {len(gdf):,} block groups")

        # Filter to Multnomah County if columns exist
        if "STATEFP" in gdf.columns and "COUNTYFP" in gdf.columns:
            gdf = gdf[(gdf["STATEFP"] == "41") & (gdf["COUNTYFP"] == "051")].copy()
            logger.success(f"  ✅ Filtered to {len(gdf):,} Multnomah County block groups")

        # Validate and standardize CRS
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        elif gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs("EPSG:4326")

        # Fix invalid geometries
        invalid_geom = gdf.geometry.isna() | (~gdf.geometry.is_valid)
        if invalid_geom.sum() > 0:
            logger.warning(f"  ⚠️ Fixing {invalid_geom.sum()} invalid geometries")
            gdf.geometry = gdf.geometry.buffer(0)

        return gdf

    except Exception as e:
        logger.critical(f"❌ Error loading block group geometries: {e}")
        return None


def load_pps_district_boundaries(config: Config) -> Optional[gpd.GeoDataFrame]:
    """
    Load PPS district boundaries.
    Moved from process_voters_file.py
    """
    pps_path = config.get_input_path("pps_boundary_geojson")
    logger.info(f"🎯 Loading PPS district boundaries from {pps_path}")

    if not pps_path.exists():
        logger.critical(f"❌ PPS district file not found: {pps_path}")
        return None

    try:
        gdf = gpd.read_file(pps_path)
        logger.success(f"  ✅ Loaded PPS district boundaries")

        # Validate and standardize CRS
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326")
        elif gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs("EPSG:4326")

        return gdf

    except Exception as e:
        logger.critical(f"❌ Error loading PPS boundaries: {e}")
        return None


def filter_to_pps_district(gdf: gpd.GeoDataFrame, config: Config) -> Optional[gpd.GeoDataFrame]:
    """
    Filter data to PPS district using spatial operations.
    Moved from process_census_households.py
    """
    pps_region = load_pps_district_boundaries(config)
    if pps_region is None:
        return None

    try:
        # Ensure consistent CRS
        if gdf.crs != pps_region.crs:
            gdf = gdf.to_crs(pps_region.crs)

        # Project for geometric operations
        target_crs = "EPSG:2913"  # Oregon North State Plane
        pps_proj = pps_region.to_crs(target_crs)
        gdf_proj = gdf.to_crs(target_crs)

        # Create union and filter by centroid
        pps_union = pps_proj.geometry.unary_union
        centroids = gdf_proj.geometry.centroid
        mask = centroids.within(pps_union)

        # Apply filter and reproject
        pps_gdf = gdf_proj[mask].to_crs("EPSG:4326")
        pps_gdf["within_pps"] = True

        logger.success(f"  ✅ Filtered to {len(pps_gdf):,} features within PPS district")
        return pps_gdf

    except Exception as e:
        logger.critical(f"❌ Error filtering to PPS district: {e}")
        return None


def clean_numeric(series: pd.Series, is_percent: bool = False) -> pd.Series:
    """
    Cleans a pandas Series to numeric type, handling commas and percent signs.
    FIXED for new percentage data scale (already 0-100, don't divide by 100 again).

    Args:
        series: The pandas Series to clean.
        is_percent: If True, data is already in percentage format (0-100), don't convert

    Returns:
        A pandas Series with numeric data.
    """
    s = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip()
    )
    vals = pd.to_numeric(s, errors="coerce")
    # Don't divide by 100 - our new data is already in percentage format
    return vals


def validate_and_reproject_to_wgs84(
    gdf: gpd.GeoDataFrame, config: Config, source_description: str = "GeoDataFrame"
) -> gpd.GeoDataFrame:
    """
    Validates and reprojects a GeoDataFrame to WGS84 (EPSG:4326) if needed.

    Args:
        gdf: Input GeoDataFrame
        config: Configuration instance
        source_description: Description for logging

    Returns:
        GeoDataFrame in WGS84 coordinate system
    """
    logger.debug(f"🗺️ Validating and reprojecting {source_description}:")

    # Check original CRS
    original_crs = gdf.crs
    logger.debug(f"  📍 Original CRS: {original_crs}")

    # Get CRS settings from config
    input_crs = config.get_system_setting("input_crs")
    output_crs = config.get_system_setting("output_crs")

    # Handle missing CRS
    if original_crs is None:
        logger.warning("  ⚠️ No CRS specified in data")

        # Try to detect coordinate system from sample coordinates
        if not gdf.empty and "geometry" in gdf.columns:
            sample_geom = gdf.geometry.dropna().iloc[0] if len(gdf.geometry.dropna()) > 0 else None
            if sample_geom is not None:
                # Get first coordinate pair
                coords = None
                if hasattr(sample_geom, "exterior"):  # Polygon
                    coords = list(sample_geom.exterior.coords)[0]
                elif hasattr(sample_geom, "coords"):  # Point or LineString
                    coords = list(sample_geom.coords)[0]

                if coords:
                    x, y = coords[0], coords[1]
                    logger.debug(f"  🔍 Sample coordinates: x={x:.2f}, y={y:.2f}")

                    # Check if coordinates look like configured input CRS
                    if input_crs == "EPSG:2913" and abs(x) > 1000000 and abs(y) > 1000000:
                        logger.debug(f"  🎯 Coordinates appear to be {input_crs}")
                        gdf = gdf.set_crs(input_crs, allow_override=True)
                    # Check if coordinates look like WGS84 (longitude/latitude)
                    elif -180 <= x <= 180 and -90 <= y <= 90:
                        logger.debug(f"  🎯 Coordinates appear to be {output_crs}")
                        gdf = gdf.set_crs(output_crs, allow_override=True)
                    else:
                        logger.warning(f"  ❓ Unknown coordinate system, assuming {input_crs}")
                        gdf = gdf.set_crs(input_crs, allow_override=True)
                else:
                    logger.warning(
                        f"  ❓ Could not extract sample coordinates, assuming {output_crs}"
                    )
                    gdf = gdf.set_crs(output_crs, allow_override=True)
            else:
                logger.warning(f"  ❓ No valid geometry found, assuming {output_crs}")
                gdf = gdf.set_crs(output_crs, allow_override=True)

    # Reproject to output CRS if needed
    current_crs = gdf.crs
    if current_crs is not None:
        try:
            current_epsg = current_crs.to_epsg()
            target_epsg = int(output_crs.split(":")[1])
            if current_epsg != target_epsg:
                logger.debug(f"  🔄 Reprojecting from EPSG:{current_epsg} to {output_crs}")
                gdf_reprojected = gdf.to_crs(output_crs)

                # Validate reprojection worked
                if not gdf_reprojected.empty and "geometry" in gdf_reprojected.columns:
                    sample_geom = (
                        gdf_reprojected.geometry.dropna().iloc[0]
                        if len(gdf_reprojected.geometry.dropna()) > 0
                        else None
                    )
                    if sample_geom is not None:
                        coords = None
                        if hasattr(sample_geom, "exterior"):  # Polygon
                            coords = list(sample_geom.exterior.coords)[0]
                        elif hasattr(sample_geom, "coords"):  # Point or LineString
                            coords = list(sample_geom.coords)[0]

                        if coords:
                            x, y = coords[0], coords[1]
                            logger.debug(f"  ✓ Reprojected coordinates: lon={x:.6f}, lat={y:.6f}")

                            # Validate coordinates are in valid WGS84 range
                            if -180 <= x <= 180 and -90 <= y <= 90:
                                logger.debug("  ✓ Coordinates are valid WGS84")
                            else:
                                logger.warning(f"  ⚠️ Coordinates may be invalid: lon={x}, lat={y}")
                        else:
                            logger.warning("  ⚠️ Could not validate reprojected coordinates")

                gdf = gdf_reprojected
            else:
                logger.debug(f"  ✓ Already in {output_crs}")
        except Exception as e:
            logger.error(f"  ❌ Error during reprojection: {e}")
            logger.debug(f"  🔧 Attempting to set CRS as {output_crs}")
            gdf = gdf.set_crs(output_crs, allow_override=True)

    # Final validation
    if gdf.crs is not None:
        try:
            final_epsg = gdf.crs.to_epsg()
            logger.debug(f"  ✅ Final CRS: EPSG:{final_epsg}")
        except Exception:
            logger.error(f"  ❌ Final CRS: {gdf.crs}")
    else:
        logger.warning("  ⚠️ Warning: Final CRS is None")

    # Validate geometry
    valid_geom_count = gdf.geometry.notna().sum()
    total_count = len(gdf)
    logger.debug(
        f"  📊 Geometry validation: {valid_geom_count}/{total_count} features have valid geometry"
    )

    return gdf


def consolidate_split_precincts(gdf: gpd.GeoDataFrame, precinct_col: str) -> gpd.GeoDataFrame:
    """
    Consolidate split precincts (e.g., 2801a, 2801b, 2801c) into single features.
    Generalizes to work with any precinct-like data that may have split geometries.

    Args:
        gdf: GeoDataFrame with precinct data
        precinct_col: Name of the precinct column

    Returns:
        GeoDataFrame with consolidated precincts
    """
    logger.debug(f"🔄 Consolidating split precincts in column '{precinct_col}':")

    # Create a copy to work with
    gdf_work = gdf.copy()

    # Convert ALL numeric columns to proper numeric types BEFORE processing
    logger.debug("  🔧 Converting columns to proper data types...")

    # Identify boolean columns first to exclude them from numeric conversion
    boolean_cols = [
        "is_pps_precinct", "has_election_results", "has_voter_registration",
        "is_summary", "is_complete_record", "is_county_rollup"
    ]

    # Identify categorical columns that should NOT be converted to numeric
    categorical_cols = [
        "political_lean", "competitiveness", "leading_candidate", "second_candidate",
        "record_type", "turnout_quartile", "margin_category", "precinct_size_category"
    ]

    # Identify ALL columns that should be numeric and convert them
    numeric_conversion_cols = []
    for col in gdf_work.columns:
        if col in ["geometry", precinct_col, "base_precinct"] + boolean_cols + categorical_cols:
            continue
        # Check if this looks like a numeric column based on content
        sample_values = gdf_work[col].dropna().head(10)
        if len(sample_values) > 0:
            # Try to convert sample to see if it's numeric
            try:
                converted = pd.to_numeric(sample_values, errors="coerce")
                # Only include if the conversion actually worked (not all NaN)
                if not converted.isna().all():
                    numeric_conversion_cols.append(col)
            except Exception:
                pass

    # Convert identified numeric columns
    for col in numeric_conversion_cols:
        gdf_work[col] = pd.to_numeric(gdf_work[col], errors="coerce").fillna(0)

    # Handle boolean columns separately
    for col in boolean_cols:
        if col in gdf_work.columns:
            gdf_work[col] = gdf_work[col].astype(str).str.lower().isin(["true", "1", "yes"])

    logger.debug(f"  📊 Converted {len(numeric_conversion_cols)} columns to numeric")
    logger.debug(
        f"  📊 Converted {sum(1 for col in boolean_cols if col in gdf_work.columns)} columns to boolean"
    )

    # Extract base precinct numbers (remove a,b,c suffixes)
    gdf_work["base_precinct"] = (
        gdf_work[precinct_col].astype(str).str.replace(r"[a-zA-Z]+$", "", regex=True).str.strip()
    )

    # Count how many precincts have splits
    precinct_counts = gdf_work["base_precinct"].value_counts()
    split_precincts = precinct_counts[precinct_counts > 1]

    logger.debug(f"  📊 Found {len(split_precincts)} precincts with splits:")
    for base, count in split_precincts.head(5).items():
        logger.debug(f"    - Precinct {base}: {count} parts")
    if len(split_precincts) > 5:
        logger.debug(f"    ... and {len(split_precincts) - 5} more")

    # Group by base precinct and consolidate
    consolidated_features = []

    for base_precinct in gdf_work["base_precinct"].unique():
        if pd.isna(base_precinct) or base_precinct == "":
            continue

        precinct_parts = gdf_work[gdf_work["base_precinct"] == base_precinct]

        if len(precinct_parts) == 1:
            # Single precinct, just update the precinct name to base
            feature = precinct_parts.copy()
            feature[precinct_col] = base_precinct
            consolidated_features.append(feature)
        else:
            # Multiple parts - consolidate them
            consolidated = precinct_parts.iloc[0:1].copy()
            consolidated[precinct_col] = base_precinct

            # Take values from the first part (should be identical for split precincts)
            for col in numeric_conversion_cols:
                if col in precinct_parts.columns:
                    first_value = precinct_parts[col].iloc[0]
                    consolidated.loc[consolidated.index[0], col] = first_value

            # Handle boolean columns with logical OR
            for col in boolean_cols:
                if col in precinct_parts.columns:
                    bool_values = precinct_parts[col].astype(bool)
                    consolidated_value = bool_values.any()
                    consolidated.loc[consolidated.index[0], col] = consolidated_value

            # Handle categorical columns (take first value)
            for col in categorical_cols:
                if col in precinct_parts.columns:
                    first_value = precinct_parts[col].iloc[0]
                    consolidated.loc[consolidated.index[0], col] = first_value

            # Dissolve geometries (combine all parts into one shape)
            try:
                # Clean geometries first to avoid edge artifacts
                cleaned_geoms = []
                for geom in precinct_parts.geometry:
                    if geom is not None and geom.is_valid:
                        cleaned_geom = geom.buffer(0.0000001).buffer(-0.0000001)
                        if cleaned_geom.is_valid and not cleaned_geom.is_empty:
                            cleaned_geoms.append(cleaned_geom)
                        else:
                            cleaned_geoms.append(geom)
                    elif geom is not None:
                        try:
                            fixed_geom = geom.buffer(0)
                            if fixed_geom.is_valid and not fixed_geom.is_empty:
                                cleaned_geoms.append(fixed_geom)
                        except Exception:
                            pass

                # Dissolve using cleaned geometries
                if cleaned_geoms:
                    dissolved_geom = gpd.GeoSeries(cleaned_geoms).unary_union
                    if dissolved_geom.is_valid:
                        consolidated.loc[consolidated.index[0], "geometry"] = dissolved_geom
                    else:
                        fixed_geom = dissolved_geom.buffer(0)
                        if fixed_geom.is_valid:
                            consolidated.loc[consolidated.index[0], "geometry"] = fixed_geom
                        else:
                            consolidated.loc[consolidated.index[0], "geometry"] = precinct_parts.geometry.iloc[0]
                else:
                    consolidated.loc[consolidated.index[0], "geometry"] = precinct_parts.geometry.iloc[0]

            except Exception as e:
                logger.warning(f"    ⚠️ Error dissolving geometry for precinct {base_precinct}: {e}")
                consolidated.loc[consolidated.index[0], "geometry"] = precinct_parts.geometry.iloc[0]

            consolidated_features.append(consolidated)

    # Combine all consolidated features
    if consolidated_features:
        gdf_consolidated = pd.concat(consolidated_features, ignore_index=True)
        logger.debug(
            f"  ✅ Consolidated {len(gdf_work)} features into {len(gdf_consolidated)} features"
        )
        return gdf_consolidated
    else:
        logger.warning("  ⚠️ Warning: No features to consolidate")
        return gdf_work


def classify_by_spatial_join(points_gdf: gpd.GeoDataFrame, polygons_gdf: gpd.GeoDataFrame, 
                           classification_col: str = "within_polygon") -> gpd.GeoDataFrame:
    """
    Generalized spatial join to classify points by polygon containment.
    
    Args:
        points_gdf: GeoDataFrame with point geometries
        polygons_gdf: GeoDataFrame with polygon geometries 
        classification_col: Name for the boolean classification column
        
    Returns:
        Points GeoDataFrame with classification column added
    """
    logger.info(f"🎯 Classifying points by polygon containment...")

    try:
        # Ensure consistent CRS
        if points_gdf.crs != polygons_gdf.crs:
            polygons_gdf = polygons_gdf.to_crs(points_gdf.crs)

        # Spatial join to classify points
        points_with_classification = points_gdf.sjoin(polygons_gdf, how="left", predicate="within")
        
        # Add classification column
        points_with_classification[classification_col] = ~points_with_classification.index_right.isna()
        
        logger.success(f"  ✅ Classified {len(points_with_classification):,} points")
        
        # Summary statistics
        within_count = points_with_classification[classification_col].sum()
        logger.info(f"     📊 Points within polygons: {within_count:,}")
        logger.info(f"     📊 Coverage: {within_count / len(points_with_classification) * 100:.1f}%")
        
        return points_with_classification

    except Exception as e:
        logger.critical(f"❌ Error in spatial classification: {e}")
        return None


def create_grid_aggregation(points_gdf: gpd.GeoDataFrame, grid_size: float = 0.01, 
                           count_col: str = "point_count") -> gpd.GeoDataFrame:
    """
    Create grid-based aggregation of point data.
    
    Args:
        points_gdf: GeoDataFrame with point geometries
        grid_size: Grid cell size in degrees
        count_col: Name for the count column
        
    Returns:
        Grid GeoDataFrame with point counts
    """
    logger.info(f"📐 Creating grid aggregation (grid size: {grid_size}°)...")

    try:
        # Get bounds of point data
        bounds = points_gdf.total_bounds
        minx, miny, maxx, maxy = bounds

        # Create grid
        grid_cells = []
        x = minx
        while x < maxx:
            y = miny
            while y < maxy:
                grid_cells.append(box(x, y, x + grid_size, y + grid_size))
                y += grid_size
            x += grid_size

        # Create grid GeoDataFrame
        grid_gdf = gpd.GeoDataFrame(geometry=grid_cells, crs=points_gdf.crs)
        grid_gdf["grid_id"] = range(len(grid_gdf))

        # Spatial join to count points per grid cell
        point_counts = points_gdf.sjoin(grid_gdf, how="right", predicate="within")
        grid_stats = point_counts.groupby("grid_id").size().reset_index(name=count_col)

        # Merge back with grid
        result_gdf = grid_gdf.merge(grid_stats, on="grid_id", how="left")
        result_gdf[count_col] = result_gdf[count_col].fillna(0).astype(int)

        # Filter to non-empty cells
        result_gdf = result_gdf[result_gdf[count_col] > 0]

        logger.success(f"  ✅ Created {len(result_gdf):,} grid cells with point data")
        return result_gdf

    except Exception as e:
        logger.critical(f"❌ Error creating grid aggregation: {e}")
        return None


def analyze_points_by_polygons(points_gdf: gpd.GeoDataFrame, polygons_gdf: gpd.GeoDataFrame,
                              polygon_id_col: str = "polygon_id", 
                              point_id_col: str = "point_id",
                              count_col: str = "point_count",
                              density_col: str = "point_density") -> gpd.GeoDataFrame:
    """
    Analyze point distribution by polygon areas (generalized block group analysis).
    
    Args:
        points_gdf: GeoDataFrame with point geometries
        polygons_gdf: GeoDataFrame with polygon geometries
        polygon_id_col: ID column in polygons
        point_id_col: ID column in points
        count_col: Name for point count column
        density_col: Name for point density column
        
    Returns:
        Polygons GeoDataFrame with point statistics
    """
    logger.info(f"🏘️ Analyzing point distribution by polygons...")

    try:
        # Ensure consistent CRS
        if points_gdf.crs != polygons_gdf.crs:
            polygons_gdf = polygons_gdf.to_crs(points_gdf.crs)

        # Spatial join
        points_with_polygons = points_gdf.sjoin(polygons_gdf, how="left", predicate="within")
        
        # Aggregate by polygon
        if polygon_id_col in points_with_polygons.columns:
            polygon_stats = points_with_polygons.groupby(polygon_id_col).agg({
                point_id_col: "count"
            }).rename(columns={point_id_col: count_col}).reset_index()

            # Merge with polygon geometries
            result_gdf = polygons_gdf.merge(polygon_stats, on=polygon_id_col, how="left")
        else:
            # Use index if no ID column
            polygon_stats = points_with_polygons.groupby(level=0).size().reset_index(name=count_col)
            result_gdf = polygons_gdf.copy()
            result_gdf[count_col] = 0
            
        result_gdf[count_col] = result_gdf[count_col].fillna(0).astype(int)

        # Calculate point density
        result_gdf_proj = result_gdf.to_crs("EPSG:3857")
        result_gdf["area_km2"] = (result_gdf_proj.geometry.area / 1e6).round(3)
        result_gdf[density_col] = (result_gdf[count_col] / result_gdf["area_km2"]).round(1)
        result_gdf[density_col] = result_gdf[density_col].replace([float("inf"), -float("inf")], 0)

        logger.success(f"  ✅ Analyzed {len(result_gdf):,} polygons")
        
        # Summary statistics
        with_points = result_gdf[result_gdf[count_col] > 0]
        logger.info(f"     📊 Polygons with points: {len(with_points):,}")
        if len(with_points) > 0:
            logger.info(f"     📊 Average point density: {with_points[density_col].mean():.1f}/km²")
        
        return result_gdf

    except Exception as e:
        logger.critical(f"❌ Error analyzing points by polygons: {e}")
        return None


# Property optimization helper functions (from process_election_results.py)
def _is_boolean_data(series: pd.Series) -> bool:
    """Duck-type detection of boolean data."""
    unique_vals = set(str(v).lower() for v in series.dropna().unique())
    boolean_values = {"true", "false", "1", "0", "yes", "no"}
    return len(unique_vals) <= 2 and unique_vals.issubset(boolean_values)


def _is_count_field(col: str, series: pd.Series) -> bool:
    """Detect count fields by pattern and data characteristics."""
    # Pattern-based detection
    if col.startswith("votes_") or col in ["TOTAL", "DEM", "REP", "NAV", "vote_margin"]:
        return True

    # Duck-type detection: integer data with reasonable range for counts
    try:
        numeric_data = pd.to_numeric(series.dropna(), errors="coerce")
        if numeric_data.notna().any():
            is_integer = (numeric_data % 1 == 0).all()
            is_non_negative = (numeric_data >= 0).all()
            reasonable_range = (numeric_data <= 100000).all()  # Generalized upper bound
            return bool(is_integer and is_non_negative and reasonable_range)
    except Exception:
        pass
    return False


def _is_percentage_field(col: str, series: pd.Series) -> bool:
    """Detect percentage fields by pattern and data characteristics."""
    # Pattern-based detection
    percentage_patterns = ["_pct_", "_rate", "_advantage", "_score", "_efficiency", "_potential"]
    if any(pattern in col for pattern in percentage_patterns):
        return True

    # Duck-type detection: numeric data in percentage-like range
    try:
        numeric_data = pd.to_numeric(series.dropna(), errors="coerce")
        if numeric_data.notna().any():
            min_val, max_val = numeric_data.min(), numeric_data.max()
            return bool(-200 <= min_val <= 200 and -200 <= max_val <= 200)
    except Exception:
        pass
    return False


def _is_categorical_field(col: str, series: pd.Series) -> bool:
    """Detect categorical fields by data characteristics."""
    # Skip if looks like numeric data
    try:
        numeric_data = pd.to_numeric(series.dropna(), errors="coerce")
        if numeric_data.notna().sum() > len(series.dropna()) * 0.8:  # 80% numeric
            return False
    except Exception:
        pass

    # Check if it has limited unique values (typical for categories)
    unique_count = series.nunique()
    total_count = len(series.dropna())

    # Consider categorical if: few unique values OR string-like data
    if unique_count <= 20 or (total_count > 0 and unique_count / total_count < 0.1):
        return True

    # Check for common categorical indicators
    sample_values = set(str(v).lower() for v in series.dropna().head(10))
    categorical_indicators = {
        "low", "medium", "high", "small", "large", "strong", "weak",
        "competitive", "safe", "likely", "tossup", "close", "clear", "landslide",
        "dem", "rep", "unknown", "no data", "tie"
    }

    return bool(len(sample_values & categorical_indicators) > 0)


def _is_identifier_field(col: str) -> bool:
    """Detect identifier/name fields by pattern."""
    identifier_patterns = ["precinct", "candidate", "name", "id", "_id", "identifier"]
    return any(pattern in col.lower() for pattern in identifier_patterns)


def _optimize_boolean_field(series: pd.Series) -> pd.Series:
    """Optimize boolean field for web display."""
    return (
        series.astype(str)
        .str.lower()
        .map({"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False})
        .fillna(False)
    )


def _optimize_count_field(series: pd.Series) -> pd.Series:
    """Optimize count field for web display."""
    numeric_series = pd.to_numeric(series, errors="coerce").fillna(0)
    return numeric_series.astype(int)


def _optimize_percentage_field(series: pd.Series, precision: int) -> pd.Series:
    """Optimize percentage field for web display."""
    numeric_series = pd.to_numeric(series, errors="coerce").fillna(0)
    return numeric_series.round(precision)


def _optimize_categorical_field(col: str, series: pd.Series) -> pd.Series:
    """Optimize categorical field for web display."""
    optimized = series.astype(str).replace(["nan", "None", "<NA>", ""], "No Data")

    # Set appropriate defaults for specific field patterns
    if "political" in col.lower() or "lean" in col.lower():
        optimized = optimized.replace("No Data", "Unknown")
    elif "competitive" in col.lower():
        optimized = optimized.replace("No Data", "No Election Data")
    elif "candidate" in col.lower():
        optimized = optimized.replace("No Data", "No Data")

    return optimized


def _optimize_identifier_field(series: pd.Series) -> pd.Series:
    """Optimize identifier field for web display."""
    return series.astype(str).str.strip()


def _optimize_unknown_field(col: str, series: pd.Series, precision: int) -> pd.Series:
    """Fallback optimization for unknown field types."""
    # Try numeric first
    try:
        numeric_series = pd.to_numeric(series, errors="coerce")
        if numeric_series.notna().sum() > len(series) * 0.7:  # 70% numeric
            # If mostly integers, treat as count
            if (numeric_series.dropna() % 1 == 0).all():
                return numeric_series.fillna(0).astype(int)
            # Otherwise, treat as decimal with precision
            else:
                return numeric_series.fillna(0).round(precision)
    except Exception:
        pass

    # Fall back to string
    return series.astype(str).str.strip()


def optimize_geojson_properties(gdf: gpd.GeoDataFrame, config: Config) -> gpd.GeoDataFrame:
    """
    Optimizes GeoDataFrame properties for web display and vector tile generation.
    Uses field registry and duck-typing instead of hardcoded column names.

    Args:
        gdf: Input GeoDataFrame
        config: Configuration instance

    Returns:
        GeoDataFrame with optimized properties
    """
    logger.debug("🔧 Optimizing properties for web display using dynamic field detection:")

    # Create a copy to avoid modifying original
    gdf_optimized = gdf.copy()

    # Get precision settings from config
    prop_precision = config.get_system_setting("property_precision")

    # Clean up property names and values for web consumption
    columns_to_clean = gdf_optimized.columns.tolist()
    if "geometry" in columns_to_clean:
        columns_to_clean.remove("geometry")

    # Track optimization stats and collect optimized columns
    optimized_counts = {
        "boolean": 0, "count": 0, "percentage": 0, 
        "categorical": 0, "identifier": 0, "unknown": 0
    }

    # Collect all optimized columns to update at once
    optimized_data = {}

    for col in columns_to_clean:
        if col in gdf_optimized.columns:
            series = gdf_optimized[col]

            # Use duck typing to determine field type and optimize accordingly
            if col.startswith(("is_", "has_")) or _is_boolean_data(series):
                optimized_data[col] = _optimize_boolean_field(series)
                optimized_counts["boolean"] += 1
            elif _is_count_field(col, series):
                optimized_data[col] = _optimize_count_field(series)
                optimized_counts["count"] += 1
            elif _is_percentage_field(col, series):
                optimized_data[col] = _optimize_percentage_field(series, prop_precision)
                optimized_counts["percentage"] += 1
            elif _is_categorical_field(col, series):
                optimized_data[col] = _optimize_categorical_field(col, series)
                optimized_counts["categorical"] += 1
            elif _is_identifier_field(col):
                optimized_data[col] = _optimize_identifier_field(series)
                optimized_counts["identifier"] += 1
            else:
                optimized_data[col] = _optimize_unknown_field(col, series, prop_precision)
                optimized_counts["unknown"] += 1

    # Update all optimized columns at once to avoid DataFrame fragmentation
    if optimized_data:
        gdf_optimized = gdf_optimized.assign(**optimized_data)

    # Log optimization results
    total_optimized = sum(optimized_counts.values())
    logger.debug(f"  ✓ Optimized {total_optimized} property columns:")
    for field_type, count in optimized_counts.items():
        if count > 0:
            logger.debug(f"    - {field_type}: {count} fields")

    # Add web-friendly geometry validation
    invalid_geom = gdf_optimized.geometry.isna() | (~gdf_optimized.geometry.is_valid)
    invalid_count = invalid_geom.sum()

    if invalid_count > 0:
        logger.warning(f"  ⚠️ Found {invalid_count} invalid geometries, attempting to fix...")
        gdf_optimized.geometry = gdf_optimized.geometry.buffer(0)

        still_invalid = gdf_optimized.geometry.isna() | (~gdf_optimized.geometry.is_valid)
        still_invalid_count = still_invalid.sum()

        if still_invalid_count > 0:
            logger.warning(f"  ⚠️ {still_invalid_count} geometries still invalid after fix attempt")
        else:
            logger.debug("  ✓ Fixed all invalid geometries")
    else:
        logger.debug("  ✓ All geometries are valid")

    return gdf_optimized 