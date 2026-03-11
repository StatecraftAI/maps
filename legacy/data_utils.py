"""
Data Utilities - Common Data Loading and Validation

This module extracts the common data loading patterns from all processing files
to eliminate duplication and provide consistent data handling.

Replaces repeated data loading logic with simple, reusable functions.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import geopandas as gpd
import pandas as pd
from loguru import logger

# Import spatial utilities for coordinate validation
from spatial_utils import clean_numeric


def load_csv(
    file_path: Union[str, Path],
    dtype: Optional[Union[str, Dict[str, str]]] = None,
    clean_numeric_cols: Optional[List[str]] = None,
    validate_coords: bool = False,
    coord_bounds: Optional[Dict[str, float]] = None,
    **kwargs,
) -> Optional[pd.DataFrame]:
    """
    Load CSV file with standard error handling and optional data cleaning.

    Args:
        file_path: Path to CSV file
        dtype: Data type specification for pandas
        clean_numeric_cols: List of columns to clean with clean_numeric()
        validate_coords: Whether to validate coordinate columns
        coord_bounds: Coordinate bounds for validation (lat_min, lat_max, lon_min, lon_max)
        **kwargs: Additional arguments passed to pd.read_csv()

    Returns:
        Loaded DataFrame or None if failed
    """
    file_path = Path(file_path)
    logger.info(f"📄 Loading CSV from {file_path}")

    if not file_path.exists():
        logger.error(f"❌ CSV file not found: {file_path}")
        return None

    try:
        # Set default parameters
        csv_params = {"low_memory": False, **kwargs}

        if dtype is not None:
            csv_params["dtype"] = dtype

        # Load the CSV
        df = pd.read_csv(file_path, **csv_params)
        logger.success(f"  ✅ Loaded CSV with {len(df):,} rows, {len(df.columns)} columns")

        # Clean numeric columns if specified
        if clean_numeric_cols:
            for col in clean_numeric_cols:
                if col in df.columns:
                    df[col] = clean_numeric(df[col])
                    logger.debug(f"    Cleaned numeric column: {col}")
                else:
                    logger.warning(f"    Column not found for cleaning: {col}")

        # Validate coordinates if specified
        if validate_coords:
            df = _validate_coordinates(df, coord_bounds)

        return df

    except Exception as e:
        logger.error(f"❌ Error loading CSV: {e}")
        return None


def load_geojson(
    file_path: Union[str, Path],
    validate_crs: bool = True,
    fix_geometries: bool = True,
    filter_county: Optional[str] = None,
    **kwargs,
) -> Optional[gpd.GeoDataFrame]:
    """
    Load GeoJSON file with standard error handling and validation.

    Args:
        file_path: Path to GeoJSON file
        validate_crs: Whether to validate and standardize CRS to WGS84
        fix_geometries: Whether to fix invalid geometries
        filter_county: County FIPS code to filter to (e.g., "051" for Multnomah)
        **kwargs: Additional arguments passed to gpd.read_file()

    Returns:
        Loaded GeoDataFrame or None if failed
    """
    file_path = Path(file_path)
    logger.info(f"🗺️ Loading GeoJSON from {file_path}")

    if not file_path.exists():
        logger.error(f"❌ GeoJSON file not found: {file_path}")
        return None

    try:
        # Load the GeoJSON
        gdf = gpd.read_file(file_path, **kwargs)
        logger.success(f"  ✅ Loaded GeoJSON with {len(gdf):,} features")

        # Filter to specific county if requested
        if filter_county and "STATEFP" in gdf.columns and "COUNTYFP" in gdf.columns:
            original_count = len(gdf)
            gdf = gdf[(gdf["STATEFP"] == "41") & (gdf["COUNTYFP"] == filter_county)].copy()
            logger.info(
                f"    Filtered to county {filter_county}: {len(gdf):,} features (was {original_count:,})"
            )

        # Validate and standardize CRS
        if validate_crs:
            gdf = _validate_crs(gdf)

        # Fix invalid geometries
        if fix_geometries:
            gdf = _fix_geometries(gdf)

        return gdf

    except Exception as e:
        logger.error(f"❌ Error loading GeoJSON: {e}")
        return None


def load_json_array(
    file_path: Union[str, Path], header_row: int = 0, data_start_row: int = 1
) -> Optional[pd.DataFrame]:
    """
    Load JSON file with array format (like ACS data).

    Args:
        file_path: Path to JSON file
        header_row: Row index containing column headers
        data_start_row: Row index where data starts

    Returns:
        Loaded DataFrame or None if failed
    """
    file_path = Path(file_path)
    logger.info(f"📄 Loading JSON array from {file_path}")

    if not file_path.exists():
        logger.error(f"❌ JSON file not found: {file_path}")
        return None

    try:
        with open(file_path, "r") as f:
            data_array = json.load(f)

        if not isinstance(data_array, list) or len(data_array) < 2:
            logger.error("❌ JSON file must contain an array with at least header and data rows")
            return None

        # Extract header and data
        header = data_array[header_row]
        records = data_array[data_start_row:]

        # Create DataFrame
        df = pd.DataFrame(records, columns=header)
        logger.success(f"  ✅ Loaded JSON array with {len(df):,} rows, {len(df.columns)} columns")

        return df

    except Exception as e:
        logger.error(f"❌ Error loading JSON array: {e}")
        return None


def load_json(file_path: Union[str, Path]) -> Optional[Dict[str, Any]]:
    """
    Load standard JSON file.

    Args:
        file_path: Path to JSON file

    Returns:
        Loaded JSON data or None if failed
    """
    file_path = Path(file_path)
    logger.info(f"📄 Loading JSON from {file_path}")

    if not file_path.exists():
        logger.error(f"❌ JSON file not found: {file_path}")
        return None

    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        logger.success("  ✅ Loaded JSON data")
        return data

    except Exception as e:
        logger.error(f"❌ Error loading JSON: {e}")
        return None


def validate_required_columns(
    df: pd.DataFrame, required_cols: List[str], data_name: str = "data"
) -> bool:
    """
    Validate that DataFrame contains all required columns.

    Args:
        df: DataFrame to validate
        required_cols: List of required column names
        data_name: Name of the data for error messages

    Returns:
        True if all columns present, False otherwise
    """
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        logger.error(f"❌ {data_name} missing required columns: {missing_cols}")
        logger.info(f"    Available columns: {list(df.columns)}")
        return False

    logger.debug(f"✅ {data_name} has all required columns: {required_cols}")
    return True


def clean_coordinate_columns(
    df: pd.DataFrame, lat_col: str = "Latitude", lon_col: str = "Longitude"
) -> pd.DataFrame:
    """
    Clean and validate coordinate columns.

    Args:
        df: DataFrame with coordinate columns
        lat_col: Name of latitude column
        lon_col: Name of longitude column

    Returns:
        DataFrame with cleaned coordinates
    """
    if lat_col not in df.columns or lon_col not in df.columns:
        logger.warning(f"⚠️ Coordinate columns not found: {lat_col}, {lon_col}")
        return df

    original_count = len(df)

    # Clean numeric values
    df[lat_col] = clean_numeric(df[lat_col])
    df[lon_col] = clean_numeric(df[lon_col])

    # Remove invalid coordinates (Oregon bounds)
    valid_coords = (df[lat_col].between(45.0, 46.0)) & (df[lon_col].between(-123.5, -122.0))
    df = df[valid_coords].copy()

    removed_count = original_count - len(df)
    if removed_count > 0:
        logger.info(f"  🧹 Removed {removed_count:,} records with invalid coordinates")

    logger.success(f"  ✅ Validated coordinates: {len(df):,} records with valid coordinates")
    return df


def create_point_geodataframe(
    df: pd.DataFrame, lat_col: str = "Latitude", lon_col: str = "Longitude", crs: str = "EPSG:4326"
) -> Optional[gpd.GeoDataFrame]:
    """
    Create GeoDataFrame from DataFrame with coordinate columns.

    Args:
        df: DataFrame with coordinate columns
        lat_col: Name of latitude column
        lon_col: Name of longitude column
        crs: Coordinate reference system

    Returns:
        GeoDataFrame with Point geometries or None if failed
    """
    if lat_col not in df.columns or lon_col not in df.columns:
        logger.error(f"❌ Coordinate columns not found: {lat_col}, {lon_col}")
        return None

    try:
        from shapely.geometry import Point

        logger.info("🗺️ Creating Point geometries...")

        # Create Point geometries
        geometry = [Point(lon, lat) for lon, lat in zip(df[lon_col], df[lat_col])]

        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=crs)

        logger.success(f"  ✅ Created GeoDataFrame with {len(gdf):,} point geometries")
        return gdf

    except Exception as e:
        logger.error(f"❌ Error creating point GeoDataFrame: {e}")
        return None


def log_data_info(data: Union[pd.DataFrame, gpd.GeoDataFrame], data_name: str):
    """
    Log detailed information about loaded data.

    Args:
        data: DataFrame or GeoDataFrame to analyze
        data_name: Human-readable name for the data
    """
    logger.info(f"📊 {data_name} summary:")
    logger.info(f"    Rows: {len(data):,}")
    logger.info(f"    Columns: {len(data.columns)}")

    if isinstance(data, gpd.GeoDataFrame):
        logger.info(f"    CRS: {data.crs}")
        logger.info(
            f"    Geometry type: {data.geometry.geom_type.iloc[0] if len(data) > 0 else 'None'}"
        )

    # Show sample columns
    sample_cols = list(data.columns)[:10]
    if len(data.columns) > 10:
        sample_cols.append("...")
    logger.debug(f"    Sample columns: {sample_cols}")

    # Show data types
    numeric_cols = data.select_dtypes(include=["number"]).columns.tolist()
    text_cols = data.select_dtypes(include=["object"]).columns.tolist()

    if numeric_cols:
        logger.debug(f"    Numeric columns: {len(numeric_cols)}")
    if text_cols:
        logger.debug(f"    Text columns: {len(text_cols)}")


# Private helper functions


def _validate_coordinates(
    df: pd.DataFrame, coord_bounds: Optional[Dict[str, float]] = None
) -> pd.DataFrame:
    """Validate coordinate columns with optional custom bounds."""
    if coord_bounds is None:
        # Default Oregon bounds
        coord_bounds = {"lat_min": 45.0, "lat_max": 46.0, "lon_min": -123.5, "lon_max": -122.0}

    lat_cols = [col for col in df.columns if "lat" in col.lower()]
    lon_cols = [col for col in df.columns if "lon" in col.lower()]

    if not lat_cols or not lon_cols:
        logger.debug("No coordinate columns found for validation")
        return df

    lat_col, lon_col = lat_cols[0], lon_cols[0]
    return clean_coordinate_columns(df, lat_col, lon_col)


def _validate_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Validate and standardize CRS to WGS84."""
    if gdf.crs is None:
        logger.info("    Setting CRS to WGS84")
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        logger.info(f"    Reprojecting from {gdf.crs} to WGS84")
        gdf = gdf.to_crs("EPSG:4326")
    else:
        logger.debug("    CRS already WGS84")

    return gdf


def _fix_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Fix invalid geometries."""
    invalid_geom = gdf.geometry.isna() | (~gdf.geometry.is_valid)
    invalid_count = invalid_geom.sum()

    if invalid_count > 0:
        logger.warning(f"    ⚠️ Fixing {invalid_count} invalid geometries")
        gdf.geometry = gdf.geometry.buffer(0)
    else:
        logger.debug("    All geometries valid")

    return gdf


# Example usage and testing
if __name__ == "__main__":
    logger.info("Testing data utilities...")

    # Test CSV loading (would need actual file)
    # df = load_csv("test.csv", clean_numeric_cols=["votes_total"])

    # Test GeoJSON loading (would need actual file)
    # gdf = load_geojson("test.geojson", validate_crs=True, fix_geometries=True)

    logger.info("Data utilities ready for use!")
