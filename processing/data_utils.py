#!/usr/bin/env python3
"""
data_utils.py - Shared Data Processing Utilities

Common functions used across all data processing scripts.
Eliminates code duplication and ensures consistency.
"""

import re
from pathlib import Path
from typing import List, Tuple

import geopandas as gpd
import pandas as pd
from loguru import logger

# Proper imports - no path hacking needed
try:
    from .config_loader import Config
    from .supabase_integration import SupabaseUploader
except ImportError:
    # Fallback for development when running as script
    from config_loader import Config
    from supabase_integration import SupabaseUploader


def sanitize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Convert column names to clean snake_case format.

    This function standardizes all column names to snake_case, making
    downstream processing predictable and eliminating the need for
    config-based column mapping.

    Args:
        df: DataFrame with potentially messy column names

    Returns:
        DataFrame with clean snake_case column names
    """
    logger.info("🧹 Sanitizing column names...")

    original_cols = df.columns.tolist()

    # Convert to snake_case
    clean_cols = []
    for col in original_cols:
        # Convert to string and handle basic cleaning
        clean_col = str(col).strip()

        # Replace spaces and special chars with underscores
        clean_col = re.sub(r"[^\w\s]", "_", clean_col)
        clean_col = re.sub(r"\s+", "_", clean_col)

        # Convert to lowercase
        clean_col = clean_col.lower()

        # Remove multiple underscores
        clean_col = re.sub(r"_+", "_", clean_col)

        # Remove leading/trailing underscores
        clean_col = clean_col.strip("_")

        # Handle empty or problematic names
        if not clean_col or clean_col.isdigit():
            clean_col = f"column_{len(clean_cols)}"

        clean_cols.append(clean_col)

    # Create mapping for logging
    col_mapping = dict(zip(original_cols, clean_cols))

    # Log significant changes
    changed_cols = [(orig, new) for orig, new in col_mapping.items() if orig != new]
    if changed_cols:
        logger.info(f"  📝 Cleaned {len(changed_cols)} column names:")
        for orig, new in changed_cols[:5]:  # Show first 5
            logger.info(f"    '{orig}' → '{new}'")
        if len(changed_cols) > 5:
            logger.info(f"    ... and {len(changed_cols) - 5} more")

    # Apply new column names
    df.columns = clean_cols

    logger.info("  ✅ All columns now in snake_case format")
    return df


def find_column_by_pattern(df: pd.DataFrame, patterns: list, description: str = "column") -> str:
    """Find a column by matching patterns (case-insensitive).

    Args:
        df: DataFrame to search
        patterns: List of patterns to match (e.g., ["precinct", "district"])
        description: Description for logging

    Returns:
        Column name if found, None if not found
    """
    for pattern in patterns:
        matching_cols = [col for col in df.columns if pattern.lower() in col.lower()]
        if matching_cols:
            logger.info(f"  📍 Found {description} column: {matching_cols[0]} (pattern: {pattern})")
            return matching_cols[0]

    logger.warning(f"  ⚠️ No {description} column found for patterns: {patterns}")
    return None


def validate_required_columns(df: pd.DataFrame, required_patterns: dict) -> bool:
    """Validate that required columns exist in DataFrame.

    Args:
        df: DataFrame to validate
        required_patterns: Dict of {description: [patterns]} for required columns

    Returns:
        True if all required columns found, False otherwise
    """
    missing_columns = []

    for description, patterns in required_patterns.items():
        if not find_column_by_pattern(df, patterns, description):
            missing_columns.append(description)

    if missing_columns:
        logger.error(f"❌ Missing required columns: {missing_columns}")
        logger.info(f"Available columns: {list(df.columns)}")
        return False

    return True


def clean_and_validate(gdf: gpd.GeoDataFrame, data_type: str = "geodata") -> gpd.GeoDataFrame:
    """Clean data types and validate geometries - UNIVERSAL function.

    This function handles the common cleaning and validation needed by ALL
    processing scripts. No more duplication!

    Args:
        gdf: GeoDataFrame to clean and validate
        data_type: Type of data for context ("election", "household", "voter", etc.)

    Returns:
        Cleaned and validated GeoDataFrame
    """
    logger.info(f"🧹 Cleaning and validating {data_type} data...")

    # Ensure WGS84 for output (web standard)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
        logger.info("  🌍 Set CRS to WGS84 (was None)")
    elif gdf.crs.to_epsg() != 4326:
        logger.info(f"  🔄 Reprojecting from {gdf.crs} to WGS84")
        gdf = gdf.to_crs("EPSG:4326")

    # Clean numeric columns (universal patterns)
    numeric_patterns = [
        "votes_",
        "vote_pct_",
        "reg_pct_",
        "pct_",
        "total_",
        "household",
        "density",
        "margin",
        "turnout",
        "score",
        "weight",
        "dominance",
        "contribution",
        "area_sq",
    ]

    numeric_cols = []
    for col in gdf.columns:
        if col == "geometry":
            continue
        if any(pattern in col.lower() for pattern in numeric_patterns):
            numeric_cols.append(col)

    for col in numeric_cols:
        if col in gdf.columns:
            gdf[col] = pd.to_numeric(gdf[col], errors="coerce").fillna(0)

    if numeric_cols:
        logger.info(f"  🔢 Cleaned {len(numeric_cols)} numeric columns")

    # Clean categorical columns for GeoJSON compatibility
    categorical_patterns = [
        "category",
        "classification",
        "lean",
        "composition",
        "competitiveness",
        "leading_candidate",
        "margin_category",
        "density_category",
        "family_composition",
    ]

    categorical_cols = []
    for col in gdf.columns:
        if col == "geometry":
            continue
        if any(pattern in col.lower() for pattern in categorical_patterns):
            categorical_cols.append(col)

    for col in categorical_cols:
        if col in gdf.columns:
            gdf[col] = gdf[col].astype(str).replace(["nan", "None", "<NA>"], "No Data")

    if categorical_cols:
        logger.info(f"  📝 Cleaned {len(categorical_cols)} categorical columns")

    # Validate geometries
    invalid_geom = gdf[~gdf.geometry.is_valid]
    if len(invalid_geom) > 0:
        logger.warning(f"  ⚠️ Found {len(invalid_geom)} invalid geometries, fixing...")
        gdf.geometry = gdf.geometry.buffer(0)  # Fix topology errors
        logger.info("  🔧 Fixed invalid geometries")

    logger.info(f"  ✅ {data_type.title()} data cleaned and validated")
    return gdf


def ensure_output_directory(output_path: str | Path) -> Path:
    """Ensure output directory exists and return Path object.

    Args:
        output_path: Output file path (string or Path)

    Returns:
        Path object with directory created
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path


def upload_geo_file(file_path: str, table_name: str) -> bool:
    """Upload any geospatial file to maps database. Core upload function.

    Args:
        file_path: Path to geospatial file
        table_name: Target table name in database

    Returns:
        Success status
    """
    file_path = Path(file_path)

    # Check file exists
    if not file_path.exists():
        logger.error(f"❌ File not found: {file_path}")
        return False

    logger.info(f"🗺️ Loading {file_path}")

    try:
        # Load any geo format (geopandas handles everything)
        gdf = gpd.read_file(file_path)
        logger.info(f"  ✅ Loaded {len(gdf):,} features")

        # Standardize for web (WGS84 is web standard)
        if gdf.crs is None:
            logger.warning("  ⚠️ No CRS found, assuming WGS84")
            gdf = gdf.set_crs("EPSG:4326")
        elif gdf.crs.to_epsg() != 4326:
            logger.info(f"  🔄 Converting from {gdf.crs} to WGS84")
            gdf = gdf.to_crs("EPSG:4326")

        # Fix basic issues
        original_count = len(gdf)
        gdf = gdf[gdf.geometry.notna()]
        if len(gdf) < original_count:
            logger.info(f"  🧹 Removed {original_count - len(gdf)} features with invalid geometry")

        # Upload to Supabase
        try:
            uploader = SupabaseUploader(Config())

            logger.info(f"🚀 Uploading to table '{table_name}'...")
            success = uploader.upload_geodataframe(
                gdf, table_name=table_name, description=f"Uploaded from {file_path.name}"
            )

            if success:
                logger.success(f"✅ Success: {len(gdf):,} features → {table_name}")
                return True
            else:
                logger.error("❌ Upload failed")
                return False

        except ImportError as e:
            logger.error(f"❌ Supabase integration not available: {e}")
            return False

    except Exception as e:
        logger.error(f"❌ Failed to process {file_path}: {e}")
        return False


def get_processed_files() -> List[Tuple[str, str]]:
    """Get processed files from pipeline outputs.

    Returns:
        List of (file_path, table_name) tuples
    """
    # Check both possible locations for processed files
    possible_locations = [
        Path("../data/processed"),  # Preferred location (from processing/)
        Path("data/processed"),  # If called from root
        Path("processing/data"),  # Legacy location
        Path("data"),  # Fallback
    ]

    # Standard processed file patterns
    processed_patterns = [
        ("processed_election_data.geojson", "election_analysis"),
        ("processed_households_data.geojson", "household_demographics"),
        ("processed_voters_data.geojson", "voter_analysis"),
        ("processed_voter_hexagons.geojson", "voter_hexagons"),
        ("processed_voter_blockgroups.geojson", "voter_blockgroups"),
    ]

    files_found = []

    for location in possible_locations:
        if not location.exists():
            continue

        for pattern, table_name in processed_patterns:
            file_path = location / pattern
            if file_path.exists():
                files_found.append((str(file_path), table_name))
                break  # Found it, don't check other locations

    return files_found


def get_reference_files() -> List[Tuple[str, str]]:
    """Dynamically discover reference data files.

    Returns:
        List of (file_path, table_name) tuples
    """
    # Check possible locations for reference data
    possible_locations = [
        Path("../data/geospatial"),  # From processing/
        Path("data/geospatial"),  # From root
    ]

    reference_dir = None
    for location in possible_locations:
        if location.exists():
            reference_dir = location
            break

    if not reference_dir:
        logger.warning("⚠️ Reference data directory not found")
        return []

    files_found = []

    # PPS files with smart table naming
    pps_files = list(reference_dir.glob("pps_*.geojson"))
    for file_path in pps_files:
        # Convert filename to clean table name
        table_name = file_path.stem  # Remove .geojson
        # Normalize: pps_elementary_school_boundaries -> pps_elementary_boundaries
        if "school_boundaries" in table_name:
            table_name = table_name.replace("_school_boundaries", "_boundaries")
        elif "school_locations" in table_name:
            table_name = table_name.replace("_school_locations", "_schools")

        files_found.append((str(file_path), table_name))

    # Other reference files
    other_files = [
        ("multnomah_elections_precinct_split_2024.geojson", "election_precincts"),
        ("multnomah_election_precincts.geojson", "election_precincts_simple"),
        ("tl_2022_41_bg.geojson", "census_block_groups"),
    ]

    for pattern, table_name in other_files:
        file_path = reference_dir / pattern
        if file_path.exists():
            files_found.append((str(file_path), table_name))

    return files_found


def upload_batch(files: List[Tuple[str, str]], category: str) -> bool:
    """Upload a batch of files.

    Args:
        files: List of (file_path, table_name) tuples
        category: Category description for logging

    Returns:
        Success status (True if all files uploaded successfully)
    """
    if not files:
        logger.warning(f"⚠️ No {category} files found")
        return True

    logger.info(f"📤 Uploading {category} data ({len(files)} files)...")

    success_count = 0
    for file_path, table_name in files:
        logger.info(f"  📊 {Path(file_path).name} → {table_name}")

        if upload_geo_file(file_path, table_name):
            success_count += 1
        else:
            logger.error(f"  ❌ Failed to upload {file_path}")

    logger.info(f"📊 {category} summary: {success_count}/{len(files)} files uploaded successfully")
    return success_count == len(files)


def upload_processed_data() -> bool:
    """Upload all processed data files to Supabase.

    Returns:
        Success status
    """
    logger.info("📤 Uploading processed data to Supabase...")
    processed_files = get_processed_files()
    return upload_batch(processed_files, "processed")


def upload_reference_data() -> bool:
    """Upload reference geospatial data to Supabase.

    Returns:
        Success status
    """
    logger.info("🗺️ Uploading reference geospatial data to Supabase...")
    reference_files = get_reference_files()
    return upload_batch(reference_files, "reference")


def upload_all_data() -> bool:
    """Upload all data (processed + reference) to Supabase.

    Returns:
        Success status
    """
    logger.info("🚀 Uploading ALL data to Supabase...")

    success = True
    success &= upload_processed_data()
    success &= upload_reference_data()

    if success:
        logger.success("🎉 All data uploaded successfully!")
    else:
        logger.error("💥 Some uploads failed")

    return success


# ============================================================================
# CLI Interface using Click
# ============================================================================

try:
    import click

    @click.group(invoke_without_command=True)
    @click.pass_context
    @click.option("--file", "file_path", help="Single file to upload")
    @click.option("--table", "table_name", help="Table name for single file")
    def cli(ctx, file_path, table_name):
        """🗺️ MVP Geospatial Upload Tool

        Upload geospatial data to Supabase PostGIS database.
        """
        if ctx.invoked_subcommand is None:
            if file_path and table_name:
                # Single file upload
                logger.info("🗺️ MVP Geospatial Upload Tool")
                logger.info("=" * 40)

                if upload_geo_file(file_path, table_name):
                    logger.success(f"🎉 Done! Data available in maps database as '{table_name}'")
                    ctx.exit(0)
                else:
                    logger.error("💥 Upload failed")
                    ctx.exit(1)
            else:
                click.echo(ctx.get_help())
                ctx.exit(1)

    @cli.command()
    def processed():
        """Upload all processed pipeline outputs"""
        logger.info("🗺️ MVP Geospatial Upload Tool")
        logger.info("=" * 40)

        if upload_processed_data():
            logger.success("🎉 Processed data uploaded successfully!")
        else:
            click.echo("💥 Upload failed", err=True)
            raise click.Abort()

    @cli.command()
    def reference():
        """Upload all reference geospatial data"""
        logger.info("🗺️ MVP Geospatial Upload Tool")
        logger.info("=" * 40)

        if upload_reference_data():
            logger.success("🎉 Reference data uploaded successfully!")
        else:
            click.echo("💥 Upload failed", err=True)
            raise click.Abort()

    @cli.command()
    def all():
        """Upload everything (processed + reference)"""
        logger.info("🗺️ MVP Geospatial Upload Tool")
        logger.info("=" * 40)

        if upload_all_data():
            logger.success("🎉 All data uploaded successfully!")
        else:
            click.echo("💥 Some uploads failed", err=True)
            raise click.Abort()

    # Make CLI available when run as script
    if __name__ == "__main__":
        cli()

except ImportError:
    # Fallback if click not available
    def cli():
        logger.error("❌ Click not installed. Install with: pip install click")
        logger.info("💡 Or use functions directly:")
        logger.info("   from processing.data_utils import upload_geo_file, upload_all_data")

    if __name__ == "__main__":
        cli()
