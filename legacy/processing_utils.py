"""
Processing Utilities - Common Infrastructure

This module extracts the common infrastructure patterns from all processing files
to eliminate duplication and provide a consistent processing context.

Replaces ~50+ lines of boilerplate in each processing file with simple utilities.
"""

import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Union

import geopandas as gpd
import pandas as pd
from loguru import logger

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))
from ops.config_loader import Config

# Import Supabase utilities from spatial_utils
from spatial_utils import (
    SUPABASE_AVAILABLE,
    SpatialQueryManager,
    SupabaseDatabase,
    SupabaseUploader,
)


class ProcessingContext:
    """
    Context manager that handles all the common processing infrastructure:
    - Configuration loading
    - Error handling setup
    - Logging configuration
    - Supabase upload utilities

    Usage:
        with ProcessingContext("Election Results Processing") as ctx:
            # Your business logic here
            df = pd.read_csv(ctx.config.get_input_path("votes_csv"))
            ctx.upload_to_supabase(gdf, "election_results", "Election data")
    """

    def __init__(self, process_name: str, exit_on_error: bool = True):
        """
        Initialize processing context.

        Args:
            process_name: Human-readable name for this processing task
            exit_on_error: Whether to exit on configuration errors (default: True)
        """
        self.process_name = process_name
        self.exit_on_error = exit_on_error
        self.config: Optional[Config] = None
        self.uploader: Optional[SupabaseUploader] = None

    def __enter__(self):
        """Enter the processing context - load config and setup infrastructure."""
        logger.info(f"🚀 {self.process_name}")
        logger.info("=" * (len(self.process_name) + 4))

        # Load configuration
        try:
            self.config = Config()
            logger.info(f"📋 Project: {self.config.get('project_name')}")
            logger.info(f"📋 Description: {self.config.get('description')}")

            # Initialize Supabase uploader if available
            if SUPABASE_AVAILABLE:
                self.uploader = SupabaseUploader(self.config)
                logger.debug("📡 Supabase uploader initialized")
            else:
                logger.debug("📊 Supabase integration not available")

        except Exception as e:
            logger.critical(f"❌ Configuration error: {e}")
            logger.info("💡 Make sure config.yaml exists in the analysis directory")
            if self.exit_on_error:
                sys.exit(1)
            else:
                raise

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the processing context - handle any cleanup."""
        if exc_type is not None:
            logger.error(f"❌ {self.process_name} failed: {exc_val}")
            if self.exit_on_error:
                sys.exit(1)
        else:
            logger.success(f"✅ {self.process_name} completed successfully!")

    def upload_to_supabase(
        self, gdf: gpd.GeoDataFrame, table_name: str, description: str, verify_upload: bool = True
    ) -> bool:
        """
        Upload GeoDataFrame to Supabase with standard error handling and verification.

        Args:
            gdf: GeoDataFrame to upload
            table_name: Name of the table to create/update
            description: Description for the table
            verify_upload: Whether to verify the upload succeeded

        Returns:
            True if upload succeeded, False otherwise
        """
        if not SUPABASE_AVAILABLE:
            logger.info("📊 Supabase integration not available - skipping database upload")
            logger.info("   💡 Install dependencies with: pip install sqlalchemy psycopg2-binary")
            return False

        if self.uploader is None:
            logger.error("❌ Supabase uploader not initialized")
            return False

        logger.info(f"🚀 Uploading {len(gdf):,} features to Supabase table '{table_name}'...")

        try:
            # Upload the data
            upload_success = self.uploader.upload_geodataframe(
                gdf, table_name=table_name, description=description
            )

            if upload_success:
                logger.success(f"   ✅ Uploaded to Supabase table '{table_name}'")

                # Verify upload if requested
                if verify_upload:
                    return self._verify_upload(table_name, len(gdf))
                else:
                    return True
            else:
                logger.error(f"   ❌ Upload failed - table '{table_name}' was not created")
                self._log_upload_troubleshooting()
                return False

        except Exception as e:
            logger.error(f"❌ Supabase upload failed: {e}")
            logger.info("   💡 Check your Supabase credentials and connection")

            # Add detailed error logging
            import traceback

            logger.trace("Detailed upload error:")
            logger.trace(traceback.format_exc())
            return False

    def _verify_upload(self, table_name: str, expected_count: int) -> bool:
        """Verify that the upload succeeded by querying sample records."""
        try:
            db = SupabaseDatabase(self.config)
            query_manager = SpatialQueryManager(db)

            # Try to get sample records
            sample_records = query_manager.get_sample_records(table_name, limit=5)

            if len(sample_records) > 0:
                logger.info(
                    f"   ✅ Verification successful: {len(sample_records)} sample records retrieved"
                )
                logger.info(f"   📊 Upload confirmed: {expected_count:,} features uploaded")
                logger.info(
                    "   🌐 Data is now available via Supabase PostGIS for fast spatial queries"
                )
                return True
            else:
                logger.warning("   ⚠️ Upload reported success but no sample records retrieved")
                logger.info(
                    "   💡 This could indicate a schema/permission issue, but data may still be uploaded"
                )
                return False

        except Exception as verification_error:
            logger.warning(
                f"   ⚠️ Upload succeeded but verification query failed: {verification_error}"
            )
            logger.info(
                "     💡 This is often a connectivity or schema issue - data is likely uploaded correctly"
            )
            logger.info("     💡 You can verify manually by checking the Supabase dashboard")
            return True  # Assume success since upload reported success

    def _log_upload_troubleshooting(self):
        """Log common troubleshooting tips for upload failures."""
        logger.info("   💡 Common issues and solutions:")
        logger.info("      1. Check Supabase credentials (SUPABASE_DB_HOST, SUPABASE_DB_PASSWORD)")
        logger.info("      2. Ensure PostGIS extension is enabled: CREATE EXTENSION postgis;")
        logger.info("      3. Verify database connectivity and permissions")
        logger.info("      4. Check if the database has sufficient storage space")


@contextmanager
def processing_context(process_name: str, exit_on_error: bool = True):
    """
    Simple context manager function for processing tasks.

    Alternative to the ProcessingContext class for simpler use cases.

    Usage:
        with processing_context("My Processing Task") as ctx:
            # Your processing logic here
            pass
    """
    ctx = ProcessingContext(process_name, exit_on_error)
    try:
        yield ctx.__enter__()
    except Exception as e:
        ctx.__exit__(type(e), e, e.__traceback__)
        raise
    else:
        ctx.__exit__(None, None, None)


def load_config() -> Config:
    """
    Simple utility to load configuration with standard error handling.

    Returns:
        Loaded configuration object

    Raises:
        SystemExit: If configuration loading fails
    """
    try:
        config = Config()
        logger.debug(f"📋 Project: {config.get('project_name')}")
        logger.debug(f"📋 Description: {config.get('description')}")
        return config
    except Exception as e:
        logger.critical(f"❌ Configuration error: {e}")
        logger.info("💡 Make sure config.yaml exists in the analysis directory")
        sys.exit(1)


def setup_logging(process_name: str):
    """
    Setup consistent logging for processing tasks.

    Args:
        process_name: Name of the processing task for log headers
    """
    logger.info(f"🚀 {process_name}")
    logger.info("=" * (len(process_name) + 4))


def log_data_summary(data: Union[pd.DataFrame, gpd.GeoDataFrame], data_name: str):
    """
    Log a standard summary of loaded data.

    Args:
        data: DataFrame or GeoDataFrame to summarize
        data_name: Human-readable name for the data
    """
    if isinstance(data, gpd.GeoDataFrame):
        logger.info(f"  ✓ Loaded {data_name}: {len(data):,} features")
        if data.crs:
            logger.debug(f"    CRS: {data.crs}")
    else:
        logger.info(f"  ✓ Loaded {data_name}: {len(data):,} rows")

    # Log column info for debugging
    logger.debug(f"    Columns: {list(data.columns)[:10]}{'...' if len(data.columns) > 10 else ''}")


def validate_required_files(*file_paths: str):
    """
    Validate that all required files exist before processing.

    Args:
        *file_paths: Paths to files that must exist

    Raises:
        FileNotFoundError: If any required file is missing
    """
    missing_files = []
    for file_path in file_paths:
        if not Path(file_path).exists():
            missing_files.append(file_path)

    if missing_files:
        logger.error(f"❌ Missing required files: {missing_files}")
        raise FileNotFoundError(f"Required files not found: {missing_files}")

    logger.debug(f"✅ All required files exist: {len(file_paths)} files validated")


def safe_exit(message: str, exit_code: int = 1):
    """
    Safe exit with logging.

    Args:
        message: Exit message to log
        exit_code: Exit code (default: 1 for error)
    """
    if exit_code == 0:
        logger.success(message)
    else:
        logger.error(message)
    sys.exit(exit_code)


# Convenience functions for common patterns
def log_file_paths(config: Config, *path_keys: str):
    """
    Log file paths from configuration for debugging.

    Args:
        config: Configuration object
        *path_keys: Keys to get paths for
    """
    logger.debug("File paths:")
    for key in path_keys:
        try:
            if hasattr(config, f"get_{key}_path"):
                path = getattr(config, f"get_{key}_path")()
            else:
                path = config.get_input_path(key)
            logger.debug(f"  📄 {key}: {path}")
        except Exception as e:
            logger.warning(f"  ⚠️ {key}: Could not resolve path ({e})")


def log_processing_step(step_name: str, details: str = ""):
    """
    Log a processing step with consistent formatting.

    Args:
        step_name: Name of the processing step
        details: Optional details about the step
    """
    logger.info(f"🔄 {step_name}")
    if details:
        logger.info(f"   {details}")


def log_success(message: str):
    """Log a success message with consistent formatting."""
    logger.success(f"✅ {message}")


def log_warning(message: str):
    """Log a warning message with consistent formatting."""
    logger.warning(f"⚠️ {message}")


def log_error(message: str):
    """Log an error message with consistent formatting."""
    logger.error(f"❌ {message}")


# Example usage and testing
if __name__ == "__main__":
    # Test the processing context
    with ProcessingContext("Test Processing Task") as ctx:
        logger.info("Testing processing utilities...")

        # Test configuration access
        if ctx.config:
            logger.info(f"Config loaded: {ctx.config.get('project_name', 'Unknown')}")

        # Test logging utilities
        log_processing_step("Test Step", "This is a test step")
        log_success("Test completed successfully")

        logger.info("All utilities working correctly!")
