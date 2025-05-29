"""
Processing package for StatecraftAI Maps

This package contains all data processing utilities and scripts for the maps pipeline.
"""

__version__ = "0.1.0"

# Import key utilities for easy access
from .data_utils import (
    clean_and_validate,
    find_column_by_pattern,
    sanitize_column_names,
    upload_all_data,
    upload_geo_file,
    upload_processed_data,
    upload_reference_data,
    validate_required_columns,
)

__all__ = [
    "sanitize_column_names",
    "find_column_by_pattern",
    "validate_required_columns",
    "clean_and_validate",
    "upload_geo_file",
    "upload_processed_data",
    "upload_reference_data",
    "upload_all_data",
]
