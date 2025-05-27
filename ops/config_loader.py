"""
Configuration Loader for Election Analysis Pipeline

This module provides a centralized way to load and access configuration
settings from the config.yaml file.

Usage:
    from config_loader import Config

    config = Config()
    votes_csv = config.get_input_path('votes_csv')
    output_dir = config.get_output_dir('maps')
"""

import os
import pathlib
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml  # type: ignore[import-untyped]
from loguru import logger


class Config:
    """Configuration manager for the election analysis pipeline."""

    # Default values that can be overridden in config
    DEFAULTS: Dict[str, Any] = {
        "columns": {
            "precinct_csv": "precinct",
            "precinct_geojson": "Precinct",
            "total_votes": "total_votes",
            "total_voters": "TOTAL",
            "latitude": "latitude",
            "longitude": "longitude",
            "dem_registration": "DEM",
            "rep_registration": "REP",
            "nav_registration": "NAV",
        },
        "analysis": {"registration_diversity_weight": 0.5, "turnout_weight": 0.5},
        "visualization": {
            "map_dpi": 300,
            "figure_max_width": 14,
            "colormap_default": "OrRd",
            "colormap_diverging": "RdBu_r",
            "min_zoom": 9,
            "max_zoom": 13,
            "base_zoom": 10,
            "buffer_size": 64,
            "simplification": 10,
        },
        "system": {
            "input_crs": "EPSG:2913",
            "output_crs": "EPSG:4326",
            "precision_decimals": 6,
            "property_precision": 3,
        },
    }

    def __init__(
        self,
        config_file: Optional[str] = None,
        project_root_override: Optional[Union[str, Path]] = None,
    ):
        """
        Initialize configuration manager.

        Args:
            config_file: Path to config file. If None, looks for:
                        1. Environment variable PIPELINE_CONFIG_PATH
                        2. config.yaml in current directory
                        3. ../ops/config.yaml (if running from analysis/)
            project_root_override: Override project root detection (useful for temp configs)
        """
        if config_file is None:
            # Check environment variable first (for CLI overrides)
            env_config = os.environ.get("PIPELINE_CONFIG_PATH")
            if env_config and Path(env_config).exists():
                config_file = env_config
                logger.debug(f"Using config from environment: {config_file}")
            elif Path("config.yaml").exists():
                config_file = "config.yaml"
            elif Path("../ops/config.yaml").exists():
                config_file = "../ops/config.yaml"
                logger.debug("Using ops/config.yaml from analysis directory")
            else:
                raise FileNotFoundError(
                    "No config.yaml found. Check current directory or set PIPELINE_CONFIG_PATH"
                )

        self.config_path = Path(config_file).resolve()
        self.config_dir = self.config_path.parent

        # Use project root override if provided (for temp configs)
        if project_root_override:
            self.project_root = Path(project_root_override).resolve()
            logger.debug(f"Using project root override: {self.project_root}")
        elif os.environ.get("PROJECT_ROOT_OVERRIDE"):
            # Check environment variable for project root override (for CLI subprocesses)
            self.project_root = Path(os.environ["PROJECT_ROOT_OVERRIDE"]).resolve()
            logger.debug(f"Using project root from environment: {self.project_root}")
        else:
            self.project_root = self._find_project_root()

        logger.debug(f"Loading config from: {self.config_path}")
        logger.debug(f"Project root: {self.project_root}")

        # Load configuration
        with open(self.config_path, "r") as f:
            self.data = yaml.safe_load(f)

        self._setup_paths()
        self._extract_base_names()

    def _setup_paths(self) -> None:
        """Setup base directory paths relative to project root."""
        self.analysis_dir = self.project_root / "analysis"

        # Create directory structure using project_root as base
        dirs = self.data.get("directories", {})

        self.data_dir = self.project_root / dirs.get("data", "data")
        self.elections_dir = self.project_root / dirs.get("elections", "data/elections")
        self.geospatial_dir = self.project_root / dirs.get("geospatial", "data/geospatial")
        self.census_dir = self.project_root / dirs.get("census", "data/census")
        self.html_dir = self.project_root / dirs.get("html", "html")

        # Create directories if they don't exist
        for directory in [
            self.data_dir,
            self.elections_dir,
            self.geospatial_dir,
            self.census_dir,
            self.html_dir,
        ]:
            directory.mkdir(parents=True, exist_ok=True)

    def _extract_base_names(self) -> None:
        """Extract base names from input filenames automatically."""
        self.base_names: Dict[str, str] = {}
        input_files_config = self.data.get("input_files", {})

        if "votes_csv" in input_files_config:
            # Get the full relative path from config, then get the stem
            votes_file_relative_path = input_files_config["votes_csv"]
            self.base_names["election_data"] = pathlib.Path(votes_file_relative_path).stem

        if "precincts_geojson" in input_files_config:
            boundaries_file_relative_path = input_files_config["precincts_geojson"]
            self.base_names["boundaries"] = pathlib.Path(boundaries_file_relative_path).stem

        if "precincts_voter_summary_csv" in input_files_config:
            voters_file_relative_path = input_files_config["precincts_voter_summary_csv"]
            self.base_names["voter_registration"] = pathlib.Path(voters_file_relative_path).stem

    def get_input_path(self, filename_key: str) -> pathlib.Path:
        """
        Get full path to an input file. The path is taken directly from config.yaml
        and joined with the project root.

        Args:
            filename_key: Key for the filename in input_files

        Returns:
            Full absolute path to the input file
        """
        # Get the relative path string directly from the config
        relative_path_str = self.data.get("input_files", {}).get(filename_key)
        if not relative_path_str:
            raise ValueError(
                f"Input filename key '{filename_key}' not found in config: input_files"
            )

        # Join with project_dir to get the absolute path
        absolute_path = self.project_root / relative_path_str

        # Ensure parent directory exists (important for cases where scripts might create them)
        absolute_path.parent.mkdir(parents=True, exist_ok=True)

        return absolute_path

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation with intelligent defaults.

        Args:
            key_path: Dot-separated path to the configuration value
            default: Default value if key not found

        Returns:
            Configuration value
        """
        keys = key_path.split(".")

        # Try to get from config first
        value: Any = self.data
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                value = None
                break

        # If not found in config, try defaults
        if value is None:
            value = self.DEFAULTS
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return default

        return value

    def get_base_name(self, base_key: str) -> str:
        """Get automatically extracted base name."""
        if base_key not in self.base_names:
            raise ValueError(f"Base name not found: {base_key}")
        return self.base_names[base_key]

    def generate_derived_filename(self, base_name: str, suffix: str, extension: str) -> str:
        """Generate a derived filename."""
        return f"{base_name}{suffix}{extension}"

    # Convenience methods for common derived files
    def get_enriched_csv_path(self) -> pathlib.Path:
        """Get path to enriched CSV file."""
        base_name = self.get_base_name("election_data")
        filename = self.generate_derived_filename(base_name, "_enriched", ".csv")
        return pathlib.Path(self.elections_dir) / filename

    def get_web_geojson_path(self) -> pathlib.Path:
        """Get path to web-ready GeoJSON file."""
        base_name = self.get_base_name("election_data")
        filename = self.generate_derived_filename(base_name, "_results", ".geojson")
        return pathlib.Path(self.geospatial_dir) / filename

    def get_processed_geojson_path(self) -> pathlib.Path:
        """Get path to processed GeoJSON file."""
        base_name = self.get_base_name("election_data")
        filename = self.generate_derived_filename(base_name, "_processed", ".geojson")
        return pathlib.Path(self.geospatial_dir) / filename

    def get_voters_inside_csv_path(self) -> pathlib.Path:
        """Get path to voters inside district CSV."""
        filename = self.generate_derived_filename("voters", "_inside_pps", ".csv")
        return pathlib.Path(self.data_dir) / filename

    def get_voters_outside_csv_path(self) -> pathlib.Path:
        """Get path to voters outside district CSV."""
        filename = self.generate_derived_filename("voters", "_outside_pps", ".csv")
        return pathlib.Path(self.data_dir) / filename

    def get_voter_heatmap_path(self) -> pathlib.Path:
        """Get path to voter heatmap HTML file."""
        return pathlib.Path(self.html_dir) / "voter_heatmap.html"

    def get_households_analysis_csv_path(self) -> pathlib.Path:
        """Get path to household analysis CSV."""
        filename = self.generate_derived_filename("hh_no_minors", "_pps_bgs", ".csv")
        return pathlib.Path(self.census_dir) / filename

    def get_households_report_path(self) -> pathlib.Path:
        """Get path to household analysis report."""
        filename = self.generate_derived_filename("hh_no_minors", "_report", ".md")
        return pathlib.Path(self.census_dir) / filename

    def get_households_map_path(self) -> pathlib.Path:
        """Get path to household demographics map HTML file."""
        return pathlib.Path(self.html_dir) / "household_demographics.html"

    def get_output_dir(self, dir_key: str) -> pathlib.Path:
        """
        Get full path to an output directory.

        Args:
            dir_key: Directory key ('maps', 'geospatial', etc.)

        Returns:
            Full path to the directory
        """
        if dir_key == "geospatial":
            return pathlib.Path(self.geospatial_dir)
        elif dir_key == "data":
            return pathlib.Path(self.data_dir)
        elif dir_key == "elections":
            return pathlib.Path(self.elections_dir)
        elif dir_key == "census":
            return pathlib.Path(self.census_dir)
        elif dir_key == "html":
            return pathlib.Path(self.html_dir)
        else:
            raise ValueError(f"Unknown directory key: {dir_key}")

    def get_data_dir(self) -> pathlib.Path:
        """Get the data directory path."""
        return pathlib.Path(self.data_dir)

    def get_census_dir(self) -> pathlib.Path:
        """Get the census directory path."""
        return pathlib.Path(self.census_dir)

    def get_html_dir(self) -> pathlib.Path:
        """Get the html directory path."""
        return pathlib.Path(self.html_dir)

    def get_column_name(self, column_key: str) -> str:
        """Get column name with intelligent defaults."""
        result = self.get(f"columns.{column_key}")
        if isinstance(result, str):
            return result
        raise ValueError(f"Column name not found or not a string: {column_key}")

    def get_analysis_setting(self, setting_key: str) -> Any:
        """Get analysis setting with intelligent defaults."""
        return self.get(f"analysis.{setting_key}")

    def get_visualization_setting(self, setting_key: str) -> Any:
        """Get visualization setting with intelligent defaults."""
        return self.get(f"visualization.{setting_key}")

    def get_system_setting(self, setting_key: str) -> Any:
        """Get system setting with intelligent defaults."""
        return self.get(f"system.{setting_key}")

    def get_metadata(self, key: str) -> str:
        """Get metadata value."""
        result = self.get(f"metadata.{key}", "")
        if isinstance(result, str):
            return result
        return str(result)

    # Legacy compatibility method
    def get_output_path(self, filename_key: str) -> pathlib.Path:
        """Legacy method for backward compatibility."""
        method_map = {
            "enriched_csv": self.get_enriched_csv_path,
            "web_geojson": self.get_web_geojson_path,
            "processed_geojson": self.get_processed_geojson_path,
            "voters_inside_csv": self.get_voters_inside_csv_path,
            "voters_outside_csv": self.get_voters_outside_csv_path,
            "voter_heatmap_html": self.get_voter_heatmap_path,
            "households_analysis_csv": self.get_households_analysis_csv_path,
            "households_report_md": self.get_households_report_path,
            "households_map_html": self.get_households_map_path,
        }

        if filename_key in method_map:
            return method_map[filename_key]()
        else:
            raise ValueError(f"Unknown output file key: {filename_key}")

    def validate_input_files(self) -> Dict[str, bool]:
        """Validate that input files exist."""
        results: Dict[str, bool] = {}
        input_files = self.data.get("input_files", {})

        for filename_key in input_files:
            try:
                file_path = self.get_input_path(filename_key)
                file_exists = file_path.exists()
                results[filename_key] = file_exists
            except Exception:
                results[filename_key] = False

        return results

    def print_config_summary(self) -> None:
        """Print a summary of the current configuration."""
        logger.debug("📋 Configuration Summary")
        logger.debug("=" * 50)
        logger.debug(f"Project: {self.get('project_name', 'Unknown')}")
        logger.debug(f"Description: {self.get('description', 'No description')}")
        logger.debug(f"Config file: {self.config_path}")
        logger.debug(f"Analysis directory: {self.analysis_dir}")

        logger.debug("📊 Extracted Base Names:")
        for key, name in self.base_names.items():
            logger.debug(f"  {key}: {name}")

        logger.debug("📁 Directories:")
        for key in ["data", "elections", "geospatial", "maps", "census"]:
            dir_path = self.get_output_dir(key)
            exists = "✅" if dir_path.exists() else "❌"
            logger.debug(f"  {exists} {key}: {dir_path}")

        logger.debug("📊 Input Files:")
        validation = self.validate_input_files()
        for file_key, exists in validation.items():
            status = "✅" if exists else "❌"
            logger.debug(f"  {status} {file_key}")

    def _find_project_root(self) -> Path:
        """Find the project root directory by looking for characteristic files/directories."""
        # Start from the config file directory and work up
        current = self.config_path.parent

        # Look for characteristic project files/directories
        project_markers = ["analysis", "data", "ops", "requirements.txt", "pyproject.toml", ".git"]

        # Walk up the directory tree
        for _ in range(5):  # Limit to 5 levels up
            # Check if this directory contains project markers
            markers_found = sum(1 for marker in project_markers if (current / marker).exists())

            # If we find multiple markers, this is likely the project root
            if markers_found >= 2:
                return current

            # If we're at filesystem root, stop
            parent = current.parent
            if parent == current:
                break
            current = parent

        # Fallback: if config is in ops/, project root is parent
        if self.config_path.parent.name == "ops":
            return self.config_path.parent.parent

        # Final fallback: use config directory
        logger.warning(
            f"Could not reliably detect project root, using config directory: {self.config_path.parent}"
        )
        return self.config_path.parent


# Convenience function for easy importing
def load_config(config_file: Optional[str] = None) -> Config:
    """
    Load configuration from file.

    Args:
        config_file: Path to configuration file

    Returns:
        Config instance
    """
    return Config(config_file)
