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

import pathlib
from typing import Any, Dict

import yaml


class Config:
    """Configuration manager for the election analysis pipeline."""

    # Default values that can be overridden in config
    DEFAULTS = {
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

    def __init__(self, config_file: str = "config.yaml"):
        """
        Initialize configuration from YAML file.

        Args:
            config_file: Path to the configuration file (relative to script directory)
        """
        self.script_dir = pathlib.Path(__file__).parent.resolve()
        self.config_path = self.script_dir / config_file
        self._config = self._load_config()
        self._setup_paths()
        self._extract_base_names()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {self.config_path}\n"
                f"Please create a config.yaml file in the analysis directory."
            )

        try:
            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)
            return config
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing configuration file: {e}")

    def _setup_paths(self):
        """Setup base directory paths."""
        self.analysis_dir = self.script_dir
        self.project_dir = self.analysis_dir.parent

        # Create directory structure
        dirs = self._config.get("directories", {})

        self.data_dir = self.analysis_dir / dirs.get("data", "data")
        self.elections_dir = self.analysis_dir / dirs.get("elections", "data/elections")
        self.geospatial_dir = self.analysis_dir / dirs.get("geospatial", "geospatial")
        self.maps_dir = self.analysis_dir / dirs.get("maps", "maps")
        self.tiles_dir = self.analysis_dir / dirs.get("tiles", "tiles")
        self.census_dir = self.analysis_dir / dirs.get("census", "data/census")

        # Create directories if they don't exist
        for directory in [
            self.data_dir,
            self.elections_dir,
            self.geospatial_dir,
            self.maps_dir,
            self.tiles_dir,
            self.census_dir,
        ]:
            directory.mkdir(parents=True, exist_ok=True)

    def _extract_base_names(self):
        """Extract base names from input filenames automatically."""
        self.base_names = {}

        input_files = self._config.get("input_files", {})

        # Extract base name for election data (from votes_csv)
        if "votes_csv" in input_files:
            votes_file = input_files["votes_csv"]
            self.base_names["election_data"] = pathlib.Path(votes_file).stem

        # Extract base name for boundaries (from boundaries_geojson)
        if "boundaries_geojson" in input_files:
            boundaries_file = input_files["boundaries_geojson"]
            self.base_names["boundaries"] = pathlib.Path(boundaries_file).stem

        # Extract base name for voter registration (from voters_csv)
        if "voters_csv" in input_files:
            voters_file = input_files["voters_csv"]
            self.base_names["voter_registration"] = pathlib.Path(voters_file).stem

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
        value = self._config
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

    def get_input_path(self, filename_key: str) -> pathlib.Path:
        """
        Get full path to an input file.

        Args:
            filename_key: Key for the filename in input_files

        Returns:
            Full path to the input file
        """
        filename = self._config.get("input_files", {}).get(filename_key)
        if not filename:
            raise ValueError(f"Input filename not found in config: {filename_key}")

        # Route files to appropriate directories
        if filename_key in ["votes_csv", "voters_csv", "boundaries_geojson"]:
            base_dir = self.elections_dir
        elif filename_key in ["acs_households_json", "block_groups_shp"]:
            base_dir = self.census_dir
        else:
            base_dir = self.data_dir

        # Support relative paths within the base directory
        file_path = base_dir / filename

        # Create parent directories if they don't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)

        return file_path

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
        return self.elections_dir / filename

    def get_web_geojson_path(self) -> pathlib.Path:
        """Get path to web-ready GeoJSON file."""
        base_name = self.get_base_name("election_data")
        filename = self.generate_derived_filename(base_name, "_results", ".geojson")
        return self.geospatial_dir / filename

    def get_processed_geojson_path(self) -> pathlib.Path:
        """Get path to processed GeoJSON file."""
        base_name = self.get_base_name("election_data")
        filename = self.generate_derived_filename(base_name, "_processed", ".geojson")
        return self.geospatial_dir / filename

    def get_mbtiles_path(self) -> pathlib.Path:
        """Get path to MBTiles file."""
        base_name = self.get_base_name("election_data")
        filename = self.generate_derived_filename(base_name, "_tiles", ".mbtiles")
        return self.tiles_dir / filename

    def get_voters_inside_csv_path(self) -> pathlib.Path:
        """Get path to voters inside district CSV."""
        filename = self.generate_derived_filename("voters", "_inside_pps", ".csv")
        return self.data_dir / filename

    def get_voters_outside_csv_path(self) -> pathlib.Path:
        """Get path to voters outside district CSV."""
        filename = self.generate_derived_filename("voters", "_outside_pps", ".csv")
        return self.data_dir / filename

    def get_voter_heatmap_path(self) -> pathlib.Path:
        """Get path to voter heatmap HTML file."""
        return self.maps_dir / "voter_heatmap.html"

    def get_households_analysis_csv_path(self) -> pathlib.Path:
        """Get path to household analysis CSV."""
        filename = self.generate_derived_filename("hh_no_minors", "_pps_bgs", ".csv")
        return self.data_dir / filename

    def get_households_report_path(self) -> pathlib.Path:
        """Get path to household analysis report."""
        filename = self.generate_derived_filename("hh_no_minors", "_report", ".md")
        return self.data_dir / filename

    def get_households_map_path(self) -> pathlib.Path:
        """Get path to household demographics map HTML file."""
        return self.maps_dir / "household_demographics.html"

    def get_output_dir(self, dir_key: str) -> pathlib.Path:
        """
        Get full path to an output directory.

        Args:
            dir_key: Directory key ('maps', 'tiles', 'geospatial', etc.)

        Returns:
            Full path to the directory
        """
        if dir_key == "maps":
            return self.maps_dir
        elif dir_key == "tiles":
            return self.tiles_dir
        elif dir_key == "geospatial":
            return self.geospatial_dir
        elif dir_key == "data":
            return self.data_dir
        elif dir_key == "elections":
            return self.elections_dir
        elif dir_key == "census":
            return self.census_dir
        else:
            raise ValueError(f"Unknown directory key: {dir_key}")

    def get_data_dir(self) -> pathlib.Path:
        """Get the data directory path."""
        return self.data_dir

    def get_census_dir(self) -> pathlib.Path:
        """Get the census directory path."""
        return self.census_dir

    def get_column_name(self, column_key: str) -> str:
        """Get column name with intelligent defaults."""
        return self.get(f"columns.{column_key}")

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
        return self.get(f"metadata.{key}", "")

    # Legacy compatibility method
    def get_output_path(self, filename_key: str) -> pathlib.Path:
        """Legacy method for backward compatibility."""
        method_map = {
            "enriched_csv": self.get_enriched_csv_path,
            "web_geojson": self.get_web_geojson_path,
            "processed_geojson": self.get_processed_geojson_path,
            "mbtiles": self.get_mbtiles_path,
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
        results = {}
        input_files = self._config.get("input_files", {})

        for filename_key in input_files:
            try:
                file_path = self.get_input_path(filename_key)
                results[filename_key] = file_path.exists()
            except Exception:
                results[filename_key] = False

        return results

    def print_config_summary(self):
        """Print a summary of the current configuration."""
        print("📋 Configuration Summary")
        print("=" * 50)
        print(f"Project: {self.get('project_name', 'Unknown')}")
        print(f"Description: {self.get('description', 'No description')}")
        print(f"Config file: {self.config_path}")
        print(f"Analysis directory: {self.analysis_dir}")

        print("\n📊 Extracted Base Names:")
        for key, name in self.base_names.items():
            print(f"  {key}: {name}")

        print("\n📁 Directories:")
        for key in ["data", "elections", "geospatial", "maps", "tiles", "census"]:
            dir_path = self.get_output_dir(key)
            exists = "✅" if dir_path.exists() else "❌"
            print(f"  {exists} {key}: {dir_path}")

        print("\n📊 Input Files:")
        validation = self.validate_input_files()
        for file_key, exists in validation.items():
            status = "✅" if exists else "❌"
            print(f"  {status} {file_key}")


# Convenience function for easy importing
def load_config(config_file: str = "config.yaml") -> Config:
    """
    Load configuration from file.

    Args:
        config_file: Path to configuration file

    Returns:
        Config instance
    """
    return Config(config_file)
