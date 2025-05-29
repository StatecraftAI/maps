"""
Configuration Loader for StatecraftAI Maps Pipeline

Simplified configuration management - only what we actually use.

Usage:
    from processing.config_loader import Config

    config = Config()
    file_path = config.get_input_path('votes_csv')
    column = config.get_column_name('precinct_csv')
    setting = config.get('project_name')
"""

from pathlib import Path
from typing import Any, Optional

import yaml
from loguru import logger


class Config:
    """Simplified configuration manager - only the essentials."""

    # Simple defaults for column names and common settings
    DEFAULTS = {
        "columns": {
            "precinct_csv": "precinct",
            "precinct_geojson": "Precinct",
            "latitude": "latitude",
            "longitude": "longitude",
        }
    }

    def __init__(self, config_file: Optional[str] = None):
        """Initialize with config file."""

        # Find config file
        if config_file is None:
            if Path("config.yaml").exists():
                config_file = "config.yaml"
                logger.debug("Using config.yaml from current directory")
            elif Path("processing/config.yaml").exists():
                config_file = "processing/config.yaml"
                logger.debug("Using processing/config.yaml from root directory")
            else:
                raise FileNotFoundError("No config.yaml found in current directory or processing/")

        self.config_path = Path(config_file).resolve()
        self.project_root = self._find_project_root()

        logger.debug(f"Loading config from: {self.config_path}")
        logger.debug(f"Project root: {self.project_root}")

        # Load YAML
        with open(self.config_path, "r") as f:
            self.data = yaml.safe_load(f)

    def _find_project_root(self) -> Path:
        """Simple project root detection."""
        # If config is in processing/, project root is parent
        if self.config_path.parent.name == "processing":
            return self.config_path.parent.parent
        # Otherwise use config directory
        return self.config_path.parent

    def get_input_path(self, filename_key: str) -> Path:
        """Get full path to an input file."""
        relative_path = self.data.get("input_files", {}).get(filename_key)
        if not relative_path:
            raise ValueError(f"Input file '{filename_key}' not found in config")

        return self.project_root / relative_path

    def get_column_name(self, column_key: str) -> str:
        """Get column name with defaults."""
        # Try config first, then defaults
        columns = self.data.get("columns", {})
        if column_key in columns:
            return columns[column_key]

        # Fall back to defaults
        if column_key in self.DEFAULTS["columns"]:
            return self.DEFAULTS["columns"][column_key]

        raise ValueError(f"Column '{column_key}' not found in config or defaults")

    def get(self, key: str, default: Any = None) -> Any:
        """Get any setting from config."""
        keys = key.split(".")
        value = self.data

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value
