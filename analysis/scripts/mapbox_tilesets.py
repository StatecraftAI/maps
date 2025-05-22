#!/usr/bin/env python3
"""
Batch upload/update, validate, create, and publish Mapbox tilesets using the
Mapbox Tiling Service (MTS) API.

Features:
- Minimal YAML config.
- Auto-inspects geometry/attributes per GeoJSON file.
- Whitelists attributes by geometry type (configurable).
- Creates or updates sources and tilesets.
- Validates recipes before use.
- Dry-run mode for testing.
- Comprehensive logging and error handling.

Alternative SDK Options:
- Official Mapbox SDK: pip install mapbox
- Tilesets CLI: pip install tilesets  
- GeoJSON validation: pip install geojson geojsonhint

GeoJSON Validation Tools:
- Online: https://geojsonlint.com/
- Python: geojson.is_valid() or geojsonhint
- CLI: tilesets validate-source <file>
"""
import argparse
import json
import os
import pathlib
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import geopandas as gpd
import numpy as np
import requests
import yaml
from loguru import logger
import pandas as pd

# --- Configuration ---
DEFAULT_CONFIG_PATH = "config.yaml"
MAPBOX_API_BASE_URL = "https://api.mapbox.com"

# --- Helper Functions ---
def prettify_name(source_id: str) -> str:
    """Converts a snake_case source_id to a Title Case Name."""
    return " ".join([p.capitalize() for p in source_id.split("_")])

def ensure_file_exists(path: pathlib.Path) -> None:
    """Exits if a file does not exist."""
    if not path.is_file():
        logger.error(f"Missing file: {path}")
        sys.exit(1)

def convert_geojson_to_line_delimited(geojson_path: pathlib.Path) -> pathlib.Path:
    """
    Converts a regular GeoJSON FeatureCollection to line-delimited GeoJSON.
    Also reprojects coordinates to WGS84 if needed.
    Returns the path to the temporary line-delimited file.
    """
    logger.debug(f"Converting {geojson_path} to line-delimited GeoJSON")
    
    # Use geopandas to handle CRS conversion automatically
    try:
        gdf = gpd.read_file(geojson_path)
        logger.debug(f"Original CRS: {gdf.crs}")
        
        # Reproject to WGS84 (EPSG:4326) if not already
        if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
            logger.info(f"Reprojecting from {gdf.crs} to WGS84 (EPSG:4326)")
            gdf = gdf.to_crs('EPSG:4326')
        elif gdf.crs is None:
            logger.warning("No CRS specified in GeoJSON. Assuming WGS84.")
            gdf = gdf.set_crs('EPSG:4326')
        else:
            logger.debug("Already in WGS84")
        
        # Create temporary file
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.geojson.ld', delete=False)
        temp_path = pathlib.Path(temp_file.name)
        
        valid_features = 0
        logger.debug(f"Converting {len(gdf)} features to line-delimited format")
        
        for idx, row in gdf.iterrows():
            # Convert each row to a GeoJSON feature
            # Filter properties to exclude null values and ensure JSON serializable types
            properties = {}
            for k, v in row.items():
                if k == 'geometry':
                    continue
                # Skip null/None values
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    continue
                # Convert to JSON-safe types
                if isinstance(v, (str, int, bool)):
                    properties[k] = v
                elif isinstance(v, float):
                    # Handle NaN and infinite values
                    if pd.isna(v) or not np.isfinite(v):
                        continue
                    properties[k] = v
                else:
                    # Convert other types to string
                    properties[k] = str(v)
            
            feature = {
                "type": "Feature",
                "geometry": row.geometry.__geo_interface__,
                "properties": properties
            }
            
            # Validate feature structure
            if not feature.get('geometry'):
                logger.warning(f"Skipping feature {idx} without geometry")
                continue
                
            # Write each feature as a separate line
            json.dump(feature, temp_file, separators=(',', ':'))
            temp_file.write('\n')
            valid_features += 1
        
        logger.debug(f"Wrote {valid_features} valid features to line-delimited format")
        
        temp_file.close()
        logger.debug(f"Created line-delimited GeoJSON: {temp_path}")
        
        # Debug: Check the first few lines of the converted file
        with open(temp_path, 'r') as debug_file:
            first_lines = [debug_file.readline().strip() for _ in range(min(3, valid_features))]
            logger.debug(f"First few lines of line-delimited file:")
            for i, line in enumerate(first_lines):
                logger.debug(f"  Line {i+1}: {line[:200]}{'...' if len(line) > 200 else ''}")
                
                # Check if coordinates look like WGS84 (longitude should be between -180 and 180)
                try:
                    feature_check = json.loads(line)
                    coords = feature_check['geometry']['coordinates']
                    if coords and len(coords) > 0:
                        # Get first coordinate pair - handle nested coordinate structures
                        first_coord = coords[0]
                        while isinstance(first_coord, list) and len(first_coord) > 0 and isinstance(first_coord[0], list):
                            first_coord = first_coord[0]
                        
                        if isinstance(first_coord, list) and len(first_coord) >= 2:
                            lon, lat = first_coord[0], first_coord[1]
                            if isinstance(lon, (int, float)) and isinstance(lat, (int, float)):
                                if -180 <= lon <= 180 and -90 <= lat <= 90:
                                    logger.debug(f"  Coordinates look correct: lon={lon:.6f}, lat={lat:.6f}")
                                else:
                                    logger.warning(f"  Coordinates may be wrong: lon={lon}, lat={lat}")
                except Exception as e:
                    logger.debug(f"  Could not check coordinates: {e}")
        
        return temp_path
        
    except Exception as e:
        logger.error(f"Failed to convert and reproject GeoJSON: {e}")
        # Fall back to original method as backup
        return convert_geojson_to_line_delimited_fallback(geojson_path)

def convert_geojson_to_line_delimited_fallback(geojson_path: pathlib.Path) -> pathlib.Path:
    """
    Fallback method: converts GeoJSON to line-delimited without CRS conversion.
    """
    logger.warning("Using fallback conversion method without CRS reprojection")
    
    with open(geojson_path, 'r') as f:
        geojson_data = json.load(f)
    
    if geojson_data.get('type') != 'FeatureCollection':
        raise ValueError(f"Expected FeatureCollection, got {geojson_data.get('type')}")
    
    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.geojson.ld', delete=False)
    temp_path = pathlib.Path(temp_file.name)
    
    try:
        features = geojson_data.get('features', [])
        valid_features = 0
        
        for feature in features:
            if not isinstance(feature, dict) or feature.get('type') != 'Feature':
                continue
            if 'geometry' not in feature or 'properties' not in feature:
                continue
                
            # Write each feature as a separate line
            json.dump(feature, temp_file, separators=(',', ':'))
            temp_file.write('\n')
            valid_features += 1
        
        temp_file.close()
        logger.debug(f"Fallback conversion: wrote {valid_features} features")
        return temp_path
        
    except Exception as e:
        temp_file.close()
        if temp_path.exists():
            temp_path.unlink()
        raise e

def detect_geometry_and_attributes(geojson_path: pathlib.Path) -> Tuple[str, List[str]]:
    """
    Detects the dominant geometry type and extracts attribute names from a GeoJSON file.
    """
    try:
        gdf = gpd.read_file(geojson_path)
        if gdf.empty:
            logger.warning(f"GeoJSON file {geojson_path} is empty.")
            # Return a default or handle as an error depending on requirements
            return "Unknown", []
        
        # Ensure 'geometry' column exists
        if 'geometry' not in gdf.columns:
            logger.error(f"GeoJSON file {geojson_path} is missing a 'geometry' column.")
            sys.exit(1)

        # Filter out rows with None or empty geometries before mode calculation
        valid_geometries = gdf.geometry.dropna()
        if valid_geometries.empty:
            logger.warning(f"No valid geometries found in {geojson_path}.")
            return "Unknown", []

        geometry_type = valid_geometries.geom_type.mode()
        if not geometry_type.empty:
            dominant_geometry_type = geometry_type[0]
        else:
            logger.warning(f"Could not determine dominant geometry type for {geojson_path}.")
            dominant_geometry_type = "Unknown"
            
        attributes = [col for col in gdf.columns if col != 'geometry']
        return dominant_geometry_type, attributes
    except Exception as e:
        logger.error(f"Could not read or process GeoJSON file {geojson_path}: {e}")
        sys.exit(1)

def choose_zooms(geometry_type: str, config: Dict[str, Any]) -> Tuple[int, int]:
    """Determines min/max zoom levels based on geometry type and config."""
    if geometry_type == 'Point':
        minzoom = config.get('minzoom_point', config.get('minzoom', 10))
        maxzoom = config.get('maxzoom_point', config.get('maxzoom', 15))
    else:  # Polygon, LineString, etc.
        minzoom = config.get('minzoom_default', config.get('minzoom', 8))
        maxzoom = config.get('maxzoom_default', config.get('maxzoom', 14))
    return int(minzoom), int(maxzoom)

# --- Mapbox API Client ---
class MapboxClient:
    """A client for interacting with the Mapbox Tiling Service API."""

    def __init__(self, access_token: str, username: str, dry_run: bool = False):
        self.access_token = access_token
        self.username = username
        self.dry_run = dry_run
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.access_token}"})
        self.base_params = {"access_token": self.access_token}
        self.poll_interval = 60
        self.timeout = 600

    def _request(self, method: str, endpoint: str, send_token_as_query_param: bool = True, **kwargs: Any) -> requests.Response:
        """Makes an API request and handles common errors."""
        url = f"{MAPBOX_API_BASE_URL}{endpoint}"
        
        current_params = self.base_params if send_token_as_query_param else None
        
        try:
            response = self.session.request(method, url, params=current_params, **kwargs)
            response.raise_for_status()  # Raises HTTPError for bad responses (4XX or 5XX)
            return response
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error during {method} {url}: {e.response.status_code} - {e.response.text}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception during {method} {url}: {e}")
            raise

    def validate_recipe(self, recipe: Dict[str, Any]) -> bool:
        """Validates a tileset recipe."""
        endpoint = "/tilesets/v1/validateRecipe"

        if self.dry_run:
            logger.info(f"[DRY-RUN] Would validate recipe: {json.dumps(recipe, indent=2)}")
            return True
        try:
            logger.debug(f"Validating recipe:\n{json.dumps(recipe, indent=2)}")
            response = self._request("PUT", endpoint, json=recipe)
            result = response.json()
            is_valid = result.get('valid', False)
            if not is_valid:
                logger.error(f"Invalid recipe: {result.get('errors') or result}")
            return is_valid
        except Exception as e: # Catch broader exceptions after _request
            logger.error(f"Failed to validate recipe: {e}")
            return False

    def source_exists(self, source_id: str) -> bool:
        """Checks if a tileset source exists."""
        endpoint = f"/tilesets/v1/sources/{self.username}/{source_id}"
        try:
            self._request("GET", endpoint)
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return False
            raise # Re-raise other HTTP errors

    def create_or_update_source(self, source_id: str, geojson_path: pathlib.Path) -> bool:
        """Creates or updates a tileset source from a GeoJSON file."""
        endpoint = f"/tilesets/v1/sources/{self.username}/{source_id}"
        exists = self.source_exists(source_id)
        method = 'PUT' if exists else 'POST'

        if self.dry_run:
            action = "update" if exists else "create"
            logger.info(f"[DRY-RUN] Would {action} source {self.username}/{source_id} from {geojson_path}")
            return True

        # Convert GeoJSON to line-delimited format
        temp_ld_path = None
        try:
            temp_ld_path = convert_geojson_to_line_delimited(geojson_path)
            
            with open(temp_ld_path, 'rb') as f:
                files = {'file': (temp_ld_path.name, f, 'application/geo+json')}
                self._request(method, endpoint, files=files)
            logger.info(f"Source {self.username}/{source_id} {'updated' if exists else 'created'} successfully from {geojson_path}.")
            return True
        except Exception as e:
            logger.error(f"Failed to {method.lower()} source {source_id}: {e}")
            return False
        finally:
            # Clean up temporary file
            if temp_ld_path and temp_ld_path.exists():
                temp_ld_path.unlink()
                logger.debug(f"Cleaned up temporary file: {temp_ld_path}")

    def tileset_exists(self, tileset_id: str) -> bool:
        """Checks if a tileset exists."""
        endpoint = f"/tilesets/v1/{tileset_id}"
        try:
            self._request("GET", endpoint)
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return False
            # Log other HTTP errors but don't raise them
            logger.warning(f"Unexpected error checking tileset existence: {e.response.status_code}")
            raise

    def create_or_update_tileset(self, tileset_id: str, recipe: Dict[str, Any], display_name: str) -> bool:
        """Creates a new tileset or updates an existing one's recipe/metadata."""
        exists = self.tileset_exists(tileset_id)
        
        body = {
            "recipe": recipe,
            "name": display_name,
            "description": f"Tileset for {display_name}, batch processed.",
            "type": "vector", # MTS generally implies vector
            # "privacy": "private" # or "public"
        }

        if self.dry_run:
            action = "update" if exists else "create"
            logger.info(f"[DRY-RUN] Would {action} tileset {tileset_id} (Name: {display_name})")
            logger.debug(f"[DRY-RUN] Tileset body: {json.dumps(body, indent=2)}")
            return True

        try:
            if exists:
                # For existing tilesets, update the recipe using the dedicated recipe endpoint
                logger.info(f"Updating recipe for existing tileset {tileset_id}")
                recipe_endpoint = f"/tilesets/v1/{tileset_id}/recipe"
                self._request("PATCH", recipe_endpoint, json=recipe)
                logger.info(f"Recipe updated successfully for tileset {tileset_id}")
                
                # Optionally update metadata (name, description) if supported
                try:
                    metadata = {
                        "name": display_name,
                        "description": f"Tileset for {display_name}, batch processed.",
                    }
                    metadata_endpoint = f"/tilesets/v1/{tileset_id}"
                    self._request("PATCH", metadata_endpoint, json=metadata)
                    logger.info(f"Metadata updated for tileset {tileset_id}")
                except Exception as e:
                    logger.warning(f"Could not update metadata for {tileset_id}: {e}")
            else:
                # Create new tileset
                endpoint = f"/tilesets/v1/{tileset_id}" # tileset_id is {username}.{actual_id}
                self._request("POST", endpoint, json=body)
                logger.info(f"Tileset {tileset_id} (Name: {display_name}) created successfully.")
            return True
        except Exception as e:
            logger.error(f"Failed to {'update' if exists else 'create'} tileset {tileset_id}: {e}")
            return False

    def publish_tileset(self, tileset_id: str) -> bool:
        """Publishes a tileset."""
        endpoint = f"/tilesets/v1/{tileset_id}/publish"
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would publish tileset {tileset_id}")
            return True
        try:
            self._request("POST", endpoint)
            logger.info(f"Publish job started for tileset {tileset_id}.")
            return True
        except Exception as e:
            logger.error(f"Failed to publish tileset {tileset_id}: {e}")
            return False

    def poll_status(self, tileset_id: str) -> bool:
        """Polls the status of a tileset until it's processed or times out."""
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would poll status for {tileset_id}")
            return True

        endpoint = f"/tilesets/v1/{tileset_id}"
        start_time = time.time()
        logger.info(f"Polling status for tileset {tileset_id}...")
        while time.time() - start_time < self.poll_interval:
            try:
                response = self._request("GET", endpoint)
                data = response.json()
                status = data.get('status')
                latest_job = data.get('latest_job_info', {}) # MTS provides job info
                job_status = latest_job.get('status') if latest_job else status # Fallback

                logger.info(f"Tileset {tileset_id} status: {status}, Latest Job Status: {job_status}")

                if job_status in ['success', 'published'] or status == 'available':
                    logger.info(f"Tileset {tileset_id} processing complete (status: {status}, job_status: {job_status}).")
                    
                    # Check if tileset has data
                    filesize = data.get('filesize', 0)
                    if filesize == 0:
                        logger.warning(f"Tileset {tileset_id} is available but appears to be empty (filesize: 0). This may indicate no features were processed from the source data.")
                    
                    return True
                if job_status in ['failed', 'error']:
                    logger.error(f"Tileset {tileset_id} processing failed (status: {job_status}). Errors: {latest_job.get('errors')}")
                    return False
                if status in ['queued', 'processing'] or job_status in ['processing', 'queued']:
                     # Continue polling
                    pass
                else: # Unknown or unexpected status
                    logger.warning(f"Tileset {tileset_id} has an unexpected status: {status}, job_status: {job_status}. Data: {data}")
                    # For robustness, return True for unknown but stable statuses after some iterations
                    # This prevents infinite loops while still logging unexpected behavior

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    # 404 is expected immediately after creation, only log if it persists
                    elapsed = time.time() - start_time
                    if elapsed > self.poll_interval:
                        logger.warning(f"Tileset {tileset_id} still not found after {elapsed:.0f}s")
                    else:
                        logger.debug(f"Tileset {tileset_id} not yet available (404) - waiting...")
                else:
                    logger.error(f"HTTP error polling status for {tileset_id}: {e.response.status_code} - {e.response.text}. Retrying in {self.poll_interval}s.")
            except Exception as e:
                logger.error(f"Error polling status for {tileset_id}: {e}. Retrying in {self.poll_interval}s.")
            
            time.sleep(self.poll_interval)

        logger.error(f"Timeout polling status for tileset {tileset_id} after {self.timeout} seconds.")
        return False

    @staticmethod
    def build_vector_recipe(
        username: str,
        source_id: str,
        layer_name: str,
        minzoom: int,
        maxzoom: int,
        attributes_to_include: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Builds a Mapbox vector tileset recipe."""
        recipe: Dict[str, Any] = {
            "version": 1,
            "layers": {
                layer_name: {
                    "source": f"mapbox://tileset-source/{username}/{source_id}",
                    "minzoom": minzoom,
                    "maxzoom": maxzoom,
                }
            }
        }
        if attributes_to_include:
            # Attributes must be wrapped in a "set" object according to Mapbox docs
            attributes_dict = {attr: ["get", attr] for attr in attributes_to_include}
            recipe["layers"][layer_name]["features"] = {
                "attributes": {
                    "set": attributes_dict
                }
            }
        logger.info(f"Built recipe for layer '{layer_name}' (Source: {source_id})")
        return recipe

# --- Main Processing Logic ---
def process_source_file(
    client: MapboxClient,
    config: Dict[str, Any],
    geojson_file_path_str: str,
) -> None:
    """Processes a single GeoJSON source file."""
    geojson_path = pathlib.Path(geojson_file_path_str).resolve()
    logger.info(f"Processing source file: {geojson_path}")

    if not client.dry_run: # Only check file existence if not dry run
        ensure_file_exists(geojson_path)

    # Derive IDs and names
    base_filename = geojson_path.stem # Filename without extension
    source_id = base_filename # Use filename as source_id for simplicity
    tileset_name_part = base_filename # Part of the tileset ID after username.
    tileset_id = f"{client.username}.{tileset_name_part}"
    display_name = prettify_name(base_filename) # For tileset name property
    layer_name = base_filename # Layer name within the tileset

    logger.info(f"Source ID: {source_id}, Tileset ID: {tileset_id}, Display Name: {display_name}, Layer Name: {layer_name}")

    # Detect geometry and attributes
    if client.dry_run:
        logger.info(f"[DRY-RUN] Would detect geometry and attributes for {geojson_path}")
        # Provide mock data for dry-run to proceed
        geometry_type, all_attributes = "Polygon", ["mock_attr1", "mock_attr2"]
    else:
        geometry_type, all_attributes = detect_geometry_and_attributes(geojson_path)
    
    logger.info(f"Detected geometry: {geometry_type}, Attributes: {all_attributes}")

    # Filter attributes based on whitelist
    attribute_whitelist_config = config.get('attribute_whitelist', {})
    geometry_specific_whitelist = attribute_whitelist_config.get(geometry_type)
    
    attributes_to_include = all_attributes
    if geometry_specific_whitelist is not None: # If None, means no whitelist for this geom type, include all
        attributes_to_include = [attr for attr in all_attributes if attr in geometry_specific_whitelist]
        logger.info(f"Whitelisted attributes for {geometry_type}: {attributes_to_include}")
    else:
        logger.info(f"No specific whitelist for {geometry_type}, including all attributes: {attributes_to_include}")


    minzoom, maxzoom = choose_zooms(geometry_type, config)
    logger.info(f"Using minzoom: {minzoom}, maxzoom: {maxzoom}")

    # Build recipe
    recipe = client.build_vector_recipe(
        client.username, source_id, layer_name, minzoom, maxzoom, attributes_to_include
    )

    # Create/Update source BEFORE validating recipe (recipe validation needs the source to exist)
    if not client.create_or_update_source(source_id, geojson_path):
        logger.error(f"Source creation/update failed for {geojson_path}. Skipping.")
        return

    # Validate recipe
    if not client.validate_recipe(recipe):
        logger.error(f"Recipe validation failed for {geojson_path}. Skipping.")
        return

    # Create/Update tileset
    if not client.create_or_update_tileset(tileset_id, recipe, display_name):
        logger.error(f"Tileset creation/update failed for {tileset_id}. Skipping.")
        return

    # Publish tileset
    if not client.publish_tileset(tileset_id):
        logger.error(f"Publishing failed for {tileset_id}. Skipping.")
        return

    # Brief delay to allow tileset to become available for polling
    if not client.dry_run:
        logger.debug("Waiting 10 seconds for tileset to become available...")
        time.sleep(10)

    # Poll status
    if client.poll_status(tileset_id):
        logger.success(f"Successfully processed and published tileset: {tileset_id} from {geojson_path}")
    else:
        logger.error(f"Processing/Publishing for tileset {tileset_id} did not complete successfully or timed out.")


def main() -> None:
    """Main script execution."""
    parser = argparse.ArgumentParser(description="Batch process GeoJSON files to Mapbox tilesets.")
    parser.add_argument(
        '-c', '--config',
        default=DEFAULT_CONFIG_PATH,
        type=pathlib.Path,
        help=f"Path to the YAML configuration file (default: {DEFAULT_CONFIG_PATH})"
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Show actions that would be taken, but do not execute them."
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set the logging level (default: INFO)."
    )
    args = parser.parse_args()

    # Configure logger
    logger.remove() # Remove default handler
    logger.add(sys.stderr, level=args.log_level)

    logger.info(f"Starting Mapbox batch tileset processing. Dry run: {args.dry_run}")

    # Load configuration
    if not args.config.is_file():
        logger.error(f"Configuration file not found: {args.config}")
        sys.exit(1)
    with open(args.config, 'r') as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration file {args.config}: {e}")
            sys.exit(1)

    access_token = config.get('access_token') or os.getenv('MAPBOX_ACCESS_TOKEN') # Allow env var override
    username = config.get('username')

    if not access_token:
        logger.error("Mapbox access token not found in config or MAPBOX_ACCESS_TOKEN env var.")
        sys.exit(1)
    if not username:
        logger.error("Mapbox username not found in config.")
        sys.exit(1)

    client = MapboxClient(access_token, username, args.dry_run)

    sources_to_process = config.get('sources')
    if not sources_to_process or not isinstance(sources_to_process, list):
        logger.error("No 'sources' list found in configuration or it's empty/invalid.")
        sys.exit(1)

    logger.info(f"Found {len(sources_to_process)} sources to process.")
    for source_file_path_str in sources_to_process:
        try:
            process_source_file(client, config, source_file_path_str)
            logger.info("-" * 30) # Separator for visual clarity
        except Exception as e:
            logger.error(f"Unhandled error processing source {source_file_path_str}: {e}", exc_info=True)
            logger.info(f"Continuing with next source if any.")
    
    logger.info("Batch processing finished.")

if __name__ == '__main__':
    main()
