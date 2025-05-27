"""Spatial data repository following platform patterns."""

from typing import Any, Dict, List, Optional

import geopandas as gpd
from loguru import logger

from ..supabase_integration import SupabaseDatabase


class SpatialRepository:
    """Repository for spatial data operations."""

    def __init__(self, db: SupabaseDatabase):
        """Initialize the spatial repository.

        Args:
            db: SupabaseDatabase instance
        """
        self.db = db

    def get_features_by_bounds(
        self,
        table: str,
        bounds: List[float],
        columns: Optional[List[str]] = None,
        additional_filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Get features within bounding box.

        Args:
            table: Table name
            bounds: [min_x, min_y, max_x, max_y] in EPSG:4326
            columns: Columns to select
            additional_filters: Additional filter conditions

        Returns:
            List of features
        """
        try:
            # Note: This would require PostGIS functions in a real implementation
            # For now, we'll use the standard select method
            filters = additional_filters or {}
            
            # In a real implementation, you'd add spatial filtering here
            # filters["geometry"] = {"intersects": bounds}
            
            return self.db.select(
                table=table,
                columns=columns,
                filters=filters,
            )
        except Exception as e:
            logger.error(f"Error getting features by bounds from {table}: {str(e)}")
            raise

    def get_features_by_state(
        self,
        table: str,
        state: str,
        columns: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Get features by state.

        Args:
            table: Table name
            state: State abbreviation (e.g., 'CA', 'TX')
            columns: Columns to select

        Returns:
            List of features
        """
        try:
            return self.db.select(
                table=table,
                columns=columns,
                filters={"state": state},
            )
        except Exception as e:
            logger.error(f"Error getting features by state from {table}: {str(e)}")
            raise

    def get_voter_density_hexagons(
        self,
        state: Optional[str] = None,
        min_density: Optional[float] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Get voter density hexagons with optional filtering.

        Args:
            state: Optional state filter
            min_density: Minimum voter density threshold
            limit: Maximum number of records

        Returns:
            List of hexagon features
        """
        try:
            filters = {}
            if state:
                filters["state"] = state
            if min_density is not None:
                # Note: This would need proper numeric filtering in a real implementation
                filters["voter_density"] = {"gte": min_density}

            return self.db.select(
                table="voter_hexagons",
                filters=filters,
                limit=limit,
                order_by="voter_density desc",
            )
        except Exception as e:
            logger.error(f"Error getting voter density hexagons: {str(e)}")
            raise

    def create_spatial_feature(
        self,
        table: str,
        feature_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Create a new spatial feature.

        Args:
            table: Table name
            feature_data: Feature data including geometry

        Returns:
            Created feature
        """
        try:
            result = self.db.insert(
                table=table,
                data=feature_data,
                returning="representation",
            )
            return result[0] if result else {}
        except Exception as e:
            logger.error(f"Error creating spatial feature in {table}: {str(e)}")
            raise

    def update_spatial_feature(
        self,
        table: str,
        feature_id: str,
        update_data: Dict[str, Any],
        id_column: str = "id",
    ) -> Dict[str, Any]:
        """Update a spatial feature.

        Args:
            table: Table name
            feature_id: Feature ID
            update_data: Data to update
            id_column: Name of the ID column

        Returns:
            Updated feature
        """
        try:
            result = self.db.update(
                table=table,
                data=update_data,
                filters={id_column: feature_id},
            )
            return result[0] if result else {}
        except Exception as e:
            logger.error(f"Error updating spatial feature in {table}: {str(e)}")
            raise

    def delete_spatial_feature(
        self,
        table: str,
        feature_id: str,
        id_column: str = "id",
    ) -> bool:
        """Delete a spatial feature.

        Args:
            table: Table name
            feature_id: Feature ID
            id_column: Name of the ID column

        Returns:
            True if deleted successfully
        """
        try:
            result = self.db.delete(
                table=table,
                filters={id_column: feature_id},
            )
            return len(result) > 0
        except Exception as e:
            logger.error(f"Error deleting spatial feature from {table}: {str(e)}")
            raise 