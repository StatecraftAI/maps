"""Repository classes for maps data access."""

from .spatial import SpatialQueryManager

# Backward compatibility alias (deprecated - use SpatialQueryManager)
SpatialRepository = SpatialQueryManager

__all__ = ["SpatialQueryManager", "SpatialRepository"]
