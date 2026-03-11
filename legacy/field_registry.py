#!/usr/bin/env python3
"""
Field Registry for Election Analysis Pipeline

This module provides field registration and explanation capabilities for data layers.
It automatically detects and registers common field patterns to provide explanations
of what's in a data layer.
"""

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Set

import geopandas as gpd
from loguru import logger


@dataclass
class FieldDefinition:
    """Definition of a calculated field with its explanation and formula."""

    name: str
    description: str
    formula: str
    field_type: str  # 'percentage', 'count', 'ratio', 'categorical', 'boolean'
    category: str  # 'analytical', 'electoral', 'demographic', 'administrative', 'informational', 'geographic'
    units: Optional[str] = None
    calculation_func: Optional[Callable[..., Any]] = None


class FieldRegistry:
    """
    Adaptive registry for tracking all calculated fields and their explanations.
    Handles schema drift by auto-detecting and registering common field patterns.
    """

    def __init__(self, strict_mode: bool = False):
        self._fields: Dict[str, FieldDefinition] = {}
        self.strict_mode = (
            strict_mode  # If True, fail on missing fields; if False, warn and continue
        )
        self._register_base_fields()

    def register(self, field_def: FieldDefinition) -> None:
        """Register a field definition."""
        self._fields[field_def.name] = field_def
        logger.debug(f"Registered field: {field_def.name}")

    def auto_register_field_patterns(self, gdf_fields: Set[str]) -> None:
        """
        Automatically register fields based on common patterns.
        This handles schema drift by detecting and registering new field types.
        """
        logger.debug("  🔄 Auto-registering field patterns...")

        auto_registered = 0

        for field_name in gdf_fields:
            if field_name in self._fields or field_name == "geometry":
                continue

            # Pattern 1: Candidate vote counts (votes_*)
            if field_name.startswith("votes_") and field_name != "votes_total":
                candidate_name = field_name.replace("votes_", "")
                display_name = candidate_name.replace("_", " ").title()
                self.register(
                    FieldDefinition(
                        name=field_name,
                        description=f"Vote count for candidate {display_name}",
                        formula=f"COUNT(votes_for_{candidate_name})",
                        field_type="count",
                        category="electoral",
                        units="votes",
                    )
                )
                auto_registered += 1

            # Pattern 2: Candidate percentages (vote_pct_*)
            elif field_name.startswith("vote_pct_") and not field_name.startswith(
                "vote_pct_contribution_"
            ):
                candidate_name = field_name.replace("vote_pct_", "")
                display_name = candidate_name.replace("_", " ").title()
                self.register(
                    FieldDefinition(
                        name=field_name,
                        description=f"Vote percentage for candidate {display_name}",
                        formula=f"(votes_{candidate_name} / votes_total) * 100",
                        field_type="percentage",
                        category="electoral",
                        units="percent",
                    )
                )
                auto_registered += 1

            # Pattern 3: Vote contributions (vote_pct_contribution_*)
            elif (
                field_name.startswith("vote_pct_contribution_")
                and field_name != "vote_pct_contribution_total_votes"
            ):
                candidate_name = field_name.replace("vote_pct_contribution_", "")
                display_name = candidate_name.replace("_", " ").title()
                self.register(
                    FieldDefinition(
                        name=field_name,
                        description=f"Percentage of {display_name}'s total votes from this precinct",
                        formula=f"(votes_{candidate_name} / SUM(all_precincts.votes_{candidate_name})) * 100",
                        field_type="percentage",
                        category="analytical",
                        units="percent",
                    )
                )
                auto_registered += 1

            # Pattern 4: Registration percentages (reg_pct_*)
            elif field_name.startswith("reg_pct_"):
                party_code = field_name.replace("reg_pct_", "").upper()
                reg_field = field_name.replace("_pct", "")
                self.register(
                    FieldDefinition(
                        name=field_name,
                        description=f"Percentage of voters registered as {party_code}",
                        formula=f"({reg_field} / total_voters) * 100",
                        field_type="percentage",
                        category="demographic",
                        units="percent",
                    )
                )
                auto_registered += 1

            # Pattern 5: Candidate fields (candidate_*)
            elif field_name.startswith("candidate_"):
                candidate_name = field_name.replace("candidate_", "")
                display_name = candidate_name.replace("_", " ").title()
                self.register(
                    FieldDefinition(
                        name=field_name,
                        description=f"Data field related to candidate {display_name}",
                        formula="DERIVED_FROM_UPSTREAM",
                        field_type="categorical",
                        category="electoral",
                    )
                )
                auto_registered += 1

            # Pattern 6: Geographic/Administrative fields
            elif field_name in [
                "TOTAL",
                "DEM",
                "REP",
                "NAV",
                "OTH",
                "IND",
                "CON",
                "LBT",
                "NLB",
                "PGP",
                "PRO",
                "WFP",
                "WTP",
            ]:
                if field_name == "TOTAL":
                    description = "Total registered voters in precinct"
                    field_type = "count"
                    units = "voters"
                    category = "demographic"
                else:
                    description = f"Number of voters registered as {field_name}"
                    field_type = "count"
                    units = "voters"
                    category = "demographic"

                self.register(
                    FieldDefinition(
                        name=field_name,
                        description=description,
                        formula="FROM_UPSTREAM_VOTER_REGISTRATION",
                        field_type=field_type,
                        category=category,
                        units=units,
                    )
                )
                auto_registered += 1

            # Pattern 7: Geographic districts and boundaries
            elif field_name in [
                "OR_House",
                "OR_Senate",
                "USCongress",
                "CITY",
                "SchoolDist",
                "FIRE_DIST",
                "TRAN_DIST",
                "WaterDist",
                "SewerDist",
                "PUD",
                "ESD",
                "METRO",
                "Mult_Comm",
                "CommColleg",
                "CoP_Dist",
                "Soil_Water",
                "UFSWQD",
                "Unincorp",
            ]:
                self.register(
                    FieldDefinition(
                        name=field_name,
                        description=f"Geographic district assignment: {field_name}",
                        formula="FROM_UPSTREAM_GEOGRAPHIC_DATA",
                        field_type="categorical",
                        category="administrative",
                    )
                )
                auto_registered += 1

            # Pattern 8: Shape/Geometry metadata
            elif field_name in ["Shape_Area", "Shape_Leng"]:
                units = "square meters" if "Area" in field_name else "meters"
                self.register(
                    FieldDefinition(
                        name=field_name,
                        description=f"Geographic shape {field_name.split('_')[1].lower()}",
                        formula="CALCULATED_FROM_GEOMETRY",
                        field_type="ratio",
                        category="geographic",
                        units=units,
                    )
                )
                auto_registered += 1

        if auto_registered > 0:
            logger.debug(f"  ✅ Auto-registered {auto_registered} field patterns")

    def get_explanation(self, field_name: str) -> str:
        """Get explanation for a specific field."""
        if field_name in self._fields:
            field_def = self._fields[field_name]
            explanation = f"**{field_def.description}**"
            if field_def.units:
                explanation += f"\n\n**Units:** {field_def.units}"
            if field_def.formula and field_def.formula != "DERIVED_FROM_UPSTREAM":
                explanation += f"\n\n**Formula:** `{field_def.formula}`"
            return explanation
        else:
            if not self.strict_mode:
                logger.warning(f"No explanation registered for field: {field_name}")
                return f"Field: {field_name} (no explanation available)"
            else:
                raise ValueError(f"Field {field_name} not found in registry")

    def get_all_explanations(self) -> Dict[str, str]:
        """Get all field explanations as a dictionary."""
        return {name: self.get_explanation(name) for name in self._fields.keys()}

    def validate_gdf_completeness(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """
        Validate that all fields in the GeoDataFrame are registered.
        Returns a validation report.
        """
        gdf_fields = set(gdf.columns) - {"geometry"}
        registered_fields = set(self._fields.keys())
        manually_registered = self._get_manually_registered_fields()

        missing_fields = gdf_fields - registered_fields
        extra_registered = registered_fields - gdf_fields

        # Auto-register missing fields if possible
        if missing_fields:
            logger.debug(
                f"Found {len(missing_fields)} unregistered fields, attempting auto-registration..."
            )
            self.auto_register_field_patterns(gdf_fields)

            # Recalculate after auto-registration
            registered_fields = set(self._fields.keys())
            missing_fields = gdf_fields - registered_fields

        validation_report = {
            "total_fields": len(gdf_fields),
            "registered_fields": len(registered_fields & gdf_fields),
            "missing_fields": list(missing_fields),
            "extra_registered": list(extra_registered),
            "manually_registered": list(manually_registered & gdf_fields),
            "auto_registered": list((registered_fields & gdf_fields) - manually_registered),
            "coverage_percentage": (len(registered_fields & gdf_fields) / len(gdf_fields)) * 100
            if gdf_fields
            else 100,
        }

        if missing_fields and self.strict_mode:
            raise ValueError(f"Missing field registrations: {missing_fields}")
        elif missing_fields:
            logger.warning(f"Missing field registrations: {missing_fields}")

        return validation_report

    def _get_manually_registered_fields(self) -> Set[str]:
        """Get fields that were manually registered (not auto-detected)."""
        # This is a simplified version - in practice you might track this differently
        base_fields = {
            "precinct",
            "Precinct",
            "base_precinct",
            "record_type",
            "votes_total",
            "margin_pct",
            "total_votes",
            "pps_vote_share",
            "precinct_size",
            "competitiveness_score",
            "engagement_rate",
            "swing_potential",
            "is_pps_precinct",
            "has_split_precincts",
        }
        return base_fields & set(self._fields.keys())

    def _register_base_fields(self) -> None:
        """Register commonly used base fields."""
        base_fields = [
            FieldDefinition(
                name="precinct",
                description="Precinct identifier or name",
                formula="FROM_UPSTREAM_DATA",
                field_type="categorical",
                category="administrative",
            ),
            FieldDefinition(
                name="Precinct",
                description="Precinct identifier or name (capitalized)",
                formula="FROM_UPSTREAM_DATA",
                field_type="categorical",
                category="administrative",
            ),
            FieldDefinition(
                name="base_precinct",
                description="Base precinct identifier (for split precincts)",
                formula="DERIVED_FROM_PRECINCT_CONSOLIDATION",
                field_type="categorical",
                category="administrative",
            ),
            FieldDefinition(
                name="record_type",
                description="Type of record (e.g., 'precinct', 'split')",
                formula="DERIVED_FROM_DATA_PROCESSING",
                field_type="categorical",
                category="informational",
            ),
            FieldDefinition(
                name="votes_total",
                description="Total votes cast in precinct",
                formula="SUM(all_candidate_votes)",
                field_type="count",
                category="electoral",
                units="votes",
            ),
            FieldDefinition(
                name="margin_pct",
                description="Victory margin as percentage of total votes",
                formula="(winner_votes - runner_up_votes) / votes_total * 100",
                field_type="percentage",
                category="analytical",
                units="percent",
            ),
            FieldDefinition(
                name="total_votes",
                description="Total votes cast (alternative naming)",
                formula="SUM(all_candidate_votes)",
                field_type="count",
                category="electoral",
                units="votes",
            ),
            FieldDefinition(
                name="pps_vote_share",
                description="Percentage of votes for PPS-supporting candidates",
                formula="(pps_supporting_votes / votes_total) * 100",
                field_type="percentage",
                category="analytical",
                units="percent",
            ),
            FieldDefinition(
                name="precinct_size",
                description="Size category of precinct based on voter turnout",
                formula="CATEGORIZE_BY_VOTE_COUNT",
                field_type="categorical",
                category="analytical",
            ),
            FieldDefinition(
                name="competitiveness_score",
                description="Measure of how competitive the race was in this precinct",
                formula="1 - (margin_pct / 100)",
                field_type="ratio",
                category="analytical",
                units="score (0-1)",
            ),
            FieldDefinition(
                name="engagement_rate",
                description="Voter engagement rate in precinct",
                formula="(votes_total / registered_voters) * 100",
                field_type="percentage",
                category="analytical",
                units="percent",
            ),
            FieldDefinition(
                name="swing_potential",
                description="Potential for precinct to swing in future elections",
                formula="CALCULATED_FROM_HISTORICAL_VARIANCE",
                field_type="ratio",
                category="analytical",
                units="score",
            ),
            FieldDefinition(
                name="is_pps_precinct",
                description="Whether precinct is within PPS district boundaries",
                formula="SPATIAL_JOIN_WITH_PPS_BOUNDARIES",
                field_type="boolean",
                category="administrative",
            ),
            FieldDefinition(
                name="has_split_precincts",
                description="Whether this precinct has been split across districts",
                formula="COUNT(precinct_parts) > 1",
                field_type="boolean",
                category="informational",
            ),
        ]

        for field_def in base_fields:
            self.register(field_def)


def generate_layer_explanations(gdf: gpd.GeoDataFrame) -> Dict[str, str]:
    """
    Generate explanations for all fields in a GeoDataFrame using the field registry.

    Args:
        gdf: GeoDataFrame to generate explanations for

    Returns:
        Dictionary mapping field names to their explanations
    """
    registry = FieldRegistry()

    # Auto-register any unregistered fields
    gdf_fields = set(gdf.columns) - {"geometry"}
    registry.auto_register_field_patterns(gdf_fields)

    # Get explanations for all fields
    explanations = registry.get_all_explanations()

    # Validate completeness
    validation = registry.validate_gdf_completeness(gdf)

    logger.debug(f"Generated explanations for {len(explanations)} fields")
    logger.debug(f"Field registry coverage: {validation['coverage_percentage']:.1f}%")

    return explanations


def export_complete_field_registry(gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
    """
    Export complete field registry information for a GeoDataFrame.

    Args:
        gdf: GeoDataFrame to export registry for

    Returns:
        Dictionary containing field definitions and metadata
    """
    registry = FieldRegistry()

    # Auto-register fields
    gdf_fields = set(gdf.columns) - {"geometry"}
    registry.auto_register_field_patterns(gdf_fields)

    # Build complete registry export
    field_definitions = {}
    for column in gdf.columns:
        if column == "geometry":
            continue

        field_def = registry._fields.get(column)
        if field_def:
            field_definitions[column] = {
                "name": field_def.name,
                "description": field_def.description,
                "formula": field_def.formula,
                "field_type": field_def.field_type,
                "category": field_def.category,
                "units": field_def.units,
            }
        else:
            # Fallback for unregistered fields
            field_definitions[column] = {
                "name": column,
                "description": f"Field: {column}",
                "formula": "UNKNOWN",
                "field_type": "unknown",
                "category": "other",
                "units": None,
            }

    # Get validation report
    validation = registry.validate_gdf_completeness(gdf)

    return {
        "field_definitions": field_definitions,
        "registry_metadata": {
            "total_fields": validation["total_fields"],
            "registered_fields": validation["registered_fields"],
            "coverage_percentage": validation["coverage_percentage"],
            "auto_registered_count": len(validation["auto_registered"]),
            "manually_registered_count": len(validation["manually_registered"]),
        },
    }
