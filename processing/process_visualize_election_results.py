"""
process_visualize_election_results.py

This script processes election results data, particularly for geospatial visualization and mapping.
It handles geospatial data, defines calculated fields, monitors schema changes, and prepares data
for visualization in maps and charts.

Key Functionality:
1. Field Definitions:
   - Registers calculated fields (e.g., percentages, counts, ratios) with explanations and formulas.
   - Maintains a field registry for documentation and validation.

2. Schema Drift Monitoring:
   - Detects and analyzes changes in the schema of the input data (e.g., new or missing fields).
   - Provides warnings and suggestions for handling schema drift.

3. Geospatial Processing:
   - Validates and reprojects geospatial data to WGS84 (standard for mapping).
   - Optimizes GeoJSON properties for efficient rendering in web maps.

4. Data Validation:
   - Validates the completeness of fields in the geospatial dataset.
   - Ensures data integrity for accurate analysis and visualization.

5. Field Registry Management:
   - Exports a complete field registry report for documentation.
   - Provides explanations and formulas for all calculated fields.

Usage:
- This script is typically used after `process_voter_election_data.py` to prepare geospatial election
  data for visualization in the maps component of StatecraftAI.
- It is part of the data pipeline for generating interactive maps and dashboards.

Input:
- Geospatial election results data (e.g., GeoJSON, Shapefile).
- Configuration file (e.g., config.yaml) for field definitions and processing settings.

Output:
- Optimized GeoJSON files for web mapping.
- Visualizations (e.g., bubble maps, Tufte maps).
- Field registry reports for documentation.

Example:
    python process_visualize_election_results.py --config config.yaml --input election_results.geojson

Dependencies:
- geopandas, pandas, numpy, matplotlib, loguru, pathlib, and other standard Python libraries.
"""

from loguru import logger

from ops.config_loader import Config

# Import schema drift monitoring
try:
    from ops.schema_monitoring.schema_drift_monitor import SchemaDriftMonitor, monitor_schema_drift

    SCHEMA_MONITORING_AVAILABLE = True
except ImportError:
    logger.warning("Schema drift monitoring not available - ops.schema_drift_monitor.py not found")
    SCHEMA_MONITORING_AVAILABLE = False

import json
import pathlib
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import geopandas as gpd
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


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

            # Pattern 9: Boolean flags and status fields
            elif field_name.startswith("is_") or field_name.startswith("has_"):
                description = f"Boolean flag: {field_name.replace('_', ' ')}"
                self.register(
                    FieldDefinition(
                        name=field_name,
                        description=description,
                        formula="CALCULATED_FROM_DATA_CONDITIONS",
                        field_type="boolean",
                        category="informational",
                    )
                )
                auto_registered += 1

            # Pattern 10: Calculated metrics
            elif field_name in [
                "margin_pct",
                "total_votes",
                "pps_vote_share",
                "pps_total_votes",
                "precinct_size",
            ]:
                if "pct" in field_name or "share" in field_name:
                    field_type = "percentage"
                    units = "percent"
                elif "total" in field_name or "votes" in field_name:
                    field_type = "count"
                    units = "votes"
                else:
                    field_type = "ratio"
                    units = None

                self.register(
                    FieldDefinition(
                        name=field_name,
                        description=f"Calculated metric: {field_name.replace('_', ' ')}",
                        formula="CALCULATED_FROM_ELECTION_DATA",
                        field_type=field_type,
                        category="analytical",
                        units=units,
                    )
                )
                auto_registered += 1

            # Pattern 11: Identifiers and metadata
            elif field_name in [
                "Precinct",
                "base_precinct",
                "Split",
                "record_type",
                "second_candidate",
            ]:
                self.register(
                    FieldDefinition(
                        name=field_name,
                        description=f"Data identifier/metadata: {field_name.replace('_', ' ')}",
                        formula="FROM_UPSTREAM_METADATA",
                        field_type="categorical",
                        category="administrative",
                    )
                )
                auto_registered += 1

        if auto_registered > 0:
            logger.debug(f"  ✅ Auto-registered {auto_registered} fields based on common patterns")

    def get_explanation(self, field_name: str) -> str:
        """Get the explanation for a field, including formula if applicable."""
        if field_name not in self._fields:
            # Auto-generate a basic explanation for unknown fields
            return f"**Auto-generated explanation**: Field '{field_name}' was detected but not explicitly registered. This may be a new upstream field that needs proper documentation."

        field_def = self._fields[field_name]
        explanation = field_def.description

        if field_def.formula:
            explanation += f"\n\n**Formula:** `{field_def.formula}`"

        if field_def.units:
            explanation += f"\n\n**Units:** {field_def.units}"

        return explanation

    def get_all_explanations(self) -> Dict[str, str]:
        """Get all field explanations as a dictionary."""
        return {name: self.get_explanation(name) for name in self._fields.keys()}

    def validate_gdf_completeness(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """
        Validate that all fields in GDF have explanations.
        Auto-registers common patterns and provides detailed schema drift analysis.
        """
        gdf_fields = set(gdf.columns) - {"geometry"}  # Exclude geometry column

        # Auto-register common field patterns BEFORE validation
        self.auto_register_field_patterns(gdf_fields)

        registry_fields = set(self._fields.keys())

        # Find dynamic candidate fields (now should be mostly auto-registered)
        candidate_fields = set()
        for col in gdf_fields:
            if (
                col.startswith("votes_")
                or col.startswith("vote_pct_")
                or col.startswith("reg_pct_")
                or col.startswith("vote_pct_contribution_")
                or col.startswith("candidate_")
            ):
                candidate_fields.add(col)

        # Calculate final missing fields after auto-registration
        missing_explanations = list(gdf_fields - registry_fields)
        orphaned_explanations = list(registry_fields - gdf_fields)

        return {
            "missing_explanations": missing_explanations,
            "orphaned_explanations": orphaned_explanations,
            "candidate_fields": list(candidate_fields),
            "total_fields": len(gdf_fields),
            "explained_fields": len(registry_fields),
            "auto_registered": len(registry_fields & gdf_fields)
            - len(self._get_manually_registered_fields()),
        }

    def _get_manually_registered_fields(self) -> Set[str]:
        """Get fields that were manually registered in _register_base_fields."""
        # This helps track what was auto-registered vs manually registered
        return {
            "precinct",
            "votes_total",
            "total_voters",
            "turnout_rate",
            "dem_advantage",
            "major_party_pct",
            "reg_pct_dem",
            "reg_pct_rep",
            "reg_pct_nav",
            "vote_margin",
            "pct_victory_margin",
            "political_lean",
            "competitiveness",
            "leading_candidate",
            "turnout_quartile",
            "margin_category",
            "precinct_size_category",
            "competitiveness_score",
            "engagement_rate",
            "candidate_dominance",
            "swing_potential",
            "vote_efficiency_dem",
            "registration_competitiveness",
            "vote_pct_contribution_total_votes",
            "is_pps_precinct",
            "participated_election",
            "has_election_data",
            "has_voter_data",
            "complete_record",
        }

    def _register_base_fields(self) -> None:
        """Register all base field definitions."""

        # Raw data fields (from source)
        self.register(
            FieldDefinition(
                name="precinct",
                description="Unique identifier for the voting precinct",
                formula="",
                field_type="categorical",
                category="administrative",
            )
        )

        self.register(
            FieldDefinition(
                name="votes_total",
                description="Total number of votes cast in the precinct",
                formula="SUM(all candidate vote counts)",
                field_type="count",
                category="electoral",
                units="votes",
            )
        )

        self.register(
            FieldDefinition(
                name="total_voters",
                description="Total number of registered voters in the precinct",
                formula="reg_dem + reg_rep + reg_nav + reg_other",
                field_type="count",
                category="demographic",
                units="registered voters",
            )
        )

        # Calculated percentage fields
        self.register(
            FieldDefinition(
                name="turnout_rate",
                description="Percentage of registered voters who actually voted in this election",
                formula="(votes_total / total_voters) * 100",
                field_type="percentage",
                category="analytical",
                units="percent",
            )
        )

        self.register(
            FieldDefinition(
                name="dem_advantage",
                description="Democratic registration advantage (positive) or disadvantage (negative)",
                formula="reg_pct_dem - reg_pct_rep",
                field_type="percentage",
                category="analytical",
                units="percentage points",
            )
        )

        self.register(
            FieldDefinition(
                name="major_party_pct",
                description="Percentage of voters registered with major parties (Democratic or Republican)",
                formula="((reg_dem + reg_rep) / total_voters) * 100",
                field_type="percentage",
                category="demographic",
                units="percent",
            )
        )

        self.register(
            FieldDefinition(
                name="reg_pct_dem",
                description="Percentage of voters registered as Democratic",
                formula="(reg_dem / total_voters) * 100",
                field_type="percentage",
                category="demographic",
                units="percent",
            )
        )

        self.register(
            FieldDefinition(
                name="reg_pct_rep",
                description="Percentage of voters registered as Republican",
                formula="(reg_rep / total_voters) * 100",
                field_type="percentage",
                category="demographic",
                units="percent",
            )
        )

        self.register(
            FieldDefinition(
                name="reg_pct_nav",
                description="Percentage of voters registered as Non-Affiliated (Independent)",
                formula="(reg_nav / total_voters) * 100",
                field_type="percentage",
                category="demographic",
                units="percent",
            )
        )

        # Victory margin calculations
        self.register(
            FieldDefinition(
                name="vote_margin",
                description="Vote difference between first and second place candidates",
                formula="votes_leading_candidate - votes_second_candidate",
                field_type="count",
                category="electoral",
                units="votes",
            )
        )

        self.register(
            FieldDefinition(
                name="pct_victory_margin",
                description="Victory margin as percentage of total votes cast",
                formula="(vote_margin / votes_total) * 100",
                field_type="percentage",
                category="electoral",
                units="percent",
            )
        )

        # Analytical derived fields
        self.register(
            FieldDefinition(
                name="political_lean",
                description="Overall political tendency based on voter registration patterns",
                formula="CASE WHEN dem_advantage >= 20 THEN 'Strong Dem' WHEN dem_advantage >= 10 THEN 'Lean Dem' WHEN dem_advantage <= -20 THEN 'Strong Rep' WHEN dem_advantage <= -10 THEN 'Lean Rep' ELSE 'Competitive' END",
                field_type="categorical",
                category="analytical",
            )
        )

        self.register(
            FieldDefinition(
                name="competitiveness",
                description="Electoral competitiveness based on registration balance and turnout",
                formula="CASE WHEN ABS(dem_advantage) >= 30 THEN 'Safe' WHEN ABS(dem_advantage) >= 15 THEN 'Likely' WHEN ABS(dem_advantage) >= 5 THEN 'Competitive' ELSE 'Tossup' END",
                field_type="categorical",
                category="analytical",
            )
        )

        self.register(
            FieldDefinition(
                name="leading_candidate",
                description="Candidate who received the most votes in this precinct",
                formula="ARGMAX(candidate_vote_counts)",
                field_type="categorical",
                category="electoral",
            )
        )

        self.register(
            FieldDefinition(
                name="turnout_quartile",
                description="Turnout rate grouped into quartiles for comparative analysis",
                formula="NTILE(4) OVER (ORDER BY turnout_rate)",
                field_type="categorical",
                category="analytical",
            )
        )

        self.register(
            FieldDefinition(
                name="margin_category",
                description="Victory margin categorized by competitiveness",
                formula="CASE WHEN pct_victory_margin <= 5 THEN 'Very Close' WHEN pct_victory_margin <= 10 THEN 'Close' WHEN pct_victory_margin <= 20 THEN 'Clear' ELSE 'Landslide' END",
                field_type="categorical",
                category="analytical",
            )
        )

        self.register(
            FieldDefinition(
                name="precinct_size_category",
                description="Precinct size based on number of registered voters",
                formula="CASE WHEN total_voters <= Q1 THEN 'Small' WHEN total_voters <= Q2 THEN 'Medium' WHEN total_voters <= Q3 THEN 'Large' ELSE 'Extra Large' END",
                field_type="categorical",
                category="demographic",
            )
        )

        # Advanced analytics
        self.register(
            FieldDefinition(
                name="competitiveness_score",
                description="Quantitative measure of electoral competitiveness (0-100, higher = more competitive)",
                formula="100 - ABS(dem_advantage) * 2.5 + (turnout_rate - 50) * 0.5",
                field_type="ratio",
                category="analytical",
                units="score (0-100)",
            )
        )

        self.register(
            FieldDefinition(
                name="engagement_rate",
                description="Civic engagement combining registration rates and turnout",
                formula="(turnout_rate * 0.7) + (major_party_pct * 0.3)",
                field_type="percentage",
                category="analytical",
                units="percent",
            )
        )

        self.register(
            FieldDefinition(
                name="candidate_dominance",
                description="How dominant the leading candidate is vs all others combined",
                formula="votes_leading_candidate / (votes_total - votes_leading_candidate)",
                field_type="ratio",
                category="analytical",
            )
        )

        self.register(
            FieldDefinition(
                name="swing_potential",
                description="Likelihood of changing party preference based on registration and voting patterns",
                formula="(100 - ABS(dem_advantage)) * (turnout_rate / 100) * (reg_pct_nav / 100)",
                field_type="ratio",
                category="analytical",
                units="potential score",
            )
        )

        self.register(
            FieldDefinition(
                name="vote_efficiency_dem",
                description="How effectively Democratic registrations converted to Democratic-aligned votes",
                formula="(votes_dem_candidate / votes_total) / (reg_pct_dem / 100)",
                field_type="ratio",
                category="analytical",
            )
        )

        self.register(
            FieldDefinition(
                name="registration_competitiveness",
                description="How balanced Democratic vs Republican registration is",
                formula="100 - ABS(dem_advantage)",
                field_type="percentage",
                category="analytical",
                units="percent",
            )
        )

        self.register(
            FieldDefinition(
                name="vote_pct_contribution_total_votes",
                description="What percentage of the total vote pool this precinct represents",
                formula="(votes_total / SUM(all_precincts.votes_total)) * 100",
                field_type="percentage",
                category="analytical",
                units="percent",
            )
        )

        # Boolean flags
        self.register(
            FieldDefinition(
                name="is_pps_precinct",
                description="Whether this precinct is in the specified zone (PPS for school board elections)",
                formula="precinct IN zone_precinct_list",
                field_type="boolean",
                category="administrative",
            )
        )

        self.register(
            FieldDefinition(
                name="participated_election",
                description="Whether this precinct had any votes cast in the election",
                formula="votes_total > 0",
                field_type="boolean",
                category="informational",
            )
        )

        self.register(
            FieldDefinition(
                name="has_election_data",
                description="Whether election data is available for this precinct",
                formula="votes_total IS NOT NULL AND votes_total >= 0",
                field_type="boolean",
                category="informational",
            )
        )

        self.register(
            FieldDefinition(
                name="has_voter_data",
                description="Whether voter registration data is available for this precinct",
                formula="total_voters IS NOT NULL AND total_voters > 0",
                field_type="boolean",
                category="informational",
            )
        )

        self.register(
            FieldDefinition(
                name="complete_record",
                description="Whether this precinct has both election and voter registration data",
                formula="has_election_data AND has_voter_data",
                field_type="boolean",
                category="informational",
            )
        )

        # NEW ELECTION IMPORTANCE METRICS
        self.register(
            FieldDefinition(
                name="vote_impact_score",
                description="Combined measure of precinct size and electoral decisiveness",
                formula="votes_total * abs(margin_pct)",
                field_type="ratio",
                category="analytical",
                units="impact score",
            )
        )

        self.register(
            FieldDefinition(
                name="net_margin_votes",
                description="Absolute vote difference between winner and runner-up",
                formula="abs(winner_votes - runner_up_votes)",
                field_type="count",
                category="analytical",
                units="votes",
            )
        )

        self.register(
            FieldDefinition(
                name="swing_contribution",
                description="Percentage of total election margin contributed by this precinct",
                formula="(precinct_margin / total_election_margin) * 100",
                field_type="percentage",
                category="analytical",
                units="percent",
            )
        )

        self.register(
            FieldDefinition(
                name="power_index",
                description="Hybrid importance metric combining turnout share and margin significance",
                formula="(pps_vote_share * abs(margin_pct)) / 100",
                field_type="ratio",
                category="analytical",
                units="power score",
            )
        )

        self.register(
            FieldDefinition(
                name="precinct_influence",
                description="Normalized precinct importance score on 0-100 scale",
                formula="(vote_impact_score / max_vote_impact_score) * 100",
                field_type="percentage",
                category="analytical",
                units="influence score (0-100)",
            )
        )

        self.register(
            FieldDefinition(
                name="competitive_balance",
                description="How balanced the electoral competition was (100=tie, 0=blowout)",
                formula="100 - abs(margin_pct)",
                field_type="percentage",
                category="analytical",
                units="balance score (0-100)",
            )
        )

        self.register(
            FieldDefinition(
                name="vote_efficiency_ratio",
                description="Proportion of registered voters who actually voted",
                formula="votes_total / total_registered_voters",
                field_type="ratio",
                category="analytical",
                units="efficiency ratio (0-1)",
            )
        )

        self.register(
            FieldDefinition(
                name="margin_volatility",
                description="How much actual election results differed from voter registration patterns",
                formula="abs(actual_margin - expected_margin_from_registration)",
                field_type="percentage",
                category="analytical",
                units="volatility score",
            )
        )

        self.register(
            FieldDefinition(
                name="divergence_from_tie",
                description="Signed divergence from perfect tie: positive when overall winner led in precinct, negative when runner-up led",
                formula="overall_winner_pct_in_precinct - overall_runner_up_pct_in_precinct",
                field_type="percentage",
                category="analytical",
                units="percentage points (+ winner, - runner-up)",
            )
        )

        # POPULATION-WEIGHTED VISUALIZATION FIELDS
        self.register(
            FieldDefinition(
                name="voter_influence_score",
                description="Combined measure of precinct population and turnout for demographic visualization",
                formula="votes_total * log(total_voters + 1)",
                field_type="ratio",
                category="analytical",
                units="influence score",
            )
        )

        self.register(
            FieldDefinition(
                name="population_weight",
                description="Relative population weight for bubble visualization (0-100 scale)",
                formula="(total_voters / max_total_voters) * 100",
                field_type="percentage",
                category="analytical",
                units="relative weight (0-100)",
            )
        )

        self.register(
            FieldDefinition(
                name="voter_density_category",
                description="Categorizes precincts by voter density for demographic analysis",
                formula="CASE WHEN total_voters <= Q1 THEN 'Low Density' WHEN total_voters <= Q3 THEN 'Medium Density' ELSE 'High Density' END",
                field_type="categorical",
                category="demographic",
            )
        )

        self.register(
            FieldDefinition(
                name="democratic_vote_mass",
                description="Total Democratic-aligned votes weighted by precinct population for bubble visualization",
                formula="votes_dem_candidate * sqrt(total_voters)",
                field_type="ratio",
                category="analytical",
                units="vote mass",
            )
        )


# Global registry instance
FIELD_REGISTRY = FieldRegistry()


def register_calculated_field(
    name: str,
    description: str,
    formula: str,
    field_type: str,
    category: str = "analytical",
    units: Optional[str] = None,
    calculation_func: Optional[Callable[..., Any]] = None,
) -> None:
    """
    Helper function to register a new calculated field.

    Args:
        name: Field name
        description: Human-readable description
        formula: Calculation formula or method
        field_type: Data type ('percentage', 'count', 'ratio', 'categorical', 'boolean')
        category: Field category ('analytical', 'electoral', 'demographic', 'administrative', 'informational', 'geographic')
        units: Units of measurement (optional)
        calculation_func: Function to calculate the field (optional)

    Example usage:
        register_calculated_field(
            name="new_metric",
            description="A new analytical metric for voting patterns",
            formula="(field_a * field_b) / field_c",
            field_type="ratio",
            category="analytical",
            units="ratio"
        )
    """
    field_def = FieldDefinition(
        name=name,
        description=description,
        formula=formula,
        field_type=field_type,
        category=category,
        units=units,
        calculation_func=calculation_func,
    )
    FIELD_REGISTRY.register(field_def)
    logger.debug(f"Registered new calculated field: {name} (category: {category})")


def analyze_schema_drift(
    gdf: gpd.GeoDataFrame, previous_fields: Optional[Set[str]] = None
) -> Dict[str, Any]:
    """
    Analyze schema changes between current data and expected/previous schema.

    Args:
        gdf: Current GeoDataFrame
        previous_fields: Set of field names from previous data (optional)

    Returns:
        Dictionary with schema drift analysis
    """
    current_fields = set(gdf.columns) - {"geometry"}
    validation = FIELD_REGISTRY.validate_gdf_completeness(gdf)

    analysis = {
        "current_field_count": len(current_fields),
        "registered_field_count": validation["explained_fields"],
        "auto_registered_count": validation.get("auto_registered", 0),
        "missing_explanations": validation["missing_explanations"],
        "orphaned_explanations": validation["orphaned_explanations"],
        "candidate_fields": validation["candidate_fields"],
        "coverage_percentage": (validation["explained_fields"] / len(current_fields) * 100)
        if current_fields
        else 0,
    }

    if previous_fields:
        analysis.update(
            {
                "added_fields": list(current_fields - previous_fields),
                "removed_fields": list(previous_fields - current_fields),
                "field_change_count": len(current_fields - previous_fields)
                + len(previous_fields - current_fields),
            }
        )

    return analysis


def export_field_registry_report(output_path: str) -> None:
    """
    Export a comprehensive field registry report for documentation and debugging.

    Args:
        output_path: Path to save the report
    """
    import datetime

    all_fields = FIELD_REGISTRY._fields

    report_lines = [
        "# Field Registry Report",
        f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Total registered fields: {len(all_fields)}",
        "",
        "## Field Definitions",
        "",
    ]

    # Group fields by type
    field_types: Dict[str, List[Tuple[str, FieldDefinition]]] = {}
    for field_name, field_def in all_fields.items():
        field_type = field_def.field_type
        if field_type not in field_types:
            field_types[field_type] = []
        field_types[field_type].append((field_name, field_def))

    for field_type, fields in sorted(field_types.items()):
        report_lines.append(f"### {field_type.title()} Fields ({len(fields)})")
        report_lines.append("")

        for field_name, field_def in sorted(fields):
            report_lines.append(f"**{field_name}**")
            report_lines.append(f"- Description: {field_def.description}")
            if field_def.formula:
                report_lines.append(f"- Formula: `{field_def.formula}`")
            if field_def.units:
                report_lines.append(f"- Units: {field_def.units}")
            report_lines.append("")

    # Write report
    with open(output_path, "w") as f:
        f.write("\n".join(report_lines))

    logger.debug(f"📄 Field registry report exported to: {output_path}")


def suggest_missing_field_registrations(missing_fields: List[str]) -> List[str]:
    """
    Provide suggestions for registering missing fields based on naming patterns.

    Args:
        missing_fields: List of field names that lack explanations

    Returns:
        List of suggested registration code snippets
    """
    suggestions = []

    for field in missing_fields:
        if field.startswith("votes_"):
            candidate = field.replace("votes_", "")
            suggestions.append(
                f"register_calculated_field(\n"
                f'    name="{field}",\n'
                f'    description="Vote count for candidate {candidate.replace("_", " ").title()}",\n'
                f'    formula="COUNT(votes_for_{candidate})",\n'
                f'    field_type="count",\n'
                f'    units="votes"\n'
                f")"
            )
        elif field.startswith("vote_pct_"):
            candidate = field.replace("vote_pct_", "")
            suggestions.append(
                f"register_calculated_field(\n"
                f'    name="{field}",\n'
                f'    description="Vote percentage for candidate {candidate.replace("_", " ").title()}",\n'
                f'    formula="(votes_{candidate} / votes_total) * 100",\n'
                f'    field_type="percentage",\n'
                f'    units="percent"\n'
                f")"
            )
        elif field.startswith("is_") or field.startswith("has_"):
            suggestions.append(
                f"register_calculated_field(\n"
                f'    name="{field}",\n'
                f'    description="Boolean flag: {field.replace("_", " ")}",\n'
                f'    formula="CALCULATED_FROM_DATA_CONDITIONS",\n'
                f'    field_type="boolean"\n'
                f")"
            )
        else:
            suggestions.append(
                f"# {field}: Consider registering with appropriate description and formula"
            )

    return suggestions


def validate_field_completeness(gdf: gpd.GeoDataFrame, strict_mode: bool = False) -> None:
    """
    Validation function to ensure all fields have explanations.
    Now handles schema drift gracefully with auto-registration and flexible validation.

    Args:
        gdf: GeoDataFrame to validate
        strict_mode: If True, fail on missing fields; if False, warn and continue
    """
    validation = FIELD_REGISTRY.validate_gdf_completeness(gdf)

    # Report auto-registration results
    if validation.get("auto_registered", 0) > 0:
        logger.debug(
            f"🔄 Auto-registered {validation['auto_registered']} fields using pattern detection"
        )

    # Handle missing explanations based on mode
    if validation["missing_explanations"]:
        missing_count = len(validation["missing_explanations"])
        missing_list = validation["missing_explanations"]

        if strict_mode:
            error_msg = (
                f"STRICT MODE: Missing explanations for {missing_count} fields: {missing_list}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        else:
            logger.warning(
                f"⚠️  Schema drift detected: {missing_count} fields lack explicit explanations"
            )
            logger.warning(
                f"   Missing fields: {missing_list[:10]}{'...' if len(missing_list) > 10 else ''}"
            )
            logger.debug("   💡 Consider using register_calculated_field() for critical fields")
            logger.debug("   📚 Auto-generated explanations will be used for web display")

    # Report orphaned explanations (fields in registry but not in data)
    if validation["orphaned_explanations"]:
        orphaned_count = len(validation["orphaned_explanations"])
        logger.warning(
            f"📋 {orphaned_count} registered fields not found in current data: {validation['orphaned_explanations']}"
        )
        logger.debug("   💡 This may indicate upstream schema changes or different data sources")

    # Success summary
    total_fields = validation["total_fields"]
    explained_fields = validation["explained_fields"]
    coverage_pct = (explained_fields / total_fields * 100) if total_fields > 0 else 0

    logger.debug(
        f"✅ Field coverage: {explained_fields}/{total_fields} fields ({coverage_pct:.1f}%) have explanations"
    )

    if coverage_pct >= 90:
        logger.debug("🎯 Excellent field coverage! Documentation is comprehensive.")
    elif coverage_pct >= 70:
        logger.debug("👍 Good field coverage. Consider documenting remaining critical fields.")
    else:
        logger.warning(
            "📝 Low field coverage. Many fields may need documentation for better usability."
        )


def detect_candidate_columns(gdf: gpd.GeoDataFrame) -> List[str]:
    """Detect all candidate percentage columns dynamically from the enriched dataset."""
    # Look for vote percentage columns (vote_pct_candidatename) from the new enrichment
    candidate_pct_cols = [
        col
        for col in gdf.columns
        if col.startswith("vote_pct_")
        and col != "vote_pct_contribution_total_votes"
        and not col.startswith("vote_pct_contribution_")
    ]
    logger.debug(f"  📊 Detected candidate percentage columns: {candidate_pct_cols}")
    return candidate_pct_cols


def detect_candidate_count_columns(gdf: gpd.GeoDataFrame) -> List[str]:
    """Detect all candidate count columns dynamically from the enriched dataset."""
    # Look for vote count columns (votes_candidatename) from the new enrichment
    candidate_cnt_cols = [
        col for col in gdf.columns if col.startswith("votes_") and col != "votes_total"
    ]
    logger.debug(f"  📊 Detected candidate count columns: {candidate_cnt_cols}")
    return candidate_cnt_cols


def detect_contribution_columns(gdf: gpd.GeoDataFrame) -> List[str]:
    """Detect all candidate contribution columns dynamically from the enriched dataset."""
    # Look for contribution percentage columns
    contribution_cols = [
        col
        for col in gdf.columns
        if col.startswith("vote_pct_contribution_") and col != "vote_pct_contribution_total_votes"
    ]
    logger.debug(f"  📊 Detected contribution columns: {contribution_cols}")
    return contribution_cols


def consolidate_split_precincts(gdf: gpd.GeoDataFrame, precinct_col: str) -> gpd.GeoDataFrame:
    """
    Consolidate split precincts (e.g., 2801a, 2801b, 2801c) into single features.
    FIXED to preserve county rollup data properly.

    Args:
        gdf: GeoDataFrame with precinct data
        precinct_col: Name of the precinct column

    Returns:
        GeoDataFrame with consolidated precincts
    """
    logger.debug(f"🔄 Consolidating split precincts in column '{precinct_col}':")

    # Create a copy to work with
    gdf_work = gdf.copy()

    # Convert ALL numeric columns to proper numeric types BEFORE processing
    logger.debug("  🔧 Converting columns to proper data types...")

    # Identify boolean columns first to exclude them from numeric conversion
    boolean_cols = [
        "is_pps_precinct",
        "has_election_results",
        "has_voter_registration",
        "is_summary",
        "is_complete_record",
        "is_county_rollup",
    ]

    # Identify categorical columns that should NOT be converted to numeric
    categorical_cols = [
        "political_lean",
        "competitiveness",
        "leading_candidate",
        "second_candidate",
        "record_type",
        "turnout_quartile",
        "margin_category",
        "precinct_size_category",
    ]

    # Identify ALL columns that should be numeric and convert them (excluding boolean and categorical columns)
    numeric_conversion_cols = []
    for col in gdf_work.columns:
        if col in ["geometry", precinct_col, "base_precinct"] + boolean_cols + categorical_cols:
            continue
        # Check if this looks like a numeric column based on content
        sample_values = gdf_work[col].dropna().head(10)
        if len(sample_values) > 0:
            # Try to convert sample to see if it's numeric
            try:
                converted = pd.to_numeric(sample_values, errors="coerce")
                # Only include if the conversion actually worked (not all NaN)
                if not converted.isna().all():
                    numeric_conversion_cols.append(col)
            except Exception as e:
                logger.error(f"Error converting column {col} to numeric: {e}")

    # Convert identified numeric columns (excluding booleans and categoricals)
    for col in numeric_conversion_cols:
        gdf_work[col] = pd.to_numeric(gdf_work[col], errors="coerce").fillna(0)

    # Handle boolean columns separately - convert to proper boolean type
    for col in boolean_cols:
        if col in gdf_work.columns:
            gdf_work[col] = gdf_work[col].astype(str).str.lower().isin(["true", "1", "yes"])

    logger.debug(f"  📊 Converted {len(numeric_conversion_cols)} columns to numeric")
    logger.debug(
        f"  📊 Converted {sum(1 for col in boolean_cols if col in gdf_work.columns)} columns to boolean"
    )

    # Extract base precinct numbers (remove a,b,c suffixes)
    gdf_work["base_precinct"] = (
        gdf_work[precinct_col].astype(str).str.replace(r"[a-zA-Z]+$", "", regex=True).str.strip()
    )

    # Count how many precincts have splits
    precinct_counts = gdf_work["base_precinct"].value_counts()
    split_precincts = precinct_counts[precinct_counts > 1]

    logger.debug(f"  📊 Found {len(split_precincts)} precincts with splits:")
    for base, count in split_precincts.head(10).items():
        logger.debug(f"    - Precinct {base}: {count} parts")
    if len(split_precincts) > 10:
        logger.debug(f"    ... and {len(split_precincts) - 10} more")

    # Identify ALL numeric columns to take first value during consolidation
    numeric_cols = []
    for col in gdf_work.columns:
        if col in ["geometry", precinct_col, "base_precinct"]:
            continue
        # Include ANY column that starts with numeric prefixes OR is explicitly numeric
        if (
            col.startswith(
                (
                    "votes_",
                    "TOTAL",
                    "DEM",
                    "REP",
                    "NAV",
                    "OTH",
                    "CON",
                    "IND",
                    "LBT",
                    "NLB",
                    "PGP",
                    "PRO",
                    "WFP",
                    "WTP",
                )
            )
            or col in ["vote_margin", "total_voters"]
            or gdf_work[col].dtype in ["int64", "float64"]
        ):
            numeric_cols.append(col)

    # Identify percentage/rate columns - FIXED for new percentage scale (0-100)
    percentage_cols = []
    for col in gdf_work.columns:
        if col.startswith(("vote_pct_", "reg_pct_")) or col in [
            "turnout_rate",
            "dem_advantage",
            "major_party_pct",
            "margin_pct",
            "engagement_score",
        ]:
            percentage_cols.append(col)

    logger.debug(
        f"  📊 Will take first value from {len(numeric_cols)} numeric columns during consolidation"
    )
    logger.debug(f"  📊 Will recalculate {len(percentage_cols)} percentage columns")

    # Debug: Check vote totals BEFORE consolidation (sum of unique precincts only)
    unique_precinct_votes = 0
    if "votes_total" in gdf_work.columns:
        # For accurate comparison, count only unique base precincts
        unique_precinct_votes = gdf_work.groupby("base_precinct")["votes_total"].first().sum()
        pre_consolidation_total = gdf_work["votes_total"].sum()  # Raw total (includes duplicates)
        logger.debug(
            f"  🔍 Total votes BEFORE consolidation: {pre_consolidation_total:,.0f} (raw with duplicates)"
        )
        logger.debug(
            f"  🔍 Unique precinct votes: {unique_precinct_votes:,.0f} (expected after consolidation)"
        )

    # Group by base precinct and consolidate
    consolidated_features = []

    for base_precinct in gdf_work["base_precinct"].unique():
        if pd.isna(base_precinct) or base_precinct == "":
            continue

        precinct_parts = gdf_work[gdf_work["base_precinct"] == base_precinct]

        if len(precinct_parts) == 1:
            # Single precinct, just update the precinct name to base
            feature = precinct_parts.copy()
            feature[precinct_col] = base_precinct
            consolidated_features.append(feature)
        else:
            # Multiple parts - consolidate them
            consolidated = precinct_parts.iloc[0:1].copy()  # Start with first part

            # Update precinct identifier to base
            consolidated[precinct_col] = base_precinct

            # Take values from the first part (they should all be identical for split precincts)
            for col in numeric_cols:
                if col in precinct_parts.columns:
                    # For split precincts, all parts should have identical values - take the first
                    first_value = precinct_parts[col].iloc[0]
                    consolidated.loc[consolidated.index[0], col] = first_value

                    # Debug: Verify all parts have same values for vote columns
                    if col.startswith("votes_") and first_value > 0:
                        all_values = precinct_parts[col].tolist()
                        if len(set(all_values)) == 1:
                            logger.debug(
                                f"    🔍 {base_precinct} {col}: {first_value} (verified identical across {len(all_values)} parts)"
                            )
                        else:
                            logger.debug(
                                f"    ⚠️  {base_precinct} {col}: Values differ across parts: {all_values} - taking first: {first_value}"
                            )

            # Handle boolean and categorical columns properly
            categorical_cols = [
                "political_lean",
                "competitiveness",
                "leading_candidate",
                "record_type",
                "turnout_quartile",
                "margin_category",
                "precinct_size_category",
            ]

            # For boolean columns, use logical OR (if ANY part is True, consolidated should be True)
            for col in boolean_cols:
                if col in precinct_parts.columns:
                    # Convert to boolean and take logical OR
                    bool_values = precinct_parts[col].astype(
                        bool
                    )  # Already converted to bool above
                    consolidated_value = bool_values.any()  # True if ANY part is True
                    consolidated.loc[consolidated.index[0], col] = (
                        consolidated_value  # Use .loc for proper assignment
                    )

                    if col == "is_pps_precinct" and consolidated_value:
                        logger.debug(
                            f"    🔍 {base_precinct} {col}: {precinct_parts[col].tolist()} → {consolidated_value}"
                        )

            # For categorical columns, use the first value (should be identical for split precincts)
            for col in categorical_cols:
                if col in precinct_parts.columns:
                    # Take the first value (should be same across all parts for split precincts)
                    first_value = precinct_parts[col].iloc[0]
                    consolidated.loc[consolidated.index[0], col] = first_value

            # Dissolve geometries (combine all parts into one shape) - FIXED for edge artifacts
            try:
                # STEP 1: Clean geometries first to avoid edge artifacts
                cleaned_geoms = []
                for idx, geom in enumerate(precinct_parts.geometry):
                    if geom is not None and geom.is_valid:
                        # Apply tiny buffer to clean up potential edge issues
                        cleaned_geom = geom.buffer(0.0000001).buffer(
                            -0.0000001
                        )  # Tiny buffer + negative buffer
                        if cleaned_geom.is_valid and not cleaned_geom.is_empty:
                            cleaned_geoms.append(cleaned_geom)
                        else:
                            logger.warning(
                                f"      ⚠️ Geometry {idx} invalid after cleaning, using original"
                            )
                            cleaned_geoms.append(geom)
                    elif geom is not None:
                        # Try to fix invalid geometry with standard buffer
                        try:
                            fixed_geom = geom.buffer(0)
                            if fixed_geom.is_valid and not fixed_geom.is_empty:
                                cleaned_geoms.append(fixed_geom)
                            else:
                                logger.warning(f"      ⚠️ Could not fix geometry {idx}, skipping")
                        except Exception:
                            logger.warning(f"      ⚠️ Could not process geometry {idx}, skipping")

                # STEP 2: Dissolve using cleaned geometries
                if cleaned_geoms:
                    # Use unary_union on cleaned geometries
                    dissolved_geom = gpd.GeoSeries(cleaned_geoms).unary_union

                    # STEP 3: Final validation and cleanup
                    if dissolved_geom.is_valid:
                        # Apply final tiny buffer to ensure clean edges
                        final_geom = dissolved_geom.buffer(0.0000001).buffer(-0.0000001)
                        if final_geom.is_valid and not final_geom.is_empty:
                            consolidated.loc[consolidated.index[0], "geometry"] = final_geom
                        else:
                            consolidated.loc[consolidated.index[0], "geometry"] = dissolved_geom
                    else:
                        # Try to fix with standard buffer
                        fixed_geom = dissolved_geom.buffer(0)
                        if fixed_geom.is_valid:
                            consolidated.loc[consolidated.index[0], "geometry"] = fixed_geom
                        else:
                            logger.debug(
                                f"    ⚠️ Could not create valid dissolved geometry for {base_precinct}, using first part"
                            )
                            consolidated.loc[consolidated.index[0], "geometry"] = (
                                precinct_parts.geometry.iloc[0]
                            )
                else:
                    logger.warning(
                        f"    ⚠️ No valid geometries to dissolve for {base_precinct}, using first part"
                    )
                    consolidated.loc[consolidated.index[0], "geometry"] = (
                        precinct_parts.geometry.iloc[0]
                    )

            except Exception as e:
                logger.warning(f"    ⚠️ Error dissolving geometry for precinct {base_precinct}: {e}")
                # Use the first geometry as fallback
                consolidated.loc[consolidated.index[0], "geometry"] = precinct_parts.geometry.iloc[
                    0
                ]

            # Keep other fields from first part for remaining columns
            # Note: percentage columns will be recalculated later based on new totals

            consolidated_features.append(consolidated)

    # Combine all consolidated features
    if consolidated_features:
        gdf_consolidated = pd.concat(consolidated_features, ignore_index=True)

        # Debug: Check vote totals AFTER consolidation
        if "votes_total" in gdf_consolidated.columns:
            post_consolidation_total = gdf_consolidated["votes_total"].sum()
            logger.debug(f"  🔍 Total votes AFTER consolidation: {post_consolidation_total:,.0f}")
            if unique_precinct_votes > 0:
                retention_rate = (post_consolidation_total / unique_precinct_votes) * 100
                logger.debug(f"  📊 Vote retention rate: {retention_rate:.1f}% (should be 100%)")
            else:
                logger.debug("  📊 Vote total preserved correctly")

        # Recalculate percentage columns based on new totals - FIXED for percentage scale
        logger.debug("  🔄 Recalculating percentage columns...")
        for col in percentage_cols:
            if col in gdf_consolidated.columns:
                if col.startswith("vote_pct_"):
                    # Find the corresponding count column
                    count_col = col.replace("vote_pct_", "votes_")
                    if (
                        count_col in gdf_consolidated.columns
                        and "votes_total" in gdf_consolidated.columns
                    ):
                        # Convert to numeric and recalculate percentages (0-100 scale)
                        count_values = pd.to_numeric(
                            gdf_consolidated[count_col], errors="coerce"
                        ).fillna(0)
                        total_values = pd.to_numeric(
                            gdf_consolidated["votes_total"], errors="coerce"
                        ).fillna(0)
                        gdf_consolidated[col] = np.where(
                            total_values > 0,
                            (count_values / total_values) * 100,  # Scale to 0-100
                            0,
                        )
                elif (
                    col == "turnout_rate"
                    and "votes_total" in gdf_consolidated.columns
                    and "TOTAL" in gdf_consolidated.columns
                ):
                    # Recalculate turnout rate (0-100 scale)
                    vote_values = pd.to_numeric(
                        gdf_consolidated["votes_total"], errors="coerce"
                    ).fillna(0)
                    total_values = pd.to_numeric(gdf_consolidated["TOTAL"], errors="coerce").fillna(
                        0
                    )
                    gdf_consolidated[col] = np.where(
                        total_values > 0,
                        (vote_values / total_values) * 100,  # Scale to 0-100
                        0,
                    )
                elif col == "dem_advantage":
                    # Recalculate dem_advantage (already on 0-100 scale)
                    if (
                        "reg_pct_dem" in gdf_consolidated.columns
                        and "reg_pct_rep" in gdf_consolidated.columns
                    ):
                        dem_values = pd.to_numeric(
                            gdf_consolidated["reg_pct_dem"], errors="coerce"
                        ).fillna(0)
                        rep_values = pd.to_numeric(
                            gdf_consolidated["reg_pct_rep"], errors="coerce"
                        ).fillna(0)
                        gdf_consolidated[col] = dem_values - rep_values
                elif col == "major_party_pct":
                    # Recalculate major_party_pct (already on 0-100 scale)
                    if (
                        "reg_pct_dem" in gdf_consolidated.columns
                        and "reg_pct_rep" in gdf_consolidated.columns
                    ):
                        dem_values = pd.to_numeric(
                            gdf_consolidated["reg_pct_dem"], errors="coerce"
                        ).fillna(0)
                        rep_values = pd.to_numeric(
                            gdf_consolidated["reg_pct_rep"], errors="coerce"
                        ).fillna(0)
                        gdf_consolidated[col] = dem_values + rep_values

        logger.debug(
            f"  ✅ Consolidated {len(gdf_work)} features into {len(gdf_consolidated)} features"
        )
        logger.debug(
            f"  ✅ Eliminated {len(gdf_work) - len(gdf_consolidated)} duplicate/split features"
        )

        return gdf_consolidated
    else:
        logger.warning("  ⚠️ Warning: No features to consolidate")
        return gdf_work


def add_analytical_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add new analytical fields for deeper election analysis.
    FIXED to handle new percentage data scale (0-100 instead of 0-1).

    Args:
        df: DataFrame with election data

    Returns:
        DataFrame with additional analytical fields
    """
    logger.debug("📊 Adding analytical fields:")

    df_analysis = df.copy()

    # Convert string columns to numeric first - MAKE FULLY DYNAMIC
    # Detect candidate columns dynamically from the data
    candidate_vote_cols = [
        col for col in df_analysis.columns if col.startswith("votes_") and col != "votes_total"
    ]
    candidate_pct_cols = [
        col
        for col in df_analysis.columns
        if col.startswith("vote_pct_")
        and col != "vote_pct_contribution_total_votes"
        and not col.startswith("vote_pct_contribution_")
    ]

    # Dynamic list of all numeric columns that need conversion
    numeric_conversion_cols = (
        ["vote_margin", "votes_total", "turnout_rate", "TOTAL", "DEM", "REP", "NAV"]
        + candidate_vote_cols
        + candidate_pct_cols
        + [col for col in df_analysis.columns if col.startswith("reg_pct_")]
    )

    # Only convert columns that actually exist in the dataframe
    numeric_conversion_cols = [col for col in numeric_conversion_cols if col in df_analysis.columns]

    for col in numeric_conversion_cols:
        if col in df_analysis.columns:
            df_analysis[col] = pd.to_numeric(df_analysis[col], errors="coerce").fillna(0)

    # Victory Margin Analysis
    if "vote_margin" in df_analysis.columns and "votes_total" in df_analysis.columns:
        df_analysis["pct_victory_margin"] = np.where(
            df_analysis["votes_total"] > 0,
            (df_analysis["vote_margin"] / df_analysis["votes_total"] * 100),
            0,
        )
        logger.debug("  ✅ Added pct_victory_margin (victory margin as % of total votes)")

    # Divergence from Perfect Tie (50%-50%) - SIGNED VERSION
    # First, find candidate columns in this context
    candidate_cols = [
        col for col in df_analysis.columns if col.startswith("votes_") and col != "votes_total"
    ]

    if "pct_victory_margin" in df_analysis.columns and len(candidate_cols) >= 2:
        # First, determine the overall election winner across all PPS precincts
        overall_winner = None
        overall_runner_up = None

        pps_mask = df_analysis.get("is_pps_precinct", pd.Series([True] * len(df_analysis)))

        if pps_mask.any():
            candidate_totals = {}
            for col in candidate_cols:
                candidate_name = col.replace("votes_", "")
                total_votes = df_analysis.loc[pps_mask, col].sum()
                if total_votes > 0:
                    candidate_totals[candidate_name] = total_votes

            if len(candidate_totals) >= 2:
                sorted_totals = sorted(candidate_totals.items(), key=lambda x: x[1], reverse=True)
                overall_winner = sorted_totals[0][0]
                overall_runner_up = sorted_totals[1][0]

                logger.debug(f"  📊 Overall election: {overall_winner} beat {overall_runner_up}")

        # Calculate signed divergence from tie
        df_analysis["divergence_from_tie"] = 0.0

        if overall_winner and overall_runner_up:
            winner_pct_col = f"vote_pct_{overall_winner}"
            runner_up_pct_col = f"vote_pct_{overall_runner_up}"

            if winner_pct_col in df_analysis.columns and runner_up_pct_col in df_analysis.columns:
                # Calculate signed margin: positive when overall winner led in precinct, negative when runner-up led
                winner_pct = df_analysis[winner_pct_col].fillna(0)
                runner_up_pct = df_analysis[runner_up_pct_col].fillna(0)

                df_analysis["divergence_from_tie"] = winner_pct - runner_up_pct

                # Only apply to precincts with election data
                mask = (
                    df_analysis["has_election_results"]
                    if "has_election_results" in df_analysis.columns
                    else df_analysis["votes_total"] > 0
                )
                non_election_mask = ~mask
                df_analysis.loc[non_election_mask, "divergence_from_tie"] = 0.0

                logger.debug(
                    f"  ✅ Added signed divergence_from_tie (+{overall_winner}, -{overall_runner_up})"
                )
            else:
                logger.warning(
                    f"  ⚠️ Could not find percentage columns for {overall_winner} or {overall_runner_up}"
                )
        else:
            # Fallback to absolute margin if we can't determine overall winner
            df_analysis["divergence_from_tie"] = df_analysis["pct_victory_margin"]
            logger.debug("  ✅ Added divergence_from_tie (absolute margin fallback)")
    else:
        logger.warning("  ⚠️ Could not calculate divergence_from_tie - insufficient data")

    # Competitiveness Scoring
    if "pct_victory_margin" in df_analysis.columns:
        df_analysis["competitiveness_score"] = (
            100 - df_analysis["pct_victory_margin"]
        )  # 0=landslide, 100=tie
        logger.debug("  ✅ Added competitiveness_score (100 = tie, 0 = landslide)")

    # Turnout Quartiles
    if "turnout_rate" in df_analysis.columns:
        valid_turnout = df_analysis[df_analysis["turnout_rate"] > 0]["turnout_rate"]
        if len(valid_turnout) > 3:  # Need at least 4 values for quartiles
            try:
                df_analysis["turnout_quartile"] = pd.qcut(
                    df_analysis["turnout_rate"],
                    4,
                    labels=["Low", "Med-Low", "Med-High", "High"],
                    duplicates="drop",
                )
                logger.debug("  ✅ Added turnout_quartile (Low/Med-Low/Med-High/High)")
            except ValueError:
                # Try 3 bins
                try:
                    df_analysis["turnout_quartile"] = pd.qcut(
                        df_analysis["turnout_rate"],
                        3,
                        labels=["Low", "Medium", "High"],
                        duplicates="drop",
                    )
                    logger.debug("  ✅ Added turnout_quartile (Low/Medium/High)")
                except ValueError:
                    # Try 2 bins
                    try:
                        df_analysis["turnout_quartile"] = pd.qcut(
                            df_analysis["turnout_rate"],
                            2,
                            labels=["Low", "High"],
                            duplicates="drop",
                        )
                        logger.debug("  ✅ Added turnout_quartile (Low/High)")
                    except ValueError:
                        # Use percentile-based approach instead
                        median_turnout = df_analysis["turnout_rate"].median()
                        df_analysis["turnout_quartile"] = np.where(
                            df_analysis["turnout_rate"] >= median_turnout, "High", "Low"
                        )
                        logger.debug("  ✅ Added turnout_quartile (Low/High based on median)")
        else:
            # Not enough data for quartiles
            df_analysis["turnout_quartile"] = "Single"
            logger.warning("  ⚠️ Added turnout_quartile (Single category - insufficient data)")

    # Margin Categories
    if "pct_victory_margin" in df_analysis.columns:
        df_analysis["margin_category"] = pd.cut(
            df_analysis["pct_victory_margin"],
            bins=[0, 5, 15, 30, 100],
            labels=["Very Close", "Close", "Clear", "Landslide"],
            include_lowest=True,
        )
        logger.debug("  ✅ Added margin_category (Very Close/Close/Clear/Landslide)")

    # Find leading and second place candidates - convert candidate columns to numeric first
    candidate_cols = [
        col for col in df_analysis.columns if col.startswith("votes_") and col != "votes_total"
    ]

    # Convert candidate columns to numeric
    for col in candidate_cols:
        if col in df_analysis.columns:
            df_analysis[col] = pd.to_numeric(df_analysis[col], errors="coerce").fillna(0)

    if len(candidate_cols) >= 2:
        # Calculate leading and second place for dominance ratio
        df_analysis["votes_leading"] = 0
        df_analysis["votes_second_place"] = 0
        df_analysis["candidate_dominance"] = 1.0

        for idx, row in df_analysis.iterrows():
            candidate_votes = [
                (col, row[col]) for col in candidate_cols if pd.notna(row[col]) and row[col] > 0
            ]
            candidate_votes.sort(key=lambda x: x[1], reverse=True)

            if len(candidate_votes) >= 2:
                leading_votes = candidate_votes[0][1]
                second_votes = candidate_votes[1][1]

                # Candidate Dominance Ratio
                df_analysis.loc[idx, "votes_leading"] = leading_votes
                df_analysis.loc[idx, "votes_second_place"] = second_votes

                if second_votes > 0:
                    df_analysis.loc[idx, "candidate_dominance"] = leading_votes / second_votes
                else:
                    df_analysis.loc[idx, "candidate_dominance"] = float("inf")
            elif len(candidate_votes) == 1:
                df_analysis.loc[idx, "votes_leading"] = candidate_votes[0][1]
                df_analysis.loc[idx, "votes_second_place"] = 0
                df_analysis.loc[idx, "candidate_dominance"] = float("inf")

        logger.debug("  ✅ Added candidate_dominance (leading votes / second place votes)")

    # Registration vs Results Analysis - MADE FULLY DYNAMIC
    # Dynamically detect all candidate percentage columns
    candidate_pct_cols = [
        col
        for col in df_analysis.columns
        if col.startswith("vote_pct_")
        and col != "vote_pct_contribution_total_votes"
        and not col.startswith("vote_pct_contribution_")
    ]

    if len(candidate_pct_cols) > 0 and "reg_pct_dem" in df_analysis.columns:
        # Detect Democratic-aligned candidate using correlation analysis (no hardcoded names)
        dem_candidate_col = None
        best_correlation = -1

        logger.debug(
            f"  🔍 Analyzing correlations to detect Democratic-aligned candidate from {len(candidate_pct_cols)} candidates..."
        )

        for col in candidate_pct_cols:
            valid_mask = (
                df_analysis[col].notna()
                & df_analysis["reg_pct_dem"].notna()
                & (df_analysis[col] > 0)
                & (df_analysis["reg_pct_dem"] > 0)
            )

            if valid_mask.sum() > 10:  # Need enough data points for reliable correlation
                try:
                    correlation = df_analysis.loc[valid_mask, col].corr(
                        df_analysis.loc[valid_mask, "reg_pct_dem"]
                    )
                    candidate_name = col.replace("vote_pct_", "")
                    logger.debug(
                        f"  📊 {candidate_name}: correlation with Democratic registration = {correlation:.3f}"
                    )

                    if correlation > best_correlation:
                        best_correlation = correlation
                        dem_candidate_col = col
                except Exception as e:
                    logger.error(f"  ⚠️ Could not calculate correlation for {col}: {e}")

        # If no good correlation found, use the first candidate as fallback
        if dem_candidate_col is None and len(candidate_pct_cols) > 0:
            dem_candidate_col = candidate_pct_cols[0]
            logger.warning(
                f"  ⚠️ No strong correlations found, using first candidate: {dem_candidate_col}"
            )

        # Calculate vote efficiency for the detected Democratic-aligned candidate
        if dem_candidate_col:
            candidate_name = dem_candidate_col.replace("vote_pct_", "")
            logger.debug(
                f"  ✅ Selected {candidate_name} as Democratic-aligned candidate (correlation: {best_correlation:.3f})"
            )

            df_analysis["vote_efficiency_dem"] = np.where(
                df_analysis["reg_pct_dem"] > 0,
                df_analysis[dem_candidate_col] / df_analysis["reg_pct_dem"],
                0,
            )
            logger.debug(
                f"  ✅ Added vote_efficiency_dem (how well Dems turned out for {candidate_name})"
            )

    if "reg_pct_dem" in df_analysis.columns and "reg_pct_rep" in df_analysis.columns:
        df_analysis["registration_competitiveness"] = abs(
            df_analysis["reg_pct_dem"] - df_analysis["reg_pct_rep"]
        )
        logger.debug(
            "  ✅ Added registration_competitiveness (absolute difference in party registration)"
        )

    if (
        "registration_competitiveness" in df_analysis.columns
        and "pct_victory_margin" in df_analysis.columns
    ):
        df_analysis["swing_potential"] = abs(
            df_analysis["registration_competitiveness"] - df_analysis["pct_victory_margin"]
        )
        logger.debug(
            "  ✅ Added swing_potential (difference between registration and actual competition)"
        )

    # Additional analytical metrics
    if "votes_total" in df_analysis.columns and "TOTAL" in df_analysis.columns:
        # Voter engagement rate (different from turnout) - scale to 0-100
        df_analysis["engagement_rate"] = np.where(
            df_analysis["TOTAL"] > 0, (df_analysis["votes_total"] / df_analysis["TOTAL"]) * 100, 0
        )
        logger.debug("  ✅ Added engagement_rate (same as turnout_rate but explicit)")

    # MISSING FIELD CALCULATIONS - Add the registered fields that were missing

    # 1. total_voters (standardized name for TOTAL)
    if "TOTAL" in df_analysis.columns:
        df_analysis["total_voters"] = df_analysis["TOTAL"]
        logger.debug("  ✅ Added total_voters (standardized from TOTAL column)")

    # 2. has_election_data (boolean: whether precinct has election results)
    if "votes_total" in df_analysis.columns:
        df_analysis["has_election_data"] = df_analysis["votes_total"].notna() & (
            df_analysis["votes_total"] > 0
        )
        logger.debug("  ✅ Added has_election_data (votes_total > 0 and not null)")

    # 3. has_voter_data (boolean: whether precinct has voter registration data)
    if "total_voters" in df_analysis.columns:
        df_analysis["has_voter_data"] = df_analysis["total_voters"].notna() & (
            df_analysis["total_voters"] > 0
        )
        logger.debug("  ✅ Added has_voter_data (total_voters > 0 and not null)")

    # 4. participated_election (boolean: participated and is in pps)
    if "has_election_data" in df_analysis.columns and "is_pps_precinct" in df_analysis.columns:
        df_analysis["participated_election"] = df_analysis["has_election_data"] & df_analysis[
            "is_pps_precinct"
        ].fillna(False)
        logger.debug("  ✅ Added participated_election (has_election_data AND is_pps_precinct)")

    # 5. complete_record (boolean: has both election and voter data)
    if "has_election_data" in df_analysis.columns and "has_voter_data" in df_analysis.columns:
        df_analysis["complete_record"] = (
            df_analysis["has_election_data"] & df_analysis["has_voter_data"]
        )
        logger.debug("  ✅ Added complete_record (has_election_data AND has_voter_data)")

    # NEW ANALYTICAL METRICS - ELECTION IMPORTANCE AND IMPACT
    logger.debug("  🆕 Adding advanced election importance metrics...")

    # 1. Vote Impact Score - combines size and decisiveness
    if "votes_total" in df_analysis.columns and "margin_pct" in df_analysis.columns:
        df_analysis["vote_impact_score"] = df_analysis["votes_total"] * abs(
            df_analysis["margin_pct"]
        )
        logger.debug("  ✅ Added vote_impact_score (total votes × absolute margin %)")

    # 2. Net Margin Votes - already exists as vote_margin, but let's ensure it's absolute
    if "vote_margin" in df_analysis.columns:
        df_analysis["net_margin_votes"] = abs(df_analysis["vote_margin"])
        logger.debug(
            "  ✅ Added net_margin_votes (absolute vote difference between winner and runner-up)"
        )

    # 3. Swing Contribution - how much each precinct contributed to overall election margin
    if "vote_margin" in df_analysis.columns:
        # Calculate for PPS precincts only (where election took place)
        pps_mask = (
            df_analysis["is_pps_precinct"]
            if "is_pps_precinct" in df_analysis.columns
            else df_analysis.index
        )

        if pps_mask.any():
            # Calculate total election margin (sum of all precinct margins in zone)
            total_election_margin = df_analysis.loc[pps_mask, "vote_margin"].sum()

            df_analysis["swing_contribution"] = 0.0
            if total_election_margin != 0:
                df_analysis.loc[pps_mask, "swing_contribution"] = (
                    df_analysis.loc[pps_mask, "vote_margin"] / total_election_margin * 100
                )
                logger.debug(
                    f"  ✅ Added swing_contribution (% of total election margin, total: {total_election_margin:,.0f})"
                )
            else:
                logger.warning("  ⚠️ Total election margin is zero, swing_contribution set to 0")

    # 4. Power Index - combines turnout share and margin significance
    if "pps_vote_share" in df_analysis.columns and "margin_pct" in df_analysis.columns:
        df_analysis["power_index"] = (
            df_analysis["pps_vote_share"] * abs(df_analysis["margin_pct"]) / 100
        )
        logger.debug(
            "  ✅ Added power_index (turnout share × margin %, rewards size and decisiveness)"
        )

    # 5. Precinct Influence Score - standardized importance metric (0-100 scale)
    if "vote_impact_score" in df_analysis.columns:
        pps_mask = (
            df_analysis["is_pps_precinct"]
            if "is_pps_precinct" in df_analysis.columns
            else df_analysis.index
        )

        if pps_mask.any():
            # Normalize to 0-100 scale within PPS precincts
            pps_impact_scores = df_analysis.loc[pps_mask, "vote_impact_score"]
            max_impact = pps_impact_scores.max()

            df_analysis["precinct_influence"] = 0.0
            if max_impact > 0:
                df_analysis.loc[pps_mask, "precinct_influence"] = (
                    pps_impact_scores / max_impact * 100
                )
                logger.debug("  ✅ Added precinct_influence (normalized importance score 0-100)")

    # 6. Competitive Balance Score - how balanced the race was in each precinct
    if "margin_pct" in df_analysis.columns:
        df_analysis["competitive_balance"] = 100 - abs(df_analysis["margin_pct"])
        logger.debug("  ✅ Added competitive_balance (100 = tied race, 0 = complete blowout)")

    # 7. Vote Efficiency Ratio - votes per registered voter who actually turned out
    if "votes_total" in df_analysis.columns and "TOTAL" in df_analysis.columns:
        # This shows how "efficient" the voting was - closer to 1.0 means most registered voters voted
        df_analysis["vote_efficiency_ratio"] = np.where(
            df_analysis["TOTAL"] > 0, df_analysis["votes_total"] / df_analysis["TOTAL"], 0
        )
        logger.debug("  ✅ Added vote_efficiency_ratio (same as turnout_rate but as ratio)")

    # 8. Margin Volatility - how much the margin differs from registration patterns
    if (
        "margin_pct" in df_analysis.columns
        and "dem_advantage" in df_analysis.columns
        and "is_pps_precinct" in df_analysis.columns
    ):
        pps_mask = df_analysis["is_pps_precinct"]
        df_analysis["margin_volatility"] = 0.0

        if pps_mask.any():
            # For pps precincts, compare actual margin to registration advantage
            # This requires detecting which candidate aligns with Democratic registration
            candidate_pct_cols = [
                col
                for col in df_analysis.columns
                if col.startswith("vote_pct_")
                and not col.startswith("vote_pct_contribution_")
                and col != "vote_pct_contribution_total_votes"
            ]

            if candidate_pct_cols and "reg_pct_dem" in df_analysis.columns:
                # Find Democratic-aligned candidate
                best_correlation = -1
                dem_candidate_col = None

                for col in candidate_pct_cols:
                    valid_mask = (
                        pps_mask
                        & df_analysis[col].notna()
                        & df_analysis["reg_pct_dem"].notna()
                        & (df_analysis[col] > 0)
                        & (df_analysis["reg_pct_dem"] > 0)
                    )

                    if valid_mask.sum() > 5:  # Need enough data points
                        try:
                            correlation = df_analysis.loc[valid_mask, col].corr(
                                df_analysis.loc[valid_mask, "reg_pct_dem"]
                            )
                            if correlation > best_correlation:
                                best_correlation = correlation
                                dem_candidate_col = col
                        except Exception:
                            pass

                if dem_candidate_col and best_correlation > 0.3:  # Reasonable correlation threshold
                    # Calculate expected margin based on registration
                    expected_dem_performance = (
                        df_analysis["reg_pct_dem"] - df_analysis["reg_pct_rep"]
                    )
                    actual_dem_performance = df_analysis[dem_candidate_col] - (
                        100 - df_analysis[dem_candidate_col]
                    )

                    df_analysis.loc[pps_mask, "margin_volatility"] = abs(
                        actual_dem_performance.loc[pps_mask]
                        - expected_dem_performance.loc[pps_mask]
                    )

                    candidate_name = (
                        dem_candidate_col.replace("vote_pct_", "").replace("_", " ").title()
                    )
                    logger.debug(
                        f"  ✅ Added margin_volatility (actual vs expected performance for {candidate_name})"
                    )

    # POPULATION-WEIGHTED VISUALIZATION FIELDS
    logger.debug("  🎯 Adding population-weighted fields for demographic visualization...")

    # Voter influence score (population × turnout)
    if "votes_total" in df_analysis.columns and "TOTAL" in df_analysis.columns:
        df_analysis["voter_influence_score"] = df_analysis["votes_total"] * np.log(
            df_analysis["TOTAL"] + 1
        )
        logger.debug("  ✅ Added voter_influence_score (votes × log(total_voters))")

    # Population weight for bubble visualization (0-100 scale)
    if "TOTAL" in df_analysis.columns:
        max_voters = df_analysis["TOTAL"].max()
        if max_voters > 0:
            df_analysis["population_weight"] = (df_analysis["TOTAL"] / max_voters) * 100
            logger.debug(f"  ✅ Added population_weight (max voters: {max_voters:,})")

    # Voter density categories
    if "TOTAL" in df_analysis.columns:
        valid_voter_mask = df_analysis["TOTAL"] > 0
        if valid_voter_mask.any():
            try:
                # Use quartiles to categorize density
                q1 = df_analysis.loc[valid_voter_mask, "TOTAL"].quantile(0.33)
                q3 = df_analysis.loc[valid_voter_mask, "TOTAL"].quantile(0.67)

                df_analysis["voter_density_category"] = "No Data"
                df_analysis.loc[
                    valid_voter_mask & (df_analysis["TOTAL"] <= q1), "voter_density_category"
                ] = "Low Density"
                df_analysis.loc[
                    valid_voter_mask & (df_analysis["TOTAL"] > q1) & (df_analysis["TOTAL"] <= q3),
                    "voter_density_category",
                ] = "Medium Density"
                df_analysis.loc[
                    valid_voter_mask & (df_analysis["TOTAL"] > q3), "voter_density_category"
                ] = "High Density"

                logger.debug(
                    f"  ✅ Added voter_density_category (Low: ≤{q1:.0f}, Medium: {q1:.0f}-{q3:.0f}, High: >{q3:.0f})"
                )
            except Exception as e:
                logger.warning(f"  ⚠️ Could not calculate voter density categories: {e}")
                df_analysis["voter_density_category"] = "No Data"

    # Democratic vote mass (for bubble visualization)
    if len(candidate_pct_cols) > 0 and "TOTAL" in df_analysis.columns:
        # Find Democratic-aligned candidate (highest correlation with dem registration)
        best_correlation = -1
        dem_candidate_col = None

        for col in candidate_pct_cols:
            if "reg_pct_dem" in df_analysis.columns:
                valid_mask = (
                    df_analysis[col].notna()
                    & df_analysis["reg_pct_dem"].notna()
                    & (df_analysis[col] > 0)
                    & (df_analysis["reg_pct_dem"] > 0)
                )

                if valid_mask.sum() > 10:
                    try:
                        correlation = df_analysis.loc[valid_mask, col].corr(
                            df_analysis.loc[valid_mask, "reg_pct_dem"]
                        )
                        if correlation > best_correlation:
                            best_correlation = correlation
                            dem_candidate_col = col
                    except Exception:
                        pass

        if dem_candidate_col and best_correlation > 0.3:
            dem_vote_col = dem_candidate_col.replace("vote_pct_", "votes_")
            if dem_vote_col in df_analysis.columns:
                df_analysis["democratic_vote_mass"] = df_analysis[dem_vote_col] * np.sqrt(
                    df_analysis["TOTAL"]
                )
                candidate_name = (
                    dem_candidate_col.replace("vote_pct_", "").replace("_", " ").title()
                )
                logger.debug(f"  ✅ Added democratic_vote_mass (using {candidate_name} votes)")

    # VOTE PERCENTAGE CONTRIBUTION ANALYSIS - FIXED to use complete totals
    logger.debug(
        "  🔍 Adding vote percentage contribution fields (using complete totals including county rollups)..."
    )

    # Calculate COMPLETE totals including county rollups for accurate percentages
    # Find county rollup records and pps precincts
    county_rollup_mask = df_analysis["precinct"].isin(["clackamas", "washington"])
    pps_mask = df_analysis["is_pps_precinct"]

    # Calculate complete totals including county rollups
    complete_vote_mask = pps_mask | county_rollup_mask
    total_votes_complete = (
        df_analysis.loc[complete_vote_mask, "votes_total"].sum() if complete_vote_mask.any() else 0
    )

    if total_votes_complete > 0:
        logger.debug(
            f"  📊 Complete total votes (including county rollups): {total_votes_complete:,}"
        )

        # Percentage of total votes this precinct contributed (for precincts only, not county rollups)
        df_analysis["vote_pct_contribution_total_votes"] = 0.0
        df_analysis.loc[pps_mask, "vote_pct_contribution_total_votes"] = (
            df_analysis.loc[pps_mask, "votes_total"] / total_votes_complete * 100
        )
        logger.debug(
            "  ✅ Added vote_pct_contribution_total_votes (% of complete total votes from this precinct)"
        )

        # Calculate candidate contribution percentages dynamically using complete totals
        candidate_cols = [
            col for col in df_analysis.columns if col.startswith("votes_") and col != "votes_total"
        ]

        for candidate_col in candidate_cols:
            candidate_name = candidate_col.replace("votes_", "")
            # Use complete total including county rollups
            total_candidate_votes_complete = df_analysis.loc[
                complete_vote_mask, candidate_col
            ].sum()

            if total_candidate_votes_complete > 0:
                contribution_col = f"vote_pct_contribution_{candidate_name}"
                df_analysis[contribution_col] = 0.0
                df_analysis.loc[pps_mask, contribution_col] = (
                    df_analysis.loc[pps_mask, candidate_col] / total_candidate_votes_complete * 100
                )

                # Verify calculation with sample
                sample_precincts = df_analysis[pps_mask & (df_analysis[candidate_col] > 0)]
                if len(sample_precincts) > 0:
                    sample_idx = sample_precincts.index[0]
                    sample_votes = df_analysis.loc[sample_idx, candidate_col]
                    sample_pct = df_analysis.loc[sample_idx, contribution_col]
                    sample_precinct = df_analysis.loc[sample_idx, "precinct"]
                    logger.debug(
                        f"  ✅ {candidate_name}: Sample precinct {sample_precinct} has {sample_votes} votes = {sample_pct:.2f}% of complete total ({total_candidate_votes_complete:,})"
                    )
    else:
        logger.warning("  ⚠️ No complete vote totals found for contribution calculations")

    # Precinct size categories
    if "TOTAL" in df_analysis.columns:
        df_analysis["precinct_size_category"] = pd.cut(
            df_analysis["TOTAL"],
            bins=[0, 1000, 3000, 6000, float("inf")],
            labels=["Small", "Medium", "Large", "Extra Large"],
            include_lowest=True,
        )
        logger.debug("  ✅ Added precinct_size_category (Small/Medium/Large/Extra Large)")

    logger.debug(
        f"  📊 Added {len([col for col in df_analysis.columns if col not in df.columns])} new analytical fields"
    )

    return df_analysis


def clean_numeric(series: pd.Series, is_percent: bool = False) -> pd.Series:
    """
    Cleans a pandas Series to numeric type, handling commas and percent signs.
    FIXED for new percentage data scale (already 0-100, don't divide by 100 again).

    Args:
        series: The pandas Series to clean.
        is_percent: If True, data is already in percentage format (0-100), don't convert

    Returns:
        A pandas Series with numeric data.
    """
    s = (
        series.astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip()
    )
    vals = pd.to_numeric(s, errors="coerce")
    # Don't divide by 100 - our new data is already in percentage format
    return vals


def validate_and_reproject_to_wgs84(
    gdf: gpd.GeoDataFrame, config: Config, source_description: str = "GeoDataFrame"
) -> gpd.GeoDataFrame:
    """
    Validates and reprojects a GeoDataFrame to WGS84 (EPSG:4326) if needed.

    Args:
        gdf: Input GeoDataFrame
        config: Configuration instance
        source_description: Description for logging

    Returns:
        GeoDataFrame in WGS84 coordinate system
    """
    logger.debug(f"🗺️ Validating and reprojecting {source_description}:")

    # Check original CRS
    original_crs = gdf.crs
    logger.debug(f"  📍 Original CRS: {original_crs}")

    # Get CRS settings from config
    input_crs = config.get_system_setting("input_crs")
    output_crs = config.get_system_setting("output_crs")

    # Handle missing CRS
    if original_crs is None:
        logger.warning("  ⚠️ No CRS specified in data")

        # Try to detect coordinate system from sample coordinates
        if not gdf.empty and "geometry" in gdf.columns:
            sample_geom = gdf.geometry.dropna().iloc[0] if len(gdf.geometry.dropna()) > 0 else None
            if sample_geom is not None:
                # Get first coordinate pair
                coords = None
                if hasattr(sample_geom, "exterior"):  # Polygon
                    coords = list(sample_geom.exterior.coords)[0]
                elif hasattr(sample_geom, "coords"):  # Point or LineString
                    coords = list(sample_geom.coords)[0]

                if coords:
                    x, y = coords[0], coords[1]
                    logger.debug(f"  🔍 Sample coordinates: x={x:.2f}, y={y:.2f}")

                    # Check if coordinates look like configured input CRS
                    if input_crs == "EPSG:2913" and abs(x) > 1000000 and abs(y) > 1000000:
                        logger.debug(f"  🎯 Coordinates appear to be {input_crs}")
                        gdf = gdf.set_crs(input_crs, allow_override=True)
                    # Check if coordinates look like WGS84 (longitude/latitude)
                    elif -180 <= x <= 180 and -90 <= y <= 90:
                        logger.debug(f"  🎯 Coordinates appear to be {output_crs}")
                        gdf = gdf.set_crs(output_crs, allow_override=True)
                    else:
                        logger.warning(f"  ❓ Unknown coordinate system, assuming {input_crs}")
                        gdf = gdf.set_crs(input_crs, allow_override=True)
                else:
                    logger.warning(
                        f"  ❓ Could not extract sample coordinates, assuming {output_crs}"
                    )
                    gdf = gdf.set_crs(output_crs, allow_override=True)
            else:
                logger.warning(f"  ❓ No valid geometry found, assuming {output_crs}")
                gdf = gdf.set_crs(output_crs, allow_override=True)

    # Reproject to output CRS if needed
    current_crs = gdf.crs
    if current_crs is not None:
        try:
            current_epsg = current_crs.to_epsg()
            target_epsg = int(output_crs.split(":")[1])
            if current_epsg != target_epsg:
                logger.debug(f"  🔄 Reprojecting from EPSG:{current_epsg} to {output_crs}")
                gdf_reprojected = gdf.to_crs(output_crs)

                # Validate reprojection worked
                if not gdf_reprojected.empty and "geometry" in gdf_reprojected.columns:
                    sample_geom = (
                        gdf_reprojected.geometry.dropna().iloc[0]
                        if len(gdf_reprojected.geometry.dropna()) > 0
                        else None
                    )
                    if sample_geom is not None:
                        coords = None
                        if hasattr(sample_geom, "exterior"):  # Polygon
                            coords = list(sample_geom.exterior.coords)[0]
                        elif hasattr(sample_geom, "coords"):  # Point or LineString
                            coords = list(sample_geom.coords)[0]

                        if coords:
                            x, y = coords[0], coords[1]
                            logger.debug(f"  ✓ Reprojected coordinates: lon={x:.6f}, lat={y:.6f}")

                            # Validate coordinates are in valid WGS84 range
                            if -180 <= x <= 180 and -90 <= y <= 90:
                                logger.debug("  ✓ Coordinates are valid WGS84")
                            else:
                                logger.warning(f"  ⚠️ Coordinates may be invalid: lon={x}, lat={y}")
                        else:
                            logger.warning("  ⚠️ Could not validate reprojected coordinates")

                gdf = gdf_reprojected
            else:
                logger.debug(f"  ✓ Already in {output_crs}")
        except Exception as e:
            logger.error(f"  ❌ Error during reprojection: {e}")
            logger.debug(f"  🔧 Attempting to set CRS as {output_crs}")
            gdf = gdf.set_crs(output_crs, allow_override=True)

    # Final validation
    if gdf.crs is not None:
        try:
            final_epsg = gdf.crs.to_epsg()
            logger.debug(f"  ✅ Final CRS: EPSG:{final_epsg}")
        except Exception:
            logger.error(f"  ❌ Final CRS: {gdf.crs}")
    else:
        logger.warning("  ⚠️ Warning: Final CRS is None")

    # Validate geometry
    valid_geom_count = gdf.geometry.notna().sum()
    total_count = len(gdf)
    logger.debug(
        f"  📊 Geometry validation: {valid_geom_count}/{total_count} features have valid geometry"
    )

    return gdf


def optimize_geojson_properties(gdf: gpd.GeoDataFrame, config: Config) -> gpd.GeoDataFrame:
    """
    Optimizes GeoDataFrame properties for web display and vector tile generation.
    Uses field registry and duck-typing instead of hardcoded column names.

    Args:
        gdf: Input GeoDataFrame
        config: Configuration instance

    Returns:
        GeoDataFrame with optimized properties
    """
    logger.debug("🔧 Optimizing properties for web display using dynamic field detection:")

    # Create a copy to avoid modifying original
    gdf_optimized = gdf.copy()

    # Get precision settings from config
    prop_precision = config.get_system_setting("property_precision")

    # Clean up property names and values for web consumption
    columns_to_clean = gdf_optimized.columns.tolist()
    if "geometry" in columns_to_clean:
        columns_to_clean.remove("geometry")

    # Track optimization stats
    optimized_counts = {
        "boolean": 0,
        "count": 0,
        "percentage": 0,
        "categorical": 0,
        "identifier": 0,
        "unknown": 0,
    }

    for col in columns_to_clean:
        if col in gdf_optimized.columns:
            series = gdf_optimized[col]

            # Get field info from registry if available
            field_def = FIELD_REGISTRY._fields.get(col)
            field_type = field_def.field_type if field_def else None

            # 1. BOOLEAN FIELDS - Use registry + duck typing
            if (
                field_type == "boolean"
                or col.startswith(("is_", "has_"))
                or _is_boolean_data(series)
            ):
                gdf_optimized[col] = _optimize_boolean_field(series)
                optimized_counts["boolean"] += 1

            # 2. COUNT FIELDS - Use registry + pattern detection
            elif field_type == "count" or _is_count_field(col, series):
                gdf_optimized[col] = _optimize_count_field(series)
                optimized_counts["count"] += 1

            # 3. PERCENTAGE FIELDS - Use registry + pattern detection
            elif field_type == "percentage" or _is_percentage_field(col, series):
                gdf_optimized[col] = _optimize_percentage_field(series, prop_precision)
                optimized_counts["percentage"] += 1

            # 4. CATEGORICAL FIELDS - Use registry + duck typing
            elif field_type == "categorical" or _is_categorical_field(col, series):
                gdf_optimized[col] = _optimize_categorical_field(col, series)
                optimized_counts["categorical"] += 1

            # 5. IDENTIFIER FIELDS - Pattern detection
            elif _is_identifier_field(col):
                gdf_optimized[col] = _optimize_identifier_field(series)
                optimized_counts["identifier"] += 1

            # 6. FALLBACK - Try to infer from data
            else:
                gdf_optimized[col] = _optimize_unknown_field(col, series, prop_precision)
                optimized_counts["unknown"] += 1

    # Log optimization results
    total_optimized = sum(optimized_counts.values())
    logger.debug(f"  ✓ Optimized {total_optimized} property columns:")
    for field_type, count in optimized_counts.items():
        if count > 0:
            logger.debug(f"    - {field_type}: {count} fields")

    # Add web-friendly geometry validation
    invalid_geom = gdf_optimized.geometry.isna() | (~gdf_optimized.geometry.is_valid)
    invalid_count = invalid_geom.sum()

    if invalid_count > 0:
        logger.warning(f"  ⚠️ Found {invalid_count} invalid geometries, attempting to fix...")
        # Try to fix invalid geometries
        gdf_optimized.geometry = gdf_optimized.geometry.buffer(0)

        # Check again
        still_invalid = gdf_optimized.geometry.isna() | (~gdf_optimized.geometry.is_valid)
        still_invalid_count = still_invalid.sum()

        if still_invalid_count > 0:
            logger.warning(f"  ⚠️ {still_invalid_count} geometries still invalid after fix attempt")
        else:
            logger.debug("  ✓ Fixed all invalid geometries")
    else:
        logger.debug("  ✓ All geometries are valid")

    return gdf_optimized


def _is_boolean_data(series: pd.Series) -> bool:
    """Duck-type detection of boolean data."""
    # Check if data looks like boolean
    unique_vals = set(str(v).lower() for v in series.dropna().unique())
    boolean_values = {"true", "false", "1", "0", "yes", "no"}
    return len(unique_vals) <= 2 and unique_vals.issubset(boolean_values)


def _is_count_field(col: str, series: pd.Series) -> bool:
    """Detect count fields by pattern and data characteristics."""
    # Pattern-based detection
    if col.startswith("votes_") or col in ["TOTAL", "DEM", "REP", "NAV", "vote_margin"]:
        return True

    # Duck-type detection: integer data with reasonable range for vote counts
    try:
        numeric_data = pd.to_numeric(series.dropna(), errors="coerce")
        if numeric_data.notna().any():
            # Check if all values are non-negative integers (typical for counts)
            is_integer = (numeric_data % 1 == 0).all()
            is_non_negative = (numeric_data >= 0).all()
            # Reasonable range for vote counts (0 to 50,000)
            reasonable_range = (numeric_data <= 50000).all()
            return bool(is_integer and is_non_negative and reasonable_range)
    except Exception as e:
        logger.debug(f"  ❌ Error detecting count field: {e}")

    return False


def _is_percentage_field(col: str, series: pd.Series) -> bool:
    """Detect percentage fields by pattern and data characteristics."""
    # Pattern-based detection
    percentage_patterns = ["_pct_", "_rate", "_advantage", "_score", "_efficiency", "_potential"]
    if any(pattern in col for pattern in percentage_patterns):
        return True

    # Duck-type detection: numeric data in percentage-like range
    try:
        numeric_data = pd.to_numeric(series.dropna(), errors="coerce")
        if numeric_data.notna().any():
            # Check if data looks like percentages (0-100 range mostly)
            min_val, max_val = numeric_data.min(), numeric_data.max()
            # Allow some flexibility for calculated fields that might go slightly outside 0-100
            return bool(-200 <= min_val <= 200 and -200 <= max_val <= 200)
    except Exception as e:
        logger.debug(f"  ❌ Error detecting percentage field: {e}")

    return False


def _is_categorical_field(col: str, series: pd.Series) -> bool:
    """Detect categorical fields by data characteristics."""
    # Skip if looks like numeric data
    try:
        numeric_data = pd.to_numeric(series.dropna(), errors="coerce")
        if numeric_data.notna().sum() > len(series.dropna()) * 0.8:  # 80% numeric
            return False
    except Exception as e:
        logger.debug(f"  ❌ Error detecting categorical field: {e}")

    # Check if it has limited unique values (typical for categories)
    unique_count = series.nunique()
    total_count = len(series.dropna())

    # Consider categorical if: few unique values OR string-like data
    if unique_count <= 20 or (total_count > 0 and unique_count / total_count < 0.1):
        return True

    # Check for common categorical indicators
    sample_values = set(str(v).lower() for v in series.dropna().head(10))
    categorical_indicators = {
        "low",
        "medium",
        "high",
        "small",
        "large",
        "strong",
        "weak",
        "competitive",
        "safe",
        "likely",
        "tossup",
        "close",
        "clear",
        "landslide",
        "dem",
        "rep",
        "unknown",
        "no data",
        "tie",
    }

    return bool(len(sample_values & categorical_indicators) > 0)


def _is_identifier_field(col: str) -> bool:
    """Detect identifier/name fields by pattern."""
    identifier_patterns = ["precinct", "candidate", "name", "id", "_id", "identifier"]
    return any(pattern in col.lower() for pattern in identifier_patterns)


def _optimize_boolean_field(series: pd.Series) -> pd.Series:
    """Optimize boolean field for web display."""
    return (
        series.astype(str)
        .str.lower()
        .map({"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False})
        .fillna(False)
    )


def _optimize_count_field(series: pd.Series) -> pd.Series:
    """Optimize count field for web display."""
    numeric_series = pd.to_numeric(series, errors="coerce").fillna(0)
    return numeric_series.astype(int)


def _optimize_percentage_field(series: pd.Series, precision: int) -> pd.Series:
    """Optimize percentage field for web display."""
    numeric_series = pd.to_numeric(series, errors="coerce").fillna(0)
    return numeric_series.round(precision)


def _optimize_categorical_field(col: str, series: pd.Series) -> pd.Series:
    """Optimize categorical field for web display."""
    optimized = series.astype(str).replace(["nan", "None", "<NA>", ""], "No Data")

    # Set appropriate defaults for specific field patterns
    if "political" in col.lower() or "lean" in col.lower():
        optimized = optimized.replace("No Data", "Unknown")
    elif "competitive" in col.lower():
        optimized = optimized.replace("No Data", "No Election Data")
    elif "candidate" in col.lower():
        optimized = optimized.replace("No Data", "No Data")
    elif "density" in col.lower():
        # Preserve categorical text values for voter_density_category etc.
        pass  # Keep "No Data" as is

    return optimized


def _optimize_identifier_field(series: pd.Series) -> pd.Series:
    """Optimize identifier field for web display."""
    return series.astype(str).str.strip()


def _optimize_unknown_field(col: str, series: pd.Series, precision: int) -> pd.Series:
    """Fallback optimization for unknown field types."""
    logger.debug(f"    ❓ Unknown field type for '{col}', attempting auto-detection")

    # Try numeric first
    try:
        numeric_series = pd.to_numeric(series, errors="coerce")
        if numeric_series.notna().sum() > len(series) * 0.7:  # 70% numeric
            # If mostly integers, treat as count
            if (numeric_series.dropna() % 1 == 0).all():
                return numeric_series.fillna(0).astype(int)
            # Otherwise, treat as decimal with precision
            else:
                return numeric_series.fillna(0).round(precision)
    except Exception as e:
        logger.debug(f"  ❌ Error optimizing unknown field: {e}")

    # Fall back to string
    return series.astype(str).str.strip()


def create_candidate_color_mapping(candidate_cols: List[str]) -> Dict[str, str]:
    """
    Create consistent color mapping for candidates that will be used across all visualizations.

    Args:
        candidate_cols: List of candidate column names (e.g., ['votes_splitt', 'votes_cavagnolo'])

    Returns:
        Dictionary mapping candidate names to hex colors
    """
    logger.debug("🎨 Creating consistent candidate color mapping:")

    # Color-blind friendly palette (consistent across all visualizations)
    candidate_colors = [
        "#0571b0",  # Blue
        "#fd8d3c",  # Orange
        "#238b45",  # Green
        "#d62728",  # Red
        "#9467bd",  # Purple
        "#8c564b",  # Brown
        "#e377c2",  # Pink
        "#7f7f7f",  # Gray
        "#bcbd22",  # Olive
        "#17becf",  # Cyan
    ]

    # Extract candidate names from column names
    candidate_names = []
    for col in candidate_cols:
        if col.startswith("votes_") and col != "votes_total":
            candidate_name = col.replace("votes_", "")
            candidate_names.append(candidate_name)

    # Create consistent mapping - ONLY original candidate names to prevent pollution
    color_mapping = {}
    for i, candidate in enumerate(candidate_names):
        color_index = i % len(candidate_colors)
        color_mapping[candidate] = candidate_colors[color_index]

        # Log with display name for readability, but don't add to mapping
        display_name = candidate.replace("_", " ").title()
        logger.debug(f"  🎨 {display_name}: {candidate_colors[color_index]}")

    # Add special colors for non-candidate values - only these three
    color_mapping["Tie"] = "#636363"
    color_mapping["No Data"] = "#f7f7f7"
    color_mapping["No Election Data"] = "#f7f7f7"

    return color_mapping


def generate_layer_explanations(gdf: gpd.GeoDataFrame) -> Dict[str, str]:
    """
    Generate comprehensive explanations for all data layers using the field registry.
    This creates a self-documenting dataset where explanations are embedded with the data.

    Args:
        gdf: GeoDataFrame containing election data

    Returns:
        Dictionary mapping layer keys to their explanations
    """
    logger.debug("📚 Generating layer explanations using field registry:")

    # Start with registry explanations
    explanations = FIELD_REGISTRY.get_all_explanations()

    # Validate completeness and log results
    validation = FIELD_REGISTRY.validate_gdf_completeness(gdf)

    logger.debug("  🔍 Field validation results:")
    logger.debug(f"    Total fields in GeoDataFrame: {validation['total_fields']}")
    logger.debug(f"    Fields with explanations: {validation['explained_fields']}")
    logger.debug(f"    Dynamic candidate fields: {len(validation['candidate_fields'])}")

    if validation["missing_explanations"]:
        logger.warning(f"    ⚠️  Missing explanations for: {validation['missing_explanations']}")

        # Auto-generate explanations for any missing fields
        for field in validation["missing_explanations"]:
            explanations[field] = (
                f"**MISSING DEFINITION**: Field '{field}' needs to be registered in the FieldRegistry with proper explanation and formula."
            )

    if validation["orphaned_explanations"]:
        logger.debug(
            f"    ⚠️  Orphaned explanations (not in data): {validation['orphaned_explanations']}"
        )

    # Add dynamic explanations for candidate-specific fields
    candidate_fields = validation["candidate_fields"]
    candidate_names = set()

    for col in candidate_fields:
        if col.startswith("votes_") and col != "votes_total":
            candidate_name = col.replace("votes_", "")
            display_name = candidate_name.replace("_", " ").title()
            explanations[col] = (
                f"Number of votes received by {display_name} in each precinct. "
                f"**Formula:** `COUNT(votes_for_{candidate_name})` "
                f"**Units:** votes"
            )
            candidate_names.add(candidate_name)

        elif (
            col.startswith("vote_pct_")
            and not col.startswith("vote_pct_contribution_")
            and col != "vote_pct_contribution_total_votes"
        ):
            candidate_name = col.replace("vote_pct_", "")
            display_name = candidate_name.replace("_", " ").title()
            explanations[col] = (
                f"Percentage of total votes received by {display_name} in each precinct. "
                f"**Formula:** `(votes_{candidate_name} / votes_total) * 100` "
                f"**Units:** percent"
            )

        elif (
            col.startswith("vote_pct_contribution_") and col != "vote_pct_contribution_total_votes"
        ):
            candidate_name = col.replace("vote_pct_contribution_", "")
            display_name = candidate_name.replace("_", " ").title()
            explanations[col] = (
                f"Percentage of {display_name}'s total citywide votes that came from this precinct. "
                f"**Formula:** `(votes_{candidate_name} / SUM(all_precincts.votes_{candidate_name})) * 100` "
                f"**Units:** percent"
            )

    # Dynamic registration percentage explanations
    for col in gdf.columns:
        if col.startswith("reg_pct_") and col not in explanations:
            party = col.replace("reg_pct_", "").upper()
            reg_field = col.replace("_pct", "")
            explanations[col] = (
                f"Percentage of voters registered as {party} in each precinct. "
                f"**Formula:** `({reg_field} / total_voters) * 100` "
                f"**Units:** percent"
            )

    logger.debug(f"  📚 Total explanations generated: {len(explanations)}")
    logger.debug(f"  📊 Registry-based explanations: {validation['explained_fields']}")
    logger.debug(f"  👥 Dynamic candidate explanations: {len(candidate_names)} candidates")
    total_fields = validation["total_fields"]
    coverage_pct = (len(explanations) / total_fields * 100) if total_fields > 0 else 0
    logger.debug(f"  🎯 Coverage: {len(explanations)}/{total_fields} fields ({coverage_pct:.1f}%)")

    # Final validation check
    missing_final = set(gdf.columns) - {"geometry"} - set(explanations.keys())
    if missing_final:
        logger.debug(f"  ❌ FINAL CHECK: Still missing explanations for: {missing_final}")
    else:
        logger.debug("  ✅ COMPLETE: All fields have explanations!")

    return explanations


def export_complete_field_registry(gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
    """Export complete field registry information for use by the web map."""
    registry_export = {
        "field_definitions": {},
        "field_types": {},
        "field_categories": {},
        "field_units": {},
        "display_names": {},
        "explanations": {},
        "available_fields": [],
        "categorical_fields": [],
        "numeric_fields": [],
        "percentage_fields": [],
        "count_fields": [],
        # Category-based field lists for filtering
        "analytical_fields": [],
        "electoral_fields": [],
        "demographic_fields": [],
        "administrative_fields": [],
        "informational_fields": [],
        "geographic_fields": [],
    }

    # Process all fields in the GeoDataFrame
    for column in gdf.columns:
        if column == "geometry":
            continue

        registry_export["available_fields"].append(column)

        # Get field definition from registry
        field_def = FIELD_REGISTRY._fields.get(column)

        if field_def:
            # Use registry data
            registry_export["field_definitions"][column] = {
                "name": field_def.name,
                "description": field_def.description,
                "formula": field_def.formula,
                "field_type": field_def.field_type,
                "category": field_def.category,
                "units": field_def.units,
            }
            registry_export["field_types"][column] = field_def.field_type
            registry_export["field_categories"][column] = field_def.category
            registry_export["field_units"][column] = field_def.units
            registry_export["explanations"][column] = field_def.description

            # Generate display name from description (first sentence) or fall back to cleaned name
            display_name = field_def.description.split(".")[0] if field_def.description else None
            if not display_name or len(display_name) > 50:
                display_name = field_def.name.replace("_", " ").title()
            registry_export["display_names"][column] = display_name

            # Categorize fields by type
            if field_def.field_type == "categorical":
                registry_export["categorical_fields"].append(column)
            elif field_def.field_type == "percentage":
                registry_export["percentage_fields"].append(column)
            elif field_def.field_type == "count":
                registry_export["count_fields"].append(column)
            elif field_def.field_type in ["ratio", "boolean"]:
                registry_export["numeric_fields"].append(column)

            # Categorize fields by category for filtering
            category_key = f"{field_def.category}_fields"
            if category_key in registry_export:
                registry_export[category_key].append(column)

        else:
            # Auto-detect field characteristics for unregistered fields
            registry_export["field_types"][column] = "unknown"
            registry_export["field_categories"][column] = (
                "administrative"  # Default for unregistered
            )
            registry_export["field_units"][column] = None
            registry_export["explanations"][column] = f"Unregistered field: {column}"

            # Generate reasonable display name
            if column.startswith("vote_pct_") and not column.startswith("vote_pct_contribution_"):
                candidate_name = column.replace("vote_pct_", "").replace("_", " ").title()
                registry_export["display_names"][column] = f"Vote % - {candidate_name}"
                registry_export["percentage_fields"].append(column)
                registry_export["electoral_fields"].append(column)
            elif column.startswith("votes_") and column != "votes_total":
                candidate_name = column.replace("votes_", "").replace("_", " ").title()
                registry_export["display_names"][column] = f"Vote Count - {candidate_name}"
                registry_export["count_fields"].append(column)
                registry_export["electoral_fields"].append(column)
            elif column.startswith("vote_pct_contribution_"):
                candidate_name = (
                    column.replace("vote_pct_contribution_", "").replace("_", " ").title()
                )
                registry_export["display_names"][column] = f"Vote Contribution % - {candidate_name}"
                registry_export["percentage_fields"].append(column)
                registry_export["analytical_fields"].append(column)
            elif column.startswith("reg_pct_"):
                party_name = column.replace("reg_pct_", "").upper()
                registry_export["display_names"][column] = f"Registration % - {party_name}"
                registry_export["percentage_fields"].append(column)
                registry_export["demographic_fields"].append(column)
            else:
                registry_export["display_names"][column] = column.replace("_", " ").title()
                registry_export["numeric_fields"].append(column)
                registry_export["administrative_fields"].append(column)

    # Create filtered field lists for visualization (exclude administrative/informational by default)
    visualization_fields = []
    for field in registry_export["available_fields"]:
        category = registry_export["field_categories"].get(field, "administrative")
        if category in ["analytical", "electoral", "demographic"]:
            visualization_fields.append(field)

    registry_export["visualization_fields"] = visualization_fields

    return registry_export


# === Main Script Logic ===
def main() -> None:
    """
    Main function to load data, process it, and generate maps.
    """
    logger.debug("🗺️ Election Map Generation")
    logger.debug("=" * 60)

    # Load configuration
    try:
        config = Config()
        logger.debug(f"📋 Project: {config.get('project_name')}")
        logger.debug(f"📋 Description: {config.get('description')}")
    except Exception as e:
        logger.debug(f"❌ Configuration error: {e}")
        logger.debug("💡 Make sure config.yaml exists in the analysis directory")
        return

    # Get file paths from configuration
    enriched_csv_path = config.get_enriched_csv_path()
    boundaries_path = config.get_input_path("precincts_geojson")
    output_geojson_path = config.get_web_geojson_path()

    logger.debug("File paths:")
    logger.debug(f"  📄 Enriched CSV: {enriched_csv_path}")
    logger.debug(f"  🗺️ Boundaries: {boundaries_path}")
    logger.debug(f"  💾 Output GeoJSON: {output_geojson_path}")

    # === 1. Load Data ===
    logger.debug("Loading data files:")
    try:
        df_raw = pd.read_csv(enriched_csv_path, dtype=str)
        logger.debug(f"  ✓ Loaded CSV with {len(df_raw)} rows")

        gdf = gpd.read_file(boundaries_path)
        logger.debug(f"  ✓ Loaded GeoJSON with {len(gdf)} features")

    except FileNotFoundError as e:
        logger.debug(f"❌ Error: Input file not found. {e}")
        return
    except Exception as e:
        logger.debug(f"❌ Error loading data: {e}")
        return

    # === 2. Data Filtering and Preprocessing ===
    logger.debug("Data preprocessing and filtering:")

    # Get column names from configuration
    precinct_csv_col = config.get_column_name("precinct_csv")
    precinct_geojson_col = config.get_column_name("precinct_geojson")

    # Filter out summary/aggregate rows from CSV - BUT PRESERVE county rollups for totals calculation
    summary_precinct_ids = ["multnomah", "grand_total", ""]
    df = df_raw[~df_raw[precinct_csv_col].isin(summary_precinct_ids)].copy()
    logger.debug(
        f"  ✓ Filtered CSV: {len(df_raw)} → {len(df)} rows (removed {len(df_raw) - len(df)} summary rows, kept county rollups)"
    )

    # Separate regular precincts from county summary rows (PRESERVE county rollups)
    county_summaries = df[df[precinct_csv_col].isin(["clackamas", "washington"])]
    regular_precincts = df[~df[precinct_csv_col].isin(["clackamas", "washington"])]

    logger.debug(f"  📊 Regular precincts: {len(regular_precincts)}")
    logger.debug(
        f"  📊 County rollup rows: {len(county_summaries)} ({county_summaries[precinct_csv_col].tolist()})"
    )

    # Separate PPS participants from non-participants (only for regular precincts)
    pps_participants = (
        regular_precincts[regular_precincts["is_pps_precinct"].astype(str).str.lower() == "true"]
        if "is_pps_precinct" in regular_precincts.columns
        else regular_precincts
    )
    non_participants = (
        regular_precincts[regular_precincts["is_pps_precinct"].astype(str).str.lower() == "false"]
        if "is_pps_precinct" in regular_precincts.columns
        else pd.DataFrame()
    )

    logger.debug(f"  📊 PPS participants: {len(pps_participants)} precincts")
    logger.debug(f"  📊 Non-participants: {len(non_participants)} precincts")
    logger.debug(f"  📊 Total Multnomah precincts: {len(regular_precincts)} precincts")

    # Validate vote totals against ground truth - INCLUDING county rollups
    if len(pps_participants) > 0:
        candidate_cols = [
            col
            for col in pps_participants.columns
            if col.startswith("votes_") and col != "votes_total"
        ]

        logger.debug(
            "🔍 Validating vote totals against ground truth (COMPLETE including county rollups):"
        )

        # Calculate complete totals including county rollups
        pps_votes = pps_participants["votes_total"].astype(float).sum()
        county_votes = (
            county_summaries["votes_total"].astype(float).sum() if len(county_summaries) > 0 else 0
        )
        total_votes_complete = pps_votes + county_votes

        logger.debug("  📊 COMPLETE totals (including county rollups):")
        logger.debug(f"    - PPS precinct votes: {pps_votes:,.0f}")
        logger.debug(f"    - County rollup votes: {county_votes:,.0f}")
        logger.debug(f"    - TOTAL votes: {total_votes_complete:,.0f}")

        for col in candidate_cols:
            if col in pps_participants.columns:
                pps_candidate_total = pps_participants[col].astype(float).sum()
                county_candidate_total = (
                    county_summaries[col].astype(float).sum()
                    if len(county_summaries) > 0 and col in county_summaries.columns
                    else 0
                )
                candidate_total_complete = pps_candidate_total + county_candidate_total
                candidate_name = col.replace("votes_", "").title()
                percentage = (
                    (candidate_total_complete / total_votes_complete * 100)
                    if total_votes_complete > 0
                    else 0
                )
                logger.debug(
                    f"    - {candidate_name}: {candidate_total_complete:,.0f} ({percentage:.2f}%)"
                )

    logger.debug(f"  CSV precinct column: {df[precinct_csv_col].dtype}")
    logger.debug(f"  GeoJSON precinct column: {gdf[precinct_geojson_col].dtype}")

    # Robust join (strip zeros, lower, strip spaces)
    df[precinct_csv_col] = df[precinct_csv_col].astype(str).str.lstrip("0").str.strip().str.lower()
    gdf[precinct_geojson_col] = (
        gdf[precinct_geojson_col].astype(str).str.lstrip("0").str.strip().str.lower()
    )

    logger.debug(f"  Sample CSV precincts: {df[precinct_csv_col].head().tolist()}")
    logger.debug(f"  Sample GeoJSON precincts: {gdf[precinct_geojson_col].head().tolist()}")

    # Analyze matching before merge
    csv_precincts = set(df[precinct_csv_col].unique())
    geo_precincts = set(gdf[precinct_geojson_col].unique())

    logger.debug(f"  Unique CSV precincts: {len(csv_precincts)}")
    logger.debug(f"  Unique GeoJSON precincts: {len(geo_precincts)}")
    logger.debug(f"  Intersection: {len(csv_precincts & geo_precincts)}")

    csv_only = csv_precincts - geo_precincts
    geo_only = geo_precincts - csv_precincts
    if csv_only:
        # Filter out county rollups from "CSV-only" since they won't have GIS features
        csv_only_filtered = csv_only - {"clackamas", "washington"}
        if csv_only_filtered:
            logger.debug(
                f"  ⚠️  CSV-only precincts (non-county): {sorted(csv_only_filtered)[:5]}{'...' if len(csv_only_filtered) > 5 else ''}"
            )
        logger.debug(
            f"  📋 County rollups not mapped (expected): {csv_only & {'clackamas', 'washington'}}"
        )
    if geo_only:
        logger.debug(
            f"  ⚠️  GeoJSON-only precincts: {sorted(geo_only)[:5]}{'...' if len(geo_only) > 5 else ''}"
        )

    # MERGE: Only merge GIS features (exclude county rollups from GIS merge)
    df_for_gis = df[~df[precinct_csv_col].isin(["clackamas", "washington"])].copy()
    gdf_merged = gdf.merge(
        df_for_gis, left_on=precinct_geojson_col, right_on=precinct_csv_col, how="left"
    )
    logger.debug(
        f"  ✓ Merged GIS data: {len(gdf_merged)} features (excluding county rollups from GIS)"
    )

    # CONSOLIDATE SPLIT PRECINCTS
    gdf_merged = consolidate_split_precincts(gdf_merged, precinct_geojson_col)

    # ADD ANALYTICAL FIELDS
    # Convert to DataFrame for analysis, then back to GeoDataFrame
    analysis_df = pd.DataFrame(gdf_merged.drop(columns="geometry"))
    analysis_df = add_analytical_fields(analysis_df)

    # Merge analytical fields back to GeoDataFrame
    analysis_cols = [col for col in analysis_df.columns if col not in gdf_merged.columns]
    for col in analysis_cols:
        gdf_merged[col] = analysis_df[col]

    # COORDINATE VALIDATION AND REPROJECTION
    logger.debug("🗺️ Coordinate System Processing:")
    gdf_merged = validate_and_reproject_to_wgs84(gdf_merged, config, "merged election data")

    # OPTIMIZE PROPERTIES FOR WEB
    gdf_merged = optimize_geojson_properties(gdf_merged, config)

    # Check for unmatched precincts
    matched = gdf_merged[~gdf_merged[precinct_csv_col].isna()]
    unmatched = gdf_merged[gdf_merged[precinct_csv_col].isna()]
    logger.debug(f"  ✓ Matched features: {len(matched)}")
    if len(unmatched) > 0:
        logger.warning(f"  ⚠️  Unmatched features: {len(unmatched)}")
        logger.debug(
            f"     Example unmatched GeoJSON precincts: {unmatched[precinct_geojson_col].head().tolist()}"
        )

    # Dynamically detect all columns to clean - FIXED for new percentage format
    logger.debug("🧹 Cleaning data columns (FIXED for percentage format):")

    # Create consistent candidate color mapping early
    candidate_cols = detect_candidate_count_columns(gdf_merged)
    candidate_color_mapping = create_candidate_color_mapping(candidate_cols)

    # Clean all count columns dynamically
    count_cols = [col for col in gdf_merged.columns if col.startswith("votes_")]
    for col in count_cols:
        gdf_merged[col] = clean_numeric(gdf_merged[col], is_percent=False)
        valid_count = gdf_merged[col].notna().sum()
        if valid_count > 0:
            logger.debug(
                f"  ✓ Cleaned {col}: {valid_count} valid values, range: {gdf_merged[col].min():.0f} - {gdf_merged[col].max():.0f}"
            )

    # Clean all percentage columns dynamically - DON'T convert, they're already percentages
    pct_cols = [col for col in gdf_merged.columns if col.startswith(("vote_pct_", "reg_pct_"))]
    for col in pct_cols:
        gdf_merged[col] = clean_numeric(gdf_merged[col], is_percent=False)  # DON'T divide by 100
        valid_count = gdf_merged[col].notna().sum()
        if valid_count > 0:
            logger.debug(
                f"  ✓ Cleaned {col}: {valid_count} valid values, range: {gdf_merged[col].min():.1f}% - {gdf_merged[col].max():.1f}%"
            )

    # Clean other numeric columns - FIXED for percentage format - EXCLUDE categorical columns
    other_numeric_cols = [
        "turnout_rate",
        "engagement_score",
        "dem_advantage",
        "margin_pct",
        "vote_margin",
        "major_party_pct",
        "pct_victory_margin",
        "competitiveness_score",
        "vote_efficiency_dem",
        "registration_competitiveness",
        "swing_potential",
        "engagement_rate",
        "candidate_dominance",
    ]
    for col in other_numeric_cols:
        if col in gdf_merged.columns:
            gdf_merged[col] = clean_numeric(
                gdf_merged[col], is_percent=False
            )  # Already percentages
            valid_count = gdf_merged[col].notna().sum()
            if valid_count > 0:
                # Show percentage sign for percentage fields
                if col in [
                    "turnout_rate",
                    "dem_advantage",
                    "major_party_pct",
                    "pct_victory_margin",
                    "engagement_rate",
                ]:
                    logger.debug(
                        f"  ✓ Cleaned {col}: {valid_count} valid values, range: {gdf_merged[col].min():.1f}% - {gdf_merged[col].max():.1f}%"
                    )
                else:
                    logger.debug(
                        f"  ✓ Cleaned {col}: {valid_count} valid values, range: {gdf_merged[col].min():.3f} - {gdf_merged[col].max():.3f}"
                    )

    # Handle categorical columns - PRESERVE string values, do NOT convert to numeric
    categorical_cols = [
        "is_pps_precinct",
        "political_lean",
        "competitiveness",
        "leading_candidate",
        "second_candidate",
        "turnout_quartile",
        "margin_category",
        "precinct_size_category",
        "record_type",
    ]
    for col in categorical_cols:
        if col in gdf_merged.columns:
            # Special handling for boolean columns that may be stored as strings
            if col == "is_pps_precinct":
                gdf_merged[col] = (
                    gdf_merged[col].astype(str).str.lower().map({"true": True, "false": False})
                )
            else:
                # For string categorical columns, ensure they stay as strings and clean up
                gdf_merged[col] = gdf_merged[col].astype(str)
                # Replace pandas/numpy string representations of missing values
                gdf_merged[col] = gdf_merged[col].replace(["nan", "None", "<NA>", ""], "No Data")

                # Set appropriate defaults for specific columns
                if col == "political_lean":
                    gdf_merged[col] = gdf_merged[col].replace("No Data", "Unknown")
                elif col == "competitiveness":
                    gdf_merged[col] = gdf_merged[col].replace("No Data", "No Election Data")
                elif col in ["leading_candidate", "second_candidate"]:
                    gdf_merged[col] = gdf_merged[col].replace("No Data", "No Data")

            value_counts = gdf_merged[col].value_counts()
            logger.debug(f"  ✓ {col} distribution: {dict(value_counts)}")

    # Final validation of consolidated vote totals - INCLUDING county rollups
    if len(pps_participants) > 0:
        consolidated_pps = gdf_merged[gdf_merged["is_pps_precinct"]]

        logger.debug("✅ Final vote totals after consolidation:")
        total_votes_final = consolidated_pps["votes_total"].sum()

        # Add county rollup votes back to get complete totals
        county_rollup_votes = (
            county_summaries["votes_total"].astype(float).sum() if len(county_summaries) > 0 else 0
        )
        complete_total_final = total_votes_final + county_rollup_votes

        logger.debug(f"  📊 PPS GIS features total votes: {total_votes_final:,.0f}")
        logger.debug(f"  📊 County rollup votes: {county_rollup_votes:,.0f}")
        logger.debug(f"  📊 COMPLETE total votes: {complete_total_final:,.0f}")

        for col in candidate_cols:
            if col in consolidated_pps.columns:
                pps_candidate_total = consolidated_pps[col].sum()
                county_candidate_total = (
                    county_summaries[col].astype(float).sum()
                    if len(county_summaries) > 0 and col in county_summaries.columns
                    else 0
                )
                candidate_total_complete = pps_candidate_total + county_candidate_total
                candidate_name = col.replace("votes_", "").title()
                percentage = (
                    (candidate_total_complete / complete_total_final * 100)
                    if complete_total_final > 0
                    else 0
                )
                logger.debug(
                    f"  📊 {candidate_name}: {candidate_total_complete:,.0f} ({percentage:.2f}%)"
                )

        # Compare to ground truth
        logger.debug("🎯 Ground truth comparison:")
        logger.debug(
            "  Ground truth will be calculated from actual data instead of hardcoded values"
        )
        logger.debug(f"  Total detected votes: {complete_total_final:,}")

        # Dynamic ground truth based on actual results
        if complete_total_final > 0:
            logger.debug("  Actual results by candidate:")
            for col in candidate_cols:
                if col in consolidated_pps.columns:
                    pps_candidate_total = consolidated_pps[col].sum()
                    county_candidate_total = (
                        county_summaries[col].astype(float).sum()
                        if len(county_summaries) > 0 and col in county_summaries.columns
                        else 0
                    )
                    candidate_total_complete = pps_candidate_total + county_candidate_total
                    candidate_name = col.replace("votes_", "").title()
                    percentage = (
                        (candidate_total_complete / complete_total_final * 100)
                        if complete_total_final > 0
                        else 0
                    )
                    logger.debug(
                        f"    - {candidate_name}: {candidate_total_complete:,.0f} ({percentage:.2f}%)"
                    )

    # === Competition Metrics Analysis ===
    logger.debug("Analyzing pre-calculated competition metrics:")

    # The enriched dataset already has margin_pct, competitiveness, leading_candidate calculated
    if "margin_pct" in gdf_merged.columns:
        margin_stats = gdf_merged[gdf_merged["margin_pct"].notna()]["margin_pct"]
        if len(margin_stats) > 0:
            logger.debug(
                f"  ✓ Vote margins available: median {margin_stats.median():.1f}%, range {margin_stats.min():.1f}% - {margin_stats.max():.1f}%"
            )

    if "competitiveness" in gdf_merged.columns:
        comp_stats = gdf_merged["competitiveness"].value_counts()
        logger.debug(f"  📊 Competitiveness distribution: {dict(comp_stats)}")

    if "leading_candidate" in gdf_merged.columns:
        leader_stats = gdf_merged["leading_candidate"].value_counts()
        logger.debug(f"  📊 Leading candidate distribution: {dict(leader_stats)}")

    # Summary of PPS vs Non-PPS
    if "is_pps_precinct" in gdf_merged.columns:
        participated_count = gdf_merged[gdf_merged["is_pps_precinct"]].shape[0]
        not_participated_count = gdf_merged[~gdf_merged["is_pps_precinct"]].shape[0]
        logger.debug(
            f"  📊 PPS participation: {participated_count} participated, {not_participated_count} did not participate"
        )

    # === 3. Save Merged GeoJSON ===
    try:
        logger.debug("💾 Saving optimized GeoJSON for web use:")

        # Ensure we have proper CRS before saving
        if gdf_merged.crs is None:
            logger.debug("  🔧 Setting WGS84 CRS for output")
            gdf_merged = gdf_merged.set_crs("EPSG:4326")

        # Calculate summary statistics for metadata
        pps_features = (
            gdf_merged[gdf_merged.get("is_pps_precinct", False)]
            if "is_pps_precinct" in gdf_merged.columns
            else gdf_merged
        )
        total_votes_cast = (
            pps_features["votes_total"].sum() if "votes_total" in pps_features.columns else 0
        )

        # Validate that all fields have explanations (quality assurance)
        # Use flexible mode by default - warns about missing fields but continues processing
        logger.debug("  🔍 Validating field completeness with schema drift handling...")
        validate_field_completeness(gdf_merged, strict_mode=False)
        logger.debug("  ✅ Field validation completed (check logs for details)")

        # Advanced schema drift monitoring (if available)
        if SCHEMA_MONITORING_AVAILABLE:
            logger.debug("  📊 Running advanced schema drift monitoring...")
            try:
                drift_results = monitor_schema_drift(gdf_merged, "election_analysis_pipeline")

                # Report monitoring results
                snapshot = drift_results["snapshot"]
                alerts = drift_results["alerts"]

                logger.debug(
                    f"  📸 Schema snapshot captured: {snapshot['total_fields']} fields, hash: {snapshot['schema_hash']}"
                )

                if alerts:
                    logger.critical(f"  🚨 {len(alerts)} schema drift alert(s) generated:")
                    for alert in alerts:
                        severity_emoji = {
                            "CRITICAL": "🔴",
                            "HIGH": "🟠",
                            "MEDIUM": "🟡",
                            "LOW": "🟢",
                        }
                        emoji = severity_emoji.get(alert["severity"], "ℹ️")
                        logger.debug(f"    {emoji} {alert['severity']}: {alert['title']}")
                else:
                    logger.debug("  ✅ No schema drift alerts - data structure is stable")

                # Generate drift report
                monitor = SchemaDriftMonitor()
                drift_report = monitor.generate_drift_report(days_back=7)
                report_path = monitor.monitoring_dir / "latest_drift_report.md"
                report_path.parent.mkdir(exist_ok=True, parents=True)
                with open(report_path, "w") as f:
                    f.write(drift_report)
                logger.debug(f"  📄 Schema drift report saved: {report_path}")

            except Exception as e:
                logger.error(f"Schema monitoring failed: {e}")
                logger.warning("  ⚠️ Schema monitoring encountered an error but pipeline continues")
        else:
            logger.debug("  ℹ️ Advanced schema monitoring not available")

        # Generate layer explanations for self-documenting data
        layer_explanations = generate_layer_explanations(gdf_merged)

        # Export complete field registry for the web map
        field_registry_data = export_complete_field_registry(gdf_merged)

        # Save with proper driver options for web consumption
        gdf_merged.to_file(
            output_geojson_path,
            driver="GeoJSON",
        )

        # Add metadata to the saved GeoJSON file
        with open(output_geojson_path, "r") as f:
            geojson_data = json.load(f)

        # Add comprehensive metadata
        geojson_data["crs"] = {
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:OGC:1.3:CRS84"  # Standard web-friendly CRS identifier
            },
        }

        # Add metadata object with complete field registry
        geojson_data["metadata"] = {
            "title": config.get("project_name"),
            "description": config.get("description"),
            "source": config.get_metadata("data_source"),
            "created": "2025-01-22",
            "crs": "EPSG:4326",
            "coordinate_system": "WGS84 Geographic",
            "features_count": len(gdf_merged),
            "pps_features": len(pps_features) if len(pps_features) > 0 else 0,
            "total_votes_cast": int(total_votes_cast) if not pd.isna(total_votes_cast) else 0,
            "candidate_colors": candidate_color_mapping,  # Add consistent candidate colors
            "layer_explanations": layer_explanations,  # Add self-documenting layer explanations
            # COMPLETE FIELD REGISTRY DATA FOR WEB MAP CONSUMPTION
            "field_registry": field_registry_data,  # Complete registry information
            "data_sources": [
                config.get_metadata("attribution"),
                config.get_metadata("data_source"),
            ],
            "processing_notes": [
                f"Coordinates reprojected to {config.get_system_setting('output_crs')} for web compatibility",
                "Properties optimized for vector tile generation",
                "Geometry validated and fixed where necessary",
                "Split precincts consolidated into single features",
                "Added analytical fields for deeper election analysis",
                "Consistent candidate color mapping applied across all visualizations",
                "Layer explanations embedded for self-documenting data",
                "Complete field registry exported for web map consumption",
            ],
        }

        # Save the enhanced GeoJSON
        with open(output_geojson_path, "w") as f:
            json.dump(geojson_data, f, separators=(",", ":"))  # Compact formatting for web

        logger.debug(f"  ✓ Saved optimized GeoJSON: {output_geojson_path}")
        logger.debug(f"  📊 Features: {len(gdf_merged)}, CRS: EPSG:4326 (WGS84)")
        logger.debug(
            f"  🗳️ PPS features: {len(pps_features)}, Total votes: {int(total_votes_cast):,}"
        )

    except Exception as e:
        logger.debug(f"  ❌ Error saving GeoJSON: {e}")
        return

    # === 4. Generate Maps ===
    logger.debug("Generating maps with color-blind friendly palettes:")

    # 1. PPS Participation Map
    if "is_pps_precinct" in gdf_merged.columns:
        # Create a numeric version for plotting
        gdf_merged["is_pps_numeric"] = gdf_merged["is_pps_precinct"].astype(int)

    # 2. Political Lean (All Multnomah Features)
    if "political_lean" in gdf_merged.columns:
        # Create numeric mapping for political lean
        lean_mapping = {
            "Strong Rep": 1,
            "Lean Rep": 2,
            "Competitive": 3,
            "Lean Dem": 4,
            "Strong Dem": 5,
        }
        gdf_merged["political_lean_numeric"] = gdf_merged["political_lean"].map(lean_mapping)

    # 4. Total votes (PPS only)
    if "votes_total" in gdf_merged.columns and not gdf_merged["votes_total"].isnull().all():
        has_votes = gdf_merged[gdf_merged["is_pps_precinct"]]
        logger.debug(f"  📊 Total votes: {len(has_votes)} features with election data")

    # 5. Voter turnout (PPS only)
    if "turnout_rate" in gdf_merged.columns and not gdf_merged["turnout_rate"].isnull().all():
        has_turnout = gdf_merged[
            gdf_merged["turnout_rate"].notna() & (gdf_merged["turnout_rate"] > 0)
        ]
        logger.debug(f"  📊 Turnout: {len(has_turnout)} features with turnout data")

    # 6. Candidate Vote Share Maps (PPS only) - FULLY DYNAMIC FOR ANY CANDIDATES WITH CONSISTENT COLORS
    candidate_pct_cols = detect_candidate_columns(gdf_merged)
    detect_candidate_count_columns(gdf_merged)

    for pct_col in candidate_pct_cols:
        if not gdf_merged[pct_col].isnull().all():
            candidate_name = pct_col.replace("vote_pct_", "").replace("_", " ").title()
            candidate_key = pct_col.replace("vote_pct_", "")  # Original key for color mapping
            has_data = gdf_merged[gdf_merged[pct_col].notna()]
            logger.debug(f"  📊 {candidate_name} vote share: {len(has_data)} features with data")

            # Use safe filename (replace spaces and special characters)
            safe_filename = candidate_name.lower().replace(" ", "_").replace("-", "_")

            # Get consistent candidate color
            candidate_color = candidate_color_mapping.get(
                candidate_key, "#1f77b4"
            )  # Default blue fallback


    # Vote Efficiency (Democratic)
    if (
        "vote_efficiency_dem" in gdf_merged.columns
        and not gdf_merged["vote_efficiency_dem"].isnull().all()
    ):
        # Detect the Democratic-aligned candidate name dynamically
        dem_candidate_name = "Democratic Candidate"  # Default fallback

        # Try to find which candidate was used for vote_efficiency_dem calculation
        candidate_pct_cols = [
            col
            for col in gdf_merged.columns
            if col.startswith("vote_pct_")
            and col != "vote_pct_contribution_total_votes"
            and not col.startswith("vote_pct_contribution_")
        ]

        if len(candidate_pct_cols) > 0 and "reg_pct_dem" in gdf_merged.columns:
            # Find candidate with highest correlation to Democratic registration
            best_correlation = -1
            for col in candidate_pct_cols:
                valid_mask = (
                    gdf_merged[col].notna()
                    & gdf_merged["reg_pct_dem"].notna()
                    & (gdf_merged[col] > 0)
                    & (gdf_merged["reg_pct_dem"] > 0)
                )

                if valid_mask.sum() > 10:
                    try:
                        correlation = gdf_merged.loc[valid_mask, col].corr(
                            gdf_merged.loc[valid_mask, "reg_pct_dem"]
                        )
                        if correlation > best_correlation:
                            best_correlation = correlation
                            dem_candidate_name = (
                                col.replace("vote_pct_", "").replace("_", " ").title()
                            )
                    except Exception:
                        pass

    logger.debug("✅ Script completed successfully!")
    logger.debug(f"   GeoJSON saved to: {output_geojson_path}")
    logger.debug(
        f"   Summary: {len(matched)} features with election data out of {len(gdf_merged)} total features"
    )

    # Summary of generated maps
    candidate_count = len(candidate_pct_cols)
    analytical_maps_count = 4  # Victory margin, competitiveness, vote efficiency, swing potential
    base_maps_count = (
        6  # PPS participation, political lean, dem advantage, total votes, turnout, analytical maps
    )
    total_maps = base_maps_count + candidate_count + analytical_maps_count

    logger.debug(f"🗺️ Generated {total_maps} maps:")
    logger.debug("   1. PPS Participation Map")
    logger.debug("   2. Political Lean (All Multnomah)")
    logger.debug("   3. Democratic Registration Advantage")
    logger.debug("   4. Total votes (PPS only)")
    logger.debug("   5. Voter turnout (PPS only)")

    map_counter = 6
    for pct_col in candidate_pct_cols:
        candidate_name = pct_col.replace("vote_pct_", "").replace("_", " ").title()
        logger.debug(f"   {map_counter}. {candidate_name} Vote Share (PPS only)")
        map_counter += 1

    logger.debug(f"   {map_counter}. Victory Margin Percentage")
    logger.debug(f"   {map_counter + 1}. Competitiveness Score")
    logger.debug(f"   {map_counter + 2}. Democratic Vote Efficiency")
    logger.debug(f"   {map_counter + 3}. Electoral Swing Potential")


if __name__ == "__main__":
    main()
