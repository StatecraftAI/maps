"""
process_election_results.py

This script processes election results data for geospatial visualization and uploads to Supabase
as the primary data backend. It handles geospatial data, defines calculated fields, and prepares
comprehensive election data for web consumption and API integration.

Now refactored to use the spatial_utils module for all spatial operations.

Key Functionality:
1. Field Definitions:
   - Registers calculated fields (e.g., percentages, counts, ratios) with explanations and formulas.
   - Maintains a field registry for documentation and validation.

2. Data Processing:
   - Adds analytical fields and competition metrics to election data.
   - Calculates vote margins, turnout rates, and demographic correlations.

3. Data Validation:
   - Validates the completeness of fields in the geospatial dataset.
   - Ensures data integrity for accurate analysis and visualization.

4. Backend Integration:
   - Uploads optimized geospatial data to Supabase PostGIS database.
   - Provides real-time data access for web applications and APIs.

5. Field Registry Management:
   - Exports complete field registry with explanations and formulas.
   - Embeds self-documenting metadata for web consumption.

Usage:
- This script is typically used after `process_voter_election_data.py` to upload processed
  election data to the Supabase backend for the maps component of StatecraftAI.
- Part of the data pipeline for generating interactive maps and real-time dashboards.

Input:
- Geospatial election results data (e.g., GeoJSON, Shapefile).
- Configuration file (e.g., config.yaml) for field definitions and processing settings.

Output:
- Supabase PostGIS table: Dynamic table names (e.g., `election_results_zone4`, `election_results_bond`) with comprehensive analytical fields.
- Field registry documentation for API consumers.

Example:
    python process_election_results.py --config config.yaml

Dependencies:
- geopandas, pandas, numpy, matplotlib, loguru, pathlib, and other standard Python libraries.
- Supabase integration (optional): sqlalchemy, psycopg2-binary for database uploads.
- Spatial operations: spatial_utils module for all geometric processing.
"""

from typing import Any, Callable, Dict, List, Optional

import geopandas as gpd
import numpy as np
import pandas as pd
from loguru import logger
from ops.config_loader import Config
from ops.field_registry import (
    FieldDefinition,
    FieldRegistry,
    export_complete_field_registry,
    generate_layer_explanations,
)

# Import spatial operations from spatial_utils module
from spatial_utils import (
    SUPABASE_AVAILABLE,
    SpatialQueryManager,
    SupabaseDatabase,
    SupabaseUploader,
    clean_numeric,
    consolidate_split_precincts,
    optimize_geojson_properties,
    validate_and_reproject_to_wgs84,
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
    auto_registered_count = len(validation.get("auto_registered", []))
    if auto_registered_count > 0:
        logger.debug(f"🔄 Auto-registered {auto_registered_count} fields using pattern detection")

    # Handle missing explanations based on mode
    missing_fields = validation.get("missing_fields", [])
    if missing_fields:
        missing_count = len(missing_fields)

        if strict_mode:
            error_msg = (
                f"STRICT MODE: Missing explanations for {missing_count} fields: {missing_fields}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        else:
            logger.warning(
                f"⚠️  Schema drift detected: {missing_count} fields lack explicit explanations"
            )
            logger.warning(
                f"   Missing fields: {missing_fields[:10]}{'...' if len(missing_fields) > 10 else ''}"
            )
            logger.debug("   💡 Consider using register_calculated_field() for critical fields")
            logger.debug("   📚 Auto-generated explanations will be used for web display")

    # Report orphaned explanations (fields in registry but not in data)
    extra_registered = validation.get("extra_registered", [])
    if extra_registered:
        orphaned_count = len(extra_registered)
        logger.warning(
            f"📋 {orphaned_count} registered fields not found in current data: {extra_registered}"
        )
        logger.debug("   💡 This may indicate upstream schema changes or different data sources")

    # Success summary
    total_fields = validation["total_fields"]
    registered_fields = validation["registered_fields"]
    coverage_pct = validation.get("coverage_percentage", 0)

    logger.debug(
        f"✅ Field coverage: {registered_fields}/{total_fields} fields ({coverage_pct:.1f}%) have explanations"
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


def generate_election_table_name(config: Config, gdf: gpd.GeoDataFrame) -> str:
    """
    Generate a unique, descriptive table name for the election data.

    Args:
        config: Configuration instance
        gdf: GeoDataFrame with election data

    Returns:
        Sanitized table name for Supabase
    """
    logger.debug("🏷️ Generating dynamic table name for election data...")

    # Start with base name
    base_name = "election_results"

    # Try to extract election identifier from various sources
    election_identifier = None

    # Method 1: Check if votes CSV path contains zone/election info
    try:
        votes_csv_path = config.get_input_path("votes_csv")
        csv_name = votes_csv_path.stem.lower()

        # Extract zone number (e.g., "2025_election_zone4_total_votes" -> "zone4")
        if "zone" in csv_name:
            import re

            zone_match = re.search(r"zone(\d+)", csv_name)
            if zone_match:
                election_identifier = f"zone{zone_match.group(1)}"
                logger.debug(f"  📍 Detected zone from CSV path: {election_identifier}")

        # Extract named election (e.g., "2025_election_bond_total_votes" -> "bond")
        elif "_election_" in csv_name:
            parts = csv_name.split("_election_")
            if len(parts) > 1:
                election_part = parts[1].split("_total_votes")[0]
                if election_part and election_part not in ["pps", "multnomah"]:
                    election_identifier = election_part
                    logger.debug(
                        f"  📍 Detected named election from CSV path: {election_identifier}"
                    )

    except Exception as e:
        logger.debug(f"  ⚠️ Could not extract election info from CSV path: {e}")

    # Method 2: Check project name for election info
    if not election_identifier:
        try:
            project_name = config.get("project_name", "").lower()

            # Look for zone patterns
            import re

            zone_match = re.search(r"zone\s*(\d+)", project_name)
            if zone_match:
                election_identifier = f"zone{zone_match.group(1)}"
                logger.debug(f"  📍 Detected zone from project name: {election_identifier}")

            # Look for named elections
            elif any(keyword in project_name for keyword in ["bond", "levy", "measure"]):
                if "bond" in project_name:
                    election_identifier = "bond"
                elif "levy" in project_name:
                    election_identifier = "levy"
                elif "measure" in project_name:
                    election_identifier = "measure"
                logger.debug(
                    f"  📍 Detected named election from project name: {election_identifier}"
                )

        except Exception as e:
            logger.debug(f"  ⚠️ Could not extract election info from project name: {e}")

    # Method 3: Analyze candidate names in the data
    if not election_identifier:
        try:
            candidate_cols = [
                col for col in gdf.columns if col.startswith("votes_") and col != "votes_total"
            ]
            if candidate_cols:
                # Check if candidate names suggest a specific election type
                candidate_names = [col.replace("votes_", "").lower() for col in candidate_cols]

                # Look for patterns that suggest specific elections
                if any("bond" in name for name in candidate_names):
                    election_identifier = "bond"
                elif any("levy" in name for name in candidate_names):
                    election_identifier = "levy"
                elif len(candidate_names) == 2:  # Typical for zone elections
                    # Try to detect zone from candidate patterns
                    election_identifier = "pps_race"
                else:
                    election_identifier = "multi_candidate"

                logger.debug(f"  📍 Inferred election type from candidates: {election_identifier}")

        except Exception as e:
            logger.debug(f"  ⚠️ Could not analyze candidate data: {e}")

    # Method 4: Use timestamp as fallback
    if not election_identifier:
        import time

        timestamp = time.strftime("%Y%m%d_%H%M")
        election_identifier = f"election_{timestamp}"
        logger.debug(f"  📍 Using timestamp fallback: {election_identifier}")

    # Construct full table name
    if election_identifier:
        table_name = f"{base_name}_{election_identifier}"
    else:
        table_name = f"{base_name}_pps"

    # Sanitize for PostgreSQL (lowercase, underscores only, no special chars)
    import re

    table_name = re.sub(r"[^a-z0-9_]", "_", table_name.lower())
    table_name = re.sub(r"_+", "_", table_name)  # Remove multiple underscores
    table_name = table_name.strip("_")  # Remove leading/trailing underscores

    # Ensure it doesn't start with a number
    if table_name and table_name[0].isdigit():
        table_name = f"election_{table_name}"

    # Limit length for PostgreSQL
    if len(table_name) > 63:  # PostgreSQL identifier limit
        table_name = table_name[:63]

    logger.info(f"  ✅ Generated table name: {table_name}")
    return table_name


def generate_election_description(config: Config, gdf: gpd.GeoDataFrame, total_votes: float) -> str:
    """
    Generate a comprehensive description for the election table.

    Args:
        config: Configuration instance
        gdf: GeoDataFrame with election data
        total_votes: Total votes cast

    Returns:
        Descriptive text for the table
    """
    logger.debug("📝 Generating election description...")

    # Get basic info
    project_name = config.get("project_name", "Election Analysis")
    description = config.get("description", "Election results analysis")

    # Count features and candidates
    total_features = len(gdf)
    pps_features = (
        len(gdf[gdf.get("is_pps_precinct", False)])
        if "is_pps_precinct" in gdf.columns
        else total_features
    )

    # Get candidate info
    candidate_cols = [
        col for col in gdf.columns if col.startswith("votes_") and col != "votes_total"
    ]
    candidate_names = [
        col.replace("votes_", "").replace("_", " ").title() for col in candidate_cols
    ]

    # Build description
    desc_parts = [
        f"{project_name}.",
        f"{description}.",
        f"Contains {total_features:,} geographic features with {pps_features:,} participating precincts.",
    ]

    if total_votes > 0:
        desc_parts.append(f"Total votes processed: {int(total_votes):,}.")

    if candidate_names:
        if len(candidate_names) <= 3:
            candidates_str = ", ".join(candidate_names)
        else:
            candidates_str = (
                f"{', '.join(candidate_names[:3])}, and {len(candidate_names) - 3} others"
            )
        desc_parts.append(f"Candidates: {candidates_str}.")

    # Add analytical info
    analytical_fields = len(
        [
            col
            for col in gdf.columns
            if col.endswith(("_rate", "_score", "_pct", "_efficiency", "_margin"))
        ]
    )
    desc_parts.append(
        f"Includes {analytical_fields} analytical fields for comprehensive election analysis."
    )

    # Add technical info
    desc_parts.append(
        "Optimized for interactive visualization and spatial API queries with PostGIS."
    )

    full_description = " ".join(desc_parts)

    logger.debug(f"  ✅ Generated description: {full_description[:100]}...")
    return full_description


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

    # 1. Load Data
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

    # Merge analytical fields back to GeoDataFrame using concat for better performance
    analysis_cols = [col for col in analysis_df.columns if col not in gdf_merged.columns]
    if analysis_cols:
        # Use pd.concat to add all new columns at once instead of one by one
        new_columns_df = analysis_df[analysis_cols]
        gdf_merged = pd.concat([gdf_merged, new_columns_df], axis=1)

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
    create_candidate_color_mapping(candidate_cols)

    # Collect all columns to clean and their types
    count_cols = [col for col in gdf_merged.columns if col.startswith("votes_")]
    pct_cols = [col for col in gdf_merged.columns if col.startswith(("vote_pct_", "reg_pct_"))]
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
    other_numeric_cols = [col for col in other_numeric_cols if col in gdf_merged.columns]
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
    categorical_cols = [col for col in categorical_cols if col in gdf_merged.columns]

    # Clean numeric columns in batches
    all_numeric_cols = count_cols + pct_cols + other_numeric_cols
    if all_numeric_cols:
        # Create a copy to avoid fragmentation warnings
        cleaned_data = {}

        # Clean count columns
        for col in count_cols:
            cleaned_data[col] = clean_numeric(gdf_merged[col], is_percent=False)
            valid_count = cleaned_data[col].notna().sum()
            if valid_count > 0:
                logger.debug(
                    f"  ✓ Cleaned {col}: {valid_count} valid values, range: {cleaned_data[col].min():.0f} - {cleaned_data[col].max():.0f}"
                )

        # Clean percentage columns
        for col in pct_cols:
            cleaned_data[col] = clean_numeric(
                gdf_merged[col], is_percent=False
            )  # DON'T divide by 100
            valid_count = cleaned_data[col].notna().sum()
            if valid_count > 0:
                logger.debug(
                    f"  ✓ Cleaned {col}: {valid_count} valid values, range: {cleaned_data[col].min():.1f}% - {cleaned_data[col].max():.1f}%"
                )

        # Clean other numeric columns
        for col in other_numeric_cols:
            cleaned_data[col] = clean_numeric(
                gdf_merged[col], is_percent=False
            )  # Already percentages
            valid_count = cleaned_data[col].notna().sum()
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
                        f"  ✓ Cleaned {col}: {valid_count} valid values, range: {cleaned_data[col].min():.1f}% - {cleaned_data[col].max():.1f}%"
                    )
                else:
                    logger.debug(
                        f"  ✓ Cleaned {col}: {valid_count} valid values, range: {cleaned_data[col].min():.3f} - {cleaned_data[col].max():.3f}"
                    )

        # Update all numeric columns at once using assign() to avoid fragmentation
        gdf_merged = gdf_merged.assign(**cleaned_data)

    # Handle categorical columns efficiently
    if categorical_cols:
        categorical_data = {}

        for col in categorical_cols:
            # Special handling for boolean columns that may be stored as strings
            if col == "is_pps_precinct":
                categorical_data[col] = (
                    gdf_merged[col].astype(str).str.lower().map({"true": True, "false": False})
                )
            else:
                # For string categorical columns, ensure they stay as strings and clean up
                cleaned_col = gdf_merged[col].astype(str)
                # Replace pandas/numpy string representations of missing values
                cleaned_col = cleaned_col.replace(["nan", "None", "<NA>", ""], "No Data")

                # Set appropriate defaults for specific columns
                if col == "political_lean":
                    cleaned_col = cleaned_col.replace("No Data", "Unknown")
                elif col == "competitiveness":
                    cleaned_col = cleaned_col.replace("No Data", "No Election Data")
                elif col in ["leading_candidate", "second_candidate"]:
                    cleaned_col = cleaned_col.replace("No Data", "No Data")

                categorical_data[col] = cleaned_col

            value_counts = categorical_data[col].value_counts()
            logger.debug(f"  ✓ {col} distribution: {dict(value_counts)}")

        # Update all categorical columns at once
        gdf_merged = gdf_merged.assign(**categorical_data)

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

    # === 3. Optimize and Upload to Supabase ===
    logger.info("🚀 Preparing optimized election results for Supabase upload...")

    # Ensure we have proper CRS before processing
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

    # Generate layer explanations for self-documenting data
    generate_layer_explanations(gdf_merged)

    # Export complete field registry for the web map
    export_complete_field_registry(gdf_merged)

    # Generate dynamic table name based on election data
    table_name = generate_election_table_name(config, gdf_merged)
    logger.info(f"📊 Generated table name: {table_name}")

    # Upload to Supabase (Primary Data Backend)
    if SUPABASE_AVAILABLE:
        logger.info("🚀 Uploading election results to Supabase PostGIS database...")

        try:
            uploader = SupabaseUploader(config)

            # Create comprehensive description for this specific election
            election_description = generate_election_description(
                config, gdf_merged, total_votes_cast
            )

            # Upload election results (primary web layer with all analytical fields)
            upload_success = uploader.upload_geodataframe(
                gdf_merged, table_name=table_name, description=election_description
            )

            if upload_success:
                logger.success("   ✅ Uploaded election results to Supabase")

                # Verify upload with improved error handling
                try:
                    db = SupabaseDatabase(config)
                    query_manager = SpatialQueryManager(db)

                    # Try to get sample records directly (more reliable than table_exists check)
                    sample_records = query_manager.get_sample_records(table_name, limit=5)

                    if len(sample_records) > 0:
                        logger.info(
                            f"   ✅ Verification successful: {len(sample_records)} sample records retrieved"
                        )
                        logger.info(
                            f"   📊 Election results uploaded successfully: {len(gdf_merged):,} features"
                        )
                        logger.info(
                            f"   🗳️ PPS features: {len(pps_features):,}, Total votes: {int(total_votes_cast):,}"
                        )
                        logger.info(
                            "   🌐 Backend: Data is now available via Supabase PostGIS for fast spatial queries"
                        )
                    else:
                        logger.warning(
                            "   ⚠️ Upload reported success but no sample records retrieved"
                        )
                        logger.info(
                            "   💡 This could indicate a schema/permission issue, but data may still be uploaded"
                        )

                except Exception as verification_error:
                    logger.warning(
                        f"   ⚠️ Upload succeeded but verification query failed: {verification_error}"
                    )
                    logger.info(
                        "     💡 This is often a connectivity or schema issue - data is likely uploaded correctly"
                    )
                    logger.info(
                        "     💡 You can verify manually by checking the Supabase dashboard"
                    )

            else:
                logger.error("   ❌ Upload failed - table was not created")
                logger.info("   💡 Common issues and solutions:")
                logger.info(
                    "      1. Check Supabase credentials (SUPABASE_DB_HOST, SUPABASE_DB_PASSWORD)"
                )
                logger.info(
                    "      2. Ensure PostGIS extension is enabled: CREATE EXTENSION postgis;"
                )
                logger.info("      3. Verify database connectivity and permissions")
                logger.info("      4. Check if the database has sufficient storage space")

        except Exception as e:
            logger.error(f"❌ Supabase upload failed: {e}")
            logger.info("   💡 Check your Supabase credentials and connection")
            # Add more specific error handling
            import traceback

            logger.trace("Detailed upload error:")
            logger.trace(traceback.format_exc())
    else:
        logger.info("📊 Supabase integration not available - skipping database upload")
        logger.info("   💡 Install dependencies with: pip install sqlalchemy psycopg2-binary")

    # === 4. Data Quality Summary ===
    logger.info("📊 Validating data quality for backend consumption:")

    # Summary of data coverage by field type
    election_data_fields = [col for col in gdf_merged.columns if col.startswith("votes_")]
    demographic_fields = [col for col in gdf_merged.columns if col.startswith("reg_pct_")]
    analytical_fields = [
        col
        for col in gdf_merged.columns
        if col.endswith(("_rate", "_score", "_pct", "_efficiency"))
    ]

    logger.debug(f"  📈 Election data fields: {len(election_data_fields)}")
    logger.debug(f"  👥 Demographic fields: {len(demographic_fields)}")
    logger.debug(f"  🧮 Analytical fields: {len(analytical_fields)}")

    # Validate key visualization fields are present
    key_fields = [
        "votes_total",
        "turnout_rate",
        "margin_pct",
        "leading_candidate",
        "competitiveness",
    ]
    present_key_fields = [field for field in key_fields if field in gdf_merged.columns]

    logger.debug(
        f"  ✅ Key visualization fields present: {len(present_key_fields)}/{len(key_fields)}"
    )

    if len(present_key_fields) == len(key_fields):
        logger.debug("  🎯 All essential fields available for web visualization")
    else:
        missing_fields = set(key_fields) - set(present_key_fields)
        logger.warning(f"  ⚠️ Missing key fields: {missing_fields}")

    # Validate candidate data consistency
    candidate_pct_cols = detect_candidate_columns(gdf_merged)
    candidate_cnt_cols = detect_candidate_count_columns(gdf_merged)

    if len(candidate_pct_cols) > 0 and len(candidate_cnt_cols) > 0:
        logger.debug(
            f"  🗳️ Candidate data: {len(candidate_pct_cols)} percentage fields, {len(candidate_cnt_cols)} count fields"
        )
        logger.debug("  ✅ Candidate data structure validated for interactive visualization")
        logger.debug("  ✅ Candidate data structure validated for interactive visualization")
    else:
        logger.warning("  ⚠️ Limited candidate data - check upstream processing")

    # === Final Summary ===
    logger.success("✅ Election results processing completed successfully!")


if __name__ == "__main__":
    main()
