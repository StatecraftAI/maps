#!/usr/bin/env python3
"""
prepare_election_data.py - MVP Election Data Preprocessor

The preprocessing step that matters for the MVP.

Usage: 
    python prepare_election_data.py
    
Input:  
    - config.yaml (file paths and settings)
    - voter registration CSV
    - election results CSV  
    - precinct boundaries GeoJSON
    
Output: 
    - Clean election geodata ready for geo_upload.py

Result: From messy CSVs to clean election geodata in ~100 lines.
"""

import sys
from pathlib import Path
import pandas as pd
import geopandas as gpd
import numpy as np
from loguru import logger

# Add parent directory for config
sys.path.append(str(Path(__file__).parent.parent))
from ops.config_loader import Config


def load_and_merge_data(config: Config) -> pd.DataFrame:
    """Load voter registration and election results, merge them."""
    logger.info("📊 Loading and merging election data...")
    
    # For MVP, we'll work with just the election results CSV for now
    # The voter registration data would need to be added to config
    votes_path = config.get_input_path("votes_csv")
    precinct_col = config.get_column_name("precinct_csv")
    
    logger.info(f"  📄 Election results: {votes_path}")
    
    # Load election results CSV
    df = pd.read_csv(votes_path)
    logger.info(f"  ✅ Loaded {len(df)} election records")
    
    # Standardize precinct column
    df[precinct_col] = df[precinct_col].astype(str)
    
    return df


def add_vote_percentages(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate vote percentages for each candidate."""
    logger.info("🗳️ Calculating vote percentages...")
    
    # Find candidate columns
    candidate_cols = [col for col in df.columns if col.startswith("candidate_")]
    logger.info(f"  📊 Found candidates: {[col.replace('candidate_', '') for col in candidate_cols]}")
    
    # Convert to numeric and create percentage columns
    df["votes_total"] = pd.to_numeric(df["total_votes"], errors="coerce").fillna(0)
    
    for col in candidate_cols:
        candidate_name = col.replace("candidate_", "")
        votes_col = f"votes_{candidate_name}"
        pct_col = f"vote_pct_{candidate_name}"
        
        # Create votes column
        df[votes_col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        
        # Create percentage column
        df[pct_col] = np.where(
            df["votes_total"] > 0,
            (df[votes_col] / df["votes_total"]) * 100,
            0
        )
        
        logger.info(f"  ✅ Added {pct_col}")
    
    return df


def add_registration_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate voter registration metrics (if available)."""
    logger.info("👥 Checking for registration metrics...")
    
    # For MVP, just add placeholder columns if registration data isn't available
    party_cols = ["DEM", "REP", "NAV"]
    
    has_registration_data = any(col in df.columns for col in party_cols + ["TOTAL"])
    
    if has_registration_data:
        logger.info("  📊 Found registration data, calculating metrics...")
        
        for party in party_cols:
            if party in df.columns:
                pct_col = f"reg_pct_{party.lower()}"
                df[pct_col] = np.where(
                    df["TOTAL"] > 0,
                    (df[party] / df["TOTAL"]) * 100,
                    0
                )
                logger.info(f"  ✅ Added {pct_col}")
        
        # Democratic advantage
        if "reg_pct_dem" in df.columns and "reg_pct_rep" in df.columns:
            df["dem_advantage"] = df["reg_pct_dem"] - df["reg_pct_rep"]
            logger.info("  ✅ Added dem_advantage")
    else:
        logger.info("  ⚠️ No registration data found, skipping registration metrics")
    
    return df


def add_turnout_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate turnout rates (if registration data available)."""
    logger.info("📈 Checking for turnout calculation...")
    
    if "TOTAL" in df.columns:
        logger.info("  📊 Found voter registration data, calculating turnout...")
        df["turnout_rate"] = np.where(
            df["TOTAL"] > 0,
            (df["votes_total"] / df["TOTAL"]) * 100,
            0
        )
        logger.info("  ✅ Added turnout_rate")
    else:
        logger.info("  ⚠️ No voter registration data, skipping turnout calculation")
        df["turnout_rate"] = 0
    
    return df


def add_competition_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate competition metrics - winner, margin, competitiveness."""
    logger.info("🏆 Calculating competition metrics...")
    
    # Find candidate vote columns
    candidate_cols = [col for col in df.columns if col.startswith("votes_") and col != "votes_total"]
    
    if len(candidate_cols) < 2:
        logger.warning("  ⚠️ Need at least 2 candidates for competition metrics")
        return df
    
    # Initialize columns
    df["leading_candidate"] = "No Data"
    df["vote_margin"] = 0
    df["margin_pct"] = 0.0
    df["competitiveness"] = "No Election Data"
    
    # Calculate for each row
    for idx, row in df.iterrows():
        if row["votes_total"] <= 0:
            continue
            
        # Get candidate votes
        candidate_votes = [(col.replace("votes_", ""), row[col]) for col in candidate_cols if row[col] > 0]
        
        if len(candidate_votes) >= 2:
            # Sort by votes (descending)
            candidate_votes.sort(key=lambda x: x[1], reverse=True)
            
            winner, winner_votes = candidate_votes[0]
            runner_up, runner_up_votes = candidate_votes[1]
            
            # Calculate margin
            margin = winner_votes - runner_up_votes
            margin_pct = (margin / row["votes_total"]) * 100
            
            # Update row
            df.loc[idx, "leading_candidate"] = winner.replace("_", " ").title()
            df.loc[idx, "vote_margin"] = margin
            df.loc[idx, "margin_pct"] = margin_pct
            
            # Competitiveness categories
            if margin_pct < 5:
                df.loc[idx, "competitiveness"] = "Toss-up"
            elif margin_pct < 10:
                df.loc[idx, "competitiveness"] = "Competitive"
            else:
                df.loc[idx, "competitiveness"] = "Safe"
    
    logger.info("  ✅ Added competition metrics")
    return df


def classify_precincts(df: pd.DataFrame) -> pd.DataFrame:
    """Add precinct classification flags."""
    logger.info("🏷️ Classifying precincts...")
    
    precinct_col = "precinct"
    
    # Basic classifications
    df["is_county_rollup"] = df[precinct_col].isin(["clackamas", "washington", "multnomah"])
    df["has_election_results"] = df["votes_total"] > 0
    
    # Only check for voter registration if TOTAL column exists
    if "TOTAL" in df.columns:
        df["has_voter_registration"] = df["TOTAL"] > 0
    else:
        logger.info("  ⚠️ No TOTAL column found, assuming no separate voter registration data")
        df["has_voter_registration"] = False
    
    df["is_pps_precinct"] = df["has_election_results"] & ~df["is_county_rollup"]
    
    logger.info(f"  ✅ PPS precincts: {df['is_pps_precinct'].sum()}")
    logger.info(f"  ✅ County rollups: {df['is_county_rollup'].sum()}")
    
    return df


def add_basic_analytics(df: pd.DataFrame) -> pd.DataFrame:
    """Add essential political analysis metrics."""
    logger.info("📊 Adding essential analytics...")
    
    # Ensure numeric types
    numeric_cols = [col for col in df.columns if col.startswith(("votes_", "vote_pct_")) or col in ["turnout_rate", "margin_pct", "vote_margin"]]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    
    # Victory margin as percentage of total votes
    if "vote_margin" in df.columns and "votes_total" in df.columns:
        df["pct_victory_margin"] = np.where(
            df["votes_total"] > 0,
            (df["vote_margin"] / df["votes_total"]) * 100,
            0
        )
        logger.info("  ✅ Added pct_victory_margin")
    
    # Competitiveness score (100=tie, 0=landslide)
    if "pct_victory_margin" in df.columns:
        df["competitiveness_score"] = 100 - df["pct_victory_margin"]
        logger.info("  ✅ Added competitiveness_score")
    
    # Simple margin categories
    if "pct_victory_margin" in df.columns:
        df["margin_category"] = pd.cut(
            df["pct_victory_margin"],
            bins=[0, 5, 15, 30, 100],
            labels=["Very Close", "Close", "Clear", "Landslide"],
            include_lowest=True
        )
        logger.info("  ✅ Added margin_category")
    
    return df


def add_candidate_analytics(df: pd.DataFrame) -> pd.DataFrame:
    """Add candidate-specific analytics."""
    logger.info("📊 Adding candidate analytics...")
    
    candidate_cols = [col for col in df.columns if col.startswith("votes_") and col != "votes_total"]
    
    if len(candidate_cols) >= 2:
        # Simple dominance ratio (leading votes / second place votes)
        df["votes_leading"] = 0
        df["votes_second_place"] = 0
        df["candidate_dominance"] = 1.0
        
        for idx, row in df.iterrows():
            if row["votes_total"] <= 0:
                continue
                
            candidate_votes = [(col, row[col]) for col in candidate_cols if row[col] > 0]
            candidate_votes.sort(key=lambda x: x[1], reverse=True)
            
            if len(candidate_votes) >= 2:
                df.loc[idx, "votes_leading"] = candidate_votes[0][1]
                df.loc[idx, "votes_second_place"] = candidate_votes[1][1]
                
                if candidate_votes[1][1] > 0:
                    df.loc[idx, "candidate_dominance"] = candidate_votes[0][1] / candidate_votes[1][1]
        
        logger.info("  ✅ Added candidate dominance ratio")
    
    return df


def add_simple_contributions(df: pd.DataFrame) -> pd.DataFrame:
    """Add simple vote contribution percentages."""
    logger.info("📊 Adding vote contributions...")
    
    if "votes_total" in df.columns and "is_pps_precinct" in df.columns:
        pps_mask = df["is_pps_precinct"]
        
        # Simple: what % of PPS votes came from this precinct
        if pps_mask.any():
            total_pps_votes = df.loc[pps_mask, "votes_total"].sum()
            
            if total_pps_votes > 0:
                df["vote_contribution_pct"] = 0.0
                df.loc[pps_mask, "vote_contribution_pct"] = (
                    df.loc[pps_mask, "votes_total"] / total_pps_votes * 100
                )
                logger.info(f"  ✅ Added vote_contribution_pct (total: {total_pps_votes:,})")
    
    return df


def add_quality_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Add simple data quality flags."""
    logger.info("📊 Adding quality flags...")
    
    # Simple data quality indicators
    df["has_election_data"] = df.get("has_election_results", False)
    df["has_voter_data"] = df.get("has_voter_registration", False) 
    df["complete_record"] = df["has_election_data"] & df["has_voter_data"]
    
    # Simple population weight (if available)
    if "TOTAL" in df.columns:
        max_voters = df["TOTAL"].max()
        if max_voters > 0:
            df["population_weight"] = (df["TOTAL"] / max_voters) * 100
            logger.info("  ✅ Added population_weight")
    
    logger.info("  ✅ Added quality flags")
    return df


def add_analytical_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Add analytical fields - simplified and focused."""
    logger.info("📊 Adding analytical fields (simplified)...")
    
    # Break complex function into focused pieces
    df = add_basic_analytics(df)
    df = add_candidate_analytics(df)
    df = add_simple_contributions(df)
    df = add_quality_flags(df)
    
    analytical_fields_added = len([col for col in df.columns if col.endswith(("_score", "_contribution", "_category", "_weight", "_dominance"))])
    logger.info(f"  📊 Added {analytical_fields_added} analytical fields")
    
    return df


def merge_with_geography(df: pd.DataFrame, config: Config) -> gpd.GeoDataFrame:
    """Merge data with precinct boundaries."""
    logger.info("🗺️ Merging with geography...")
    
    # Load boundaries
    boundaries_path = config.get_input_path("precincts_geojson")
    gdf = gpd.read_file(boundaries_path)
    logger.info(f"  📄 Loaded {len(gdf)} boundary features")
    
    # Get column names
    precinct_csv_col = config.get_column_name("precinct_csv")  # "precinct" 
    precinct_geojson_col = config.get_column_name("precinct_geojson")  # "Precinct"
    
    # Clean precinct IDs for matching
    df[precinct_csv_col] = df[precinct_csv_col].astype(str).str.lstrip("0").str.strip().str.lower()
    gdf[precinct_geojson_col] = gdf[precinct_geojson_col].astype(str).str.lstrip("0").str.strip().str.lower()
    
    # Merge (exclude county rollups - they don't have geography)
    df_for_gis = df[~df["is_county_rollup"]].copy()
    
    merged_gdf = gdf.merge(
        df_for_gis, 
        left_on=precinct_geojson_col, 
        right_on=precinct_csv_col, 
        how="left"
    )
    
    logger.info(f"  ✅ Merged: {len(merged_gdf)} features")
    logger.info(f"  📊 Election data added to geographic features")
    
    return merged_gdf


def clean_and_validate(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Clean data types and validate geometries."""
    logger.info("🧹 Cleaning and validating...")
    
    # Ensure WGS84
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        logger.info(f"  🔄 Reprojecting from {gdf.crs} to WGS84")
        gdf = gdf.to_crs("EPSG:4326")
    
    # Clean numeric columns
    numeric_cols = [col for col in gdf.columns if col.startswith(("votes_", "vote_pct_", "reg_pct_")) or col in ["turnout_rate", "margin_pct", "vote_margin"]]
    
    for col in numeric_cols:
        if col in gdf.columns:
            gdf[col] = pd.to_numeric(gdf[col], errors="coerce").fillna(0)
    
    # Clean categorical columns - convert to strings for GeoJSON compatibility
    categorical_cols = ["leading_candidate", "competitiveness", "margin_category", "turnout_quartile", "voter_density_category"]
    for col in categorical_cols:
        if col in gdf.columns:
            gdf[col] = gdf[col].astype(str).replace(["nan", "None", "<NA>"], "No Data")
    
    # Validate geometries
    invalid_geom = gdf[~gdf.geometry.is_valid]
    if len(invalid_geom) > 0:
        logger.warning(f"  ⚠️ Found {len(invalid_geom)} invalid geometries, fixing...")
        gdf.geometry = gdf.geometry.buffer(0)  # Fix topology errors
    
    logger.info("  ✅ Data cleaned and validated")
    return gdf


def prepare_election_data() -> str:
    """Main function - prepare election data for upload."""
    logger.info("🗳️ Election Data Preparation - MVP")
    logger.info("=" * 50)
    
    # Load configuration
    config = Config()
    logger.info(f"📋 Project: {config.get('project_name')}")
    
    # Process data step by step
    df = load_and_merge_data(config)
    df = add_vote_percentages(df)
    df = add_registration_metrics(df)  
    df = add_turnout_rates(df)
    df = add_competition_metrics(df)
    df = classify_precincts(df)
    df = add_analytical_fields(df)  # Add comprehensive analytical fields
    
    # Merge with geography
    gdf = merge_with_geography(df, config)
    gdf = clean_and_validate(gdf)
    
    # Save for upload (ensure directory exists)
    output_file = Path("data/processed_election_data.geojson")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(output_file, driver="GeoJSON")
    
    logger.success(f"✅ Election data prepared: {output_file}")
    logger.info(f"  📊 {len(gdf)} features ready for upload")
    logger.info(f"  🗳️ {gdf['is_pps_precinct'].sum()} PPS precincts with election data")
    
    return str(output_file)


if __name__ == "__main__":
    prepare_election_data() 