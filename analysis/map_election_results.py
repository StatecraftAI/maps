import json
import pathlib
from typing import Optional, Union

import geopandas as gpd
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from config_loader import Config


def detect_candidate_columns(gdf: gpd.GeoDataFrame) -> list:
    """Detect all candidate percentage columns dynamically from the enriched dataset."""
    # Look for vote percentage columns (vote_pct_candidatename) from the new enrichment
    candidate_pct_cols = [
        col
        for col in gdf.columns
        if col.startswith("vote_pct_") and col != "vote_pct_contribution_total_votes" and not col.startswith("vote_pct_contribution_")
    ]
    print(f"  📊 Detected candidate percentage columns: {candidate_pct_cols}")
    return candidate_pct_cols


def detect_candidate_count_columns(gdf: gpd.GeoDataFrame) -> list:
    """Detect all candidate count columns dynamically from the enriched dataset."""
    # Look for vote count columns (votes_candidatename) from the new enrichment
    candidate_cnt_cols = [
        col for col in gdf.columns if col.startswith("votes_") and col != "votes_total"
    ]
    print(f"  📊 Detected candidate count columns: {candidate_cnt_cols}")
    return candidate_cnt_cols


def detect_contribution_columns(gdf: gpd.GeoDataFrame) -> list:
    """Detect all candidate contribution columns dynamically from the enriched dataset."""
    # Look for contribution percentage columns
    contribution_cols = [
        col for col in gdf.columns if col.startswith("vote_pct_contribution_") and col != "vote_pct_contribution_total_votes"
    ]
    print(f"  📊 Detected contribution columns: {contribution_cols}")
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
    print(f"\n🔄 Consolidating split precincts in column '{precinct_col}':")
    
    # Create a copy to work with
    gdf_work = gdf.copy()
    
    # Convert ALL numeric columns to proper numeric types BEFORE processing
    print("  🔧 Converting columns to proper data types...")
    
    # Identify boolean columns first to exclude them from numeric conversion
    boolean_cols = ['is_zone1_precinct', 'has_election_results', 'has_voter_registration', 'is_summary', 'is_complete_record', 'is_county_rollup']
    
    # Identify ALL columns that should be numeric and convert them (excluding boolean columns)
    numeric_conversion_cols = []
    for col in gdf_work.columns:
        if col in ['geometry', precinct_col, 'base_precinct'] + boolean_cols:
            continue
        # Check if this looks like a numeric column based on content
        sample_values = gdf_work[col].dropna().head(10)
        if len(sample_values) > 0:
            # Try to convert sample to see if it's numeric
            try:
                pd.to_numeric(sample_values, errors='coerce')
                numeric_conversion_cols.append(col)
            except:
                pass
    
    # Convert identified numeric columns (excluding booleans)
    for col in numeric_conversion_cols:
        gdf_work[col] = pd.to_numeric(gdf_work[col], errors='coerce').fillna(0)
    
    # Handle boolean columns separately - convert to proper boolean type
    for col in boolean_cols:
        if col in gdf_work.columns:
            gdf_work[col] = gdf_work[col].astype(str).str.lower().isin(['true', '1', 'yes'])
    
    print(f"  📊 Converted {len(numeric_conversion_cols)} columns to numeric")
    print(f"  📊 Converted {sum(1 for col in boolean_cols if col in gdf_work.columns)} columns to boolean")
    
    # Extract base precinct numbers (remove a,b,c suffixes)
    gdf_work['base_precinct'] = gdf_work[precinct_col].astype(str).str.replace(
        r'[a-zA-Z]+$', '', regex=True
    ).str.strip()
    
    # Count how many precincts have splits
    precinct_counts = gdf_work['base_precinct'].value_counts()
    split_precincts = precinct_counts[precinct_counts > 1]
    
    print(f"  📊 Found {len(split_precincts)} precincts with splits:")
    for base, count in split_precincts.head(10).items():
        print(f"    - Precinct {base}: {count} parts")
    if len(split_precincts) > 10:
        print(f"    ... and {len(split_precincts) - 10} more")
    
    # Identify ALL numeric columns to take first value during consolidation
    numeric_cols = []
    for col in gdf_work.columns:
        if col in ['geometry', precinct_col, 'base_precinct']:
            continue
        # Include ANY column that starts with numeric prefixes OR is explicitly numeric
        if (col.startswith(('votes_', 'TOTAL', 'DEM', 'REP', 'NAV', 'OTH', 'CON', 'IND', 'LBT', 'NLB', 'PGP', 'PRO', 'WFP', 'WTP')) or
            col in ['vote_margin', 'total_voters'] or
            gdf_work[col].dtype in ['int64', 'float64']):
            numeric_cols.append(col)
    
    # Identify percentage/rate columns - FIXED for new percentage scale (0-100)
    percentage_cols = []
    for col in gdf_work.columns:
        if (col.startswith(('vote_pct_', 'reg_pct_')) or 
            col in ['turnout_rate', 'dem_advantage', 'major_party_pct', 'margin_pct', 'engagement_score']):
            percentage_cols.append(col)
    
    print(f"  📊 Will take first value from {len(numeric_cols)} numeric columns during consolidation")
    print(f"  📊 Will recalculate {len(percentage_cols)} percentage columns")
    
    # Debug: Check vote totals BEFORE consolidation (sum of unique precincts only)
    unique_precinct_votes = 0
    if 'votes_total' in gdf_work.columns:
        # For accurate comparison, count only unique base precincts
        unique_precinct_votes = gdf_work.groupby('base_precinct')['votes_total'].first().sum()
        pre_consolidation_total = gdf_work['votes_total'].sum()  # Raw total (includes duplicates)
        print(f"  🔍 Total votes BEFORE consolidation: {pre_consolidation_total:,.0f} (raw with duplicates)")
        print(f"  🔍 Unique precinct votes: {unique_precinct_votes:,.0f} (expected after consolidation)")
    
    # Group by base precinct and consolidate
    consolidated_features = []
    
    for base_precinct in gdf_work['base_precinct'].unique():
        if pd.isna(base_precinct) or base_precinct == '':
            continue
            
        precinct_parts = gdf_work[gdf_work['base_precinct'] == base_precinct]
        
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
                    if col.startswith('votes_') and first_value > 0:
                        all_values = precinct_parts[col].tolist()
                        if len(set(all_values)) == 1:
                            print(f"    🔍 {base_precinct} {col}: {first_value} (verified identical across {len(all_values)} parts)")
                        else:
                            print(f"    ⚠️  {base_precinct} {col}: Values differ across parts: {all_values} - taking first: {first_value}")
            
            # Handle boolean and categorical columns properly
            categorical_cols = ['political_lean', 'competitiveness', 'leading_candidate', 'record_type', 'turnout_quartile', 'margin_category', 'precinct_size_category']
            
            # For boolean columns, use logical OR (if ANY part is True, consolidated should be True)
            for col in boolean_cols:
                if col in precinct_parts.columns:
                    # Convert to boolean and take logical OR
                    bool_values = precinct_parts[col].astype(bool)  # Already converted to bool above
                    consolidated_value = bool_values.any()  # True if ANY part is True
                    consolidated.loc[consolidated.index[0], col] = consolidated_value  # Use .loc for proper assignment
                    
                    if col == 'is_zone1_precinct' and consolidated_value:
                        print(f"    🔍 {base_precinct} {col}: {precinct_parts[col].tolist()} → {consolidated_value}")
            
            # For categorical columns, use the first value (should be identical for split precincts)
            for col in categorical_cols:
                if col in precinct_parts.columns:
                    # Take the first value (should be same across all parts for split precincts)
                    first_value = precinct_parts[col].iloc[0]
                    consolidated.loc[consolidated.index[0], col] = first_value
            
            # Dissolve geometries (combine all parts into one shape)
            try:
                dissolved_geom = precinct_parts.unary_union
                consolidated.loc[consolidated.index[0], 'geometry'] = dissolved_geom
            except Exception as e:
                print(f"    ⚠️ Warning: Could not dissolve geometry for precinct {base_precinct}: {e}")
                # Use the first geometry as fallback
                pass
            
            # Keep other fields from first part for remaining columns
            # Note: percentage columns will be recalculated later based on new totals
            
            consolidated_features.append(consolidated)
    
    # Combine all consolidated features
    if consolidated_features:
        gdf_consolidated = pd.concat(consolidated_features, ignore_index=True)
        
        # Debug: Check vote totals AFTER consolidation
        if 'votes_total' in gdf_consolidated.columns:
            post_consolidation_total = gdf_consolidated['votes_total'].sum()
            print(f"  🔍 Total votes AFTER consolidation: {post_consolidation_total:,.0f}")
            if unique_precinct_votes > 0:
                retention_rate = (post_consolidation_total / unique_precinct_votes) * 100
                print(f"  📊 Vote retention rate: {retention_rate:.1f}% (should be 100%)")
            else:
                print(f"  📊 Vote total preserved correctly")
        
        # Recalculate percentage columns based on new totals - FIXED for percentage scale
        print("  🔄 Recalculating percentage columns...")
        for col in percentage_cols:
            if col in gdf_consolidated.columns:
                if col.startswith('vote_pct_'):
                    # Find the corresponding count column
                    count_col = col.replace('vote_pct_', 'votes_')
                    if count_col in gdf_consolidated.columns and 'votes_total' in gdf_consolidated.columns:
                        # Convert to numeric and recalculate percentages (0-100 scale)
                        count_values = pd.to_numeric(gdf_consolidated[count_col], errors='coerce').fillna(0)
                        total_values = pd.to_numeric(gdf_consolidated['votes_total'], errors='coerce').fillna(0)
                        gdf_consolidated[col] = np.where(
                            total_values > 0,
                            (count_values / total_values) * 100,  # Scale to 0-100
                            0
                        )
                elif col == 'turnout_rate' and 'votes_total' in gdf_consolidated.columns and 'TOTAL' in gdf_consolidated.columns:
                    # Recalculate turnout rate (0-100 scale)
                    vote_values = pd.to_numeric(gdf_consolidated['votes_total'], errors='coerce').fillna(0)
                    total_values = pd.to_numeric(gdf_consolidated['TOTAL'], errors='coerce').fillna(0)
                    gdf_consolidated[col] = np.where(
                        total_values > 0,
                        (vote_values / total_values) * 100,  # Scale to 0-100
                        0
                    )
                elif col == 'dem_advantage':
                    # Recalculate dem_advantage (already on 0-100 scale)
                    if 'reg_pct_dem' in gdf_consolidated.columns and 'reg_pct_rep' in gdf_consolidated.columns:
                        dem_values = pd.to_numeric(gdf_consolidated['reg_pct_dem'], errors='coerce').fillna(0)
                        rep_values = pd.to_numeric(gdf_consolidated['reg_pct_rep'], errors='coerce').fillna(0)
                        gdf_consolidated[col] = dem_values - rep_values
                elif col == 'major_party_pct':
                    # Recalculate major_party_pct (already on 0-100 scale)
                    if 'reg_pct_dem' in gdf_consolidated.columns and 'reg_pct_rep' in gdf_consolidated.columns:
                        dem_values = pd.to_numeric(gdf_consolidated['reg_pct_dem'], errors='coerce').fillna(0)
                        rep_values = pd.to_numeric(gdf_consolidated['reg_pct_rep'], errors='coerce').fillna(0)
                        gdf_consolidated[col] = dem_values + rep_values
        
        print(f"  ✅ Consolidated {len(gdf_work)} features into {len(gdf_consolidated)} features")
        print(f"  ✅ Eliminated {len(gdf_work) - len(gdf_consolidated)} duplicate/split features")
        
        return gdf_consolidated
    else:
        print("  ⚠️ Warning: No features to consolidate")
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
    print(f"\n📊 Adding analytical fields:")
    
    df_analysis = df.copy()
    
    # Convert string columns to numeric first
    numeric_conversion_cols = [
        'vote_margin', 'votes_total', 'turnout_rate', 'vote_pct_dem', 'vote_pct_rep',
        'vote_pct_cavagnolo', 'vote_pct_splitt', 'vote_pct_leof', 'vote_pct_sanchez_bautista', 'vote_pct_la_forte',
        'TOTAL', 'DEM', 'REP', 'NAV'
    ]
    
    for col in numeric_conversion_cols:
        if col in df_analysis.columns:
            df_analysis[col] = pd.to_numeric(df_analysis[col], errors='coerce').fillna(0)
    
    # Victory Margin Analysis
    if 'vote_margin' in df_analysis.columns and 'votes_total' in df_analysis.columns:
        df_analysis['pct_victory_margin'] = np.where(
            df_analysis['votes_total'] > 0,
            (df_analysis['vote_margin'] / df_analysis['votes_total'] * 100),
            0
        )
        print(f"  ✅ Added pct_victory_margin (victory margin as % of total votes)")
    
    # Competitiveness Scoring  
    if 'pct_victory_margin' in df_analysis.columns:
        df_analysis['competitiveness_score'] = 100 - df_analysis['pct_victory_margin']  # 0=landslide, 100=tie
        print(f"  ✅ Added competitiveness_score (100 = tie, 0 = landslide)")
    
    # Turnout Quartiles
    if 'turnout_rate' in df_analysis.columns:
        valid_turnout = df_analysis[df_analysis['turnout_rate'] > 0]['turnout_rate']
        if len(valid_turnout) > 3:  # Need at least 4 values for quartiles
            try:
                df_analysis['turnout_quartile'] = pd.qcut(
                    df_analysis['turnout_rate'], 
                    4, 
                    labels=['Low','Med-Low','Med-High','High'],
                    duplicates='drop'
                )
                print(f"  ✅ Added turnout_quartile (Low/Med-Low/Med-High/High)")
            except ValueError:
                # Try 3 bins
                try:
                    df_analysis['turnout_quartile'] = pd.qcut(
                        df_analysis['turnout_rate'],
                        3,
                        labels=['Low', 'Medium', 'High'],
                        duplicates='drop'
                    )
                    print(f"  ✅ Added turnout_quartile (Low/Medium/High)")
                except ValueError:
                    # Try 2 bins
                    try:
                        df_analysis['turnout_quartile'] = pd.qcut(
                            df_analysis['turnout_rate'],
                            2,
                            labels=['Low', 'High'],
                            duplicates='drop'
                        )
                        print(f"  ✅ Added turnout_quartile (Low/High)")
                    except ValueError:
                        # Use percentile-based approach instead
                        median_turnout = df_analysis['turnout_rate'].median()
                        df_analysis['turnout_quartile'] = np.where(
                            df_analysis['turnout_rate'] >= median_turnout,
                            'High',
                            'Low'
                        )
                        print(f"  ✅ Added turnout_quartile (Low/High based on median)")
        else:
            # Not enough data for quartiles
            df_analysis['turnout_quartile'] = 'Single'
            print(f"  ⚠️ Added turnout_quartile (Single category - insufficient data)")
    
    # Margin Categories
    if 'pct_victory_margin' in df_analysis.columns:
        df_analysis['margin_category'] = pd.cut(
            df_analysis['pct_victory_margin'], 
            bins=[0, 5, 15, 30, 100], 
            labels=['Very Close', 'Close', 'Clear', 'Landslide'],
            include_lowest=True
        )
        print(f"  ✅ Added margin_category (Very Close/Close/Clear/Landslide)")
    
    # Find leading and second place candidates - convert candidate columns to numeric first
    candidate_cols = [col for col in df_analysis.columns if col.startswith('votes_') and col != 'votes_total']
    
    # Convert candidate columns to numeric
    for col in candidate_cols:
        if col in df_analysis.columns:
            df_analysis[col] = pd.to_numeric(df_analysis[col], errors='coerce').fillna(0)
    
    if len(candidate_cols) >= 2:
        # Calculate leading and second place for dominance ratio
        df_analysis['votes_leading'] = 0
        df_analysis['votes_second_place'] = 0
        df_analysis['candidate_dominance'] = 1.0
        
        for idx, row in df_analysis.iterrows():
            candidate_votes = [(col, row[col]) for col in candidate_cols if pd.notna(row[col]) and row[col] > 0]
            candidate_votes.sort(key=lambda x: x[1], reverse=True)
            
            if len(candidate_votes) >= 2:
                leading_votes = candidate_votes[0][1]
                second_votes = candidate_votes[1][1]
                
                # Candidate Dominance Ratio
                df_analysis.loc[idx, 'votes_leading'] = leading_votes
                df_analysis.loc[idx, 'votes_second_place'] = second_votes
                
                if second_votes > 0:
                    df_analysis.loc[idx, 'candidate_dominance'] = leading_votes / second_votes
                else:
                    df_analysis.loc[idx, 'candidate_dominance'] = float('inf')
            elif len(candidate_votes) == 1:
                df_analysis.loc[idx, 'votes_leading'] = candidate_votes[0][1]
                df_analysis.loc[idx, 'votes_second_place'] = 0
                df_analysis.loc[idx, 'candidate_dominance'] = float('inf')
        
        print(f"  ✅ Added candidate_dominance (leading votes / second place votes)")
    
    # Registration vs Results Analysis - FIXED for percentage scale
    # Dynamically detect the Democratic-aligned candidate (usually first in list or based on naming)
    candidate_pct_cols = [col for col in df_analysis.columns if col.startswith('vote_pct_') and col != 'vote_pct_contribution_total_votes' and not col.startswith('vote_pct_contribution_')]
    
    if len(candidate_pct_cols) > 0 and 'reg_pct_dem' in df_analysis.columns:
        # Try to detect Democratic-aligned candidate intelligently
        # Look for common Democratic candidate name patterns or use the first candidate
        dem_candidate_col = None
        
        for col in candidate_pct_cols:
            candidate_name = col.replace('vote_pct_', '').lower()
            # Check for common Democratic-aligned name patterns
            if any(pattern in candidate_name for pattern in ['cavagnolo', 'sanchez', 'bautista']):
                dem_candidate_col = col
                break
        
        # If no pattern match, use the candidate with highest correlation to Democratic registration
        if dem_candidate_col is None and len(candidate_pct_cols) > 0:
            best_correlation = -1
            for col in candidate_pct_cols:
                valid_mask = df_analysis[col].notna() & df_analysis['reg_pct_dem'].notna()
                if valid_mask.sum() > 10:  # Need enough data points
                    correlation = df_analysis.loc[valid_mask, col].corr(df_analysis.loc[valid_mask, 'reg_pct_dem'])
                    if correlation > best_correlation:
                        best_correlation = correlation
                        dem_candidate_col = col
        
        # If still no candidate found, use the first one
        if dem_candidate_col is None:
            dem_candidate_col = candidate_pct_cols[0]
        
        # Calculate vote efficiency for the detected Democratic-aligned candidate
        if dem_candidate_col:
            candidate_name = dem_candidate_col.replace('vote_pct_', '')
            df_analysis['vote_efficiency_dem'] = np.where(
                df_analysis['reg_pct_dem'] > 0,
                df_analysis[dem_candidate_col] / df_analysis['reg_pct_dem'],
                0
            )
            print(f"  ✅ Added vote_efficiency_dem (how well Dems turned out for {candidate_name})")
    
    if 'reg_pct_dem' in df_analysis.columns and 'reg_pct_rep' in df_analysis.columns:
        df_analysis['registration_competitiveness'] = abs(df_analysis['reg_pct_dem'] - df_analysis['reg_pct_rep'])
        print(f"  ✅ Added registration_competitiveness (absolute difference in party registration)")
    
    if 'registration_competitiveness' in df_analysis.columns and 'pct_victory_margin' in df_analysis.columns:
        df_analysis['swing_potential'] = abs(df_analysis['registration_competitiveness'] - df_analysis['pct_victory_margin'])
        print(f"  ✅ Added swing_potential (difference between registration and actual competition)")
    
    # Additional analytical metrics
    if 'votes_total' in df_analysis.columns and 'TOTAL' in df_analysis.columns:
        # Voter engagement rate (different from turnout) - scale to 0-100
        df_analysis['engagement_rate'] = np.where(
            df_analysis['TOTAL'] > 0,
            (df_analysis['votes_total'] / df_analysis['TOTAL']) * 100,
            0
        )
        print(f"  ✅ Added engagement_rate (same as turnout_rate but explicit)")
    
    # VOTE PERCENTAGE CONTRIBUTION ANALYSIS - FIXED to use complete totals
    print(f"  🔍 Adding vote percentage contribution fields (using complete totals including county rollups)...")
    
    # Calculate COMPLETE totals including county rollups for accurate percentages
    # Find county rollup records and zone 1 precincts
    county_rollup_mask = df_analysis['precinct'].isin(['clackamas', 'washington'])
    zone1_mask = df_analysis['is_zone1_precinct'] == True
    
    # Calculate complete totals including county rollups
    complete_vote_mask = zone1_mask | county_rollup_mask
    total_votes_complete = df_analysis.loc[complete_vote_mask, 'votes_total'].sum() if complete_vote_mask.any() else 0
    
    if total_votes_complete > 0:
        print(f"  📊 Complete total votes (including county rollups): {total_votes_complete:,}")
        
        # Percentage of total votes this precinct contributed (for precincts only, not county rollups)
        df_analysis['vote_pct_contribution_total_votes'] = 0.0
        df_analysis.loc[zone1_mask, 'vote_pct_contribution_total_votes'] = (
            df_analysis.loc[zone1_mask, 'votes_total'] / total_votes_complete * 100
        )
        print(f"  ✅ Added vote_pct_contribution_total_votes (% of complete total votes from this precinct)")
        
        # Calculate candidate contribution percentages dynamically using complete totals
        candidate_cols = [col for col in df_analysis.columns if col.startswith('votes_') and col != 'votes_total']
        
        for candidate_col in candidate_cols:
            candidate_name = candidate_col.replace('votes_', '')
            # Use complete total including county rollups
            total_candidate_votes_complete = df_analysis.loc[complete_vote_mask, candidate_col].sum()
            
            if total_candidate_votes_complete > 0:
                contribution_col = f'vote_pct_contribution_{candidate_name}'
                df_analysis[contribution_col] = 0.0
                df_analysis.loc[zone1_mask, contribution_col] = (
                    df_analysis.loc[zone1_mask, candidate_col] / total_candidate_votes_complete * 100
                )
                
                # Verify calculation with sample
                sample_precincts = df_analysis[zone1_mask & (df_analysis[candidate_col] > 0)]
                if len(sample_precincts) > 0:
                    sample_idx = sample_precincts.index[0]
                    sample_votes = df_analysis.loc[sample_idx, candidate_col]
                    sample_pct = df_analysis.loc[sample_idx, contribution_col]
                    sample_precinct = df_analysis.loc[sample_idx, 'precinct']
                    print(f"  ✅ {candidate_name}: Sample precinct {sample_precinct} has {sample_votes} votes = {sample_pct:.2f}% of complete total ({total_candidate_votes_complete:,})")
    else:
        print(f"  ⚠️ No complete vote totals found for contribution calculations")
    
    # Precinct size categories
    if 'TOTAL' in df_analysis.columns:
        df_analysis['precinct_size_category'] = pd.cut(
            df_analysis['TOTAL'],
            bins=[0, 1000, 3000, 6000, float('inf')],
            labels=['Small', 'Medium', 'Large', 'Extra Large'],
            include_lowest=True
        )
        print(f"  ✅ Added precinct_size_category (Small/Medium/Large/Extra Large)")
    
    print(f"  📊 Added {len([col for col in df_analysis.columns if col not in df.columns])} new analytical fields")
    
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
    print(f"\n🗺️ Validating and reprojecting {source_description}:")

    # Check original CRS
    original_crs = gdf.crs
    print(f"  📍 Original CRS: {original_crs}")

    # Get CRS settings from config
    input_crs = config.get_system_setting("input_crs")
    output_crs = config.get_system_setting("output_crs")

    # Handle missing CRS
    if original_crs is None:
        print("  ⚠️ No CRS specified in data")

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
                    print(f"  🔍 Sample coordinates: x={x:.2f}, y={y:.2f}")

                    # Check if coordinates look like configured input CRS
                    if input_crs == "EPSG:2913" and abs(x) > 1000000 and abs(y) > 1000000:
                        print(f"  🎯 Coordinates appear to be {input_crs}")
                        gdf = gdf.set_crs(input_crs, allow_override=True)
                    # Check if coordinates look like WGS84 (longitude/latitude)
                    elif -180 <= x <= 180 and -90 <= y <= 90:
                        print(f"  🎯 Coordinates appear to be {output_crs}")
                        gdf = gdf.set_crs(output_crs, allow_override=True)
                    else:
                        print(f"  ❓ Unknown coordinate system, assuming {input_crs}")
                        gdf = gdf.set_crs(input_crs, allow_override=True)
                else:
                    print(f"  ❓ Could not extract sample coordinates, assuming {output_crs}")
                    gdf = gdf.set_crs(output_crs, allow_override=True)
            else:
                print(f"  ❓ No valid geometry found, assuming {output_crs}")
                gdf = gdf.set_crs(output_crs, allow_override=True)

    # Reproject to output CRS if needed
    current_crs = gdf.crs
    if current_crs is not None:
        try:
            current_epsg = current_crs.to_epsg()
            target_epsg = int(output_crs.split(":")[1])
            if current_epsg != target_epsg:
                print(f"  🔄 Reprojecting from EPSG:{current_epsg} to {output_crs}")
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
                            print(f"  ✓ Reprojected coordinates: lon={x:.6f}, lat={y:.6f}")

                            # Validate coordinates are in valid WGS84 range
                            if -180 <= x <= 180 and -90 <= y <= 90:
                                print("  ✓ Coordinates are valid WGS84")
                            else:
                                print(f"  ⚠️ Coordinates may be invalid: lon={x}, lat={y}")
                        else:
                            print("  ⚠️ Could not validate reprojected coordinates")

                gdf = gdf_reprojected
            else:
                print(f"  ✓ Already in {output_crs}")
        except Exception as e:
            print(f"  ❌ Error during reprojection: {e}")
            print(f"  🔧 Attempting to set CRS as {output_crs}")
            gdf = gdf.set_crs(output_crs, allow_override=True)

    # Final validation
    if gdf.crs is not None:
        try:
            final_epsg = gdf.crs.to_epsg()
            print(f"  ✅ Final CRS: EPSG:{final_epsg}")
        except Exception:
            print(f"  ✅ Final CRS: {gdf.crs}")
    else:
        print("  ⚠️ Warning: Final CRS is None")

    # Validate geometry
    valid_geom_count = gdf.geometry.notna().sum()
    total_count = len(gdf)
    print(
        f"  📊 Geometry validation: {valid_geom_count}/{total_count} features have valid geometry"
    )

    return gdf


def optimize_geojson_properties(gdf: gpd.GeoDataFrame, config: Config) -> gpd.GeoDataFrame:
    """
    Optimizes GeoDataFrame properties for web display and vector tile generation.

    Args:
        gdf: Input GeoDataFrame
        config: Configuration instance

    Returns:
        GeoDataFrame with optimized properties
    """
    print("\n🔧 Optimizing properties for web display:")

    # Create a copy to avoid modifying original
    gdf_optimized = gdf.copy()

    # Get precision settings from config
    config.get_system_setting("precision_decimals")
    prop_precision = config.get_system_setting("property_precision")

    # Clean up property names and values for web consumption
    columns_to_clean = gdf_optimized.columns.tolist()
    if "geometry" in columns_to_clean:
        columns_to_clean.remove("geometry")

    for col in columns_to_clean:
        if col in gdf_optimized.columns:
            # Handle different data types
            series = gdf_optimized[col]

            # Convert boolean columns stored as strings
            if col in [
                "is_zone1_precinct",
                "has_election_results",
                "has_voter_registration",
                "is_summary",
                "is_complete_record",
            ]:
                gdf_optimized[col] = (
                    series.astype(str)
                    .str.lower()
                    .map({"true": True, "false": False, "1": True, "0": False})
                    .fillna(False)
                )

            # Clean numeric columns - detect count columns dynamically
            elif col.startswith("votes_") or col in [
                "TOTAL",
                "DEM",
                "REP",
                "NAV",
                "vote_margin",
            ]:
                # Convert to int, handling NaN
                numeric_series = pd.to_numeric(series, errors="coerce").fillna(0)
                gdf_optimized[col] = numeric_series.astype(int)

            # Clean percentage/rate columns - detect percentage columns dynamically
            elif col.startswith("vote_pct_") or col in [
                "turnout_rate",
                "engagement_score",
                "margin_pct",
                "dem_advantage",
                "major_party_pct",
                "reg_pct_victory_margin",
                "reg_pct_competitiveness",
                "pct_victory_margin",
                "competitiveness_score",
                "vote_efficiency_dem",
                "registration_competitiveness",
                "swing_potential",
                "engagement_rate",
            ]:
                numeric_series = pd.to_numeric(series, errors="coerce").fillna(0)
                # Round to configured precision for web optimization
                gdf_optimized[col] = numeric_series.round(prop_precision)

            # Handle string columns - ensure they're proper strings
            elif col in [
                "political_lean",
                "competitiveness",
                "leading_candidate",
                "record_type",
                "turnout_quartile",
                "margin_category", 
                "precinct_size_category",
            ]:
                gdf_optimized[col] = series.astype(str).replace("nan", "").replace("None", "")
                # Replace empty strings with appropriate defaults
                if col == "political_lean":
                    gdf_optimized[col] = gdf_optimized[col].replace("", "Unknown")
                elif col == "competitiveness":
                    gdf_optimized[col] = gdf_optimized[col].replace("", "No Election Data")
                elif col == "leading_candidate":
                    gdf_optimized[col] = gdf_optimized[col].replace("", "No Data")

            # Handle precinct identifiers
            elif col in ["precinct", "Precinct", "base_precinct"]:
                gdf_optimized[col] = series.astype(str).str.strip()

    print(f"  ✓ Optimized {len(columns_to_clean)} property columns")

    # Add web-friendly geometry validation
    invalid_geom = gdf_optimized.geometry.isna() | (~gdf_optimized.geometry.is_valid)
    invalid_count = invalid_geom.sum()

    if invalid_count > 0:
        print(f"  ⚠️ Found {invalid_count} invalid geometries, attempting to fix...")
        # Try to fix invalid geometries
        gdf_optimized.geometry = gdf_optimized.geometry.buffer(0)

        # Check again
        still_invalid = gdf_optimized.geometry.isna() | (~gdf_optimized.geometry.is_valid)
        still_invalid_count = still_invalid.sum()

        if still_invalid_count > 0:
            print(f"  ⚠️ {still_invalid_count} geometries still invalid after fix attempt")
        else:
            print("  ✓ Fixed all invalid geometries")
    else:
        print("  ✓ All geometries are valid")

    return gdf_optimized


def tufte_map(
    gdf: gpd.GeoDataFrame,
    column: str,
    fname: Union[str, pathlib.Path],
    config: Config,
    cmap: str = None,
    title: str = "",
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    label: str = "",
    note: Optional[str] = None,
    diverging: bool = False,
    zoom_to_data: bool = False,
) -> None:
    """
    Generates and saves a minimalist Tufte-style map with optimized layout.

    Args:
        gdf: GeoDataFrame containing the data to plot.
        column: The name of the column in gdf to plot.
        fname: Filename (including path) to save the map.
        config: Configuration instance
        cmap: Colormap to use (uses config default if None).
        title: Title of the map.
        vmin: Minimum value for the color scale.
        vmax: Maximum value for the color scale.
        label: Label for the colorbar.
        note: Annotation note to display at the bottom of the map.
        diverging: Whether this is a diverging color scheme (centers on 0).
        zoom_to_data: If True, zoom to only areas with data in the specified column.
    """
    # Get visualization settings from config
    if cmap is None:
        # Use color-blind friendly palettes
        if diverging:
            cmap = 'RdBu_r'  # Color-blind friendly diverging
        else:
            cmap = 'viridis'  # Color-blind friendly sequential

    map_dpi = config.get_visualization_setting("map_dpi")
    figure_max_width = config.get_visualization_setting("figure_max_width")

    # Determine bounds - either full dataset or just areas with data
    if zoom_to_data:
        # Use only features that have valid data for this column
        data_features = gdf[gdf[column].notna() & (gdf[column] > 0)]
        if len(data_features) > 0:
            map_bounds = data_features.total_bounds
        else:
            map_bounds = gdf.total_bounds
    else:
        map_bounds = gdf.total_bounds

    # Calculate optimal figure size based on bounds aspect ratio
    data_width = map_bounds[2] - map_bounds[0]
    data_height = map_bounds[3] - map_bounds[1]
    aspect_ratio = data_width / data_height

    # Set figure size to match data aspect ratio (max from config)
    if aspect_ratio > 1:  # Wider than tall
        fig_width = min(figure_max_width, 10 * aspect_ratio)
        fig_height = fig_width / aspect_ratio
    else:  # Taller than wide
        fig_height = min(figure_max_width, 10 / aspect_ratio)
        fig_width = fig_height * aspect_ratio

    # Create figure with optimized size and DPI
    fig, ax = plt.subplots(figsize=(fig_width, fig_height), dpi=map_dpi)

    # Filter to only areas with data for better extent calculation
    data_gdf = gdf[gdf[column].notna()].copy()

    # Determine optimal vmin and vmax based on data distribution
    if vmin is None or vmax is None:
        data_values = data_gdf[column].dropna()
        if len(data_values) > 0:
            if diverging:
                # For diverging scales, center on 0 and use symmetric range
                abs_max = max(abs(data_values.min()), abs(data_values.max()))
                plot_vmin = -abs_max if vmin is None else vmin
                plot_vmax = abs_max if vmax is None else vmax
            else:
                # For sequential scales, use data range with slight padding
                data_range = data_values.max() - data_values.min()
                plot_vmin = data_values.min() - (data_range * 0.02) if vmin is None else vmin
                plot_vmax = data_values.max() + (data_range * 0.02) if vmax is None else vmax
        else:
            plot_vmin = 0
            plot_vmax = 1
    else:
        plot_vmin = vmin
        plot_vmax = vmax

    # Plot the map
    gdf.plot(
        column=column,
        cmap=cmap,
        linewidth=0.25,
        edgecolor="#444444",
        ax=ax,
        legend=False,
        vmin=plot_vmin,
        vmax=plot_vmax,
        missing_kwds={
            "color": "#f8f8f8",
            "edgecolor": "#cccccc",
            "hatch": "///",
            "linewidth": 0.25,
        },
    )

    # Set extent to optimal bounds (eliminate excessive white space)
    if zoom_to_data:
        data_features = gdf[gdf[column].notna() & (gdf[column] > 0)]
        if len(data_features) > 0:
            map_bounds = data_features.total_bounds
        else:
            map_bounds = gdf.total_bounds
    else:
        map_bounds = gdf.total_bounds

    # Be more aggressive about margin reduction - use minimal margins
    x_range = map_bounds[2] - map_bounds[0]
    y_range = map_bounds[3] - map_bounds[1]

    # Use tiny margins (1% instead of 5%) to maximize data area
    x_margin = x_range * 0.01
    y_margin = y_range * 0.01

    # Set tight bounds
    ax.set_xlim(map_bounds[0] - x_margin, map_bounds[2] + x_margin)
    ax.set_ylim(map_bounds[1] - y_margin, map_bounds[3] + y_margin)

    # Ensure equal aspect ratio to prevent distortion
    ax.set_aspect("equal")

    # Remove axes and spines for clean look
    ax.set_axis_off()

    # Add title with proper positioning and styling
    if title:
        fig.suptitle(title, fontsize=16, fontweight="bold", x=0.02, y=0.95, ha="left", va="top")

    # Create and position colorbar (optimized for tight bounds)
    if plot_vmax > plot_vmin:  # Only add colorbar if there's a range
        sm = mpl.cm.ScalarMappable(
            norm=mpl.colors.Normalize(vmin=plot_vmin, vmax=plot_vmax), cmap=cmap
        )

        # Position colorbar more precisely to avoid affecting map bounds
        cbar_ax = fig.add_axes(
            [0.92, 0.15, 0.02, 0.7]
        )  # [left, bottom, width, height] - thinner, further right
        cbar = fig.colorbar(sm, cax=cbar_ax)

        # Style the colorbar
        cbar.ax.tick_params(labelsize=10, colors="#333333")
        cbar.outline.set_edgecolor("#666666")
        cbar.outline.set_linewidth(0.5)

        # Add colorbar label
        if label:
            cbar.set_label(label, rotation=90, labelpad=12, fontsize=11, color="#333333")

    # Add note at bottom if provided
    if note:
        fig.text(
            0.02,
            0.02,
            note,
            ha="left",
            va="bottom",
            fontsize=9,
            color="#666666",
            style="italic",
            wrap=True,
        )

    # Save with optimized settings for maximum data area
    plt.savefig(
        fname,
        bbox_inches="tight",
        dpi=map_dpi,
        facecolor="white",
        edgecolor="none",
        pad_inches=0.02,
    )  # Minimal padding
    plt.close(fig)  # Close to free memory
    print(f"Map saved: {fname}")


# === Main Script Logic ===
def main() -> None:
    """
    Main function to load data, process it, and generate maps.
    """
    print("🗺️ Election Map Generation")
    print("=" * 60)

    # Load configuration
    try:
        config = Config()
        print(f"📋 Project: {config.get('project_name')}")
        print(f"📋 Description: {config.get('description')}")
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        print("💡 Make sure config.yaml exists in the analysis directory")
        return

    # Get file paths from configuration
    enriched_csv_path = config.get_enriched_csv_path()
    boundaries_path = config.get_input_path("boundaries_geojson")
    output_geojson_path = config.get_web_geojson_path()
    maps_dir = config.get_output_dir("maps")

    print("\nFile paths:")
    print(f"  📄 Enriched CSV: {enriched_csv_path}")
    print(f"  🗺️ Boundaries: {boundaries_path}")
    print(f"  💾 Output GeoJSON: {output_geojson_path}")
    print(f"  🗂️ Maps directory: {maps_dir}")

    # === 1. Load Data ===
    print("\nLoading data files:")
    try:
        df_raw = pd.read_csv(enriched_csv_path, dtype=str)
        print(f"  ✓ Loaded CSV with {len(df_raw)} rows")

        gdf = gpd.read_file(boundaries_path)
        print(f"  ✓ Loaded GeoJSON with {len(gdf)} features")

    except FileNotFoundError as e:
        print(f"❌ Error: Input file not found. {e}")
        return
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return

    # === 2. Data Filtering and Preprocessing ===
    print("\nData preprocessing and filtering:")

    # Get column names from configuration
    precinct_csv_col = config.get_column_name("precinct_csv")
    precinct_geojson_col = config.get_column_name("precinct_geojson")

    # Filter out summary/aggregate rows from CSV - BUT PRESERVE county rollups for totals calculation
    summary_precinct_ids = ["multnomah", "grand_total", ""]
    df = df_raw[~df_raw[precinct_csv_col].isin(summary_precinct_ids)].copy()
    print(
        f"  ✓ Filtered CSV: {len(df_raw)} → {len(df)} rows (removed {len(df_raw) - len(df)} summary rows, kept county rollups)"
    )

    # Separate regular precincts from county summary rows (PRESERVE county rollups)
    county_summaries = df[df[precinct_csv_col].isin(["clackamas", "washington"])]
    regular_precincts = df[~df[precinct_csv_col].isin(["clackamas", "washington"])]

    print(f"  📊 Regular precincts: {len(regular_precincts)}")
    print(f"  📊 County rollup rows: {len(county_summaries)} ({county_summaries[precinct_csv_col].tolist()})")

    # Separate Zone 1 participants from non-participants (only for regular precincts)
    zone1_participants = (
        regular_precincts[
            regular_precincts["is_zone1_precinct"].astype(str).str.lower() == "true"
        ]
        if "is_zone1_precinct" in regular_precincts.columns
        else regular_precincts
    )
    non_participants = (
        regular_precincts[
            regular_precincts["is_zone1_precinct"].astype(str).str.lower() == "false"
        ]
        if "is_zone1_precinct" in regular_precincts.columns
        else pd.DataFrame()
    )

    print(f"  📊 Zone 1 participants: {len(zone1_participants)} precincts")
    print(f"  📊 Non-participants: {len(non_participants)} precincts")
    print(f"  📊 Total Multnomah precincts: {len(regular_precincts)} precincts")

    # Validate vote totals against ground truth - INCLUDING county rollups
    if len(zone1_participants) > 0:
        candidate_cols = [col for col in zone1_participants.columns if col.startswith('votes_') and col != 'votes_total']
        
        print(f"\n🔍 Validating vote totals against ground truth (COMPLETE including county rollups):")
        
        # Calculate complete totals including county rollups
        zone1_votes = zone1_participants['votes_total'].astype(float).sum()
        county_votes = county_summaries['votes_total'].astype(float).sum() if len(county_summaries) > 0 else 0
        total_votes_complete = zone1_votes + county_votes
        
        print(f"  📊 COMPLETE totals (including county rollups):")
        print(f"    - Zone 1 precinct votes: {zone1_votes:,.0f}")
        print(f"    - County rollup votes: {county_votes:,.0f}")
        print(f"    - TOTAL votes: {total_votes_complete:,.0f}")
        
        for col in candidate_cols:
            if col in zone1_participants.columns:
                zone1_candidate_total = zone1_participants[col].astype(float).sum()
                county_candidate_total = county_summaries[col].astype(float).sum() if len(county_summaries) > 0 and col in county_summaries.columns else 0
                candidate_total_complete = zone1_candidate_total + county_candidate_total
                candidate_name = col.replace('votes_', '').title()
                percentage = (candidate_total_complete / total_votes_complete * 100) if total_votes_complete > 0 else 0
                print(f"    - {candidate_name}: {candidate_total_complete:,.0f} ({percentage:.2f}%)")

    print(f"  CSV precinct column: {df[precinct_csv_col].dtype}")
    print(f"  GeoJSON precinct column: {gdf[precinct_geojson_col].dtype}")

    # Robust join (strip zeros, lower, strip spaces)
    df[precinct_csv_col] = df[precinct_csv_col].astype(str).str.lstrip("0").str.strip().str.lower()
    gdf[precinct_geojson_col] = (
        gdf[precinct_geojson_col].astype(str).str.lstrip("0").str.strip().str.lower()
    )

    print(f"  Sample CSV precincts: {df[precinct_csv_col].head().tolist()}")
    print(f"  Sample GeoJSON precincts: {gdf[precinct_geojson_col].head().tolist()}")

    # Analyze matching before merge
    csv_precincts = set(df[precinct_csv_col].unique())
    geo_precincts = set(gdf[precinct_geojson_col].unique())

    print(f"  Unique CSV precincts: {len(csv_precincts)}")
    print(f"  Unique GeoJSON precincts: {len(geo_precincts)}")
    print(f"  Intersection: {len(csv_precincts & geo_precincts)}")

    csv_only = csv_precincts - geo_precincts
    geo_only = geo_precincts - csv_precincts
    if csv_only:
        # Filter out county rollups from "CSV-only" since they won't have GIS features
        csv_only_filtered = csv_only - {'clackamas', 'washington'}
        if csv_only_filtered:
            print(
                f"  ⚠️  CSV-only precincts (non-county): {sorted(csv_only_filtered)[:5]}{'...' if len(csv_only_filtered) > 5 else ''}"
            )
        print(f"  📋 County rollups not mapped (expected): {csv_only & {'clackamas', 'washington'}}")
    if geo_only:
        print(
            f"  ⚠️  GeoJSON-only precincts: {sorted(geo_only)[:5]}{'...' if len(geo_only) > 5 else ''}"
        )

    # MERGE: Only merge GIS features (exclude county rollups from GIS merge)
    df_for_gis = df[~df[precinct_csv_col].isin(['clackamas', 'washington'])].copy()
    gdf_merged = gdf.merge(df_for_gis, left_on=precinct_geojson_col, right_on=precinct_csv_col, how="left")
    print(f"  ✓ Merged GIS data: {len(gdf_merged)} features (excluding county rollups from GIS)")

    # CONSOLIDATE SPLIT PRECINCTS
    gdf_merged = consolidate_split_precincts(gdf_merged, precinct_geojson_col)

    # ADD ANALYTICAL FIELDS
    # Convert to DataFrame for analysis, then back to GeoDataFrame
    analysis_df = pd.DataFrame(gdf_merged.drop(columns='geometry'))
    analysis_df = add_analytical_fields(analysis_df)
    
    # Merge analytical fields back to GeoDataFrame
    analysis_cols = [col for col in analysis_df.columns if col not in gdf_merged.columns]
    for col in analysis_cols:
        gdf_merged[col] = analysis_df[col]

    # COORDINATE VALIDATION AND REPROJECTION
    print("\n🗺️ Coordinate System Processing:")
    gdf_merged = validate_and_reproject_to_wgs84(gdf_merged, config, "merged election data")

    # OPTIMIZE PROPERTIES FOR WEB
    gdf_merged = optimize_geojson_properties(gdf_merged, config)

    # Check for unmatched precincts
    matched = gdf_merged[~gdf_merged[precinct_csv_col].isna()]
    unmatched = gdf_merged[gdf_merged[precinct_csv_col].isna()]
    print(f"  ✓ Matched features: {len(matched)}")
    if len(unmatched) > 0:
        print(f"  ⚠️  Unmatched features: {len(unmatched)}")
        print(
            f"     Example unmatched GeoJSON precincts: {unmatched[precinct_geojson_col].head().tolist()}"
        )

    # Dynamically detect all columns to clean - FIXED for new percentage format
    print("\n🧹 Cleaning data columns (FIXED for percentage format):")

    # Clean all count columns dynamically
    count_cols = [col for col in gdf_merged.columns if col.startswith("votes_")]
    for col in count_cols:
        gdf_merged[col] = clean_numeric(gdf_merged[col], is_percent=False)
        valid_count = gdf_merged[col].notna().sum()
        if valid_count > 0:
            print(
                f"  ✓ Cleaned {col}: {valid_count} valid values, range: {gdf_merged[col].min():.0f} - {gdf_merged[col].max():.0f}"
            )

    # Clean all percentage columns dynamically - DON'T convert, they're already percentages
    pct_cols = [col for col in gdf_merged.columns if col.startswith(("vote_pct_", "reg_pct_"))]
    for col in pct_cols:
        gdf_merged[col] = clean_numeric(gdf_merged[col], is_percent=False)  # DON'T divide by 100
        valid_count = gdf_merged[col].notna().sum()
        if valid_count > 0:
            print(
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
            gdf_merged[col] = clean_numeric(gdf_merged[col], is_percent=False)  # Already percentages
            valid_count = gdf_merged[col].notna().sum()
            if valid_count > 0:
                # Show percentage sign for percentage fields
                if col in ['turnout_rate', 'dem_advantage', 'major_party_pct', 'pct_victory_margin', 'engagement_rate']:
                    print(
                        f"  ✓ Cleaned {col}: {valid_count} valid values, range: {gdf_merged[col].min():.1f}% - {gdf_merged[col].max():.1f}%"
                    )
                else:
                    print(
                        f"  ✓ Cleaned {col}: {valid_count} valid values, range: {gdf_merged[col].min():.3f} - {gdf_merged[col].max():.3f}"
                    )

    # Handle categorical columns - PRESERVE string values, do NOT convert to numeric
    categorical_cols = ["is_zone1_precinct", "political_lean", "competitiveness", "leading_candidate", "second_candidate", "turnout_quartile", "margin_category", "precinct_size_category", "record_type"]
    for col in categorical_cols:
        if col in gdf_merged.columns:
            # Special handling for boolean columns that may be stored as strings
            if col == "is_zone1_precinct":
                gdf_merged[col] = (
                    gdf_merged[col].astype(str).str.lower().map({"true": True, "false": False})
                )
            else:
                # For string categorical columns, ensure they stay as strings and clean up
                gdf_merged[col] = gdf_merged[col].astype(str)
                # Replace pandas/numpy string representations of missing values
                gdf_merged[col] = gdf_merged[col].replace(['nan', 'None', '<NA>', ''], 'No Data')
                
                # Set appropriate defaults for specific columns
                if col == "political_lean":
                    gdf_merged[col] = gdf_merged[col].replace("No Data", "Unknown")
                elif col == "competitiveness":
                    gdf_merged[col] = gdf_merged[col].replace("No Data", "No Election Data")
                elif col in ["leading_candidate", "second_candidate"]:
                    gdf_merged[col] = gdf_merged[col].replace("No Data", "No Data")

            value_counts = gdf_merged[col].value_counts()
            print(f"  ✓ {col} distribution: {dict(value_counts)}")

    # Final validation of consolidated vote totals - INCLUDING county rollups
    if len(zone1_participants) > 0:
        consolidated_zone1 = gdf_merged[gdf_merged["is_zone1_precinct"] == True]
        
        print(f"\n✅ Final vote totals after consolidation:")
        total_votes_final = consolidated_zone1['votes_total'].sum()
        
        # Add county rollup votes back to get complete totals
        county_rollup_votes = county_summaries['votes_total'].astype(float).sum() if len(county_summaries) > 0 else 0
        complete_total_final = total_votes_final + county_rollup_votes
        
        print(f"  📊 Zone 1 GIS features total votes: {total_votes_final:,.0f}")
        print(f"  📊 County rollup votes: {county_rollup_votes:,.0f}")
        print(f"  📊 COMPLETE total votes: {complete_total_final:,.0f}")
        
        for col in candidate_cols:
            if col in consolidated_zone1.columns:
                zone1_candidate_total = consolidated_zone1[col].sum()
                county_candidate_total = county_summaries[col].astype(float).sum() if len(county_summaries) > 0 and col in county_summaries.columns else 0
                candidate_total_complete = zone1_candidate_total + county_candidate_total
                candidate_name = col.replace('votes_', '').title()
                percentage = (candidate_total_complete / complete_total_final * 100) if complete_total_final > 0 else 0
                print(f"  📊 {candidate_name}: {candidate_total_complete:,.0f} ({percentage:.2f}%)")

        # Compare to ground truth
        print(f"\n🎯 Ground truth comparison:")
        print(f"  Expected - Splitt: 80,481 (81.78%), Cavagnolo: 16,000 (16.26%), Leof: 1,535 (1.56%)")
        print(f"  Total expected: 98,417 votes")
        
        accuracy_pct = (complete_total_final / 98417 * 100) if complete_total_final > 0 else 0
        print(f"  📊 Vote total accuracy: {accuracy_pct:.1f}% of expected")

    # === Competition Metrics Analysis ===
    print("\nAnalyzing pre-calculated competition metrics:")

    # The enriched dataset already has margin_pct, competitiveness, leading_candidate calculated
    if "margin_pct" in gdf_merged.columns:
        margin_stats = gdf_merged[gdf_merged["margin_pct"].notna()]["margin_pct"]
        if len(margin_stats) > 0:
            print(
                f"  ✓ Vote margins available: median {margin_stats.median():.1f}%, range {margin_stats.min():.1f}% - {margin_stats.max():.1f}%"
            )

    if "competitiveness" in gdf_merged.columns:
        comp_stats = gdf_merged["competitiveness"].value_counts()
        print(f"  📊 Competitiveness distribution: {dict(comp_stats)}")

    if "leading_candidate" in gdf_merged.columns:
        leader_stats = gdf_merged["leading_candidate"].value_counts()
        print(f"  📊 Leading candidate distribution: {dict(leader_stats)}")

    # Summary of Zone 1 vs Non-Zone 1
    if "is_zone1_precinct" in gdf_merged.columns:
        participated_count = gdf_merged[gdf_merged["is_zone1_precinct"]].shape[0]
        not_participated_count = gdf_merged[~gdf_merged["is_zone1_precinct"]].shape[0]
        print(
            f"  📊 Zone 1 participation: {participated_count} participated, {not_participated_count} did not participate"
        )

    # === 3. Save Merged GeoJSON ===
    try:
        print("\n💾 Saving optimized GeoJSON for web use:")

        # Ensure we have proper CRS before saving
        if gdf_merged.crs is None:
            print("  🔧 Setting WGS84 CRS for output")
            gdf_merged = gdf_merged.set_crs("EPSG:4326")

        # Calculate summary statistics for metadata
        zone1_features = (
            gdf_merged[gdf_merged.get("is_zone1_precinct", False)]
            if "is_zone1_precinct" in gdf_merged.columns
            else gdf_merged
        )
        total_votes_cast = (
            zone1_features["votes_total"].sum()
            if "votes_total" in zone1_features.columns
            else 0
        )

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

        # Add metadata object
        geojson_data["metadata"] = {
            "title": config.get("project_name"),
            "description": config.get("description"),
            "source": config.get_metadata("data_source"),
            "created": "2025-01-22",
            "crs": "EPSG:4326",
            "coordinate_system": "WGS84 Geographic",
            "features_count": len(gdf_merged),
            "zone1_features": len(zone1_features) if len(zone1_features) > 0 else 0,
            "total_votes_cast": int(total_votes_cast) if not pd.isna(total_votes_cast) else 0,
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
            ],
        }

        # Save the enhanced GeoJSON
        with open(output_geojson_path, "w") as f:
            json.dump(geojson_data, f, separators=(",", ":"))  # Compact formatting for web

        print(f"  ✓ Saved optimized GeoJSON: {output_geojson_path}")
        print(f"  📊 Features: {len(gdf_merged)}, CRS: EPSG:4326 (WGS84)")
        print(f"  🗳️ Zone 1 features: {len(zone1_features)}, Total votes: {int(total_votes_cast):,}")

    except Exception as e:
        print(f"  ❌ Error saving GeoJSON: {e}")
        return

    # === 4. Generate Maps ===
    print("\nGenerating maps with color-blind friendly palettes:")

    # 1. Zone 1 Participation Map
    if "is_zone1_precinct" in gdf_merged.columns:
        # Create a numeric version for plotting
        gdf_merged["is_zone1_numeric"] = gdf_merged["is_zone1_precinct"].astype(int)

        tufte_map(
            gdf_merged,
            "is_zone1_numeric",
            fname=maps_dir / "zone1_participation.png",
            config=config,
            cmap="viridis",
            title="Zone 1 Election Participation by Geographic Feature",
            label="Participated in Election",
            vmin=0,
            vmax=1,
            note="Dark areas participated in Zone 1 election, light areas did not",
        )

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

        tufte_map(
            gdf_merged,
            "political_lean_numeric",
            fname=maps_dir / "political_lean_all_precincts.png",
            config=config,
            cmap="RdBu_r",  # Color-blind friendly diverging
            title="Political Lean by Voter Registration (All Multnomah)",
            label="Political Lean",
            vmin=1,
            vmax=5,
            note="Based on voter registration patterns. Red=Republican lean, Blue=Democratic lean",
            diverging=True,
        )

    # 3. Democratic Registration Advantage
    if "dem_advantage" in gdf_merged.columns:
        tufte_map(
            gdf_merged,
            "dem_advantage",
            fname=maps_dir / "democratic_advantage_registration.png",
            config=config,
            title="Democratic Registration Advantage (All Multnomah)",
            label="Democratic Advantage",
            diverging=True,
            note="Blue areas have more Democratic registrations, red areas more Republican",
        )

    # 4. Total votes (Zone 1 only)
    if "votes_total" in gdf_merged.columns and not gdf_merged["votes_total"].isnull().all():
        has_votes = gdf_merged[gdf_merged["is_zone1_precinct"]]
        print(f"  📊 Total votes: {len(has_votes)} features with election data")

        tufte_map(
            gdf_merged,
            "votes_total",
            fname=maps_dir / "total_votes_zone1.png",
            config=config,
            cmap="plasma",  # Color-blind friendly
            title=f"Total Votes by Geographic Feature ({config.get('project_name')})",
            label="Number of Votes",
            vmin=0,
            zoom_to_data=True,
            note=f"Data available for {len(has_votes)} Zone 1 features. Zoomed to election area.",
        )

    # 5. Voter turnout (Zone 1 only)
    if "turnout_rate" in gdf_merged.columns and not gdf_merged["turnout_rate"].isnull().all():
        has_turnout = gdf_merged[
            gdf_merged["turnout_rate"].notna() & (gdf_merged["turnout_rate"] > 0)
        ]
        print(f"  📊 Turnout: {len(has_turnout)} features with turnout data")

        tufte_map(
            gdf_merged,
            "turnout_rate",
            fname=maps_dir / "voter_turnout_zone1.png",
            config=config,
            cmap="viridis",  # Color-blind friendly
            title=f"Voter Turnout by Geographic Feature ({config.get('project_name')})",
            label="Turnout Rate",
            vmin=0,
            vmax=0.4,
            zoom_to_data=True,
            note=f"Source: {config.get_metadata('attribution')}. Zoomed to Zone 1 election area.",
        )

    # 6. Candidate Vote Share Maps (Zone 1 only) - FULLY DYNAMIC FOR ANY CANDIDATES
    candidate_pct_cols = detect_candidate_columns(gdf_merged)
    candidate_cnt_cols = detect_candidate_count_columns(gdf_merged)

    for pct_col in candidate_pct_cols:
        if not gdf_merged[pct_col].isnull().all():
            candidate_name = pct_col.replace("vote_pct_", "").replace("_", " ").title()
            has_data = gdf_merged[gdf_merged[pct_col].notna()]
            print(f"  📊 {candidate_name} vote share: {len(has_data)} features with data")

            # Use safe filename (replace spaces and special characters)
            safe_filename = candidate_name.lower().replace(" ", "_").replace("-", "_")

            tufte_map(
                gdf_merged,
                pct_col,
                fname=maps_dir / f"{safe_filename}_vote_share.png",
                config=config,
                cmap="cividis",  # Color-blind friendly
                title=f"{candidate_name} Vote Share by Geographic Feature",
                label="Vote Share (%)",
                vmin=0,
                vmax=100,  # Use percentage scale
                zoom_to_data=True,
                note=f"Shows {candidate_name}'s performance in Zone 1 features. Zoomed to election area.",
            )

    # 7. New Analytical Maps
    
    # Victory Margin Percentage
    if "pct_victory_margin" in gdf_merged.columns and not gdf_merged["pct_victory_margin"].isnull().all():
        tufte_map(
            gdf_merged,
            "pct_victory_margin",
            fname=maps_dir / "victory_margin_percentage.png",
            config=config,
            cmap="plasma",
            title="Victory Margin Percentage by Geographic Feature",
            label="Victory Margin %",
            zoom_to_data=True,
            note="Higher values = larger victory margins. Darker = less competitive.",
        )

    # Competitiveness Score
    if "competitiveness_score" in gdf_merged.columns and not gdf_merged["competitiveness_score"].isnull().all():
        tufte_map(
            gdf_merged,
            "competitiveness_score",
            fname=maps_dir / "competitiveness_score.png",
            config=config,
            cmap="viridis",
            title="Election Competitiveness Score by Geographic Feature",
            label="Competitiveness Score",
            zoom_to_data=True,
            note="Higher scores = more competitive elections. Dark = tight races, light = landslides.",
        )

    # Vote Efficiency (Democratic)
    if "vote_efficiency_dem" in gdf_merged.columns and not gdf_merged["vote_efficiency_dem"].isnull().all():
        tufte_map(
            gdf_merged,
            "vote_efficiency_dem",
            fname=maps_dir / "democratic_vote_efficiency.png",
            config=config,
            cmap="RdBu_r",
            title="Democratic Vote Efficiency (Cavagnolo Performance vs Registration)",
            label="Vote Efficiency",
            zoom_to_data=True,
            diverging=True,
            note="How well Democratic registrations converted to Cavagnolo votes. Blue = high efficiency.",
        )

    # Swing Potential
    if "swing_potential" in gdf_merged.columns and not gdf_merged["swing_potential"].isnull().all():
        tufte_map(
            gdf_merged,
            "swing_potential",
            fname=maps_dir / "swing_potential.png",
            config=config,
            cmap="inferno",
            title="Electoral Swing Potential by Geographic Feature",
            label="Swing Potential",
            zoom_to_data=True,
            note="Difference between registration competitiveness and actual results. Higher = more volatile.",
        )

    print("\n✅ Script completed successfully!")
    print(f"   Maps saved to: {maps_dir}")
    print(f"   GeoJSON saved to: {output_geojson_path}")
    print(
        f"   Summary: {len(matched)} features with election data out of {len(gdf_merged)} total features"
    )

    # Summary of generated maps
    candidate_count = len(candidate_pct_cols)
    analytical_maps_count = 4  # Victory margin, competitiveness, vote efficiency, swing potential
    base_maps_count = 6  # Zone 1 participation, political lean, dem advantage, total votes, turnout, analytical maps
    total_maps = base_maps_count + candidate_count + analytical_maps_count

    print(f"\n🗺️ Generated {total_maps} maps:")
    print("   1. Zone 1 Participation Map")
    print("   2. Political Lean (All Multnomah)")
    print("   3. Democratic Registration Advantage")
    print("   4. Total votes (Zone 1 only)")
    print("   5. Voter turnout (Zone 1 only)")
    
    map_counter = 6
    for pct_col in candidate_pct_cols:
        candidate_name = pct_col.replace("vote_pct_", "").replace("_", " ").title()
        print(f"   {map_counter}. {candidate_name} Vote Share (Zone 1 only)")
        map_counter += 1
    
    print(f"   {map_counter}. Victory Margin Percentage")
    print(f"   {map_counter+1}. Competitiveness Score")
    print(f"   {map_counter+2}. Democratic Vote Efficiency")
    print(f"   {map_counter+3}. Electoral Swing Potential")


if __name__ == "__main__":
    main()
