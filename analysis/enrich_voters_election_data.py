import numpy as np
import pandas as pd
from config_loader import Config


def load_and_clean_data(config: Config):
    """Load and perform initial cleaning of both datasets using config."""
    print("📊 Loading data files from configuration...")

    # Get file paths from configuration
    voters_path = config.get_input_path('voters_csv')
    votes_path = config.get_input_path('votes_csv')

    print(f"  📄 Voters file: {voters_path}")
    print(f"  📄 Votes file: {votes_path}")

    # Load voters data
    voters_df = pd.read_csv(voters_path)
    print(f"  ✓ Loaded voters data: {len(voters_df)} precincts")

    # Load votes data
    votes_df = pd.read_csv(votes_path)
    print(f"  ✓ Loaded votes data: {len(votes_df)} records")

    # Get column names from configuration
    precinct_col = config.get_column_name('precinct_csv')
    total_votes_col = config.get_column_name('total_votes')

    # Standardize precinct column names and data types for merging
    if 'Precinct' in voters_df.columns:
        voters_df = voters_df.rename(columns={'Precinct': precinct_col})

    # Convert both precinct columns to string to ensure they match
    voters_df[precinct_col] = voters_df[precinct_col].astype(str)
    votes_df[precinct_col] = votes_df[precinct_col].astype(str)

    print(f"  🔗 Using precinct column: '{precinct_col}'")
    print(f"  📊 Sample voters precincts: {voters_df[precinct_col].head().tolist()}")
    print(f"  📊 Sample votes precincts: {votes_df[precinct_col].head().tolist()}")

    return voters_df, votes_df


def detect_candidate_columns(df: pd.DataFrame) -> list:
    """Detect all candidate columns from the dataset."""
    candidate_cols = [col for col in df.columns if col.startswith('candidate_')]
    print(f"  📊 Detected candidate columns: {candidate_cols}")
    return candidate_cols


def calculate_voter_metrics(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    """Calculate voter registration metrics using configuration."""
    print("\n📈 Calculating voter registration metrics:")

    # Get column names from configuration
    total_voters_col = config.get_column_name('total_voters')
    dem_col = config.get_column_name('dem_registration')
    rep_col = config.get_column_name('rep_registration')
    nav_col = config.get_column_name('nav_registration')

    # Party registration percentages
    party_cols = [dem_col, rep_col, nav_col, "OTH", "CON", "IND", "LBT", "NLB", "PGP", "PRO", "WFP", "WTP"]

    for party in party_cols:
        if party in df.columns:
            df[f"pct_{party.lower()}"] = df[party] / df[total_voters_col]

    # Major party metrics using config column names
    df["dem_advantage"] = df[f"pct_{dem_col.lower()}"] - df[f"pct_{rep_col.lower()}"]
    df["major_party_pct"] = df[f"pct_{dem_col.lower()}"] + df[f"pct_{rep_col.lower()}"]
    df["nav_pct_rank"] = df[f"pct_{nav_col.lower()}"]  # Non-affiliated voters

    # Get thresholds from configuration
    strong_threshold = config.get_analysis_setting('strong_advantage')
    lean_threshold = config.get_analysis_setting('lean_advantage')

    # Political lean categories using configured thresholds
    conditions = [
        df["dem_advantage"] > strong_threshold,
        df["dem_advantage"] > lean_threshold,
        df["dem_advantage"] > -lean_threshold,
        df["dem_advantage"] > -strong_threshold,
        df["dem_advantage"] <= -strong_threshold,
    ]
    choices = ["Strong Dem", "Lean Dem", "Competitive", "Lean Rep", "Strong Rep"]
    df["political_lean"] = np.select(conditions, choices, default="Unknown")

    print(f"  ✓ Added party registration percentages for {len([c for c in party_cols if c in df.columns])} parties")
    print(f"  ✓ Used thresholds: Strong={strong_threshold:.0%}, Lean={lean_threshold:.0%}")
    print("  ✓ Added political lean classification")

    return df


def calculate_election_metrics(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    """Calculate election-specific metrics using configuration."""
    print("\n🗳️ Calculating election metrics:")

    # Get column names from configuration
    total_votes_col = config.get_column_name('total_votes')
    total_voters_col = config.get_column_name('total_voters')

    # Detect candidate columns dynamically
    candidate_cols = detect_candidate_columns(df)
    
    if not candidate_cols:
        print("  ⚠️ No candidate columns found!")
        return df

    # Rename candidate columns to standardized format
    candidate_mapping = {}
    for col in candidate_cols:
        candidate_name = col.replace('candidate_', '')
        new_col = f"cnt_{candidate_name}"
        candidate_mapping[col] = new_col
        df[new_col] = df[col]

    print(f"  ✓ Standardized candidate columns: {list(candidate_mapping.values())}")

    # Rename total_votes to standardized format
    if total_votes_col in df.columns:
        df['cnt_total_votes'] = df[total_votes_col]

    # Election participation flag
    df["participated_election"] = df["cnt_total_votes"].notna() & (df["cnt_total_votes"] > 0)

    # Calculate vote shares for all candidates
    mask = df["participated_election"]
    
    for col in candidate_mapping.values():
        candidate_name = col.replace('cnt_', '')
        pct_col = f"pct_{candidate_name}"
        df[pct_col] = np.nan
        df.loc[mask, pct_col] = df.loc[mask, col] / df.loc[mask, "cnt_total_votes"]

    print(f"  ✓ Calculated vote shares for {len(candidate_mapping)} candidates")

    # Turnout calculation
    df["turnout_rate"] = np.nan
    df.loc[mask, "turnout_rate"] = df.loc[mask, "cnt_total_votes"] / df.loc[mask, total_voters_col]

    # Competition metrics using configured thresholds
    df = calculate_competition_metrics(df, list(candidate_mapping.values()), config)

    print("  ✓ Added election participation flags")
    print("  ✓ Added vote shares and turnout calculations")
    print("  ✓ Added competition metrics")

    return df


def calculate_competition_metrics(df: pd.DataFrame, candidate_count_cols: list, config: Config) -> pd.DataFrame:
    """Calculate competition metrics using configured thresholds."""
    print("  📊 Calculating competition metrics...")
    
    # Get thresholds from configuration
    competitive_threshold = config.get_analysis_setting('competitive_threshold')
    tossup_threshold = config.get_analysis_setting('tossup_threshold')
    
    mask = df["participated_election"]
    
    # Initialize competition columns
    df["vote_margin"] = np.nan
    df["margin_pct"] = np.nan
    df["leading_candidate"] = "No Election Data"
    df["second_candidate"] = "No Election Data"
    df["competitiveness"] = "No Election Data"

    # For each precinct with election data, find top 2 candidates
    for idx in df[mask].index:
        candidate_votes = {}
        for col in candidate_count_cols:
            candidate_name = col.replace('cnt_', '')
            votes = df.loc[idx, col]
            if pd.notna(votes):
                candidate_votes[candidate_name] = votes
        
        if len(candidate_votes) >= 2:
            # Sort candidates by vote count (descending)
            sorted_candidates = sorted(candidate_votes.items(), key=lambda x: x[1], reverse=True)
            
            # Get top 2
            first_candidate, first_votes = sorted_candidates[0]
            second_candidate, second_votes = sorted_candidates[1]
            
            # Calculate margin
            vote_margin = first_votes - second_votes
            total_votes = df.loc[idx, "cnt_total_votes"]
            
            df.loc[idx, "leading_candidate"] = first_candidate.title()
            df.loc[idx, "second_candidate"] = second_candidate.title()
            df.loc[idx, "vote_margin"] = vote_margin
            df.loc[idx, "margin_pct"] = vote_margin / total_votes if total_votes > 0 else 0
            
        elif len(candidate_votes) == 1:
            # Only one candidate with votes
            candidate_name = list(candidate_votes.keys())[0]
            df.loc[idx, "leading_candidate"] = candidate_name.title()
            df.loc[idx, "vote_margin"] = candidate_votes[candidate_name]
            df.loc[idx, "margin_pct"] = 1.0  # 100% margin

    # Competitiveness classification using configured thresholds
    competitive_mask = mask & (df["margin_pct"] < competitive_threshold)
    tossup_mask = mask & (df["margin_pct"] < tossup_threshold)
    safe_mask = mask & (df["margin_pct"] >= competitive_threshold)

    df.loc[tossup_mask, "competitiveness"] = "Toss-up"
    df.loc[competitive_mask & ~tossup_mask, "competitiveness"] = "Competitive"
    df.loc[safe_mask, "competitiveness"] = "Safe"

    print(f"    - Used thresholds: Competitive={competitive_threshold:.0%}, Toss-up={tossup_threshold:.0%}")
    print(f"    - Calculated margins for {mask.sum()} precincts with election data")
    
    return df


def calculate_cross_metrics(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    """Calculate cross-analysis metrics using configuration."""
    print("\n🔄 Calculating cross-analysis metrics:")

    mask = df["participated_election"]

    # Get engagement scoring weights from configuration
    diversity_weight = config.get_analysis_setting('registration_diversity_weight')
    turnout_weight = config.get_analysis_setting('turnout_weight')

    # Performance vs registration metrics
    df["leading_performance_vs_dem"] = np.nan
    df["leading_performance_vs_rep"] = np.nan

    for idx in df[mask].index:
        leading_candidate = df.loc[idx, "leading_candidate"]
        if leading_candidate != "No Election Data":
            leading_candidate_lower = leading_candidate.lower()
            leading_pct_col = f"pct_{leading_candidate_lower}"
            
            if leading_pct_col in df.columns:
                leading_pct = df.loc[idx, leading_pct_col]
                dem_pct = df.loc[idx, "pct_dem"]
                rep_pct = df.loc[idx, "pct_rep"]
                
                if pd.notna(leading_pct) and pd.notna(dem_pct):
                    df.loc[idx, "leading_performance_vs_dem"] = leading_pct - dem_pct
                if pd.notna(leading_pct) and pd.notna(rep_pct):
                    df.loc[idx, "leading_performance_vs_rep"] = leading_pct - rep_pct

    # Turnout analysis
    df["high_turnout"] = np.nan
    df.loc[mask, "high_turnout"] = (
        df.loc[mask, "turnout_rate"] > df.loc[mask, "turnout_rate"].median()
    ).astype(float)

    # Engagement score using configured weights
    df["engagement_score"] = np.nan
    df.loc[mask, "engagement_score"] = (
        (1 - df.loc[mask, "major_party_pct"]) * diversity_weight  # Registration diversity
        + df.loc[mask, "turnout_rate"] * turnout_weight  # Turnout
    )

    print(f"  ✓ Used engagement weights: diversity={diversity_weight}, turnout={turnout_weight}")
    print("  ✓ Added performance vs registration metrics")
    print("  ✓ Added engagement scoring")

    return df


def add_metadata_columns(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    """Add metadata and classification columns."""
    print("\n📋 Adding metadata columns:")

    precinct_col = config.get_column_name('precinct_csv')

    # Record type classification
    county_records = ["clackamas", "washington"]
    df["record_type"] = "precinct"
    df.loc[df[precinct_col].isin(county_records), "record_type"] = "county_summary"

    # Zone coverage flag
    df["in_zone1"] = df["participated_election"] | df["record_type"].eq("county_summary")

    # Data completeness flags
    total_voters_col = config.get_column_name('total_voters')
    df["has_voter_data"] = df[total_voters_col].notna()
    df["has_election_data"] = df["cnt_total_votes"].notna()
    df["complete_record"] = df["has_voter_data"] & df["has_election_data"]

    print("  ✓ Added record type classifications")
    print("  ✓ Added data completeness flags")

    return df


def main():
    """Main function using configuration system."""
    print("🗳️ Election Data Enrichment")
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

    # Load data using configuration
    try:
        voters_df, votes_df = load_and_clean_data(config)
    except Exception as e:
        print(f"❌ Data loading failed: {e}")
        return

    # Get column name for merging
    precinct_col = config.get_column_name('precinct_csv')

    # Perform full outer join
    print(f"\n🔗 Performing full outer join on '{precinct_col}':")
    merged_df = pd.merge(voters_df, votes_df, on=precinct_col, how="outer")
    print(f"  ✓ Merged dataset: {len(merged_df)} records")

    # Identify merge results
    total_votes_col = config.get_column_name('total_votes')
    total_voters_col = config.get_column_name('total_voters')
    
    voters_only = merged_df[merged_df[total_votes_col].isna()]
    votes_only = merged_df[merged_df[total_voters_col].isna()]
    both_data = merged_df[merged_df[total_voters_col].notna() & merged_df[total_votes_col].notna()]

    print(f"  📊 Voters-only records: {len(voters_only)} (precincts not in election)")
    print(f"  📊 Votes-only records: {len(votes_only)} (summary records)")
    print(f"  📊 Complete records: {len(both_data)} (precincts with both datasets)")

    # Calculate enriched metrics using configuration
    enriched_df = calculate_voter_metrics(merged_df, config)
    enriched_df = calculate_election_metrics(enriched_df, config)
    enriched_df = calculate_cross_metrics(enriched_df, config)
    enriched_df = add_metadata_columns(enriched_df, config)

    # Clean up original columns
    candidate_cols = [col for col in enriched_df.columns if col.startswith('candidate_')]
    cols_to_drop = candidate_cols + [total_votes_col]
    enriched_df = enriched_df.drop(
        columns=[col for col in cols_to_drop if col in enriched_df.columns]
    )

    # Save enriched dataset using configuration
    output_path = config.get_enriched_csv_path()
    enriched_df.to_csv(output_path, index=False)

    # Generate summary statistics
    print("\n📈 Summary Statistics:")
    print(f"   • Total records: {len(enriched_df)}")
    print(f"   • Precincts with voter data: {enriched_df['has_voter_data'].sum()}")
    print(f"   • Precincts with election data: {enriched_df['has_election_data'].sum()}")
    print(f"   • Complete records: {enriched_df['complete_record'].sum()}")

    if len(both_data) > 0:
        avg_turnout = enriched_df.loc[enriched_df['participated_election'], 'turnout_rate'].mean()
        print(f"   • Average turnout: {avg_turnout:.1%}")
        
        # Competition summary
        comp_summary = enriched_df["competitiveness"].value_counts()
        print("   • Competitiveness distribution:")
        for comp_level, count in comp_summary.items():
            if comp_level != "No Election Data":
                print(f"     - {comp_level}: {count} precincts")

        # Leading candidate summary
        leader_summary = enriched_df["leading_candidate"].value_counts()
        print("   • Leading candidate distribution:")
        for candidate, count in leader_summary.items():
            if candidate != "No Election Data":
                print(f"     - {candidate}: {count} precincts")

    print(f"\n✅ Enriched dataset saved to: {output_path}")
    print(f"   📄 Columns: {len(enriched_df.columns)}")
    print(f"   📋 Sample columns: {list(enriched_df.columns[:10])}")


if __name__ == "__main__":
    main()
