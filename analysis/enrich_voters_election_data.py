import numpy as np
import pandas as pd
from config_loader import Config


def load_and_clean_data(config: Config):
    """Load and perform initial cleaning of both datasets using config."""
    print("📊 Loading data files from configuration...")

    # Get file paths from configuration
    voters_path = config.get_input_path("voters_csv")
    votes_path = config.get_input_path("votes_csv")

    print(f"  📄 Voters file: {voters_path}")
    print(f"  📄 Votes file: {votes_path}")

    # Load voters data
    voters_df = pd.read_csv(voters_path)
    print(f"  ✓ Loaded voters data: {len(voters_df)} precincts")

    # Load votes data
    votes_df = pd.read_csv(votes_path)
    print(f"  ✓ Loaded votes data: {len(votes_df)} records")

    # Get column names from configuration
    precinct_col = config.get_column_name("precinct_csv")

    # Standardize precinct column names
    if "Precinct" in voters_df.columns:
        voters_df = voters_df.rename(columns={"Precinct": precinct_col})

    # Convert both precinct columns to string to ensure they match
    voters_df[precinct_col] = voters_df[precinct_col].astype(str)
    votes_df[precinct_col] = votes_df[precinct_col].astype(str)

    print(f"  🔗 Using precinct column: '{precinct_col}'")
    print(f"  📊 Sample voters precincts: {voters_df[precinct_col].head().tolist()}")
    print(f"  📊 Sample votes precincts: {votes_df[precinct_col].head().tolist()}")

    return voters_df, votes_df


def detect_and_standardize_candidates(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """Detect candidate columns and create both original and standardized versions."""
    print("\n📊 Detecting and standardizing candidate columns:")
    
    # Find all candidate columns
    candidate_cols = [col for col in df.columns if col.startswith("candidate_")]
    print(f"  ✓ Found candidate columns: {candidate_cols}")
    
    standardized_cols = []
    
    # Create standardized count columns while preserving originals
    for col in candidate_cols:
        candidate_name = col.replace("candidate_", "")
        standardized_col = f"votes_{candidate_name}"  # More intuitive than cnt_
        df[standardized_col] = df[col].fillna(0).astype(int)
        standardized_cols.append(standardized_col)
        print(f"  ✓ Created {standardized_col} from {col}")
    
    # Standardize total_votes if it exists
    if 'total_votes' in df.columns:
        df['votes_total'] = df['total_votes'].fillna(0).astype(int)
        print(f"  ✓ Created votes_total from total_votes")
    
    return df, standardized_cols


def add_record_classification(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    """Add clear record type classification and flags."""
    print("\n🏷️ Adding record classification:")
    
    precinct_col = config.get_column_name("precinct_csv")
    
    # Clear boolean flags for different record types
    df['is_summary'] = df[precinct_col].isin(['clackamas', 'washington'])
    df['is_zone1_precinct'] = df['votes_total'].notna() & (df['votes_total'] > 0) & ~df['is_summary']
    df['is_non_zone1_precinct'] = df['TOTAL'].notna() & (df['votes_total'].isna() | (df['votes_total'] == 0)) & ~df['is_summary']
    
    # Overall record type for clarity
    df['record_type'] = 'unknown'
    df.loc[df['is_summary'], 'record_type'] = 'county_summary'
    df.loc[df['is_zone1_precinct'], 'record_type'] = 'zone1_precinct'
    df.loc[df['is_non_zone1_precinct'], 'record_type'] = 'other_precinct'
    
    # Data availability flags
    df['has_voter_registration'] = df['TOTAL'].notna() & (df['TOTAL'] > 0)
    df['has_election_results'] = df['votes_total'].notna() & (df['votes_total'] > 0)
    df['is_complete_record'] = df['has_voter_registration'] & df['has_election_results']
    
    print(f"  ✓ Summary records: {df['is_summary'].sum()}")
    print(f"  ✓ Zone 1 precincts: {df['is_zone1_precinct'].sum()}")
    print(f"  ✓ Other precincts: {df['is_non_zone1_precinct'].sum()}")
    print(f"  ✓ Complete records: {df['is_complete_record'].sum()}")
    
    return df


def calculate_voter_metrics(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    """Calculate voter registration metrics."""
    print("\n📈 Calculating voter registration metrics:")

    # Only calculate for records with voter data
    mask = df['has_voter_registration']
    
    if not mask.any():
        print("  ⚠️ No records with voter registration data found!")
        return df

    # Party registration percentages (more intuitive names)
    party_cols = ['DEM', 'REP', 'NAV', 'OTH', 'CON', 'IND', 'LBT', 'NLB', 'PGP', 'PRO', 'WFP', 'WTP']
    
    for party in party_cols:
        if party in df.columns:
            pct_col = f"reg_pct_{party.lower()}"
            df[pct_col] = 0.0
            df.loc[mask, pct_col] = df.loc[mask, party] / df.loc[mask, 'TOTAL']
            print(f"  ✓ Added {pct_col}")

    # Political lean metrics
    df['dem_advantage'] = 0.0
    df['major_party_pct'] = 0.0
    df.loc[mask, 'dem_advantage'] = df.loc[mask, 'reg_pct_dem'] - df.loc[mask, 'reg_pct_rep']
    df.loc[mask, 'major_party_pct'] = df.loc[mask, 'reg_pct_dem'] + df.loc[mask, 'reg_pct_rep']

    # Political lean categories
    strong_threshold = config.get_analysis_setting("strong_advantage")
    lean_threshold = config.get_analysis_setting("lean_advantage")

    df['political_lean'] = 'No Data'
    conditions = [
        mask & (df['dem_advantage'] > strong_threshold),
        mask & (df['dem_advantage'] > lean_threshold),
        mask & (df['dem_advantage'] > -lean_threshold),
        mask & (df['dem_advantage'] > -strong_threshold),
        mask & (df['dem_advantage'] <= -strong_threshold),
    ]
    choices = ['Strong Dem', 'Lean Dem', 'Competitive', 'Lean Rep', 'Strong Rep']
    df['political_lean'] = np.select(conditions, choices, default='No Data')

    print(f"  ✓ Calculated metrics for {mask.sum()} records with voter data")
    
    return df


def calculate_election_metrics(df: pd.DataFrame, candidate_cols: list, config: Config) -> pd.DataFrame:
    """Calculate election-specific metrics."""
    print("\n🗳️ Calculating election metrics:")

    # Only calculate for records with election data
    mask = df['has_election_results']
    
    if not mask.any():
        print("  ⚠️ No records with election results found!")
        return df

    # Turnout calculation
    df['turnout_rate'] = 0.0
    valid_turnout_mask = mask & df['has_voter_registration']
    df.loc[valid_turnout_mask, 'turnout_rate'] = (
        df.loc[valid_turnout_mask, 'votes_total'] / df.loc[valid_turnout_mask, 'TOTAL']
    )

    # Candidate vote percentages (more intuitive names)
    for col in candidate_cols:
        candidate_name = col.replace('votes_', '')
        pct_col = f"vote_pct_{candidate_name}"
        df[pct_col] = 0.0
        df.loc[mask, pct_col] = df.loc[mask, col] / df.loc[mask, 'votes_total']
        print(f"  ✓ Added {pct_col}")

    # Competition metrics
    df = calculate_competition_metrics(df, candidate_cols, config)

    print(f"  ✓ Calculated metrics for {mask.sum()} records with election data")
    
    return df


def calculate_competition_metrics(df: pd.DataFrame, candidate_cols: list, config: Config) -> pd.DataFrame:
    """Calculate competition metrics."""
    print("  📊 Calculating competition metrics...")

    mask = df['has_election_results']
    
    # Initialize competition columns
    df['vote_margin'] = 0
    df['margin_pct'] = 0.0
    df['leading_candidate'] = 'No Data'
    df['second_candidate'] = 'No Data'
    df['competitiveness'] = 'No Data'
    df['is_competitive'] = False

    # Get thresholds from configuration
    competitive_threshold = config.get_analysis_setting("competitive_threshold")
    tossup_threshold = config.get_analysis_setting("tossup_threshold")

    # For each precinct with election data, find top 2 candidates
    for idx in df[mask].index:
        candidate_votes = {}
        for col in candidate_cols:
            candidate_name = col.replace('votes_', '')
            votes = df.loc[idx, col]
            if pd.notna(votes) and votes > 0:
                candidate_votes[candidate_name] = votes

        if len(candidate_votes) >= 2:
            # Sort candidates by vote count (descending)
            sorted_candidates = sorted(candidate_votes.items(), key=lambda x: x[1], reverse=True)

            # Get top 2
            first_candidate, first_votes = sorted_candidates[0]
            second_candidate, second_votes = sorted_candidates[1]

            # Calculate margin
            vote_margin = first_votes - second_votes
            total_votes = df.loc[idx, 'votes_total']

            df.loc[idx, 'leading_candidate'] = first_candidate.title()
            df.loc[idx, 'second_candidate'] = second_candidate.title()
            df.loc[idx, 'vote_margin'] = vote_margin
            df.loc[idx, 'margin_pct'] = vote_margin / total_votes if total_votes > 0 else 0

        elif len(candidate_votes) == 1:
            # Only one candidate with votes
            candidate_name = list(candidate_votes.keys())[0]
            df.loc[idx, 'leading_candidate'] = candidate_name.title()
            df.loc[idx, 'vote_margin'] = candidate_votes[candidate_name]
            df.loc[idx, 'margin_pct'] = 1.0  # 100% margin

    # Competitiveness classification
    df.loc[mask & (df['margin_pct'] < tossup_threshold), 'competitiveness'] = 'Toss-up'
    df.loc[mask & (df['margin_pct'] >= tossup_threshold) & (df['margin_pct'] < competitive_threshold), 'competitiveness'] = 'Competitive'
    df.loc[mask & (df['margin_pct'] >= competitive_threshold), 'competitiveness'] = 'Safe'
    
    # Boolean flag for competitive races
    df.loc[mask, 'is_competitive'] = df.loc[mask, 'margin_pct'] < competitive_threshold

    print(f"    - Calculated margins for {mask.sum()} precincts with election data")
    
    return df


def add_summary_statistics(df: pd.DataFrame) -> pd.DataFrame:
    """Add helpful summary columns for analysis."""
    print("\n📊 Adding summary statistics:")
    
    # Zone 1 specific stats
    zone1_mask = df['is_zone1_precinct']
    
    if zone1_mask.any():
        # Total vote counts across all Zone 1 precincts
        total_zone1_votes = df.loc[zone1_mask, 'votes_total'].sum()
        df['zone1_total_votes'] = total_zone1_votes
        
        # Vote share of each precinct within Zone 1
        df['zone1_vote_share'] = 0.0
        df.loc[zone1_mask, 'zone1_vote_share'] = (
            df.loc[zone1_mask, 'votes_total'] / total_zone1_votes * 100
        )
        
        print(f"  ✓ Zone 1 total votes: {total_zone1_votes:,}")
        print(f"  ✓ Added zone1_vote_share for {zone1_mask.sum()} Zone 1 precincts")
    
    # Precinct size categories
    df['precinct_size'] = 'Unknown'
    voter_mask = df['has_voter_registration']
    
    if voter_mask.any():
        df.loc[voter_mask & (df['TOTAL'] < 1000), 'precinct_size'] = 'Small'
        df.loc[voter_mask & (df['TOTAL'] >= 1000) & (df['TOTAL'] < 3000), 'precinct_size'] = 'Medium'
        df.loc[voter_mask & (df['TOTAL'] >= 3000) & (df['TOTAL'] < 6000), 'precinct_size'] = 'Large'
        df.loc[voter_mask & (df['TOTAL'] >= 6000), 'precinct_size'] = 'Extra Large'
    
    return df


def verify_data_integrity(df: pd.DataFrame) -> None:
    """Verify data integrity and report any issues."""
    print("\n🔍 Verifying data integrity:")
    
    # Check vote totals
    zone1_mask = df['is_zone1_precinct']
    if zone1_mask.any():
        # Check that vote totals match sums of candidate votes
        candidate_vote_cols = [col for col in df.columns if col.startswith('votes_') and col != 'votes_total']
        
        for idx in df[zone1_mask].index:
            recorded_total = df.loc[idx, 'votes_total']
            calculated_total = sum(df.loc[idx, col] for col in candidate_vote_cols if pd.notna(df.loc[idx, col]))
            
            if abs(recorded_total - calculated_total) > 0.1:  # Allow for tiny floating point differences
                precinct = df.loc[idx, 'precinct']
                print(f"  ⚠️ Vote total mismatch in precinct {precinct}: recorded={recorded_total}, calculated={calculated_total}")
        
        # Summary statistics
        total_votes = df.loc[zone1_mask, 'votes_total'].sum()
        total_precincts = zone1_mask.sum()
        avg_turnout = df.loc[zone1_mask & df['has_voter_registration'], 'turnout_rate'].mean()
        
        print(f"  ✅ Zone 1 verification:")
        print(f"     • Total precincts: {total_precincts}")
        print(f"     • Total votes: {total_votes:,}")
        print(f"     • Average turnout: {avg_turnout:.1%}")
        
        # Candidate totals
        for col in candidate_vote_cols:
            candidate_name = col.replace('votes_', '').title()
            candidate_total = df.loc[zone1_mask, col].sum()
            candidate_pct = candidate_total / total_votes * 100 if total_votes > 0 else 0
            print(f"     • {candidate_name}: {candidate_total:,} ({candidate_pct:.2f}%)")


def main():
    """Main function with improved data processing."""
    print("🗳️ Election Data Enrichment (Redesigned)")
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

    # Load data
    try:
        voters_df, votes_df = load_and_clean_data(config)
    except Exception as e:
        print(f"❌ Data loading failed: {e}")
        return

    # Get column name for merging
    precinct_col = config.get_column_name("precinct_csv")

    # Perform full outer join to capture all data
    print(f"\n🔗 Performing full outer join on '{precinct_col}':")
    merged_df = pd.merge(voters_df, votes_df, on=precinct_col, how="outer")
    print(f"  ✓ Merged dataset: {len(merged_df)} records")

    # Process data step by step
    print("\n🔄 Processing data...")
    
    # Step 1: Detect and standardize candidate columns
    enriched_df, candidate_cols = detect_and_standardize_candidates(merged_df)
    
    # Step 2: Add clear record classification
    enriched_df = add_record_classification(enriched_df, config)
    
    # Step 3: Calculate voter registration metrics
    enriched_df = calculate_voter_metrics(enriched_df, config)
    
    # Step 4: Calculate election metrics
    enriched_df = calculate_election_metrics(enriched_df, candidate_cols, config)
    
    # Step 5: Add summary statistics
    enriched_df = add_summary_statistics(enriched_df)
    
    # Step 6: Verify data integrity
    verify_data_integrity(enriched_df)

    # Save enriched dataset
    output_path = config.get_enriched_csv_path()
    enriched_df.to_csv(output_path, index=False)

    # Generate final summary
    print("\n📈 Final Summary:")
    print(f"   • Total records: {len(enriched_df)}")
    print(f"   • County summaries: {enriched_df['is_summary'].sum()}")
    print(f"   • Zone 1 precincts: {enriched_df['is_zone1_precinct'].sum()}")
    print(f"   • Other precincts: {enriched_df['is_non_zone1_precinct'].sum()}")
    print(f"   • Complete records: {enriched_df['is_complete_record'].sum()}")

    print(f"\n✅ Enriched dataset saved to: {output_path}")
    print(f"   📄 Total columns: {len(enriched_df.columns)}")
    
    # Show the new logical column structure
    key_columns = [
        col for col in enriched_df.columns 
        if any(col.startswith(prefix) for prefix in ['is_', 'has_', 'record_type', 'votes_', 'vote_pct_', 'reg_pct_'])
    ]
    print(f"   🔑 Key new columns: {key_columns[:15]}{'...' if len(key_columns) > 15 else ''}")


if __name__ == "__main__":
    main()
