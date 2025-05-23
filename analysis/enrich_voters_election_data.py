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
    """Detect candidate columns and create both original and standardized versions with proper data types."""
    print("\n📊 Detecting and standardizing candidate columns:")
    
    # Find all candidate columns
    candidate_cols = [col for col in df.columns if col.startswith("candidate_")]
    print(f"  ✓ Found candidate columns: {candidate_cols}")
    
    standardized_cols = []
    
    # Create standardized count columns while preserving originals
    for col in candidate_cols:
        candidate_name = col.replace("candidate_", "")
        standardized_col = f"votes_{candidate_name}"  # More intuitive than cnt_
        
        # FIXED: Ensure proper numeric conversion with error handling
        df[standardized_col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        standardized_cols.append(standardized_col)
        
        # Verify conversion worked
        non_zero_count = (df[standardized_col] > 0).sum()
        print(f"  ✓ Created {standardized_col} from {col} ({non_zero_count} non-zero values)")
    
    # Standardize total_votes if it exists with proper data type handling
    if 'total_votes' in df.columns:
        df['votes_total'] = pd.to_numeric(df['total_votes'], errors='coerce').fillna(0).astype(int)
        non_zero_total = (df['votes_total'] > 0).sum()
        print(f"  ✓ Created votes_total from total_votes ({non_zero_total} non-zero values)")
    
    # Verify standardized columns have valid data
    if standardized_cols:
        sample_df = df[df['votes_total'] > 0] if 'votes_total' in df.columns else df.head()
        if len(sample_df) > 0:
            sample_idx = sample_df.index[0]
            print(f"  🔍 Sample data check (record {sample_idx}):")
            for col in standardized_cols:
                value = df.loc[sample_idx, col]
                print(f"    - {col}: {value} (type: {type(value).__name__})")
    
    return df, standardized_cols


def add_record_classification(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    """Add clear record type classification and flags with improved validation."""
    print("\n🏷️ Adding record classification:")
    
    precinct_col = config.get_column_name("precinct_csv")
    
    # Ensure votes_total is numeric for proper classification
    if 'votes_total' in df.columns:
        df['votes_total'] = pd.to_numeric(df['votes_total'], errors='coerce').fillna(0)
    
    # Ensure TOTAL (voter registration) is numeric
    if 'TOTAL' in df.columns:
        df['TOTAL'] = pd.to_numeric(df['TOTAL'], errors='coerce').fillna(0)
    
    # Clear boolean flags for different record types
    df['is_county_rollup'] = df[precinct_col].isin(['clackamas', 'washington'])
    df['is_zone1_precinct'] = df['votes_total'].notna() & (df['votes_total'] > 0) & ~df['is_county_rollup']
    df['is_non_zone1_precinct'] = df['TOTAL'].notna() & (df['TOTAL'] > 0) & (df['votes_total'].isna() | (df['votes_total'] == 0)) & ~df['is_county_rollup']
    
    # Keep county rollups for calculations but mark them clearly
    df['is_summary'] = df['is_county_rollup']  # Preserve for compatibility
    
    # Overall record type for clarity
    df['record_type'] = 'unknown'
    df.loc[df['is_county_rollup'], 'record_type'] = 'county_rollup'
    df.loc[df['is_zone1_precinct'], 'record_type'] = 'zone1_precinct'
    df.loc[df['is_non_zone1_precinct'], 'record_type'] = 'other_precinct'
    
    # IMPROVED: Data availability flags with better validation
    df['has_voter_registration'] = df['TOTAL'].notna() & (df['TOTAL'] > 0)
    df['has_election_results'] = df['votes_total'].notna() & (df['votes_total'] > 0)
    df['is_complete_record'] = df['has_voter_registration'] & df['has_election_results']
    
    # Validation and reporting
    county_rollup_count = df['is_county_rollup'].sum()
    zone1_count = df['is_zone1_precinct'].sum()
    other_count = df['is_non_zone1_precinct'].sum()
    complete_count = df['is_complete_record'].sum()
    election_results_count = df['has_election_results'].sum()
    
    print(f"  ✅ County rollup records: {county_rollup_count}")
    print(f"  ✅ Zone 1 precincts: {zone1_count}")
    print(f"  ✅ Other precincts: {other_count}")
    print(f"  ✅ Records with election results: {election_results_count}")
    print(f"  ✅ Complete records (voter + election data): {complete_count}")
    
    # Additional validation - check for issues
    if election_results_count == 0:
        print("  ⚠️  WARNING: No records found with election results!")
        print("     This will prevent competition metrics calculation.")
        
        # Debug: Show sample vote data
        vote_cols = [col for col in df.columns if col.startswith('votes_')]
        if vote_cols:
            sample_votes = df[vote_cols].head()
            print("     Sample vote data:")
            print(sample_votes.to_string())
    
    # Check for potential data issues
    zero_total_but_votes = df[(df['votes_total'] == 0) & ((df[[col for col in df.columns if col.startswith('votes_') and col != 'votes_total']]).sum(axis=1) > 0)]
    if len(zero_total_but_votes) > 0:
        print(f"  ⚠️  Found {len(zero_total_but_votes)} records with candidate votes but zero total - fixing...")
        for idx in zero_total_but_votes.index:
            # Recalculate total from candidate votes
            candidate_cols = [col for col in df.columns if col.startswith('votes_') and col != 'votes_total']
            calculated_total = sum(df.loc[idx, col] for col in candidate_cols if pd.notna(df.loc[idx, col]))
            if calculated_total > 0:
                df.loc[idx, 'votes_total'] = calculated_total
                print(f"     Fixed record {idx}: set votes_total to {calculated_total}")
        
        # Recalculate flags after fixing
        df['has_election_results'] = df['votes_total'].notna() & (df['votes_total'] > 0)
        df['is_zone1_precinct'] = df['votes_total'].notna() & (df['votes_total'] > 0) & ~df['is_county_rollup']
        df['is_complete_record'] = df['has_voter_registration'] & df['has_election_results']
        
        print(f"     Updated: {df['has_election_results'].sum()} records now have election results")
    
    return df


def calculate_voter_metrics(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    """Calculate voter registration metrics with FIXED percentage handling."""
    print("\n📈 Calculating voter registration metrics:")

    # Only calculate for records with voter data (excluding county rollups)
    mask = df['has_voter_registration'] & ~df['is_county_rollup']
    
    if not mask.any():
        print("  ⚠️ No records with voter registration data found!")
        return df

    # Party registration percentages - FIXED to show as proper percentages
    party_cols = ['DEM', 'REP', 'NAV', 'OTH', 'CON', 'IND', 'LBT', 'NLB', 'PGP', 'PRO', 'WFP', 'WTP']
    
    for party in party_cols:
        if party in df.columns:
            pct_col = f"reg_pct_{party.lower()}"
            df[pct_col] = 0.0
            # Calculate as percentages (0-100 scale), not decimals
            df.loc[mask, pct_col] = (df.loc[mask, party] / df.loc[mask, 'TOTAL']) * 100
            print(f"  ✓ Added {pct_col} (as percentage)")

    # Political lean metrics - using percentage values
    df['dem_advantage'] = 0.0
    df['major_party_pct'] = 0.0
    df.loc[mask, 'dem_advantage'] = df.loc[mask, 'reg_pct_dem'] - df.loc[mask, 'reg_pct_rep']
    df.loc[mask, 'major_party_pct'] = df.loc[mask, 'reg_pct_dem'] + df.loc[mask, 'reg_pct_rep']

    # Political lean categories - adjusted for percentage scale
    strong_threshold = config.get_analysis_setting("strong_advantage") * 100  # Convert to percentage
    lean_threshold = config.get_analysis_setting("lean_advantage") * 100  # Convert to percentage

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
    print(f"  ✓ Sample dem_advantage: {df.loc[mask, 'dem_advantage'].head(3).tolist()}")
    
    return df


def calculate_election_metrics(df: pd.DataFrame, candidate_cols: list, config: Config) -> pd.DataFrame:
    """Calculate election-specific metrics with proper data types."""
    print("\n🗳️ Calculating election metrics:")

    # Only calculate for records with election data (including county rollups for totals)
    mask = df['has_election_results']
    
    if not mask.any():
        print("  ⚠️ No records with election results found!")
        return df

    # Turnout calculation (only for actual precincts, not county rollups)
    df['turnout_rate'] = 0.0
    valid_turnout_mask = mask & df['has_voter_registration'] & ~df['is_county_rollup']
    if valid_turnout_mask.any():
        df.loc[valid_turnout_mask, 'turnout_rate'] = (
            df.loc[valid_turnout_mask, 'votes_total'] / df.loc[valid_turnout_mask, 'TOTAL']
        ) * 100  # Store as percentage
        print(f"  ✓ Calculated turnout_rate for {valid_turnout_mask.sum()} precincts (as percentage)")

    # Candidate vote percentages - store as percentages (0-100)
    for col in candidate_cols:
        candidate_name = col.replace('votes_', '')
        pct_col = f"vote_pct_{candidate_name}"
        df[pct_col] = 0.0
        df.loc[mask, pct_col] = (df.loc[mask, col] / df.loc[mask, 'votes_total']) * 100
        print(f"  ✓ Added {pct_col} (as percentage)")

    # Competition metrics
    df = calculate_competition_metrics(df, candidate_cols, config)

    print(f"  ✓ Calculated metrics for {mask.sum()} records with election data")
    
    return df


def calculate_competition_metrics(df: pd.DataFrame, candidate_cols: list, config: Config) -> pd.DataFrame:
    """Calculate competition metrics with FIXED data handling and logic."""
    print("  📊 Calculating competition metrics...")

    mask = df['has_election_results']
    
    # Initialize competition columns with proper defaults
    df['vote_margin'] = 0
    df['margin_pct'] = 0.0
    df['leading_candidate'] = 'No Data'
    df['second_candidate'] = 'No Data'
    df['competitiveness'] = 'No Election Data'
    df['is_competitive'] = False

    # Get thresholds from configuration - DON'T convert to percentage scale, they're already correct
    competitive_threshold = config.get_analysis_setting("competitive_threshold") * 100  # 0.10 -> 10%
    tossup_threshold = config.get_analysis_setting("tossup_threshold") * 100  # 0.05 -> 5%
    
    print(f"    - Using thresholds: Toss-up < {tossup_threshold}%, Competitive < {competitive_threshold}%")

    # Ensure all candidate columns are numeric BEFORE processing
    for col in candidate_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Ensure votes_total is numeric
    df['votes_total'] = pd.to_numeric(df['votes_total'], errors='coerce').fillna(0)

    # For each record with election data, find top 2 candidates
    processed_count = 0
    for idx in df[mask].index:
        total_votes = df.loc[idx, 'votes_total']
        
        # Skip if no votes or invalid total
        if pd.isna(total_votes) or total_votes <= 0:
            continue
            
        candidate_votes = {}
        for col in candidate_cols:
            candidate_name = col.replace('votes_', '')
            votes = df.loc[idx, col]
            # Only include candidates with positive votes
            if pd.notna(votes) and votes > 0:
                candidate_votes[candidate_name] = int(votes)

        if len(candidate_votes) >= 2:
            # Sort candidates by vote count (descending)
            sorted_candidates = sorted(candidate_votes.items(), key=lambda x: x[1], reverse=True)

            # Get top 2
            first_candidate, first_votes = sorted_candidates[0]
            second_candidate, second_votes = sorted_candidates[1]

            # Calculate margin
            vote_margin = first_votes - second_votes
            margin_pct = (vote_margin / total_votes * 100) if total_votes > 0 else 0

            # Standardize candidate names consistently
            df.loc[idx, 'leading_candidate'] = first_candidate.replace('_', ' ').title()
            df.loc[idx, 'second_candidate'] = second_candidate.replace('_', ' ').title()
            df.loc[idx, 'vote_margin'] = vote_margin
            df.loc[idx, 'margin_pct'] = margin_pct
            
            processed_count += 1

        elif len(candidate_votes) == 1:
            # Only one candidate with votes - this is a landslide
            candidate_name = list(candidate_votes.keys())[0]
            candidate_votes_count = list(candidate_votes.values())[0]
            
            df.loc[idx, 'leading_candidate'] = candidate_name.replace('_', ' ').title()
            df.loc[idx, 'vote_margin'] = candidate_votes_count  # Entire vote count is the margin
            df.loc[idx, 'margin_pct'] = 100.0  # 100% margin for uncontested
            
            processed_count += 1
        else:
            # No candidates with votes - keep defaults
            pass

    # FIXED Competitiveness classification with correct logic
    competition_mask = mask & (df['margin_pct'] > 0)  # Only classify where we have margins
    
    # Toss-up: Very close races (< 5% margin)
    tossup_mask = competition_mask & (df['margin_pct'] < tossup_threshold)
    df.loc[tossup_mask, 'competitiveness'] = 'Toss-up'
    
    # Competitive: Close races (5-10% margin) 
    competitive_mask = competition_mask & (df['margin_pct'] >= tossup_threshold) & (df['margin_pct'] < competitive_threshold)
    df.loc[competitive_mask, 'competitiveness'] = 'Competitive'
    
    # Safe: Large margins (10%+ margin)
    safe_mask = competition_mask & (df['margin_pct'] >= competitive_threshold)
    df.loc[safe_mask, 'competitiveness'] = 'Safe'
    
    # Boolean flag for competitive races (anything under 10% is considered competitive)
    df.loc[mask, 'is_competitive'] = df.loc[mask, 'margin_pct'] < competitive_threshold

    # Summary statistics
    tossup_count = tossup_mask.sum()
    competitive_count = competitive_mask.sum()
    safe_count = safe_mask.sum()
    
    print(f"    - Processed competition metrics for {processed_count} records")
    print(f"    - Competitiveness breakdown: {tossup_count} Toss-up, {competitive_count} Competitive, {safe_count} Safe")
    print(f"    - Sample leading candidates: {df.loc[mask & (df['leading_candidate'] != 'No Data'), 'leading_candidate'].head(3).tolist()}")
    print(f"    - Sample margins: {df.loc[mask & (df['margin_pct'] > 0), 'margin_pct'].head(3).tolist()}")
    
    return df


def add_summary_statistics(df: pd.DataFrame) -> pd.DataFrame:
    """Add helpful summary columns for analysis."""
    print("\n📊 Adding summary statistics:")
    
    # Zone 1 specific stats - INCLUDE county rollups for accurate totals
    zone1_mask = df['is_zone1_precinct']
    county_rollup_mask = df['is_county_rollup']
    
    if zone1_mask.any():
        # Calculate accurate totals including county rollups
        zone1_precinct_votes = df.loc[zone1_mask, 'votes_total'].sum()
        county_rollup_votes = df.loc[county_rollup_mask, 'votes_total'].sum()
        total_zone1_votes_complete = zone1_precinct_votes + county_rollup_votes
        
        df['zone1_total_votes'] = total_zone1_votes_complete
        
        # Vote share of each precinct within Zone 1 (using precinct total for meaningful percentages)
        df['zone1_vote_share'] = 0.0
        df.loc[zone1_mask, 'zone1_vote_share'] = (
            df.loc[zone1_mask, 'votes_total'] / zone1_precinct_votes * 100
        )
        
        print(f"  ✅ Zone 1 precinct votes: {zone1_precinct_votes:,}")
        print(f"  ✅ County rollup votes: {county_rollup_votes:,}")
        print(f"  ✅ Zone 1 COMPLETE total: {total_zone1_votes_complete:,}")
        print(f"  ✅ Added zone1_vote_share for {zone1_mask.sum()} Zone 1 precincts")
    
    # Precinct size categories
    df['precinct_size'] = 'Unknown'
    voter_mask = df['has_voter_registration']
    
    if voter_mask.any():
        df.loc[voter_mask & (df['TOTAL'] < 1000), 'precinct_size'] = 'Small'
        df.loc[voter_mask & (df['TOTAL'] >= 1000) & (df['TOTAL'] < 3000), 'precinct_size'] = 'Medium'
        df.loc[voter_mask & (df['TOTAL'] >= 3000) & (df['TOTAL'] < 6000), 'precinct_size'] = 'Large'
        df.loc[voter_mask & (df['TOTAL'] >= 6000), 'precinct_size'] = 'Extra Large'
    
    return df


def calculate_contribution_percentages(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate vote contribution percentages using COMPLETE totals including county rollups."""
    print("\n🔍 Calculating FIXED vote contribution percentages:")
    
    zone1_mask = df['is_zone1_precinct']
    county_rollup_mask = df['is_county_rollup']
    
    if zone1_mask.any():
        # Calculate COMPLETE totals including county rollups
        candidate_vote_cols = [col for col in df.columns if col.startswith('votes_') and col != 'votes_total']
        
        # Complete totals for each candidate and overall
        complete_totals = {}
        for col in candidate_vote_cols:
            complete_totals[col] = df.loc[zone1_mask | county_rollup_mask, col].sum()
        
        complete_total_votes = df.loc[zone1_mask | county_rollup_mask, 'votes_total'].sum()
        
        print(f"  📊 COMPLETE candidate totals (including county rollups):")
        for col, total in complete_totals.items():
            candidate_name = col.replace('votes_', '').title()
            print(f"    - {candidate_name}: {total:,}")
        print(f"  📊 COMPLETE total votes: {complete_total_votes:,}")
        
        # Calculate contribution percentages for precincts only
        for col in candidate_vote_cols:
            candidate_name = col.replace('votes_', '')
            contribution_col = f'vote_pct_contribution_{candidate_name}'
            
            df[contribution_col] = 0.0
            
            if complete_totals[col] > 0:
                df.loc[zone1_mask, contribution_col] = (
                    df.loc[zone1_mask, col] / complete_totals[col] * 100
                )
                
                # Verify with sample calculation
                sample_precincts = df[zone1_mask & (df[col] > 0)]
                if len(sample_precincts) > 0:
                    sample_idx = sample_precincts.index[0]
                    sample_votes = df.loc[sample_idx, col]
                    sample_pct = df.loc[sample_idx, contribution_col]
                    sample_precinct = df.loc[sample_idx, 'precinct']
                    expected_pct = sample_votes / complete_totals[col] * 100
                    print(f"  ✅ {candidate_name}: Precinct {sample_precinct} has {sample_votes} votes = {sample_pct:.2f}% (verified: {expected_pct:.2f}%)")
        
        # Total vote contribution
        df['vote_pct_contribution_total_votes'] = 0.0
        df.loc[zone1_mask, 'vote_pct_contribution_total_votes'] = (
            df.loc[zone1_mask, 'votes_total'] / complete_total_votes * 100
        )
        
        print(f"  ✅ Added contribution percentages for {len(candidate_vote_cols)} candidates using COMPLETE totals")
    
    return df


def verify_data_integrity(df: pd.DataFrame) -> None:
    """Verify data integrity and report any issues."""
    print("\n🔍 Verifying data integrity:")
    
    # Check vote totals
    zone1_mask = df['is_zone1_precinct']
    county_rollup_mask = df['is_county_rollup']
    
    if zone1_mask.any():
        # Check that vote totals match sums of candidate votes
        candidate_vote_cols = [col for col in df.columns if col.startswith('votes_') and col != 'votes_total']
        
        errors = 0
        for idx in df[zone1_mask | county_rollup_mask].index:
            recorded_total = df.loc[idx, 'votes_total']
            calculated_total = sum(df.loc[idx, col] for col in candidate_vote_cols if pd.notna(df.loc[idx, col]))
            
            if abs(recorded_total - calculated_total) > 0.1:
                precinct = df.loc[idx, 'precinct']
                print(f"  ⚠️ Vote total mismatch in {precinct}: recorded={recorded_total}, calculated={calculated_total}")
                errors += 1
        
        if errors == 0:
            print(f"  ✅ All vote totals match candidate sums")
        
        # Summary statistics - COMPLETE including county rollups
        zone1_precinct_votes = df.loc[zone1_mask, 'votes_total'].sum()
        county_rollup_votes = df.loc[county_rollup_mask, 'votes_total'].sum()
        total_votes_complete = zone1_precinct_votes + county_rollup_votes
        
        total_precincts = zone1_mask.sum()
        avg_turnout = df.loc[zone1_mask & df['has_voter_registration'], 'turnout_rate'].mean()
        
        print(f"  ✅ Zone 1 verification (COMPLETE TOTALS):")
        print(f"     • Zone 1 precincts: {total_precincts}")
        print(f"     • Precinct votes: {zone1_precinct_votes:,}")
        print(f"     • County rollup votes: {county_rollup_votes:,}")
        print(f"     • COMPLETE total: {total_votes_complete:,}")
        print(f"     • Average turnout: {avg_turnout:.1f}%")
        
        # Candidate totals - COMPLETE including county rollups
        for col in candidate_vote_cols:
            candidate_name = col.replace('votes_', '').title()
            candidate_total_complete = df.loc[zone1_mask | county_rollup_mask, col].sum()
            candidate_pct = candidate_total_complete / total_votes_complete * 100 if total_votes_complete > 0 else 0
            print(f"     • {candidate_name}: {candidate_total_complete:,} ({candidate_pct:.2f}%)")


def main():
    """Main function with comprehensive fixes for all data issues."""
    print("🗳️ Election Data Enrichment (COMPREHENSIVE FIXES)")
    print("=" * 70)

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
    print("\n🔄 Processing data with comprehensive fixes...")
    
    # Step 1: Detect and standardize candidate columns
    enriched_df, candidate_cols = detect_and_standardize_candidates(merged_df)
    
    # Step 2: Add clear record classification (preserving county rollups)
    enriched_df = add_record_classification(enriched_df, config)
    
    # Step 3: Calculate voter registration metrics (FIXED percentages)
    enriched_df = calculate_voter_metrics(enriched_df, config)
    
    # Step 4: Calculate election metrics (FIXED data types)
    enriched_df = calculate_election_metrics(enriched_df, candidate_cols, config)
    
    # Step 5: Add summary statistics
    enriched_df = add_summary_statistics(enriched_df)
    
    # Step 6: Calculate FIXED contribution percentages
    enriched_df = calculate_contribution_percentages(enriched_df)
    
    # Step 7: Verify data integrity
    verify_data_integrity(enriched_df)

    # Save enriched dataset
    output_path = config.get_enriched_csv_path()
    enriched_df.to_csv(output_path, index=False)

    # Generate final summary
    print("\n📈 Final Summary:")
    print(f"   • Total records: {len(enriched_df)}")
    print(f"   • County rollups: {enriched_df['is_county_rollup'].sum()}")
    print(f"   • Zone 1 precincts: {enriched_df['is_zone1_precinct'].sum()}")
    print(f"   • Other precincts: {enriched_df['is_non_zone1_precinct'].sum()}")
    print(f"   • Complete records: {enriched_df['is_complete_record'].sum()}")

    print(f"\n✅ Enriched dataset saved to: {output_path}")
    print(f"   📄 Total columns: {len(enriched_df.columns)}")
    print(f"   🔑 FIXED: Registration percentages as proper %")
    print(f"   🔑 FIXED: Contribution percentages using complete totals")
    print(f"   🔑 FIXED: County rollups preserved for accurate calculations")


if __name__ == "__main__":
    main()
