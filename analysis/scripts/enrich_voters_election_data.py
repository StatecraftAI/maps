import pathlib
import pandas as pd
import numpy as np
from typing import Optional

# Directories and File Paths
SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
ANALYSIS_DIR = SCRIPT_DIR.parent
DATA_DIR = ANALYSIS_DIR / 'data/elections'
VOTERS_FILENAME = "multnomah_precinct_voter_totals.csv"
VOTES_FILENAME = "2025_election_zone1_total_votes.csv"
OUTPUT_FILENAME = "2025_election_zone1_total_votes_enriched.csv"

def load_and_clean_data():
    """Load and perform initial cleaning of both datasets."""
    print(f"Loading data files from {DATA_DIR}")
    
    # Load voters data
    voters_path = DATA_DIR / VOTERS_FILENAME
    voters_df = pd.read_csv(voters_path)
    print(f"  ✓ Loaded voters data: {len(voters_df)} precincts")
    
    # Load votes data
    votes_path = DATA_DIR / VOTES_FILENAME
    votes_df = pd.read_csv(votes_path)
    print(f"  ✓ Loaded votes data: {len(votes_df)} records")
    
    # Standardize precinct column names and data types for merging
    voters_df = voters_df.rename(columns={'Precinct': 'precinct'})
    
    # Convert both precinct columns to string to ensure they match
    voters_df['precinct'] = voters_df['precinct'].astype(str)
    votes_df['precinct'] = votes_df['precinct'].astype(str)
    
    print(f"  Sample voters precincts: {voters_df['precinct'].head().tolist()}")
    print(f"  Sample votes precincts: {votes_df['precinct'].head().tolist()}")
    print(f"  Data types - voters: {voters_df['precinct'].dtype}, votes: {votes_df['precinct'].dtype}")
    
    return voters_df, votes_df

def calculate_voter_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate voter registration metrics."""
    print("\nCalculating voter registration metrics:")
    
    # Party registration percentages
    party_cols = ['DEM', 'REP', 'NAV', 'OTH', 'CON', 'IND', 'LBT', 'NLB', 'PGP', 'PRO', 'WFP', 'WTP']
    
    for party in party_cols:
        if party in df.columns:
            df[f'pct_{party.lower()}'] = df[party] / df['TOTAL']
    
    # Major party metrics
    df['dem_advantage'] = df['pct_dem'] - df['pct_rep']
    df['major_party_pct'] = df['pct_dem'] + df['pct_rep']
    df['nav_pct_rank'] = df['pct_nav']  # Non-affiliated voters
    
    # Political lean categories
    conditions = [
        df['dem_advantage'] > 0.2,
        df['dem_advantage'] > 0.05,
        df['dem_advantage'] > -0.05,
        df['dem_advantage'] > -0.2,
        df['dem_advantage'] <= -0.2
    ]
    choices = ['Strong Dem', 'Lean Dem', 'Competitive', 'Lean Rep', 'Strong Rep']
    df['political_lean'] = np.select(conditions, choices, default='Unknown')
    
    print(f"  ✓ Added party registration percentages for {len(party_cols)} parties")
    print(f"  ✓ Added political lean classification")
    
    return df

def calculate_election_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate election-specific metrics."""
    print("\nCalculating election metrics:")
    
    # Rename candidate columns to standardized format
    candidate_mapping = {
        'splitt': 'cnt_splitt',
        'cavagnolo': 'cnt_cavagnolo',
        'leof': 'cnt_leof',
        'write_in': 'cnt_writein',
        'total_votes': 'cnt_total_votes'
    }
    
    for old_col, new_col in candidate_mapping.items():
        if old_col in df.columns:
            df[new_col] = df[old_col]
    
    # Election participation flag
    df['participated_election'] = df['cnt_total_votes'].notna() & (df['cnt_total_votes'] > 0)
    
    # Vote shares (only for precincts that participated)
    mask = df['participated_election']
    df['pct_splitt'] = np.nan
    df['pct_cavagnolo'] = np.nan
    df['pct_leof'] = np.nan
    df['pct_writein'] = np.nan
    
    df.loc[mask, 'pct_splitt'] = df.loc[mask, 'cnt_splitt'] / df.loc[mask, 'cnt_total_votes']
    df.loc[mask, 'pct_cavagnolo'] = df.loc[mask, 'cnt_cavagnolo'] / df.loc[mask, 'cnt_total_votes']
    df.loc[mask, 'pct_leof'] = df.loc[mask, 'cnt_leof'] / df.loc[mask, 'cnt_total_votes']
    df.loc[mask, 'pct_writein'] = df.loc[mask, 'cnt_writein'] / df.loc[mask, 'cnt_total_votes']
    
    # Turnout calculation
    df['turnout_rate'] = np.nan
    df.loc[mask, 'turnout_rate'] = df.loc[mask, 'cnt_total_votes'] / df.loc[mask, 'TOTAL']
    
    # Competition metrics
    df['vote_margin'] = np.nan
    df['margin_pct'] = np.nan
    df['leading_candidate'] = 'No Election Data'
    
    df.loc[mask, 'vote_margin'] = abs(df.loc[mask, 'cnt_splitt'] - df.loc[mask, 'cnt_cavagnolo'])
    df.loc[mask, 'margin_pct'] = df.loc[mask, 'vote_margin'] / df.loc[mask, 'cnt_total_votes']
    
    # Who's leading
    df.loc[mask & (df['cnt_splitt'] > df['cnt_cavagnolo']), 'leading_candidate'] = 'Splitt'
    df.loc[mask & (df['cnt_cavagnolo'] > df['cnt_splitt']), 'leading_candidate'] = 'Cavagnolo'
    df.loc[mask & (df['cnt_splitt'] == df['cnt_cavagnolo']), 'leading_candidate'] = 'Tie'
    
    # Competitiveness classification
    df['competitiveness'] = 'No Election Data'
    competitive_mask = mask & (df['margin_pct'] < 0.1)
    safe_mask = mask & (df['margin_pct'] >= 0.1)
    
    df.loc[competitive_mask, 'competitiveness'] = 'Competitive'
    df.loc[safe_mask, 'competitiveness'] = 'Safe'
    
    print(f"  ✓ Added election participation flags")
    print(f"  ✓ Added vote shares and turnout calculations")
    print(f"  ✓ Added competition metrics")
    
    return df

def calculate_cross_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate metrics that combine voter registration and election data."""
    print("\nCalculating cross-analysis metrics:")
    
    mask = df['participated_election']
    
    # Performance vs registration
    df['dem_performance_vs_reg'] = np.nan
    df['rep_performance_vs_reg'] = np.nan
    
    # Assuming Splitt is more Democratic-aligned, Cavagnolo more Republican-aligned based on vote patterns
    df.loc[mask, 'dem_performance_vs_reg'] = df.loc[mask, 'pct_splitt'] - df.loc[mask, 'pct_dem']
    df.loc[mask, 'rep_performance_vs_reg'] = df.loc[mask, 'pct_cavagnolo'] - df.loc[mask, 'pct_rep']
    
    # Turnout by party lean
    df['high_turnout'] = np.nan
    df.loc[mask, 'high_turnout'] = (df.loc[mask, 'turnout_rate'] > df.loc[mask, 'turnout_rate'].median()).astype(float)
    
    # Engagement score (combination of registration diversity and turnout)
    df['engagement_score'] = np.nan
    # Higher score = more diverse registration + higher turnout
    df.loc[mask, 'engagement_score'] = (
        (1 - df.loc[mask, 'major_party_pct']) * 0.5 +  # Registration diversity
        df.loc[mask, 'turnout_rate'] * 0.5  # Turnout
    )
    
    print(f"  ✓ Added performance vs registration metrics")
    print(f"  ✓ Added engagement scoring")
    
    return df

def add_metadata_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add metadata and classification columns."""
    print("\nAdding metadata columns:")
    
    # Record type classification
    county_records = ['clackamas', 'washington']
    df['record_type'] = 'precinct'
    df.loc[df['precinct'].isin(county_records), 'record_type'] = 'county_summary'
    
    # Zone 1 coverage flag
    df['in_zone1'] = df['participated_election'] | df['record_type'].eq('county_summary')
    
    # Data completeness flags
    df['has_voter_data'] = df['TOTAL'].notna()
    df['has_election_data'] = df['cnt_total_votes'].notna()
    df['complete_record'] = df['has_voter_data'] & df['has_election_data']
    
    print(f"  ✓ Added record type classifications")
    print(f"  ✓ Added data completeness flags")
    
    return df

def main():
    """Main function to process and enrich the voter and election data."""
    print("=== Enriching Voter and Election Data ===")
    
    # Load data
    voters_df, votes_df = load_and_clean_data()
    
    # Perform full outer join
    print(f"\nPerforming full outer join on precinct:")
    merged_df = pd.merge(voters_df, votes_df, on='precinct', how='outer')
    print(f"  ✓ Merged dataset: {len(merged_df)} records")
    
    # Identify merge results (using original column names first)
    voters_only = merged_df[merged_df['total_votes'].isna()]
    votes_only = merged_df[merged_df['TOTAL'].isna()]
    both_data = merged_df[merged_df['TOTAL'].notna() & merged_df['total_votes'].notna()]
    
    print(f"  📊 Voters-only records: {len(voters_only)} (precincts not in Zone 1)")
    print(f"  📊 Votes-only records: {len(votes_only)} (summary records)")
    print(f"  📊 Complete records: {len(both_data)} (precincts with both datasets)")
    
    # Calculate enriched metrics (this will rename the columns)
    enriched_df = calculate_voter_metrics(merged_df)
    enriched_df = calculate_election_metrics(enriched_df)
    enriched_df = calculate_cross_metrics(enriched_df)
    enriched_df = add_metadata_columns(enriched_df)
    
    # Clean up original duplicate columns
    cols_to_drop = ['splitt', 'cavagnolo', 'leof', 'write_in', 'total_votes']
    enriched_df = enriched_df.drop(columns=[col for col in cols_to_drop if col in enriched_df.columns])
    
    # Save enriched dataset
    output_path = DATA_DIR / OUTPUT_FILENAME
    enriched_df.to_csv(output_path, index=False)
    
    # Generate summary statistics
    print(f"\n📈 Summary Statistics:")
    print(f"   • Total records: {len(enriched_df)}")
    print(f"   • Precincts with voter data: {enriched_df['has_voter_data'].sum()}")
    print(f"   • Precincts with election data: {enriched_df['has_election_data'].sum()}")
    print(f"   • Complete records: {enriched_df['complete_record'].sum()}")
    
    if len(both_data) > 0:
        print(f"   • Average turnout: {enriched_df.loc[enriched_df['participated_election'], 'turnout_rate'].mean():.1%}")
        print(f"   • Competitive precincts: {(enriched_df['competitiveness'] == 'Competitive').sum()}")
        
        lean_summary = enriched_df['political_lean'].value_counts()
        print(f"   • Political lean distribution:")
        for lean, count in lean_summary.items():
            if lean != 'Unknown':
                print(f"     - {lean}: {count} precincts")
    
    print(f"\n✅ Enriched dataset saved to: {output_path}")
    print(f"   📄 Columns: {len(enriched_df.columns)}")
    print(f"   📋 Sample columns: {list(enriched_df.columns[:10])}")

if __name__ == "__main__":
    main() 