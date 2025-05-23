# 2025 Election Zone 1 Voters Enriched Dataset

## Overview

This dataset combines voter registration data with 2025 Zone 1 election results for comprehensive political analysis and mapping. Created by merging `multnomah_precinct_voter_totals.csv` (ALL Multnomah precincts) and `2025_election_zone1_total_votes.csv` (Zone 1 results only).

## Dataset Structure

- **Total Records**: 110
- **Columns**: 54
- **Complete Records**: 63 precincts with both voter registration and election data
- **Voters-only**: 45 precincts (not in Zone 1 election area)
- **Votes-only**: 2 county summary records (Clackamas, Washington)

## Key Column Categories

### Original Data

- `precinct`: Precinct identifier
- `TOTAL`: Total registered voters
- `DEM`, `REP`, `NAV`, etc.: Raw party registration counts
- `cnt_splitt`, `cnt_cavagnolo`, `cnt_leof`, `cnt_writein`: Candidate vote counts
- `cnt_total_votes`: Total votes cast

### Calculated Voter Registration Metrics

- `pct_*`: Party registration percentages (e.g., `pct_dem`, `pct_rep`)
- `dem_advantage`: Democratic advantage (% Dem - % Rep)
- `major_party_pct`: Combined Dem + Rep percentage
- `nav_pct_rank`: Non-affiliated voter percentage
- `political_lean`: Classification (Strong Dem, Lean Dem, Competitive, Lean Rep, Strong Rep)

### Election Performance Metrics

- `participated_election`: Boolean flag for election participation
- `pct_*`: Vote share percentages (e.g., `pct_splitt`, `pct_cavagnolo`)
- `turnout_rate`: Votes cast / registered voters
- `vote_margin`: Absolute vote difference between top candidates
- `margin_pct`: Margin as percentage of total votes
- `leading_candidate`: Winner in precinct (Splitt, Cavagnolo, Tie)
- `competitiveness`: Classification (Competitive < 10% margin, Safe ≥ 10% margin)

### Cross-Analysis Metrics

- `dem_performance_vs_reg`: How Splitt performed vs Dem registration
- `rep_performance_vs_reg`: How Cavagnolo performed vs Rep registration  
- `high_turnout`: Boolean flag for above-median turnout
- `engagement_score`: Combined metric of registration diversity and turnout

### Metadata

- `record_type`: precinct or county_summary
- `in_zone1`: Participated in Zone 1 election or is summary record
- `has_voter_data`, `has_election_data`, `complete_record`: Data completeness flags

## Key Findings

### Political Landscape (All Multnomah Precincts)

- **77 precincts**: Strong Democratic lean
- **24 precincts**: Lean Democratic
- **6 precincts**: Competitive registration
- **1 precinct**: Lean Republican
- **Average turnout** (Zone 1 only): 22.6%
- **Competition** (Zone 1 only): 0 competitive precincts (all had >10% margins)

### Election Results (Zone 1 Precincts Only)

- **63 precincts** participated in Zone 1 election
- **45 precincts** did NOT participate (outside Zone 1 boundaries)
- **Splitt dominance**: Won all contested precincts
- **Typical margin**: ~50-60% vote margin

### Data Coverage

- **Complete coverage**: All 108 Multnomah precincts included
- **Zone 1 coverage**: 63 of 108 precincts participated in election
- **Regional context**: County summary data for Clackamas and Washington included

## Usage Notes

1. **For Mapping**: Use with GeoJSON precinct boundaries for spatial analysis
2. **For Analysis**:
   - Filter by `participated_election == True` for Zone 1 election analysis
   - Filter by `complete_record == True` for precincts with both voter and election data
   - Use `participated_election == False` to analyze non-Zone 1 precincts
3. **For Comparisons**: Use county summary records for broader regional context
4. **Missing Data**:
   - `NaN` in election columns = precinct not in Zone 1 election
   - `0` values = precinct was in election but got 0 votes

## Files

- **Source**: `enrich_voters_election_data.py`
- **Input**: `multnomah_precinct_voter_totals.csv`, `2025_election_zone1_total_votes.csv`
- **Output**: `2025_election_zone1_voters_enriched.csv`
- **Related**: Compatible with `2025_election_zone1_results.geojson` for mapping
