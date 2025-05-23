# Field Registry Report
Generated: 2025-05-23 13:35:41
Total registered fields: 29

## Field Definitions

### Boolean Fields (5)

**complete_record**
- Description: Whether this precinct has both election and voter registration data
- Formula: `has_election_data AND has_voter_data`

**has_election_data**
- Description: Whether election data is available for this precinct
- Formula: `votes_total IS NOT NULL AND votes_total >= 0`

**has_voter_data**
- Description: Whether voter registration data is available for this precinct
- Formula: `total_voters IS NOT NULL AND total_voters > 0`

**is_zone1_precinct**
- Description: Whether this precinct is in the specified zone (Zone 1 for school board elections)
- Formula: `precinct IN zone_precinct_list`

**participated_election**
- Description: Whether this precinct had any votes cast in the election
- Formula: `votes_total > 0`

### Categorical Fields (7)

**competitiveness**
- Description: Electoral competitiveness based on registration balance and turnout
- Formula: `CASE WHEN ABS(dem_advantage) >= 30 THEN 'Safe' WHEN ABS(dem_advantage) >= 15 THEN 'Likely' WHEN ABS(dem_advantage) >= 5 THEN 'Competitive' ELSE 'Tossup' END`

**leading_candidate**
- Description: Candidate who received the most votes in this precinct
- Formula: `ARGMAX(candidate_vote_counts)`

**margin_category**
- Description: Victory margin categorized by competitiveness
- Formula: `CASE WHEN pct_victory_margin <= 5 THEN 'Very Close' WHEN pct_victory_margin <= 10 THEN 'Close' WHEN pct_victory_margin <= 20 THEN 'Clear' ELSE 'Landslide' END`

**political_lean**
- Description: Overall political tendency based on voter registration patterns
- Formula: `CASE WHEN dem_advantage >= 20 THEN 'Strong Dem' WHEN dem_advantage >= 10 THEN 'Lean Dem' WHEN dem_advantage <= -20 THEN 'Strong Rep' WHEN dem_advantage <= -10 THEN 'Lean Rep' ELSE 'Competitive' END`

**precinct**
- Description: Unique identifier for the voting precinct

**precinct_size_category**
- Description: Precinct size based on number of registered voters
- Formula: `CASE WHEN total_voters <= Q1 THEN 'Small' WHEN total_voters <= Q2 THEN 'Medium' WHEN total_voters <= Q3 THEN 'Large' ELSE 'Extra Large' END`

**turnout_quartile**
- Description: Turnout rate grouped into quartiles for comparative analysis
- Formula: `NTILE(4) OVER (ORDER BY turnout_rate)`

### Count Fields (3)

**total_voters**
- Description: Total number of registered voters in the precinct
- Formula: `reg_dem + reg_rep + reg_nav + reg_other`
- Units: registered voters

**vote_margin**
- Description: Vote difference between first and second place candidates
- Formula: `votes_leading_candidate - votes_second_candidate`
- Units: votes

**votes_total**
- Description: Total number of votes cast in the precinct
- Formula: `SUM(all candidate vote counts)`
- Units: votes

### Percentage Fields (10)

**dem_advantage**
- Description: Democratic registration advantage (positive) or disadvantage (negative)
- Formula: `reg_pct_dem - reg_pct_rep`
- Units: percentage points

**engagement_rate**
- Description: Civic engagement combining registration rates and turnout
- Formula: `(turnout_rate * 0.7) + (major_party_pct * 0.3)`
- Units: percent

**major_party_pct**
- Description: Percentage of voters registered with major parties (Democratic or Republican)
- Formula: `((reg_dem + reg_rep) / total_voters) * 100`
- Units: percent

**pct_victory_margin**
- Description: Victory margin as percentage of total votes cast
- Formula: `(vote_margin / votes_total) * 100`
- Units: percent

**reg_pct_dem**
- Description: Percentage of voters registered as Democratic
- Formula: `(reg_dem / total_voters) * 100`
- Units: percent

**reg_pct_nav**
- Description: Percentage of voters registered as Non-Affiliated (Independent)
- Formula: `(reg_nav / total_voters) * 100`
- Units: percent

**reg_pct_rep**
- Description: Percentage of voters registered as Republican
- Formula: `(reg_rep / total_voters) * 100`
- Units: percent

**registration_competitiveness**
- Description: How balanced Democratic vs Republican registration is
- Formula: `100 - ABS(dem_advantage)`
- Units: percent

**turnout_rate**
- Description: Percentage of registered voters who actually voted in this election
- Formula: `(votes_total / total_voters) * 100`
- Units: percent

**vote_pct_contribution_total_votes**
- Description: What percentage of the total vote pool this precinct represents
- Formula: `(votes_total / SUM(all_precincts.votes_total)) * 100`
- Units: percent

### Ratio Fields (4)

**candidate_dominance**
- Description: How dominant the leading candidate is vs all others combined
- Formula: `votes_leading_candidate / (votes_total - votes_leading_candidate)`

**competitiveness_score**
- Description: Quantitative measure of electoral competitiveness (0-100, higher = more competitive)
- Formula: `100 - ABS(dem_advantage) * 2.5 + (turnout_rate - 50) * 0.5`
- Units: score (0-100)

**swing_potential**
- Description: Likelihood of changing party preference based on registration and voting patterns
- Formula: `(100 - ABS(dem_advantage)) * (turnout_rate / 100) * (reg_pct_nav / 100)`
- Units: potential score

**vote_efficiency_dem**
- Description: How effectively Democratic registrations converted to Democratic-aligned votes
- Formula: `(votes_dem_candidate / votes_total) / (reg_pct_dem / 100)`
