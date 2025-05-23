# Schema Drift Monitoring Report

**Report Period:** 2025-05-16 to 2025-05-23 (7 days)
**Generated:** 2025-05-23 15:42:01

## Executive Summary

- **Schema Snapshots Captured:** 6
- **Drift Alerts Generated:** 2
- **Current Schema Hash:** 4d94408c5f435802
- **Current Field Count:** 120

### Alert Summary

| Severity | Count |
|----------|-------|
| CRITICAL | 2 |
| HIGH | 0 |
| MEDIUM | 0 |
| LOW | 0 |

## Recent Alerts

### CRITICAL: 8 Fields Removed

- **Time:** 2025-05-23 14:49
- **Type:** REMOVED_FIELDS
- **Description:** Fields have been removed from the data schema: candidate_la_forte, candidate_sanchez_bautista, vote_pct_contribution_la_forte, vote_pct_contribution_sanchez_bautista, vote_pct_la_forte, vote_pct_sanchez_bautista, votes_la_forte, votes_sanchez_bautista
- **Impact:** CRITICAL: Removed fields include critical data: votes_sanchez_bautista, votes_la_forte. This may break downstream analysis.

**Recommended Actions:**
- Review code for references to removed fields
- Update field registry to remove obsolete definitions
- Check if removed fields were used in critical calculations
- Consider graceful degradation for missing fields

### CRITICAL: 21 New Fields Detected

- **Time:** 2025-05-23 14:49
- **Type:** NEW_FIELDS
- **Description:** New fields have been added to the data schema: candidate_crowe, candidate_engelsman, candidate_galanakis, candidate_mains, complete_record, has_election_data, has_voter_data, participated_election, total_voters, vote_pct_contribution_crowe, vote_pct_contribution_engelsman, vote_pct_contribution_galanakis, vote_pct_contribution_mains, vote_pct_crowe, vote_pct_engelsman, vote_pct_galanakis, vote_pct_mains, votes_crowe, votes_engelsman, votes_galanakis, votes_mains
- **Impact:** Impact: 4 new vote counts fields, 8 new vote percentages fields, 4 new candidate metadata fields, 2 new boolean flags fields, 3 new other fields. May require field registry updates and documentation.

**Recommended Actions:**
- Review new fields and determine if they need documentation
- Update field registry with explanations for important fields
- Check if new fields affect existing analysis or visualizations
- Consider implementing auto-registration patterns for bulk field additions

## Schema Evolution

- **Field Count Change:** +13 fields
- **Record Count Change:** +0 records
- **Schema Stability:** Evolving

## Current Field Distribution

| Category | Count |
|----------|-------|
| Identifiers | 4 |
| Vote Counts | 8 |
| Vote Percentages | 13 |
| Registration Counts | 13 |
| Registration Percentages | 12 |
| Geographic Districts | 18 |
| Candidate Metadata | 6 |
| Boolean Flags | 10 |
| Calculated Metrics | 7 |
| Shape Metadata | 2 |
| Other | 27 |

## Recommendations

Based on the analysis of schema drift over the past 7 days:

⚠️ **Action Required:** Critical or high-severity alerts detected.
- Review and address high-priority alerts immediately
- Investigate upstream data source changes
- Update field registry and documentation as needed