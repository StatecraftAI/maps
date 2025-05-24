# Schema Drift Monitoring Report

**Report Period:** 2025-05-17 to 2025-05-24 (7 days)
**Generated:** 2025-05-24 12:40:31

## Executive Summary

- **Schema Snapshots Captured:** 23
- **Drift Alerts Generated:** 34
- **Current Schema Hash:** 97e3f0eb634c654e
- **Current Field Count:** 121

### Alert Summary

| Severity | Count |
|----------|-------|
| CRITICAL | 23 |
| HIGH | 6 |
| MEDIUM | 5 |
| LOW | 0 |

## Recent Alerts

### MEDIUM: Significant Null Rate Change: Unincorp

- **Time:** 2025-05-24 12:40
- **Type:** DATA_QUALITY
- **Description:** Null rate changed from 87.9% to 0.0%
- **Impact:** Data quality change may indicate upstream issues or different data collection

**Recommended Actions:**
- Investigate source of null rate change
- Review data validation rules

### MEDIUM: Significant Null Rate Change: WaterDist

- **Time:** 2025-05-24 12:40
- **Type:** DATA_QUALITY
- **Description:** Null rate changed from 100.0% to 0.0%
- **Impact:** Data quality change may indicate upstream issues or different data collection

**Recommended Actions:**
- Investigate source of null rate change
- Review data validation rules

### MEDIUM: Significant Null Rate Change: PUD

- **Time:** 2025-05-24 12:40
- **Type:** DATA_QUALITY
- **Description:** Null rate changed from 92.5% to 0.0%
- **Impact:** Data quality change may indicate upstream issues or different data collection

**Recommended Actions:**
- Investigate source of null rate change
- Review data validation rules

### MEDIUM: Significant Null Rate Change: SewerDist

- **Time:** 2025-05-24 12:40
- **Type:** DATA_QUALITY
- **Description:** Null rate changed from 100.0% to 0.0%
- **Impact:** Data quality change may indicate upstream issues or different data collection

**Recommended Actions:**
- Investigate source of null rate change
- Review data validation rules

### MEDIUM: Significant Null Rate Change: FIRE_DIST

- **Time:** 2025-05-24 12:40
- **Type:** DATA_QUALITY
- **Description:** Null rate changed from 89.7% to 0.0%
- **Impact:** Data quality change may indicate upstream issues or different data collection

**Recommended Actions:**
- Investigate source of null rate change
- Review data validation rules

### CRITICAL: 24 Field Type Changes

- **Time:** 2025-05-24 12:40
- **Type:** TYPE_CHANGES
- **Description:** Data types have changed for fields: Split, candidate_cavagnolo, PGP, LBT, IND, total_voters, SewerDist, Precinct, candidate_write_in, total_votes, is_competitive, WaterDist, CON, WTP, NLB, is_non_pps_precinct, PRO, WFP, precinct, candidate_leof, base_precinct, CoP_Dist, OTH, candidate_splitt
- **Impact:** Type changes may break analysis: Precinct: string→numeric, precinct: string→numeric, base_precinct: string→numeric

**Recommended Actions:**
- Review data cleaning and type conversion logic
- Test existing calculations with new data types
- Update validation rules if necessary
- Consider adding explicit type conversion in preprocessing

### CRITICAL: 8 Fields Removed

- **Time:** 2025-05-24 12:37
- **Type:** REMOVED_FIELDS
- **Description:** Fields have been removed from the data schema: candidate_no, candidate_yes, vote_pct_contribution_no, vote_pct_contribution_yes, vote_pct_no, vote_pct_yes, votes_no, votes_yes
- **Impact:** CRITICAL: Removed fields include critical data: votes_yes, votes_no. This may break downstream analysis.

**Recommended Actions:**
- Review code for references to removed fields
- Update field registry to remove obsolete definitions
- Check if removed fields were used in critical calculations
- Consider graceful degradation for missing fields

### CRITICAL: 16 New Fields Detected

- **Time:** 2025-05-24 12:37
- **Type:** NEW_FIELDS
- **Description:** New fields have been added to the data schema: candidate_cavagnolo, candidate_leof, candidate_splitt, candidate_write_in, vote_pct_cavagnolo, vote_pct_contribution_cavagnolo, vote_pct_contribution_leof, vote_pct_contribution_splitt, vote_pct_contribution_write_in, vote_pct_leof, vote_pct_splitt, vote_pct_write_in, votes_cavagnolo, votes_leof, votes_splitt, votes_write_in
- **Impact:** Impact: 4 new vote counts fields, 8 new vote percentages fields, 4 new candidate metadata fields. May require field registry updates and documentation.

**Recommended Actions:**
- Review new fields and determine if they need documentation
- Update field registry with explanations for important fields
- Check if new fields affect existing analysis or visualizations
- Consider implementing auto-registration patterns for bulk field additions

### CRITICAL: 20 Fields Removed

- **Time:** 2025-05-24 12:30
- **Type:** REMOVED_FIELDS
- **Description:** Fields have been removed from the data schema: candidate_crowe, candidate_engelsman, candidate_galanakis, candidate_mains, candidate_write_in, vote_pct_contribution_crowe, vote_pct_contribution_engelsman, vote_pct_contribution_galanakis, vote_pct_contribution_mains, vote_pct_contribution_write_in, vote_pct_crowe, vote_pct_engelsman, vote_pct_galanakis, vote_pct_mains, vote_pct_write_in, votes_crowe, votes_engelsman, votes_galanakis, votes_mains, votes_write_in
- **Impact:** CRITICAL: Removed fields include critical data: votes_write_in, votes_galanakis, votes_mains, votes_engelsman, votes_crowe. This may break downstream analysis.

**Recommended Actions:**
- Review code for references to removed fields
- Update field registry to remove obsolete definitions
- Check if removed fields were used in critical calculations
- Consider graceful degradation for missing fields

### HIGH: 8 New Fields Detected

- **Time:** 2025-05-24 12:30
- **Type:** NEW_FIELDS
- **Description:** New fields have been added to the data schema: candidate_no, candidate_yes, vote_pct_contribution_no, vote_pct_contribution_yes, vote_pct_no, vote_pct_yes, votes_no, votes_yes
- **Impact:** Impact: 2 new vote counts fields, 4 new vote percentages fields, 2 new candidate metadata fields. May require field registry updates and documentation.

**Recommended Actions:**
- Review new fields and determine if they need documentation
- Update field registry with explanations for important fields
- Check if new fields affect existing analysis or visualizations
- Consider implementing auto-registration patterns for bulk field additions

## Schema Evolution

- **Field Count Change:** +8 fields
- **Record Count Change:** +0 records
- **Schema Stability:** Evolving

## Current Field Distribution

| Category | Count |
|----------|-------|
| Identifiers | 4 |
| Vote Counts | 7 |
| Vote Percentages | 11 |
| Registration Counts | 13 |
| Registration Percentages | 12 |
| Geographic Districts | 18 |
| Candidate Metadata | 5 |
| Boolean Flags | 10 |
| Calculated Metrics | 7 |
| Shape Metadata | 2 |
| Other | 32 |

## Recommendations

Based on the analysis of schema drift over the past 7 days:

⚠️ **Action Required:** Critical or high-severity alerts detected.
- Review and address high-priority alerts immediately
- Investigate upstream data source changes
- Update field registry and documentation as needed