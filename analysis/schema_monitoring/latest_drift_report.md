# Schema Drift Monitoring Report

**Report Period:** 2025-05-16 to 2025-05-23 (7 days)
**Generated:** 2025-05-23 17:02:37

## Executive Summary

- **Schema Snapshots Captured:** 16
- **Drift Alerts Generated:** 16
- **Current Schema Hash:** 417b9fd834195fa8
- **Current Field Count:** 109

### Alert Summary

| Severity | Count |
|----------|-------|
| CRITICAL | 12 |
| HIGH | 4 |
| MEDIUM | 0 |
| LOW | 0 |

## Recent Alerts

### CRITICAL: 12 Fields Removed

- **Time:** 2025-05-23 17:02
- **Type:** REMOVED_FIELDS
- **Description:** Fields have been removed from the data schema: candidate_chase_miller, candidate_greene, candidate_write_in, vote_pct_chase_miller, vote_pct_contribution_chase_miller, vote_pct_contribution_greene, vote_pct_contribution_write_in, vote_pct_greene, vote_pct_write_in, votes_chase_miller, votes_greene, votes_write_in
- **Impact:** CRITICAL: Removed fields include critical data: votes_write_in, votes_greene, votes_chase_miller. This may break downstream analysis.

**Recommended Actions:**
- Review code for references to removed fields
- Update field registry to remove obsolete definitions
- Check if removed fields were used in critical calculations
- Consider graceful degradation for missing fields

### HIGH: 8 New Fields Detected

- **Time:** 2025-05-23 17:02
- **Type:** NEW_FIELDS
- **Description:** New fields have been added to the data schema: candidate_no, candidate_yes, vote_pct_contribution_no, vote_pct_contribution_yes, vote_pct_no, vote_pct_yes, votes_no, votes_yes
- **Impact:** Impact: 2 new vote counts fields, 4 new vote percentages fields, 2 new candidate metadata fields. May require field registry updates and documentation.

**Recommended Actions:**
- Review new fields and determine if they need documentation
- Update field registry with explanations for important fields
- Check if new fields affect existing analysis or visualizations
- Consider implementing auto-registration patterns for bulk field additions

### CRITICAL: 12 Fields Removed

- **Time:** 2025-05-23 17:02
- **Type:** REMOVED_FIELDS
- **Description:** Fields have been removed from the data schema: candidate_cavagnolo, candidate_leof, candidate_splitt, vote_pct_cavagnolo, vote_pct_contribution_cavagnolo, vote_pct_contribution_leof, vote_pct_contribution_splitt, vote_pct_leof, vote_pct_splitt, votes_cavagnolo, votes_leof, votes_splitt
- **Impact:** CRITICAL: Removed fields include critical data: votes_cavagnolo, votes_splitt, votes_leof. This may break downstream analysis.

**Recommended Actions:**
- Review code for references to removed fields
- Update field registry to remove obsolete definitions
- Check if removed fields were used in critical calculations
- Consider graceful degradation for missing fields

### HIGH: 8 New Fields Detected

- **Time:** 2025-05-23 17:02
- **Type:** NEW_FIELDS
- **Description:** New fields have been added to the data schema: candidate_chase_miller, candidate_greene, vote_pct_chase_miller, vote_pct_contribution_chase_miller, vote_pct_contribution_greene, vote_pct_greene, votes_chase_miller, votes_greene
- **Impact:** Impact: 2 new vote counts fields, 4 new vote percentages fields, 2 new candidate metadata fields. May require field registry updates and documentation.

**Recommended Actions:**
- Review new fields and determine if they need documentation
- Update field registry with explanations for important fields
- Check if new fields affect existing analysis or visualizations
- Consider implementing auto-registration patterns for bulk field additions

### CRITICAL: 16 Fields Removed

- **Time:** 2025-05-23 17:02
- **Type:** REMOVED_FIELDS
- **Description:** Fields have been removed from the data schema: candidate_crowe, candidate_engelsman, candidate_galanakis, candidate_mains, vote_pct_contribution_crowe, vote_pct_contribution_engelsman, vote_pct_contribution_galanakis, vote_pct_contribution_mains, vote_pct_crowe, vote_pct_engelsman, vote_pct_galanakis, vote_pct_mains, votes_crowe, votes_engelsman, votes_galanakis, votes_mains
- **Impact:** CRITICAL: Removed fields include critical data: votes_crowe, votes_engelsman, votes_mains, votes_galanakis. This may break downstream analysis.

**Recommended Actions:**
- Review code for references to removed fields
- Update field registry to remove obsolete definitions
- Check if removed fields were used in critical calculations
- Consider graceful degradation for missing fields

### CRITICAL: 12 New Fields Detected

- **Time:** 2025-05-23 17:02
- **Type:** NEW_FIELDS
- **Description:** New fields have been added to the data schema: candidate_cavagnolo, candidate_leof, candidate_splitt, vote_pct_cavagnolo, vote_pct_contribution_cavagnolo, vote_pct_contribution_leof, vote_pct_contribution_splitt, vote_pct_leof, vote_pct_splitt, votes_cavagnolo, votes_leof, votes_splitt
- **Impact:** Impact: 3 new vote counts fields, 6 new vote percentages fields, 3 new candidate metadata fields. May require field registry updates and documentation.

**Recommended Actions:**
- Review new fields and determine if they need documentation
- Update field registry with explanations for important fields
- Check if new fields affect existing analysis or visualizations
- Consider implementing auto-registration patterns for bulk field additions

### CRITICAL: 8 Fields Removed

- **Time:** 2025-05-23 17:01
- **Type:** REMOVED_FIELDS
- **Description:** Fields have been removed from the data schema: candidate_la_forte, candidate_sanchez_bautista, vote_pct_contribution_la_forte, vote_pct_contribution_sanchez_bautista, vote_pct_la_forte, vote_pct_sanchez_bautista, votes_la_forte, votes_sanchez_bautista
- **Impact:** CRITICAL: Removed fields include critical data: votes_la_forte, votes_sanchez_bautista. This may break downstream analysis.

**Recommended Actions:**
- Review code for references to removed fields
- Update field registry to remove obsolete definitions
- Check if removed fields were used in critical calculations
- Consider graceful degradation for missing fields

### CRITICAL: 16 New Fields Detected

- **Time:** 2025-05-23 17:01
- **Type:** NEW_FIELDS
- **Description:** New fields have been added to the data schema: candidate_crowe, candidate_engelsman, candidate_galanakis, candidate_mains, vote_pct_contribution_crowe, vote_pct_contribution_engelsman, vote_pct_contribution_galanakis, vote_pct_contribution_mains, vote_pct_crowe, vote_pct_engelsman, vote_pct_galanakis, vote_pct_mains, votes_crowe, votes_engelsman, votes_galanakis, votes_mains
- **Impact:** Impact: 4 new vote counts fields, 8 new vote percentages fields, 4 new candidate metadata fields. May require field registry updates and documentation.

**Recommended Actions:**
- Review new fields and determine if they need documentation
- Update field registry with explanations for important fields
- Check if new fields affect existing analysis or visualizations
- Consider implementing auto-registration patterns for bulk field additions

### CRITICAL: 8 Fields Removed

- **Time:** 2025-05-23 17:00
- **Type:** REMOVED_FIELDS
- **Description:** Fields have been removed from the data schema: candidate_chase_miller, candidate_greene, vote_pct_chase_miller, vote_pct_contribution_chase_miller, vote_pct_contribution_greene, vote_pct_greene, votes_chase_miller, votes_greene
- **Impact:** CRITICAL: Removed fields include critical data: votes_chase_miller, votes_greene. This may break downstream analysis.

**Recommended Actions:**
- Review code for references to removed fields
- Update field registry to remove obsolete definitions
- Check if removed fields were used in critical calculations
- Consider graceful degradation for missing fields

### HIGH: 8 New Fields Detected

- **Time:** 2025-05-23 17:00
- **Type:** NEW_FIELDS
- **Description:** New fields have been added to the data schema: candidate_la_forte, candidate_sanchez_bautista, vote_pct_contribution_la_forte, vote_pct_contribution_sanchez_bautista, vote_pct_la_forte, vote_pct_sanchez_bautista, votes_la_forte, votes_sanchez_bautista
- **Impact:** Impact: 2 new vote counts fields, 4 new vote percentages fields, 2 new candidate metadata fields. May require field registry updates and documentation.

**Recommended Actions:**
- Review new fields and determine if they need documentation
- Update field registry with explanations for important fields
- Check if new fields affect existing analysis or visualizations
- Consider implementing auto-registration patterns for bulk field additions

## Schema Evolution

- **Field Count Change:** +2 fields
- **Record Count Change:** +0 records
- **Schema Stability:** Evolving

## Current Field Distribution

| Category | Count |
|----------|-------|
| Identifiers | 4 |
| Vote Counts | 5 |
| Vote Percentages | 7 |
| Registration Counts | 13 |
| Registration Percentages | 12 |
| Geographic Districts | 18 |
| Candidate Metadata | 3 |
| Boolean Flags | 10 |
| Calculated Metrics | 7 |
| Shape Metadata | 2 |
| Other | 28 |

## Recommendations

Based on the analysis of schema drift over the past 7 days:

⚠️ **Action Required:** Critical or high-severity alerts detected.
- Review and address high-priority alerts immediately
- Investigate upstream data source changes
- Update field registry and documentation as needed
