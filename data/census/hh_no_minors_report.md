# Household Demographics Report - PPS District

## Executive Summary

This report analyzes household demographics within the Portland Public Schools (PPS) district,
with particular focus on households without minors (children under 18). This demographic analysis
is relevant for understanding potential voting patterns in school board elections.

## Key Findings

- **Total Block Groups Analyzed**: 416
- **Total Households**: 234,546
- **Households without Minors**: 109,039
- **Overall Percentage without Minors**: 46.5%

## Statistical Analysis

### Distribution Quartiles
- **25th Percentile**: 35.5% households without minors
- **Median (50th Percentile)**: 50.2% households without minors
- **75th Percentile**: 62.6% households without minors

### Household Density Statistics
- **Mean Density**: 1006.6 households/km²
- **Median Density**: 642.0 households/km²
- **Maximum Density**: 17474.6 households/km²
- **Standard Deviation**: 1547.8 households/km²

### Geographic Coverage
- **Block Groups with Data**: 415 out of 416
- **Data Coverage**: 99.8%

## Top 10 Block Groups by Percentage Without Minors

|        GEOID |   total_households |   households_no_minors |   pct_households_no_minors |   household_density |   area_km2 |
|-------------:|-------------------:|-----------------------:|---------------------------:|--------------------:|-----------:|
| 410510003021 |                489 |                    460 |                       94.1 |               558.9 |      0.875 |
| 410510028021 |                296 |                    264 |                       89.2 |               614.1 |      0.482 |
| 410510030004 |                510 |                    453 |                       88.8 |               636.7 |      0.801 |
| 410510017033 |                279 |                    247 |                       88.5 |               609.2 |      0.458 |
| 410510025012 |                438 |                    381 |                       87   |               922.1 |      0.475 |
| 410510065013 |                577 |                    492 |                       85.3 |               241   |      2.394 |
| 410510026002 |                330 |                    281 |                       85.2 |               628.6 |      0.525 |
| 410510003025 |                381 |                    323 |                       84.8 |               481.7 |      0.791 |
| 410510070023 |                366 |                    310 |                       84.7 |                13.1 |     27.981 |
| 410510058005 |                245 |                    207 |                       84.5 |               181.9 |      1.347 |

## Bottom 10 Block Groups by Percentage Without Minors

|        GEOID |   total_households |   households_no_minors |   pct_households_no_minors |   household_density |   area_km2 |
|-------------:|-------------------:|-----------------------:|---------------------------:|--------------------:|-----------:|
| 410510055002 |                418 |                     23 |                        5.5 |              3166.7 |      0.132 |
| 410510052013 |                796 |                     38 |                        4.8 |              2145.6 |      0.371 |
| 410510049021 |               1038 |                     49 |                        4.7 |              5463.2 |      0.19  |
| 410510106021 |                521 |                     16 |                        3.1 |               626.2 |      0.832 |
| 410510050022 |                445 |                      0 |                        0   |               789   |      0.564 |
| 410510056023 |                135 |                      0 |                        0   |               535.7 |      0.252 |
| 410510050021 |                325 |                      0 |                        0   |              1359.8 |      0.239 |
| 410510051031 |                217 |                      0 |                        0   |              1695.3 |      0.128 |
| 410510106012 |                119 |                      0 |                        0   |               414.6 |      0.287 |
| 410519800001 |                  0 |                      0 |                        0   |                 0   |     11.665 |

## Data Sources and Methodology

- **Data Source**: American Community Survey (ACS) 5-Year Estimates
- **Geographic Level**: Census Block Groups
- **Spatial Filter**: Block groups within PPS district boundaries (centroid-based)
- **Analysis Method**: Descriptive statistics and spatial analysis

## Technical Notes

- Block groups filtered using centroid-based intersection with PPS district
- Household density calculated using accurate projected coordinate system
- Missing data handled with appropriate defaults (0 for counts, blank for rates)
- All calculations validated and cross-checked for accuracy

---
*Report generated on 2025-05-27 00:12:08 by automated analysis pipeline*
*Project: 2025 Portland School Board Election Analysis*
