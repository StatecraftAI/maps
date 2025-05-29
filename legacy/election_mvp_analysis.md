# Election Data MVP: CTO Reality Check

## The Real Problem

**User Journey**: "I have voter registration CSV + election results CSV + precinct boundaries → give me clean election geodata in the database"

**Current State**: 2,460 lines across 2 scripts doing this
**MVP Goal**: One simple preprocessing script + `geo_upload.py`

## What Actually Matters (CTO Analysis)

### Core Data Pipeline
```
Raw CSVs → Process & Enrich → GeoDataFrame → Upload → Database
```

### Real Business Logic (Not Framework Fluff)

1. **Load & Merge** (20 lines)
   - Load voter registration CSV
   - Load election results CSV
   - Join on precinct ID

2. **Calculate Key Metrics** (40 lines)
   - Vote percentages per candidate
   - Turnout rates
   - Margins and competitiveness
   - Registration vs results analysis

3. **Merge with Geography** (10 lines)
   - Load precinct boundaries GeoJSON
   - Join enriched data with geometries

4. **Clean & Standardize** (20 lines)
   - Fix data types
   - Handle missing values
   - Validate geometries

5. **Output Clean GeoDataFrame** (10 lines)
   - Ready for `geo_upload.py`

**Total MVP: ~100 lines**

## What We Don't Need (Cut This)

❌ **Field Registry System** (300+ lines) - Just hardcode the important fields
❌ **Dynamic Candidate Detection** (200+ lines) - Config-driven is fine
❌ **Advanced Analytics** (500+ lines) - Calculate what maps actually show
❌ **Complex Validation** (200+ lines) - Basic checks are enough
❌ **Auto-registration** (150+ lines) - Explicit is better than implicit

## The MVP Script: `prepare_election_data.py`

```python
def prepare_election_data(voter_csv, results_csv, boundaries_geojson, output_file):
    """The only function that matters"""

    # 1. Load and merge CSVs
    voters = pd.read_csv(voter_csv)
    results = pd.read_csv(results_csv)
    data = voters.merge(results, on='precinct')

    # 2. Calculate core metrics
    data = add_vote_percentages(data)
    data = add_turnout_rates(data)
    data = add_competition_metrics(data)

    # 3. Merge with geography
    boundaries = gpd.read_file(boundaries_geojson)
    geodata = boundaries.merge(data, on='precinct')

    # 4. Clean and validate
    geodata = clean_data_types(geodata)
    geodata = validate_geometries(geodata)

    # 5. Save for upload
    geodata.to_file(output_file)
    return geodata
```

## Files That Matter for MVP

1. **`prepare_election_data.py`** (~100 lines) - The preprocessing
2. **`geo_upload.py`** (116 lines) - The upload
3. **`config.yaml`** - Column mappings and thresholds

**Total: ~220 lines**
**Reduction: 91% from 2,460 lines**

## What Gets Calculated (Keep It Simple)

### Vote Metrics
- `vote_pct_[candidate]` - Percentage each candidate got
- `turnout_rate` - Votes cast / registered voters
- `vote_margin` - Winner margin in votes
- `margin_pct` - Winner margin as percentage

### Competition Analysis
- `competitiveness` - "Competitive", "Safe", "Toss-up"
- `leading_candidate` - Who won each precinct
- `is_competitive` - Boolean for close races

### Registration Analysis
- `reg_pct_dem`, `reg_pct_rep` - Party registration %
- `dem_advantage` - Dem registration - Rep registration
- `political_lean` - Based on registration patterns

**That's it.** These 12 fields power 90% of election visualizations.

## Implementation Strategy

### Phase 1: Extract Core Logic (1 day)
- Pull the actual calculation functions from the legacy scripts
- Strip out all the framework code
- Create `prepare_election_data.py` with ~100 lines

### Phase 2: Test & Validate (1 day)
- Run on existing election data
- Verify outputs match legacy scripts
- Test with `geo_upload.py` pipeline

### Phase 3: Clean Up (1 day)
- Remove legacy scripts
- Update documentation
- Create simple usage examples

**Total: 3 days to MVP**

## The CTO Mindset

**Question**: "Will this let us ship election maps faster?"
**Answer**: Yes. 91% less code, same functionality.

**Question**: "Is this maintainable?"
**Answer**: Yes. 220 total lines vs 2,460 lines.

**Question**: "Does it solve the real problem?"
**Answer**: Yes. Raw data → clean geodata → database.

**Anti-Pattern**: Building frameworks when you need solutions.
**MVP Pattern**: Solve the specific problem simply.

---

*"The best frameworks are the ones you never had to write."*
