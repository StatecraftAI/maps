# Field Registry System Usage Guide

## Overview

The Field Registry system ensures that every calculated field in your election analysis has a proper explanation with the actual formula. This prevents drift between code and documentation and creates self-documenting datasets.

## Key Features

1. **Formula Documentation**: Shows actual calculation formulas, not just descriptions
2. **Automatic Validation**: Ensures all fields have explanations
3. **Self-Documenting Data**: Explanations are embedded in the output GeoJSON
4. **Type Safety**: Tracks field types (percentage, count, ratio, categorical, boolean)
5. **Quality Assurance**: Prevents missing or orphaned explanations

## How It Works

### 1. Field Registration

When you create a new calculated field, you MUST register it:

```python
from analysis.map_election_results import register_calculated_field

# Register your new calculated field
register_calculated_field(
    name="voter_enthusiasm_score",
    description="Measures voter enthusiasm by combining turnout and registration growth",
    formula="(turnout_rate * 0.7) + (registration_growth_rate * 0.3)",
    field_type="ratio",
    units="score (0-100)"
)
```

### 2. Field Types

- `percentage`: Values in 0-100 range (e.g., turnout_rate)
- `count`: Integer counts (e.g., votes_total)
- `ratio`: Mathematical ratios (e.g., candidate_dominance)
- `categorical`: String categories (e.g., political_lean)
- `boolean`: True/False values (e.g., is_zone1_precinct)

### 3. Automatic Validation

The system automatically validates completeness:

```python
# This is called automatically in main()
validate_field_completeness(gdf_merged)
```

If validation fails, you'll see:

```
❌ Field completeness validation FAILED: Missing explanations for fields: ['new_field', 'another_field']
   Please register missing fields using register_calculated_field()
```

### 4. Self-Documenting Output

Explanations are automatically embedded in the GeoJSON metadata:

```json
{
  "metadata": {
    "layer_explanations": {
      "turnout_rate": "Percentage of registered voters who actually voted in this election\n\n**Formula:** `(votes_total / total_voters) * 100`\n\n**Units:** percent",
      "dem_advantage": "Democratic registration advantage (positive) or disadvantage (negative)\n\n**Formula:** `reg_pct_dem - reg_pct_rep`\n\n**Units:** percentage points"
    }
  }
}
```

## Example: Adding a New Analytical Field

Let's say you want to add a "swing voter potential" metric:

### Step 1: Register the Field

```python
register_calculated_field(
    name="swing_voter_potential",
    description="Estimates the potential for swing voting based on non-affiliated registration and historical competitiveness",
    formula="(reg_pct_nav / 100) * (100 - ABS(dem_advantage)) * (turnout_rate / 100)",
    field_type="ratio",
    units="potential score (0-1)"
)
```

### Step 2: Calculate the Field

```python
def calculate_swing_voter_potential(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate swing voter potential for each precinct."""
    df = df.copy()
    
    # Ensure all components are numeric
    nav_pct = pd.to_numeric(df['reg_pct_nav'], errors='coerce').fillna(0)
    dem_adv = pd.to_numeric(df['dem_advantage'], errors='coerce').fillna(0)
    turnout = pd.to_numeric(df['turnout_rate'], errors='coerce').fillna(0)
    
    # Apply the registered formula
    df['swing_voter_potential'] = (nav_pct / 100) * (100 - abs(dem_adv)) * (turnout / 100)
    
    return df
```

### Step 3: Use in Analysis

```python
# Add to your data processing pipeline
gdf_merged = calculate_swing_voter_potential(gdf_merged)

# Validation will automatically check that swing_voter_potential has an explanation
validate_field_completeness(gdf_merged)
```

### Step 4: Generate Map

```python
# The field is now ready for visualization
tufte_map(
    gdf_merged,
    "swing_voter_potential",
    fname=maps_dir / "swing_voter_potential.png",
    config=config,
    title="Swing Voter Potential by Precinct",
    label="Swing Potential",
    note="Higher values indicate more potential for swing voting behavior"
)
```

## Registry Management

### View All Registered Fields

```python
from analysis.map_election_results import FIELD_REGISTRY

# Get all explanations
explanations = FIELD_REGISTRY.get_all_explanations()
for field, explanation in explanations.items():
    print(f"{field}: {explanation[:100]}...")
```

### Check Specific Field

```python
explanation = FIELD_REGISTRY.get_explanation("turnout_rate")
print(explanation)
```

### Validation Report

```python
validation = FIELD_REGISTRY.validate_gdf_completeness(gdf)
print(f"Missing explanations: {validation['missing_explanations']}")
print(f"Orphaned explanations: {validation['orphaned_explanations']}")
```

## Best Practices

### 1. Register Fields When You Create Them

Don't wait until the end - register fields immediately after you define their calculation logic.

### 2. Use Precise Formulas

Show the actual mathematical formula, not a prose description:

- ✅ Good: `(votes_candidate_a / votes_total) * 100`
- ❌ Bad: "Percentage of votes for candidate A"

### 3. Include Edge Case Handling

Show how you handle division by zero, null values, etc:

```python
formula="CASE WHEN total_voters > 0 THEN (votes_total / total_voters) * 100 ELSE 0 END"
```

### 4. Use Consistent Units

Be explicit about units and ranges:

- Percentages: "percent (0-100)"
- Ratios: "ratio" or "score (0-1)"
- Counts: "votes" or "registrations"

### 5. Validate Early and Often

Add validation checks in your development workflow:

```python
# After adding new fields
try:
    validate_field_completeness(gdf)
    print("✅ All fields properly documented")
except ValueError as e:
    print(f"❌ Missing documentation: {e}")
```

## Migration from Old System

If you have existing hardcoded explanations, migrate them:

### Old Way (hardcoded dictionary)

```python
explanations = {
    "my_field": "Some description of what this field means"
}
```

### New Way (registry-based)

```python
register_calculated_field(
    name="my_field",
    description="Some description of what this field means",
    formula="actual_formula_used_to_calculate_it",
    field_type="percentage",
    units="percent"
)
```

## Troubleshooting

### "Field not found in registry" Error

You need to register the field:

```python
register_calculated_field(name="missing_field", ...)
```

### Validation Fails

Check which fields are missing explanations:

```python
validation = FIELD_REGISTRY.validate_gdf_completeness(gdf)
print(validation['missing_explanations'])
```

### Dynamic Fields (Candidates)

Candidate-specific fields are handled automatically and don't need registration:

- `votes_candidate_name` ✓ (auto-generated)
- `vote_pct_candidate_name` ✓ (auto-generated)
- `vote_pct_contribution_candidate_name` ✓ (auto-generated)

## Benefits

1. **No More Documentation Drift**: Formulas stay in sync with code
2. **Quality Assurance**: Validation catches missing explanations
3. **Self-Documenting Data**: Users can see exactly how fields are calculated
4. **Maintainability**: New team members understand field definitions
5. **Transparency**: Research can be replicated using documented formulas
