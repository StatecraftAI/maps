#!/usr/bin/env python3
"""
Test script to demonstrate the adaptive field registry capabilities
"""

import sys
import os
sys.path.append('analysis')

# Import just the registry components we need
from map_election_results import FieldRegistry, FieldDefinition, export_field_registry_report
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

def create_sample_data():
    """Create sample election data to test schema drift handling"""
    
    # Simulate upstream data with various field types
    data = {
        # Core identifiers
        'precinct': ['4101', '4102', '4103'],
        'Precinct': ['4101', '4102', '4103'],  # Duplicate ID field
        'base_precinct': ['4101', '4102', '4103'],
        
        # Candidate vote counts (new pattern)
        'votes_sanchez_bautista': [510, 425, 389],
        'votes_la_forte': [658, 532, 467],
        'votes_write_in': [6, 4, 2],
        'votes_total': [1174, 961, 858],
        
        # Candidate percentages (auto-detectable)
        'vote_pct_sanchez_bautista': [43.4, 44.2, 45.3],
        'vote_pct_la_forte': [56.0, 55.4, 54.4],
        'vote_pct_write_in': [0.5, 0.4, 0.2],
        
        # Registration data (new upstream format)
        'TOTAL': [3081, 2543, 2250],
        'DEM': [1543, 1272, 1125],
        'REP': [711, 508, 450],
        'NAV': [827, 763, 675],
        'OTH': [0, 0, 0],
        'IND': [0, 0, 0],
        'LBT': [0, 0, 0],  # New party codes
        'WFP': [0, 0, 0],
        
        # Registration percentages (detectable pattern)
        'reg_pct_dem': [50.1, 50.0, 50.0],
        'reg_pct_rep': [23.1, 20.0, 20.0],
        'reg_pct_nav': [26.8, 30.0, 30.0],
        
        # Geographic districts (new upstream fields)
        'OR_House': ['41', '41', '41'],
        'OR_Senate': ['21', '21', '21'],
        'USCongress': ['3', '3', '3'],
        'CITY': ['Portland', 'Portland', 'Portland'],
        'SchoolDist': ['1J', '1J', '1J'],
        'FIRE_DIST': ['10', '10', '10'],
        
        # Boolean flags (new pattern)
        'is_zone1_precinct': [True, True, True],
        'has_election_results': [True, True, True],
        'has_voter_registration': [True, True, True],
        'is_competitive': [True, False, False],
        
        # Shape metadata (GIS fields)
        'Shape_Area': [125456.8, 98743.2, 87654.1],
        'Shape_Leng': [1543.2, 1234.5, 1098.7],
        
        # Calculated metrics (new analytical fields)
        'margin_pct': [12.6, 11.2, 9.1],
        'total_votes': [1174, 961, 858],
        'zone1_vote_share': [3.6, 2.9, 2.6],
        'precinct_size': ['Large', 'Medium', 'Medium'],
        
        # Candidate metadata (new format)
        'candidate_sanchez_bautista': ['Sanchez-Bautista', 'Sanchez-Bautista', 'Sanchez-Bautista'],
        'candidate_la_forte': ['La Forte', 'La Forte', 'La Forte'],
        'candidate_write_in': ['Write-In', 'Write-In', 'Write-In'],
        
        # Record metadata
        'record_type': ['zone1_precinct', 'zone1_precinct', 'zone1_precinct'],
        'second_candidate': ['La Forte', 'La Forte', 'La Forte'],
        'Split': ['', '', ''],
    }
    
    # Create sample geometries
    geometries = [
        Point(-122.6, 45.5),
        Point(-122.7, 45.6), 
        Point(-122.8, 45.7)
    ]
    
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(data, geometry=geometries, crs='EPSG:4326')
    
    return gdf

def test_schema_drift_handling():
    """Test the adaptive field registry with simulated schema drift"""
    
    print("🧪 Testing Adaptive Field Registry Schema Drift Handling")
    print("=" * 60)
    
    # Create sample data simulating upstream schema changes
    gdf = create_sample_data()
    
    print(f"\n📊 Sample data created with {len(gdf.columns)} fields:")
    field_types = {}
    for col in gdf.columns:
        if col == 'geometry':
            continue
        if col.startswith('votes_'):
            field_types.setdefault('Vote Counts', []).append(col)
        elif col.startswith('vote_pct_'):
            field_types.setdefault('Vote Percentages', []).append(col)
        elif col.startswith('reg_pct_'):
            field_types.setdefault('Registration Pct', []).append(col)
        elif col.startswith('candidate_'):
            field_types.setdefault('Candidate Meta', []).append(col)
        elif col.startswith('is_') or col.startswith('has_'):
            field_types.setdefault('Boolean Flags', []).append(col)
        elif col in ['OR_House', 'OR_Senate', 'CITY', 'SchoolDist', 'FIRE_DIST']:
            field_types.setdefault('Geographic Districts', []).append(col)
        elif col in ['TOTAL', 'DEM', 'REP', 'NAV', 'OTH', 'IND', 'LBT', 'WFP']:
            field_types.setdefault('Registration Counts', []).append(col)
        else:
            field_types.setdefault('Other', []).append(col)
    
    for category, fields in field_types.items():
        print(f"  • {category}: {len(fields)} fields")
        if len(fields) <= 5:
            print(f"    {fields}")
        else:
            print(f"    {fields[:3]} ... +{len(fields)-3} more")
    
    # Test the adaptive registry
    print(f"\n🔧 Testing field registry adaptation:")
    
    # Create registry in strict mode to see difference
    registry_strict = FieldRegistry(strict_mode=True)
    print(f"  📝 Base registry initialized with {len(registry_strict._fields)} core fields")
    
    # Test auto-registration
    validation = registry_strict.validate_gdf_completeness(gdf)
    
    print(f"\n📈 Auto-registration results:")
    print(f"  • Total fields in data: {validation['total_fields']}")
    print(f"  • Auto-registered fields: {validation.get('auto_registered', 0)}")
    print(f"  • Final coverage: {validation['explained_fields']}/{validation['total_fields']} ({validation['explained_fields']/validation['total_fields']*100:.1f}%)")
    
    if validation['missing_explanations']:
        print(f"  • Still missing: {len(validation['missing_explanations'])} fields")
        print(f"    {validation['missing_explanations'][:5]}...")
    else:
        print(f"  ✅ All fields have explanations!")
    
    if validation['orphaned_explanations']:
        print(f"  • Orphaned definitions: {len(validation['orphaned_explanations'])} fields")
        print(f"    {validation['orphaned_explanations'][:3]}...")
    
    # Test explanation generation
    print(f"\n📚 Sample auto-generated explanations:")
    sample_fields = ['votes_sanchez_bautista', 'reg_pct_dem', 'OR_House', 'is_zone1_precinct', 'Shape_Area']
    for field in sample_fields:
        if field in gdf.columns:
            explanation = registry_strict.get_explanation(field)
            print(f"\n  **{field}**:")
            print(f"  {explanation[:100]}...")
    
    # Export registry report
    print(f"\n📄 Exporting field registry report...")
    try:
        export_field_registry_report('field_registry_test_report.md')
        print(f"  ✅ Report saved to: field_registry_test_report.md")
    except Exception as e:
        print(f"  ⚠️  Export failed: {e}")
    
    # Test pattern suggestions
    from map_election_results import suggest_missing_field_registrations
    if validation['missing_explanations']:
        print(f"\n💡 Auto-generated registration suggestions:")
        suggestions = suggest_missing_field_registrations(validation['missing_explanations'][:3])
        for i, suggestion in enumerate(suggestions[:3], 1):
            print(f"\n  {i}. {suggestion}")
    
    print(f"\n✅ Schema drift handling test completed!")
    print(f"🎯 The registry successfully adapted to {validation.get('auto_registered', 0)} new field patterns")

if __name__ == "__main__":
    test_schema_drift_handling() 