# Election and Demographics Analysis Pipeline

Transform raw election data into beautiful, interactive web maps with comprehensive demographic analysis in just a few commands.

[![Deploy static content to Pages](https://github.com/populist-consensus/populistconsensus.github.io/actions/workflows/static.yml/badge.svg)](https://github.com/populist-consensus/populistconsensus.github.io/actions/workflows/static.yml)

[![CodeQL](https://github.com/populist-consensus/populistconsensus.github.io/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/populist-consensus/populistconsensus.github.io/actions/workflows/github-code-scanning/codeql)

## 🎯 What This Pipeline Does

This comprehensive analysis pipeline takes your election and demographic data and creates:

### 📊 **Core Election Analysis**

- ✅ **Enriched analytical dataset** (political lean, engagement scores, competitiveness)
- ✅ **Static analysis maps** (9 different visualizations)
- ✅ **Web-optimized GeoJSON** (for development)
- ✅ **Vector tiles** (for production)
- ✅ **Interactive election map** with 8 switchable data layers

### 👥 **Demographics Analysis** (Optional)

- ✅ **Voter location heatmaps** (inside/outside district analysis)
- ✅ **Household demographics** (ACS data choropleth maps)
- ✅ **Classification reports** (CSV exports and markdown summaries)
- ✅ **Interactive demographic maps** with detailed tooltips

## 🚀 Quick Start (Recommended)

### Using the Pipeline Orchestrator

```bash
# Install dependencies
pip install pandas geopandas matplotlib folium PyYAML

# Configure your data files once
cd analysis
cp config.yaml config.yaml.backup  # backup the example
nano config.yaml  # Edit filenames to match your data

# Run complete election analysis (3 steps)
python run_pipeline.py

# Run everything including demographics (5 steps)
python run_pipeline.py --include-demographics

# Run only demographic analysis
python run_pipeline.py --demographics-only
```

### Flexible Execution Options

```bash
# Skip specific steps
python run_pipeline.py --skip-enrichment    # Use existing enriched data
python run_pipeline.py --skip-maps          # Only enrich data and create tiles
python run_pipeline.py --skip-tiles         # Only enrich data and generate maps

# Shortcuts
python run_pipeline.py --maps-only          # Only generate maps
```

## 📁 Complete Directory Structure

```shell
your-project/
├── README.md                                   # This comprehensive guide
├── analysis/
│   ├── config.yaml                            # 🎯 CENTRALIZED CONFIGURATION
│   ├── config_loader.py                       # Configuration management utility
│   ├── run_pipeline.py                        # 🎯 MAIN PIPELINE ORCHESTRATOR
│   ├── enrich_voters_election_data.py         # Step 1: Data enrichment
│   ├── map_election_results.py                # Step 2: Map generation
│   ├── create_vector_tiles.py                 # Step 3: Vector tiles
│   ├── map_voters.py                          # Step 4: Voter analysis
│   ├── map_households.py                      # Step 5: Demographics
│   ├── data/
│   │   ├── elections/
│   │   │   ├── your_voter_registration.csv           # Voter registration
│   │   │   ├── your_election_results.csv             # Election results
│   │   │   └── *_enriched.csv                        # Generated enriched data
│   │   ├── voters.csv                                # Voter locations (optional)
│   │   ├── hh_no_minors_multnomah_bgs.json          # ACS data (optional)
│   │   ├── tl_2022_41_bg.shp                        # Block groups (optional)
│   │   └── your_district_boundaries.geojson          # District boundaries
│   ├── geospatial/
│   │   └── *.geojson                                 # Generated web-ready data
│   ├── maps/
│   │   ├── *.pdf                                     # Static election maps
│   │   ├── voter_heatmap.html                        # Voter location heatmap
│   │   └── household_demographics.html               # Demographics choropleth
│   └── tiles/
│       └── *.mbtiles                                 # Generated vector tiles
```

## 🔧 Installation & Setup

### 1. Install Dependencies

Choose the installation method that best fits your needs:

```bash
# Complete pipeline with all features
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
ops/setup_tools.sh
```

### 2. Prepare Your Data

#### Election Analysis (Required)

1. **Election results CSV** → `analysis/data/elections/`
   - Must have `candidate_<lastname>` columns and `total_votes`
   - Example: `candidate_smith`, `candidate_jones`, `total_votes`

2. **Voter registration CSV** → `analysis/data/elections/`
   - Must have party registration columns: `DEM`, `REP`, `NAV`, etc.

3. **District boundaries GeoJSON** → `analysis/data/`
   - Must have `Precinct` field matching CSV `precinct` columns

4. **Voter locations CSV** → `analysis/data/voters.csv`
   - Must have `latitude`/`longitude` columns

5. **ACS household data JSON** → `analysis/data/hh_no_minors_multnomah_bgs.json`

6. **Block group shapefile** → `analysis/data/tl_2022_41_bg.shp`

### 3. Configure Your Pipeline

**NEW**: Instead of editing Python scripts, just configure `config.yaml` once!

```bash
cd analysis
# config.yaml is ready with sensible defaults
nano config.yaml
```

**Edit these key sections in `config.yaml`:**

```yaml
# Input Data Files
data:
  election:
    votes_csv: "your_election_results.csv"        # ← Your election data
    voters_csv: "your_voter_registration.csv"     # ← Your voter registration
    boundaries_geojson: "your_boundaries.geojson" # ← Your district boundaries

  demographics:
    voter_locations_csv: "voters.csv"             # ← Your voter locations (optional)
    acs_households_json: "households.json"        # ← Your ACS data (optional)
    # ... other optional files

# Column Mapping (adjust to match your data)
columns:
  precinct_csv: "precinct"           # How precincts are named in CSV
  precinct_geojson: "Precinct"       # How precincts are named in GeoJSON
  total_votes: "total_votes"         # Total votes column
  dem_registration: "DEM"            # Democratic registration column
  rep_registration: "REP"            # Republican registration column
  # ... other columns

# Analysis Settings (customize thresholds)
analysis:
  competitive_threshold: 0.10        # 10% margin = competitive
  tossup_threshold: 0.05            # 5% margin = toss-up
  strong_advantage: 0.20            # 20%+ advantage = strong lean
```

## 🎯 The 5-Step Pipeline

### **Core Election Analysis (Steps 1-3)**

#### Step 1: Data Enrichment

- Merges election results with voter registration data
- Calculates political lean, engagement scores, competitiveness
- Creates 40+ analytical columns
- **Generic candidate processing** - works with any election

#### Step 2: Map Generation

- Creates 9 static choropleth maps (PDF format)
- Generates web-optimized GeoJSON with all analysis data
- Handles coordinate system conversion (EPSG:2913 → WGS84)
- Validates and cleans data for web use

#### Step 3: Vector Tile Creation

- Converts GeoJSON to optimized .mbtiles format
- Uses tippecanoe for efficient vector tile generation
- Enables high-performance interactive mapping
- Optimized for zoom levels 9-13

### **Optional Demographics Analysis (Steps 4-5)**

#### Step 4: Voter Location Analysis

- Analyzes voter locations relative to district boundaries
- Creates interactive heatmaps using Folium
- Classifies voters as inside/outside district
- Exports detailed classification data to CSV

#### Step 5: Household Demographics

- Processes ACS (American Community Survey) data
- Creates choropleth maps of household demographics
- Focuses on households without minors
- Generates comprehensive reports and interactive maps

## 🗺️ What You Get

### Interactive Election Map Features

- 🗺️ **8 different data layers** you can switch between
- 🎨 **Smart color schemes** for each data type
- 📊 **Interactive legends** that update automatically
- 🔍 **Click precincts** for detailed popups
- 📱 **Mobile-responsive** design
- 🚀 **Auto-optimized** performance (vector tiles or GeoJSON)

### Generated Election Visualizations

1. **Zone 1 Participation** - Which areas had elections
2. **Total Votes Cast** - Vote counts by precinct
3. **Voter Turnout** - Turnout rates
4. **Candidate Vote Share** - Performance by area
5. **Political Lean** - Based on voter registration (Strong Dem/Rep, Competitive, etc.)
6. **Democratic Advantage** - Registration advantage analysis
7. **Civic Engagement** - Composite engagement score (diversity + turnout)
8. **Competitiveness** - Election competition analysis (Safe vs Competitive)

### Demographics Analysis Features

- 🗺️ **Voter heatmaps** with district boundary overlays
- 🏠 **Household choropleth maps** with interactive tooltips
- 📊 **Classification reports** (inside/outside district analysis)
- 📈 **Statistical summaries** in markdown format
- 🎯 **Geographic insights** for campaign targeting

### Data Enrichment Features

- 🎯 **Political Classification** - Automatically categorizes precincts by political lean
- 📊 **Engagement Scoring** - Combines registration diversity with turnout rates
- ⚔️ **Competition Analysis** - Identifies competitive vs safe districts
- 📈 **Performance Metrics** - Compares actual results vs voter registration
- 🔢 **40+ Analytical Columns** - Comprehensive dataset for further analysis

## 💡 Usage Examples

### Complete Workflows

```bash
# Full analysis including demographics
python run_pipeline.py --include-demographics

# Election analysis only (fastest)
python run_pipeline.py

# Only demographic analysis
python run_pipeline.py --demographics-only
```

### Development Workflows

```bash
# Quick map updates from existing data
python run_pipeline.py --maps-only

# Generate tiles for web deployment
python run_pipeline.py --skip-enrichment --skip-maps

# Test with subset of data
python run_pipeline.py --skip-tiles
```

### Individual Scripts

If you prefer to run steps individually:

```bash
cd analysis

# Step 1: Enrich data
python enrich_voters_election_data.py

# Step 2: Generate maps
python map_election_results.py

# Step 3: Create vector tiles
python create_vector_tiles.py

# Step 4: Analyze voter locations (optional)
python map_voters.py

# Step 5: Analyze demographics (optional)
python map_households.py
```

## 🚀 Launching Your Interactive Maps

### Election Map

```bash
# The interactive election map works automatically
open analysis/maps/election_map.html

# Uses vector tiles if available, falls back to GeoJSON
# 8 switchable data layers with smart legends
```

### Demographics Maps

```bash
# Voter location heatmap
open analysis/maps/voter_heatmap.html

# Household demographics choropleth
open analysis/maps/household_demographics.html
```

## 🛠️ Troubleshooting

### Configuration Issues

#### "Configuration file not found"

```bash
# The config.yaml should exist with defaults
cd analysis
ls config.yaml  # should exist
```

#### Column name not found in config

- Check that your `columns:` section matches your data
- Verify column names in your CSV files: `head -1 data/elections/your_file.csv`

### Pipeline Issues

#### Missing required scripts

```bash
# Make sure you're in the analysis directory
cd analysis
python run_pipeline.py --help
```

#### No candidate columns found

- Ensure your CSV has columns starting with `candidate_`
- Check configuration: `data.election.votes_csv` points to correct file
- Verify in config.yaml: `columns.total_votes` matches your CSV

#### tippecanoe command not found

```bash
# macOS
brew install tippecanoe

# Ubuntu/Debian
sudo apt install gdal-bin
```

### Map Display Issues

#### No data appears on election map

```bash
# Check if files exist
ls analysis/geospatial/*.geojson
ls analysis/tiles/*.mbtiles

# Validate configuration
python -c "from config_loader import Config; Config().print_config_summary()"
```

#### Coordinate system problems

```shell
❌ Sample coordinates: x=7678756, y=703492
✅ The pipeline automatically fixes this (EPSG:2913 → WGS84)
✅ Configured in: system.input_crs and system.output_crs
```

#### Demographics maps not loading

- Check that optional data files exist in `analysis/data/`
- Run with `--demographics-only` to see specific error messages
- Verify demographics file paths in config.yaml

### Data Matching Issues

#### Features don't match between CSV and GeoJSON

- Check precinct ID column names in config.yaml:
  - `columns.precinct_csv` for CSV files
  - `columns.precinct_geojson` for GeoJSON files
- Pipeline handles leading zeros, spaces, case differences automatically

## 🔧 Advanced Configuration

### Custom Analysis Thresholds

Edit thresholds in `config.yaml`:

```yaml
analysis:
  competitive_threshold: 0.15      # 15% margin = competitive
  tossup_threshold: 0.05          # 5% margin = toss-up
  strong_advantage: 0.25          # 25%+ advantage = strong lean

  # Engagement scoring weights
  registration_diversity_weight: 0.6
  turnout_weight: 0.4
```

### Custom Visualization Settings

```yaml
visualization:
  map_dpi: 300                    # High resolution maps
  colormap_default: "Blues"       # Different color scheme
  min_zoom: 8                     # Wider zoom range
  max_zoom: 15
```

### Different Coordinate Systems

```yaml
system:
  input_crs: "EPSG:3857"         # Web Mercator input
  output_crs: "EPSG:4326"        # WGS84 output (required for web)
```

### Custom File Organization

```yaml
directories:
  data: "my_data"
  elections: "my_data/election_files"
  maps: "output_maps"
  tiles: "web_tiles"
```

## 📊 Configuration System Benefits

### ✅ **User-Friendly Setup**

- **No code editing** - just configure `config.yaml` once
- **Clear documentation** - every setting explained
- **Smart defaults** - works out of the box for PPS-style elections
- **File path management** - automatic file location

### ✅ **Flexible Analysis Control**

- **Threshold customization** - adjust competitiveness, engagement weights
- **Column mapping** - handle different data schemas
- **Coordinate systems** - support various projections
- **Visualization settings** - customize map appearance

### ✅ **Maintainability**

- **Centralized configuration** - all settings in one place
- **Version control friendly** - track configuration changes
- **Team collaboration** - share configurations easily
- **Environment-specific** - different configs for different deployments

## ✅ Success Indicators

### After Configuration

✅ **Config loads successfully**: `python -c "from config_loader import Config; Config().print_config_summary()"`
✅ **Input files detected**: Green checkmarks for required files
✅ **Output directories created**: All directories exist and accessible
✅ **Column mapping verified**: Matches your actual data schema

### After Election Analysis

✅ **Enriched CSV** created with 40+ analytical columns
✅ **9 PDF maps** generated in configured maps directory
✅ **GeoJSON file** created in configured geospatial directory
✅ **MBTiles file** created in configured tiles directory
✅ **Interactive map** loads with 8 switchable layers
✅ **All precincts clickable** with detailed data popups

### After Demographics Analysis

✅ **Voter heatmap** with district overlay
✅ **Household choropleth** with interactive tooltips
✅ **Classification CSVs** with inside/outside analysis
✅ **Markdown reports** with statistical summaries

## 🎉 Quick Workflow Summary

```bash
# One-time setup (configure once)
cd analysis
nano config.yaml  # Edit to match your data files

# Complete pipeline in one command
python run_pipeline.py --include-demographics

# Open your maps
open maps/election_map.html           # Interactive election analysis
open maps/voter_heatmap.html          # Voter location heatmap
open maps/household_demographics.html # Demographics choropleth
```

That's it! You now have a comprehensive analysis suite that transforms raw data into professional interactive maps with rich demographic insights - all managed through a single, user-friendly configuration file.

---

*Built with MapLibre GL JS, Folium, and tippecanoe. Completely self-hosted with zero dependence on external mapping services.*
