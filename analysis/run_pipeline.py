#!/usr/bin/env python3
"""
Election Data Processing Pipeline

This script orchestrates the complete election data processing workflow:
1. Data enrichment (enrich_voters_election_data.py)
2. Map generation (map_election_results.py)
3. Vector tile creation (create_vector_tiles.py)
4. Voter location analysis (map_voters.py) [Optional]
5. Household demographics analysis (map_households.py) [Optional]

Usage:
    python run_pipeline.py [--skip-enrichment] [--skip-maps] [--skip-tiles] [--include-demographics]
"""

import argparse
import pathlib
import subprocess
import sys
import time

# Import config system
try:
    from config_loader import Config
except ImportError:
    print("❌ Error: config_loader.py not found")
    print("💡 Make sure you're running this script from the analysis directory")
    sys.exit(1)

# Script paths (these stay hardcoded since they're part of the pipeline)
SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
ENRICHMENT_SCRIPT = SCRIPT_DIR / "enrich_voters_election_data.py"
MAPPING_SCRIPT = SCRIPT_DIR / "map_election_results.py"
TILES_SCRIPT = SCRIPT_DIR / "create_vector_tiles.py"
VOTERS_SCRIPT = SCRIPT_DIR / "map_voters.py"
HOUSEHOLDS_SCRIPT = SCRIPT_DIR / "map_households.py"


def run_script(script_path: pathlib.Path, description: str) -> bool:
    """Run a Python script and return success status."""
    print(f"\n{'=' * 60}")
    print(f"🚀 Running: {description}")
    print(f"📄 Script: {script_path.name}")
    print(f"{'=' * 60}")

    start_time = time.time()

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=script_path.parent,
            check=True,
            capture_output=False,  # Let output go to console
        )

        elapsed = time.time() - start_time
        print(f"\n✅ {description} completed successfully in {elapsed:.1f}s")
        return True

    except subprocess.CalledProcessError as e:
        elapsed = time.time() - start_time
        print(f"\n❌ {description} failed after {elapsed:.1f}s")
        print(f"   Exit code: {e.returncode}")
        return False
    except KeyboardInterrupt:
        print(f"\n⚠️ {description} interrupted by user")
        return False


def check_tippecanoe():
    """Check if tippecanoe is available for vector tile creation."""
    try:
        subprocess.run(
            ["tippecanoe", "--version"], capture_output=True, text=True, check=True
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def check_file_exists(file_path: pathlib.Path, description: str) -> bool:
    """Check if a required input file exists."""
    if file_path.exists():
        return True
    else:
        print(f"⚠️ Optional input file not found: {file_path}")
        print(f"   {description} will be skipped")
        return False


def check_demographic_data_availability(config: Config) -> bool:
    """Check if demographic data files are available using config paths."""
    print("\n📊 Checking demographic data availability...")

    demographic_files_available = True

    # Check voter locations file
    try:
        voter_csv_path = config.get_input_path("voter_locations_csv")
        if not check_file_exists(voter_csv_path, "Voter location analysis"):
            demographic_files_available = False
    except Exception as e:
        print(f"⚠️ Could not get voter locations path from config: {e}")
        demographic_files_available = False

    # Check ACS households file
    try:
        acs_json_path = config.get_input_path("acs_households_json")
        if not check_file_exists(acs_json_path, "Household demographics analysis"):
            demographic_files_available = False
    except Exception as e:
        print(f"⚠️ Could not get ACS households path from config: {e}")
        demographic_files_available = False

    # Check district boundaries file
    try:
        district_boundaries_path = config.get_input_path("district_boundaries_geojson")
        if not check_file_exists(
            district_boundaries_path, "District boundary analysis"
        ):
            demographic_files_available = False
    except Exception as e:
        print(f"⚠️ Could not get district boundaries path from config: {e}")
        demographic_files_available = False

    # Check block groups file
    try:
        block_groups_path = config.get_input_path("block_groups_shp")
        if not check_file_exists(block_groups_path, "Block group geographic analysis"):
            demographic_files_available = False
    except Exception as e:
        print(f"⚠️ Could not get block groups path from config: {e}")
        demographic_files_available = False

    return demographic_files_available


def main():
    parser = argparse.ArgumentParser(
        description="Run the complete election data processing pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py                                # Run core election pipeline (3 steps)
  python run_pipeline.py --include-demographics         # Run all analysis including demographics
  python run_pipeline.py --skip-enrichment              # Only generate maps and tiles
  python run_pipeline.py --skip-maps                    # Only enrich data and create tiles
  python run_pipeline.py --skip-tiles                   # Only enrich data and generate maps
  python run_pipeline.py --maps-only                    # Only generate maps (skip enrichment and tiles)
  python run_pipeline.py --demographics-only            # Only run demographic analysis
        """,
    )

    parser.add_argument(
        "--skip-enrichment",
        action="store_true",
        help="Skip data enrichment step (use existing enriched data)",
    )

    parser.add_argument(
        "--skip-maps",
        action="store_true",
        help="Skip map generation step (only enrich data and create tiles)",
    )

    parser.add_argument(
        "--skip-tiles",
        action="store_true",
        help="Skip vector tile creation step (only enrich data and generate maps)",
    )

    parser.add_argument(
        "--maps-only",
        action="store_true",
        help="Only generate maps (skip enrichment and tiles)",
    )

    parser.add_argument(
        "--include-demographics",
        action="store_true",
        help="Include demographic analysis (voter locations and household data)",
    )

    parser.add_argument(
        "--demographics-only",
        action="store_true",
        help="Only run demographic analysis (skip election data processing)",
    )

    args = parser.parse_args()

    # Load configuration
    try:
        config = Config()
        print(f"📋 Project: {config.get('project_name')}")
        print(f"📋 Description: {config.get('description')}")
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        print("💡 Make sure config.yaml exists in the analysis directory")
        sys.exit(1)

    # Handle shortcuts
    if args.maps_only:
        args.skip_enrichment = True
        args.skip_tiles = True

    if args.demographics_only:
        args.skip_enrichment = True
        args.skip_maps = True
        args.skip_tiles = True
        args.include_demographics = True

    # Validate that required scripts exist
    missing_scripts = []
    if not args.skip_enrichment and not ENRICHMENT_SCRIPT.exists():
        missing_scripts.append(str(ENRICHMENT_SCRIPT))
    if not args.skip_maps and not MAPPING_SCRIPT.exists():
        missing_scripts.append(str(MAPPING_SCRIPT))
    if not args.skip_tiles and not TILES_SCRIPT.exists():
        missing_scripts.append(str(TILES_SCRIPT))
    if args.include_demographics:
        if not VOTERS_SCRIPT.exists():
            missing_scripts.append(str(VOTERS_SCRIPT))
        if not HOUSEHOLDS_SCRIPT.exists():
            missing_scripts.append(str(HOUSEHOLDS_SCRIPT))

    if missing_scripts:
        print("❌ Missing required scripts:")
        for script in missing_scripts:
            print(f"   {script}")
        sys.exit(1)

    # Check dependencies
    if not args.skip_tiles:
        if not check_tippecanoe():
            print("⚠️ Warning: tippecanoe not found!")
            print(
                "   Vector tile creation will be skipped unless tippecanoe is installed."
            )
            print(
                "   Install with: brew install tippecanoe (macOS) or sudo apt install gdal-bin (Ubuntu)"
            )
            args.skip_tiles = True
        else:
            print("✅ tippecanoe found - vector tiles can be created")

    # Check demographic data availability using config
    demographic_files_available = True
    if args.include_demographics:
        demographic_files_available = check_demographic_data_availability(config)

        if not demographic_files_available and args.demographics_only:
            print(
                "❌ Demographics-only mode requested but required data files are missing"
            )
            print(
                "💡 Check the file paths in config.yaml under data.demographics section"
            )
            sys.exit(1)

    # Pipeline execution
    pipeline_name = (
        "Demographics Analysis"
        if args.demographics_only
        else "Election Data Processing Pipeline"
    )
    print(f"\n🗺️ {pipeline_name}")
    print("=" * len(f"🗺️ {pipeline_name}"))
    print(f"📁 Working directory: {SCRIPT_DIR}")

    total_start = time.time()
    success_count = 0
    total_steps = 0

    # Step 1: Data Enrichment
    if not args.skip_enrichment:
        total_steps += 1
        if run_script(ENRICHMENT_SCRIPT, "Election Data Enrichment"):
            success_count += 1
        else:
            print("\n💥 Pipeline failed at enrichment step")
            sys.exit(1)
    else:
        print("\n⏭️ Skipping data enrichment (using existing enriched data)")

    # Step 2: Map Generation
    if not args.skip_maps:
        total_steps += 1
        if run_script(MAPPING_SCRIPT, "Election Map Generation"):
            success_count += 1
        else:
            print("\n💥 Pipeline failed at map generation step")
            sys.exit(1)
    else:
        print("\n⏭️ Skipping map generation")

    # Step 3: Vector Tile Creation
    if not args.skip_tiles:
        total_steps += 1
        if run_script(TILES_SCRIPT, "Vector Tile Creation"):
            success_count += 1
        else:
            print("\n💥 Pipeline failed at vector tile creation step")
            print("   🔧 This might be due to missing tippecanoe or invalid GeoJSON")
            print(
                "   💡 Try running just the mapping step with: python run_pipeline.py --skip-tiles"
            )
            sys.exit(1)
    else:
        print("\n⏭️ Skipping vector tile creation")

    # Step 4: Voter Location Analysis (Optional)
    if args.include_demographics and demographic_files_available:
        try:
            voter_csv_path = config.get_input_path("voter_locations_csv")
            if voter_csv_path.exists():
                total_steps += 1
                if run_script(VOTERS_SCRIPT, "Voter Location Analysis"):
                    success_count += 1
                else:
                    print("\n⚠️ Voter location analysis failed but continuing...")
            else:
                print(
                    f"\n⏭️ Skipping voter location analysis ({voter_csv_path.name} not found)"
                )
        except Exception as e:
            print(f"\n⚠️ Could not run voter location analysis: {e}")

    # Step 5: Household Demographics Analysis (Optional)
    if args.include_demographics and demographic_files_available:
        try:
            acs_json_path = config.get_input_path("acs_households_json")
            if acs_json_path.exists():
                total_steps += 1
                if run_script(HOUSEHOLDS_SCRIPT, "Household Demographics Analysis"):
                    success_count += 1
                else:
                    print(
                        "\n⚠️ Household demographics analysis failed but continuing..."
                    )
            else:
                print(
                    f"\n⏭️ Skipping household demographics analysis ({acs_json_path.name} not found)"
                )
        except Exception as e:
            print(f"\n⚠️ Could not run household demographics analysis: {e}")

    # Pipeline summary
    total_elapsed = time.time() - total_start

    print(f"\n{'=' * 60}")
    print("🎉 PIPELINE COMPLETE")
    print(f"{'=' * 60}")
    print(f"✅ Completed {success_count}/{total_steps} steps successfully")
    print(f"⏱️ Total time: {total_elapsed:.1f}s")

    if success_count == total_steps:
        print("\n🗺️ Your analysis is ready!")
        if not args.demographics_only:
            # Get output directories from config
            try:
                maps_dir = config.get_output_dir("maps")
                geospatial_dir = config.get_output_dir("geospatial")
                print(f"   📊 Static maps: {maps_dir}/")
                print(f"   🌐 Web GeoJSON: {geospatial_dir}/")
                if not args.skip_tiles:
                    tiles_dir = config.get_output_dir("tiles")
                    print(f"   🗂️ Vector tiles: {tiles_dir}/")
                    print(
                        "   💡 Use vector tiles with TileServer GL or upload to mapping service"
                    )
            except Exception:
                # Fallback to hardcoded paths if config fails
                print("   📊 Static maps: analysis/maps/")
                print("   🌐 Web GeoJSON: analysis/geospatial/")
                if not args.skip_tiles:
                    print("   🗂️ Vector tiles: analysis/tiles/")

        if args.include_demographics or args.demographics_only:
            try:
                maps_dir = config.get_output_dir("maps")
                print(f"   👥 Voter heatmap: {maps_dir}/voter_heatmap.html")
                print(
                    f"   🏠 Household demographics: {maps_dir}/household_demographics.html"
                )
                print(f"   📊 Demographics data: {config.get_data_dir()}/")
            except Exception:
                # Fallback to hardcoded paths if config fails
                print("   👥 Voter heatmap: analysis/maps/voter_heatmap.html")
                print(
                    "   🏠 Household demographics: analysis/maps/household_demographics.html"
                )
                print("   📊 Demographics data: analysis/data/")

        sys.exit(0)
    else:
        print(
            f"\n⚠️ Pipeline completed with issues ({success_count}/{total_steps} successful)"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
