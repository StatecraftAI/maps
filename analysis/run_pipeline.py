#!/usr/bin/env python3
"""
Election Data Processing Pipeline

This script orchestrates the complete election data processing workflow:
1. Data enrichment (enrich_voters_election_data.py)
2. Map generation (map_election_results.py)
3. Voter location analysis (map_voters.py) [Optional]
4. Household demographics analysis (map_households.py) [Optional]

Usage:
    python run_pipeline.py [--skip-enrichment] [--skip-maps] [--include-demographics]
"""

import argparse
import pathlib
import subprocess
import sys
import time

from loguru import logger

# Import config system
try:
    from config_loader import Config
except ImportError:
    logger.info("❌ Error: config_loader.py not found")
    logger.info("💡 Make sure you're running this script from the analysis directory")
    sys.exit(1)

# Script paths (these stay hardcoded since they're part of the pipeline)
SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
ENRICHMENT_SCRIPT = SCRIPT_DIR / "enrich_voters_election_data.py"
MAPPING_SCRIPT = SCRIPT_DIR / "map_election_results.py"
VOTERS_SCRIPT = SCRIPT_DIR / "map_voters.py"
HOUSEHOLDS_SCRIPT = SCRIPT_DIR / "map_households.py"


def run_script(script_path: pathlib.Path, description: str) -> bool:
    """Run a Python script and return success status."""
    logger.info(f"{'=' * 60}")
    logger.info(f"🚀 Running: {description}")
    logger.info(f"📄 Script: {script_path.name}")
    logger.info(f"{'=' * 60}")

    start_time = time.time()

    try:
        subprocess.run(
            [sys.executable, str(script_path)],
            cwd=script_path.parent,
            check=True,
            capture_output=False,  # Let output go to console
        )

        elapsed = time.time() - start_time
        logger.info(f"✅ {description} completed successfully in {elapsed:.1f}s")
        return True

    except subprocess.CalledProcessError as e:
        elapsed = time.time() - start_time
        logger.critical(f"❌ {description} failed after {elapsed:.1f}s")
        logger.info(f"   Exit code: {e.returncode}")
        return False
    except KeyboardInterrupt:
        logger.critical(f"❌ {description} interrupted by user")
        return False


def check_file_exists(file_path: pathlib.Path, description: str) -> bool:
    """Check if a required input file exists."""
    if file_path.exists():
        return True
    else:
        logger.warning(f"⚠️ Optional input file not found: {file_path}")
        logger.info(f"   {description} will be skipped")
        return False


def check_demographic_data_availability(config: Config) -> bool:
    """Check if demographic data files are available using config paths."""
    logger.info("📊 Checking demographic data availability...")

    demographic_files_available = True

    # Check voter locations file
    try:
        voter_csv_path = config.get_input_path("voter_locations_csv")
        if not check_file_exists(voter_csv_path, "Voter location analysis"):
            demographic_files_available = False
    except Exception as e:
        logger.error(f"⚠️ Could not get voter locations path from config: {e}")
        demographic_files_available = False

    # Check ACS households file
    try:
        acs_json_path = config.get_input_path("acs_households_json")
        if not check_file_exists(acs_json_path, "Household demographics analysis"):
            demographic_files_available = False
    except Exception as e:
        logger.error(f"⚠️ Could not get ACS households path from config: {e}")
        demographic_files_available = False

    # Check district boundaries file
    try:
        district_boundaries_path = config.get_input_path("district_boundaries_geojson")
        if not check_file_exists(district_boundaries_path, "District boundary analysis"):
            demographic_files_available = False
    except Exception as e:
        logger.error(f"⚠️ Could not get district boundaries path from config: {e}")
        demographic_files_available = False

    # Check block groups file
    try:
        block_groups_path = config.get_input_path("block_groups_shp")
        if not check_file_exists(block_groups_path, "Block group geographic analysis"):
            demographic_files_available = False
    except Exception as e:
        logger.error(f"⚠️ Could not get block groups path from config: {e}")
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
  python run_pipeline.py --skip-enrichment              # Only generate maps
  python run_pipeline.py --skip-maps                    # Only enrich data
  python run_pipeline.py --maps-only                    # Only generate maps (skip enrichment)
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
        help="Skip map generation step (only enrich data)",
    )

    parser.add_argument(
        "--maps-only",
        action="store_true",
        help="Only generate maps (skip enrichment)",
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
        logger.info(f"📋 Project: {config.get('project_name')}")
        logger.info(f"📋 Description: {config.get('description')}")
    except Exception as e:
        logger.info(f"❌ Configuration error: {e}")
        logger.info("💡 Make sure config.yaml exists in the analysis directory")
        sys.exit(1)

    # Handle shortcuts
    if args.maps_only:
        args.skip_enrichment = True

    if args.demographics_only:
        args.skip_enrichment = True
        args.skip_maps = True
        args.include_demographics = True

    # Validate that required scripts exist
    missing_scripts = []
    if not args.skip_enrichment and not ENRICHMENT_SCRIPT.exists():
        missing_scripts.append(str(ENRICHMENT_SCRIPT))
    if not args.skip_maps and not MAPPING_SCRIPT.exists():
        missing_scripts.append(str(MAPPING_SCRIPT))
    if args.include_demographics:
        if not VOTERS_SCRIPT.exists():
            missing_scripts.append(str(VOTERS_SCRIPT))
        if not HOUSEHOLDS_SCRIPT.exists():
            missing_scripts.append(str(HOUSEHOLDS_SCRIPT))

    if missing_scripts:
        logger.info("❌ Missing required scripts:")
        for script in missing_scripts:
            logger.info(f"   {script}")
        sys.exit(1)

    # Check demographic data availability using config
    demographic_files_available = True
    if args.include_demographics:
        demographic_files_available = check_demographic_data_availability(config)

        if not demographic_files_available and args.demographics_only:
            logger.info("❌ Demographics-only mode requested but required data files are missing")
            logger.info("💡 Check the file paths in config.yaml under data.demographics section")
            sys.exit(1)

    # Pipeline execution
    pipeline_name = (
        "Demographics Analysis" if args.demographics_only else "Election Data Processing Pipeline"
    )
    logger.info(f"🗺️ {pipeline_name}")
    logger.info("=" * len(f"🗺️ {pipeline_name}"))
    logger.info(f"📁 Working directory: {SCRIPT_DIR}")

    total_start = time.time()
    success_count = 0
    total_steps = 0

    # Step 1: Data Enrichment
    if not args.skip_enrichment:
        total_steps += 1
        if run_script(ENRICHMENT_SCRIPT, "Election Data Enrichment"):
            success_count += 1
        else:
            logger.info("💥 Pipeline failed at enrichment step")
            sys.exit(1)
    else:
        logger.info("⏭️ Skipping data enrichment (using existing enriched data)")

    # Step 2: Map Generation
    if not args.skip_maps:
        total_steps += 1
        if run_script(MAPPING_SCRIPT, "Election Map Generation"):
            success_count += 1
        else:
            logger.error("💥 Pipeline failed at map generation step")
            sys.exit(1)
    else:
        logger.info("⏭️ Skipping map generation")

    # Step 4: Voter Location Analysis (Optional)
    if args.include_demographics and demographic_files_available:
        try:
            voter_csv_path = config.get_input_path("voter_locations_csv")
            if voter_csv_path.exists():
                total_steps += 1
                if run_script(VOTERS_SCRIPT, "Voter Location Analysis"):
                    success_count += 1
                else:
                    logger.warning("⚠️ Voter location analysis failed but continuing...")
            else:
                logger.info(f"⏭️ Skipping voter location analysis ({voter_csv_path.name} not found)")
        except Exception as e:
            logger.error(f"⚠️ Could not run voter location analysis: {e}")

    # Step 5: Household Demographics Analysis (Optional)
    if args.include_demographics and demographic_files_available:
        try:
            acs_json_path = config.get_input_path("acs_households_json")
            if acs_json_path.exists():
                total_steps += 1
                if run_script(HOUSEHOLDS_SCRIPT, "Household Demographics Analysis"):
                    success_count += 1
                else:
                    logger.warning("⚠️ Household demographics analysis failed but continuing...")
            else:
                logger.info(
                    f"\n⏭️ Skipping household demographics analysis ({acs_json_path.name} not found)"
                )
        except Exception as e:
            logger.error(f"⚠️ Could not run household demographics analysis: {e}")

    # Pipeline summary
    total_elapsed = time.time() - total_start

    logger.info(f"{'=' * 60}")
    logger.info("🎉 PIPELINE COMPLETE")
    logger.info(f"{'=' * 60}")
    logger.info(f"✅ Completed {success_count}/{total_steps} steps successfully")
    logger.info(f"⏱️ Total time: {total_elapsed:.1f}s")

    if success_count == total_steps:
        logger.info("🗺️ Your analysis is ready!")
        if not args.demographics_only:
            # Get output directories from config
            try:
                maps_dir = config.get_output_dir("maps")
                geospatial_dir = config.get_output_dir("geospatial")
                logger.info(f"   📊 Static maps: {maps_dir}/")
                logger.info(f"   🌐 Web GeoJSON: {geospatial_dir}/")
            except Exception:
                # Fallback to hardcoded paths if config fails
                logger.info("   📊 Static maps: analysis/maps/")
                logger.info("   🌐 Web GeoJSON: analysis/geospatial/")

        if args.include_demographics or args.demographics_only:
            try:
                maps_dir = config.get_output_dir("maps")
                logger.info(f"   👥 Voter heatmap: {maps_dir}/voter_heatmap.html")
                logger.info(f"   🏠 Household demographics: {maps_dir}/household_demographics.html")
                logger.info(f"   📊 Demographics data: {config.get_data_dir()}/")
            except Exception:
                # Fallback to hardcoded paths if config fails
                logger.info("   👥 Voter heatmap: analysis/maps/voter_heatmap.html")
                logger.info(
                    "   🏠 Household demographics: analysis/maps/household_demographics.html"
                )
                logger.info("   📊 Demographics data: analysis/data/")

        sys.exit(0)
    else:
        logger.warning(
            f"⚠️ Pipeline completed with issues ({success_count}/{total_steps} successful)"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
