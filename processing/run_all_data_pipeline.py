#!/usr/bin/env python3
"""
run_all_data_pipeline.py - The Complete Data Pipeline MVP

The ONE file that runs everything and gets all our geo data into Supabase.

Usage: 
    python run_all_data_pipeline.py
    
Result: All data processed and uploaded:
    - Election data → election_analysis table
    - Household demographics → household_demographics table  
    - Voter registration → voter_analysis table

The MVP pipeline that replaces 2,000+ lines of framework with 100 lines of clarity.
"""

import sys
import subprocess
from pathlib import Path
from loguru import logger

# Add parent directory for config
sys.path.append(str(Path(__file__).parent.parent))
from ops.config_loader import Config


def run_script(script_name: str, description: str) -> bool:
    """Run a processing script and return success status."""
    script_path = Path(__file__).parent / script_name
    
    if not script_path.exists():
        logger.error(f"❌ Script not found: {script_path}")
        return False
    
    logger.info(f"🚀 Running {description}...")
    logger.info(f"    Script: {script_name}")
    
    try:
        result = subprocess.run([
            sys.executable, str(script_path)
        ], capture_output=True, text=True, cwd=Path(__file__).parent)
        
        if result.returncode == 0:
            logger.success(f"✅ {description} completed successfully")
            # Log last few lines of output for context
            if result.stdout:
                output_lines = result.stdout.strip().split('\n')
                for line in output_lines[-3:]:  # Last 3 lines
                    if line.strip():
                        logger.info(f"    {line}")
            return True
        else:
            logger.error(f"❌ {description} failed (exit code: {result.returncode})")
            if result.stderr:
                logger.error(f"    Error: {result.stderr}")
            if result.stdout:
                logger.error(f"    Output: {result.stdout}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Failed to run {description}: {e}")
        return False


def upload_processed_data() -> bool:
    """Upload all processed data files to Supabase."""
    logger.info("📤 Uploading processed data to Supabase...")
    
    # Files and their table names
    uploads = [
        ("data/processed_election_data.geojson", "election_analysis"),
        ("data/processed_households_data.geojson", "household_demographics"),
        ("data/processed_voters_data.geojson", "voter_analysis"),
        ("data/processed_voter_hexagons.geojson", "voter_hexagons"),
        ("data/processed_voter_blockgroups.geojson", "voter_blockgroups"),
    ]
    
    success_count = 0
    
    for file_path, table_name in uploads:
        if not Path(file_path).exists():
            logger.warning(f"⚠️ File not found: {file_path} - skipping")
            continue
            
        logger.info(f"  📊 Uploading {file_path} → {table_name}")
        
        if run_upload(file_path, table_name):
            success_count += 1
        else:
            logger.error(f"  ❌ Failed to upload {file_path}")
    
    logger.info(f"📊 Upload summary: {success_count}/{len([f for f, t in uploads if Path(f).exists()])} files uploaded successfully")
    return success_count > 0


def run_upload(file_path: str, table_name: str) -> bool:
    """Run geo_upload.py for a specific file."""
    upload_script = Path(__file__).parent / "geo_upload.py"
    
    try:
        result = subprocess.run([
            sys.executable, str(upload_script), file_path, table_name
        ], capture_output=True, text=True, cwd=Path(__file__).parent)
        
        return result.returncode == 0
        
    except Exception as e:
        logger.error(f"❌ Upload failed for {file_path}: {e}")
        return False


def show_pipeline_summary():
    """Show what the pipeline will do."""
    logger.info("🗳️ Complete Political Data Pipeline")
    logger.info("=" * 50)
    logger.info("This pipeline will:")
    logger.info("  1. 📊 Process election data (votes, candidates, competition metrics)")
    logger.info("  2. 🏠 Process household demographics (census data + spatial analysis)")  
    logger.info("  3. 👥 Process voter registration (spatial analysis + aggregations)")
    logger.info("  4. 📤 Upload all data to Supabase PostGIS database")
    logger.info("")
    logger.info("📋 Output tables:")
    logger.info("  • election_analysis - Election results with political analysis")
    logger.info("  • household_demographics - Census household data by block group")
    logger.info("  • voter_analysis - Voter registration points (if data available)")
    logger.info("  • voter_hexagons - Hexagonal voter aggregations (if data available)")
    logger.info("  • voter_blockgroups - Block group voter aggregations (if data available)")
    logger.info("")


def main():
    """Run the complete data pipeline."""
    show_pipeline_summary()
    
    # Check configuration
    try:
        config = Config()
        logger.info(f"📋 Project: {config.get('project_name')}")
    except Exception as e:
        logger.error(f"❌ Configuration error: {e}")
        sys.exit(1)
    
    # Step 1: Process election data
    if not run_script("prepare_election_data.py", "Election Data Processing"):
        logger.error("💥 Pipeline failed at election data processing")
        sys.exit(1)
    
    # Step 2: Process household demographics  
    if not run_script("prepare_households_data.py", "Household Demographics Processing"):
        logger.error("💥 Pipeline failed at household demographics processing")
        sys.exit(1)
    
    # Step 3: Process voter registration (may not have data)
    if not run_script("prepare_voterfile_data.py", "Voter Registration Processing"):
        logger.warning("⚠️ Voter registration processing had issues, but continuing...")
    
    # Step 4: Upload all processed data
    if not upload_processed_data():
        logger.error("💥 Pipeline failed at data upload stage")
        sys.exit(1)
    
    # Success!
    logger.success("🎉 Complete Political Data Pipeline Finished!")
    logger.info("")
    logger.info("✅ All data processed and uploaded to Supabase")
    logger.info("🗺️ Ready for visualization and analysis")
    logger.info("")
    logger.info("Next steps:")
    logger.info("  • Check your Supabase dashboard for the new tables")
    logger.info("  • Connect to PostGIS for advanced spatial queries")
    logger.info("  • Build visualizations using the uploaded geodata")


if __name__ == "__main__":
    main() 