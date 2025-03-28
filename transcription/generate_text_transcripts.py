import sqlite3
import sys
from pathlib import Path
from loguru import logger
import json
import os
from typing import Optional, List, Tuple, Dict
from datetime import datetime
import re
import unicodedata

# Constants
DB_PATH = 'inventory.db'
TRANSCRIPTS_DIR = 'transcripts'
TEXT_TRANSCRIPTS_DIR = 'text_transcripts'

def configure_logging() -> None:
    """Configure loguru logger with appropriate format and level."""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )

def setup_output_directory() -> None:
    """Create the text transcripts directory if it doesn't exist."""
    try:
        output_path = Path(TEXT_TRANSCRIPTS_DIR)
        output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory ready: {output_path.absolute()}")
    except Exception as e:
        logger.error(f"Failed to create output directory: {e}")
        raise

def normalize_filename(filename: str) -> str:
    """Normalize filename to be safe for all operating systems and convert to lowercase snake_case."""
    # Convert to ASCII and replace non-ASCII characters
    filename = unicodedata.normalize('NFKD', filename).encode('ASCII', 'ignore').decode('ASCII')
    
    # Convert to lowercase
    filename = filename.lower()
    
    # Replace any non-alphanumeric characters (except spaces) with underscores
    filename = re.sub(r'[^\w\s-]', '_', filename)
    
    # Replace spaces and hyphens with underscores
    filename = re.sub(r'[\s-]+', '_', filename)
    
    # Remove multiple consecutive underscores
    filename = re.sub(r'_+', '_', filename)
    
    # Remove leading/trailing underscores
    filename = filename.strip('_')
    
    return filename

def format_timestamp(seconds: float) -> str:
    """Format seconds into HH:MM:SS.mmm format."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:06.3f}"

def generate_text_transcript(video_id: str, title: str, record_date: str, transcript_path: str) -> Optional[str]:
    """Generate a text transcript from the JSON file."""
    try:
        # Read JSON transcript
        with open(transcript_path, 'r', encoding='utf-8') as f:
            transcript_data = json.load(f)
        
        # Get segments from processed section
        segments = transcript_data.get('processed', {}).get('segments', [])
        if not segments:
            logger.warning(f"No segments found in transcript for {title}")
            return None
            
        # Generate normalized filename
        normalized_title = normalize_filename(title)
        output_filename = f"{record_date}_{normalized_title}_assemblyai.txt"
        output_path = Path(TEXT_TRANSCRIPTS_DIR) / output_filename
        
        # Generate text transcript
        with open(output_path, 'w', encoding='utf-8') as f:
            for segment in segments:
                f.write(f"start time: {format_timestamp(segment['start'])}\n")
                f.write(f"end time: {format_timestamp(segment['end'])}\n")
                f.write(f"confidence: {segment['confidence']:.2f}\n")
                f.write(f"{segment['speaker']}: {segment['text']}\n\n")
        
        logger.success(f"Generated text transcript: {output_filename}")
        return str(output_path)
        
    except json.JSONDecodeError as e:
        logger.error(f"Error reading JSON file {transcript_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error generating text transcript for {title}: {e}")
        return None

def ensure_database_schema() -> None:
    """Ensure the database has all required columns."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Add required columns if they don't exist
        cursor.execute("PRAGMA table_info(videos)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'text_transcript_path' not in columns:
            cursor.execute('''
                ALTER TABLE videos
                ADD COLUMN text_transcript_path TEXT
            ''')
            logger.info("Added text_transcript_path column to videos table")
            
        if 'text_transcript_generated' not in columns:
            cursor.execute('''
                ALTER TABLE videos
                ADD COLUMN text_transcript_generated INTEGER DEFAULT 0
            ''')
            logger.info("Added text_transcript_generated column to videos table")
            
        if 'text_transcript_date' not in columns:
            cursor.execute('''
                ALTER TABLE videos
                ADD COLUMN text_transcript_date TIMESTAMP
            ''')
            logger.info("Added text_transcript_date column to videos table")
            
        conn.commit()
        logger.debug("Database schema check completed")
        
    except sqlite3.Error as e:
        logger.error(f"Database schema error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def fetch_pending_text_transcripts() -> List[Tuple[str, str, str, str]]:
    """Fetch videos that have JSON transcripts."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT video_id, title, record_date, transcript_path
            FROM videos 
            WHERE transcript_downloaded = 1
            AND transcript_path IS NOT NULL
            ORDER BY record_date DESC
        ''')
        pending = cursor.fetchall()
        
        if not pending:
            logger.info("No JSON transcripts found")
        else:
            logger.info(f"Found {len(pending)} files with JSON transcripts")
            
        return pending
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def update_database(video_id: str, text_transcript_path: Optional[str] = None, 
                   error_message: str = "") -> None:
    """Update the database with text transcript results."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # First ensure the schema is correct
        ensure_database_schema()
        
        # Update the record
        cursor.execute('''
            UPDATE videos
            SET text_transcript_path = COALESCE(?, text_transcript_path),
                text_transcript_generated = ?,
                text_transcript_date = ?,
                error_message = ?
            WHERE video_id = ?
        ''', (text_transcript_path, bool(text_transcript_path), 
              datetime.now().isoformat() if text_transcript_path else None,
              error_message, video_id))
        
        conn.commit()
        logger.debug(f"Database updated for video {video_id}")
        
    except sqlite3.Error as e:
        logger.error(f"Database error while updating {video_id}: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def process_text_transcripts(pending_transcripts: List[Tuple[str, str, str, str]]) -> None:
    """Process a batch of pending text transcript generations."""
    for video_id, title, record_date, transcript_path in pending_transcripts:
        try:
            logger.info(f"Generating text transcript for: {title}")
            
            # Generate text transcript
            text_transcript_path = generate_text_transcript(video_id, title, record_date, transcript_path)
            if not text_transcript_path:
                update_database(video_id, error_message="Failed to generate text transcript")
                continue
                
            # Update database with text transcript path
            update_database(video_id, text_transcript_path=text_transcript_path)
            
        except Exception as e:
            logger.error(f"Error processing {video_id}: {e}")
            update_database(video_id, error_message=str(e))

def main() -> None:
    """Main entry point for the text transcript generation process."""
    configure_logging()
    logger.info("Starting text transcript generation process")
    
    try:
        # Setup
        setup_output_directory()
        ensure_database_schema()
        
        # Process pending text transcript generations
        pending_transcripts = fetch_pending_text_transcripts()
        if pending_transcripts:
            logger.info("Processing pending text transcript generations...")
            process_text_transcripts(pending_transcripts)
            logger.success("Text transcript generation completed")
        else:
            logger.info("No pending text transcript generations. Exiting.")
        
    except Exception as e:
        logger.exception("Fatal error during text transcript generation")
        sys.exit(1)

if __name__ == '__main__':
    main() 