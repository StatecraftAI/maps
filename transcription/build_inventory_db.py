import sqlite3
import json
import re
from datetime import datetime
import sys
from loguru import logger
import os
from typing import Optional, List, Dict, Any

def configure_logging() -> None:
    """Configure loguru logger with appropriate format and level."""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )

# Define regex patterns for various date formats
DATE_PATTERNS = [
    # Standard formats with separators
    re.compile(r'(?P<year>\d{4})[-/](?P<month>\d{1,2})[-/](?P<day>\d{1,2})'),
    re.compile(r'(?P<year>\d{2})[-/](?P<month>\d{1,2})[-/](?P<day>\d{1,2})'),
    re.compile(r'(?P<month>\d{1,2})/(?P<day>\d{1,2})/(?P<year>\d{2})'),
    
    # Space-separated formats
    re.compile(r'(?P<year>\d{4})\s+(?P<month>\d{1,2})\s+(?P<day>\d{1,2})'),
    re.compile(r'(?P<year>\d{2})\s+(?P<month>\d{1,2})\s+(?P<day>\d{1,2})'),
    
    # Event-specific formats (e.g., "HeART of Portland 2024")
    re.compile(r'(?P<event>.*?)\s+(?P<year>\d{4})'),
]

def extract_date(title: str) -> Optional[datetime.date]:
    """Extract date from video title using predefined patterns."""
    logger.debug(f"Attempting to extract date from title: {title}")
    
    for pattern in DATE_PATTERNS:
        match = pattern.search(title)
        if match:
            try:
                date_parts = match.groupdict()
                
                # Handle event-specific format (e.g., "HeART of Portland 2024")
                if 'event' in date_parts:
                    year = date_parts['year']
                    # For event dates, use January 1st of the given year
                    month = "01"
                    day = "01"
                else:
                    year = date_parts['year']
                    month = date_parts['month'].zfill(2)
                    day = date_parts['day'].zfill(2)
                
                if len(year) == 2:
                    year = "20" + year  # Assume 2000s for two-digit years
                
                formatted_date = f"{year}-{month}-{day}"
                parsed_date = datetime.strptime(formatted_date, "%Y-%m-%d").date()
                logger.debug(f"Successfully extracted date: {parsed_date}")
                return parsed_date
            except ValueError as e:
                logger.warning(f"Failed to parse date from match: {e}")
                continue
    
    logger.warning(f"No valid date found in title: {title}")
    return None

def create_database(db_path: str = 'inventory.db') -> None:
    """Create a SQLite database at the given path if it doesn't exist."""
    try:
        if not os.path.exists(db_path):
            logger.info(f"Creating new database at {db_path}")
            connection = sqlite3.connect(db_path)
            connection.close()
        else:
            logger.info(f"Using existing database at {db_path}")
            
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                video_id TEXT PRIMARY KEY,
                title TEXT,
                url TEXT,
                record_date DATE,
                audio_downloaded INTEGER DEFAULT 0,
                transcript_generated INTEGER DEFAULT 0,
                local_audio_path TEXT,
                download_attempts INTEGER DEFAULT 0,
                error_message TEXT
            )
        ''')
        connection.commit()
        logger.info("Database schema created/verified successfully")
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during database creation: {e}")
        raise
    finally:
        if 'connection' in locals():
            connection.close()

def load_inventory(json_file: str, db_path: str = 'inventory.db') -> None:
    """Populate the videos table in the database with data from the given JSON file.
    Expects a JSON Lines format file (one JSON object per line).
    """
    logger.info(f"Loading inventory from {json_file} into {db_path}")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        records_processed = 0
        records_skipped = 0
        
        with open(json_file, 'r', encoding='utf-8') as file:
            for line_number, line in enumerate(file, 1):
                try:
                    # Skip empty lines
                    if not line.strip():
                        continue
                        
                    # Parse each line as a separate JSON object
                    video_data = json.loads(line)
                    
                    # Extract required fields
                    video_id = video_data.get('id')
                    if not video_id:
                        logger.warning(f"Skipping record at line {line_number}: missing ID")
                        records_skipped += 1
                        continue
                        
                    title = video_data.get('title', '')
                    url = video_data.get('url', '')
                    record_date = extract_date(title)
                    
                    cursor.execute('''
                        INSERT OR IGNORE INTO videos (video_id, title, url, record_date)
                        VALUES (?, ?, ?, ?)
                    ''', (video_id, title, url, record_date))
                    
                    records_processed += 1
                    if records_processed % 100 == 0:
                        logger.info(f"Processed {records_processed} records so far...")
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON at line {line_number}: {e}")
                    records_skipped += 1
                    continue
                except Exception as e:
                    logger.warning(f"Error processing record at line {line_number}: {e}")
                    records_skipped += 1
                    continue
                    
        conn.commit()
        logger.info(f"Processed {records_processed} records, skipped {records_skipped} records")
            
    except FileNotFoundError:
        logger.error(f"JSON file not found: {json_file}")
        raise
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during inventory loading: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def main() -> None:
    """Main entry point for the inventory database build process."""
    configure_logging()
    logger.info("Starting inventory database build")
    
    try:
        if len(sys.argv) < 2:
            logger.error("Usage: build_inventory_db.py <inventory_json_file>")
            sys.exit(1)
            
        json_file = sys.argv[1]
        create_database()
        load_inventory(json_file)
        logger.success("Database build completed successfully")
        
    except Exception as e:
        logger.exception("Fatal error during database build")
        sys.exit(1)

if __name__ == '__main__':
    main()
