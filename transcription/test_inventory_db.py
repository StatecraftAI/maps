import sqlite3
import sys
from loguru import logger
from datetime import datetime
from typing import List, Tuple

def configure_logging() -> None:
    """Configure loguru logger with appropriate format and level."""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )

def test_database(db_path: str = 'inventory.db') -> None:
    """Run various tests on the database to verify its contents and functionality."""
    logger.info(f"Starting database tests on {db_path}")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Test 1: Basic table structure
        logger.info("Testing table structure...")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='videos'")
        if not cursor.fetchone():
            logger.error("❌ videos table not found!")
            return
        logger.success("✓ videos table exists")
        
        # Test 2: Count total records
        cursor.execute("SELECT COUNT(*) FROM videos")
        total_records = cursor.fetchone()[0]
        logger.info(f"Total records in database: {total_records}")
        
        # Test 3: Check for records with dates
        cursor.execute("SELECT COUNT(*) FROM videos WHERE record_date IS NOT NULL")
        records_with_dates = cursor.fetchone()[0]
        logger.info(f"Records with dates: {records_with_dates}")
        
        # Test 4: Show date range
        cursor.execute("""
            SELECT MIN(record_date), MAX(record_date)
            FROM videos
            WHERE record_date IS NOT NULL
        """)
        min_date, max_date = cursor.fetchone()
        if min_date and max_date:
            logger.info(f"Date range: {min_date} to {max_date}")
        
        # Test 5: Show sample of records with different date formats
        logger.info("\nSample records with dates:")
        cursor.execute("""
            SELECT title, record_date
            FROM videos
            WHERE record_date IS NOT NULL
            ORDER BY record_date DESC
            LIMIT 5
        """)
        for title, date in cursor.fetchall():
            logger.info(f"Title: {title}")
            logger.info(f"Date: {date}")
            logger.info("---")
        
        # Test 6: Check for potential issues
        logger.info("\nChecking for potential issues...")
        
        # Check for records without titles
        cursor.execute("SELECT COUNT(*) FROM videos WHERE title IS NULL OR title = ''")
        no_title_count = cursor.fetchone()[0]
        if no_title_count > 0:
            logger.warning(f"Found {no_title_count} records without titles")
        
        # Check for records without URLs
        cursor.execute("SELECT COUNT(*) FROM videos WHERE url IS NULL OR url = ''")
        no_url_count = cursor.fetchone()[0]
        if no_url_count > 0:
            logger.warning(f"Found {no_url_count} records without URLs")
        
        # Check for records with future dates
        today = datetime.now().date()
        cursor.execute("SELECT COUNT(*) FROM videos WHERE record_date > ?", (today,))
        future_dates_count = cursor.fetchone()[0]
        if future_dates_count > 0:
            logger.warning(f"Found {future_dates_count} records with future dates")
        
        # Test 7: Show some statistics
        logger.info("\nDatabase Statistics:")
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN record_date IS NOT NULL THEN 1 ELSE 0 END) as with_dates,
                SUM(CASE WHEN audio_downloaded = 1 THEN 1 ELSE 0 END) as audio_downloaded,
                SUM(CASE WHEN transcript_generated = 1 THEN 1 ELSE 0 END) as transcript_generated
            FROM videos
        """)
        stats = cursor.fetchone()
        logger.info(f"Total records: {stats[0]}")
        logger.info(f"Records with dates: {stats[1]}")
        logger.info(f"Records with audio downloaded: {stats[2]}")
        logger.info(f"Records with transcripts: {stats[3]}")
        
        logger.success("\nAll tests completed successfully!")
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during testing: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def main() -> None:
    """Main entry point for database testing."""
    configure_logging()
    logger.info("Starting database verification")
    
    try:
        test_database()
    except Exception as e:
        logger.exception("Fatal error during database testing")
        sys.exit(1)

if __name__ == '__main__':
    main() 