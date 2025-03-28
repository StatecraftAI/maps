import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import yt_dlp
import time
from loguru import logger
from typing import List, Tuple, Optional, Dict, Any
from pathlib import Path

# Constants
DB_PATH = 'inventory.db'
OUTPUT_DIR = 'audio_files'
MAX_RETRIES = 3
MAX_WORKERS = 4
RETRY_DELAY = 2  # seconds

def configure_logging() -> None:
    """Configure loguru logger with appropriate format and level."""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )

def download_audio(video: Tuple[str, str, str]) -> Tuple[str, bool, Optional[str], int, str]:
    """Download audio for a single video with retry logic."""
    video_id, title, video_url = video
    output_path = os.path.join(OUTPUT_DIR, f"{video_id}.flac")
    
    logger.info(f"Starting download for video: {title} (ID: {video_id})")
    
    ydl_options = {
        'format': 'bestaudio/best',
        'extractaudio': True,
        'audioformat': 'flac',
        'outtmpl': os.path.join(OUTPUT_DIR, '%(id)s.flac'),
        'quiet': True,
    }
    
    retry_count = 0
    error_message = ""

    while retry_count < MAX_RETRIES:
        try:
            logger.debug(f"Download attempt {retry_count + 1}/{MAX_RETRIES} for {video_id}")
            with yt_dlp.YoutubeDL(ydl_options) as ydl:
                ydl.download([video_url])
            
            if os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                logger.success(f"Successfully downloaded {video_id} ({file_size/1024/1024:.1f} MB)")
                return video_id, True, output_path, retry_count, ""
            else:
                raise FileNotFoundError(f"Download completed but file not found: {output_path}")
                
        except yt_dlp.utils.DownloadError as e:
            retry_count += 1
            error_message = f"Download error: {str(e)}"
            logger.warning(f"Download attempt {retry_count} failed for {video_id}: {error_message}")
            if retry_count < MAX_RETRIES:
                logger.info(f"Waiting {RETRY_DELAY} seconds before retry...")
                time.sleep(RETRY_DELAY)
                
        except Exception as e:
            retry_count += 1
            error_message = f"Unexpected error: {str(e)}"
            logger.error(f"Unexpected error on attempt {retry_count} for {video_id}: {error_message}")
            if retry_count < MAX_RETRIES:
                logger.info(f"Waiting {RETRY_DELAY} seconds before retry...")
                time.sleep(RETRY_DELAY)

    logger.error(f"All download attempts failed for {video_id}: {error_message}")
    return video_id, False, None, retry_count, error_message

def update_database(video_id: str, success: bool, local_path: Optional[str], 
                   attempts: int, error_message: str) -> None:
    """Update the database with the result of downloading the given video."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE videos
            SET audio_downloaded = ?,
                local_audio_path = ?,
                download_attempts = ?,
                error_message = ?
            WHERE video_id = ?
        ''', (int(success), local_path, attempts, error_message, video_id))
        
        conn.commit()
        logger.debug(f"Database updated for video {video_id}")
        
    except sqlite3.Error as e:
        logger.error(f"Database error while updating {video_id}: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def fetch_pending_videos() -> List[Tuple[str, str, str]]:
    """Retrieve a list of videos in the database that haven't been downloaded yet."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT video_id, title, url 
            FROM videos 
            WHERE audio_downloaded = 0 
            ORDER BY record_date DESC
        ''')
        pending_videos = cursor.fetchall()
        
        if not pending_videos:
            logger.info("No pending videos found in database")
        else:
            logger.info(f"Found {len(pending_videos)} pending videos")
            
        return pending_videos
        
    except sqlite3.Error as e:
        logger.error(f"Database error while fetching pending videos: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def setup_output_directory() -> None:
    """Create the output directory if it doesn't exist."""
    try:
        output_path = Path(OUTPUT_DIR)
        output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory ready: {output_path.absolute()}")
    except Exception as e:
        logger.error(f"Failed to create output directory: {e}")
        raise

def main() -> None:
    """Main entry point for the audio download process."""
    configure_logging()
    logger.info("Starting audio download process")
    
    try:
        # Setup
        setup_output_directory()
        videos = fetch_pending_videos()
        
        if not videos:
            logger.info("No pending videos for download. Exiting.")
            return
            
        # Process downloads
        logger.info(f"Starting parallel downloads with {MAX_WORKERS} workers")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_video = {
                executor.submit(download_audio, video): video 
                for video in videos
            }
            
            completed = 0
            total = len(videos)
            
            for future in as_completed(future_to_video):
                video = future_to_video[future]
                try:
                    video_id, success, local_path, attempts, error = future.result()
                    update_database(video_id, success, local_path, attempts, error)
                    completed += 1
                    logger.info(f"Progress: {completed}/{total} videos processed")
                    
                except Exception as exc:
                    logger.exception(f"Error processing video {video[0]}: {exc}")
                    completed += 1
                    
        logger.success(f"Download process completed. Processed {completed}/{total} videos.")
        
    except Exception as e:
        logger.exception("Fatal error during audio download process")
        sys.exit(1)

if __name__ == '__main__':
    main()
