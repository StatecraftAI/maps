import sqlite3
import sys
from pathlib import Path
from loguru import logger
import assemblyai as aai
import json
import os
from typing import Optional, List, Tuple, Dict

# Constants
DB_PATH = 'inventory.db'
TRANSCRIPTS_DIR = 'transcripts'
ASSEMBLY_API_KEY = os.getenv('ASSEMBLY_API_KEY')
MAX_WORKERS = 4  # Number of parallel processes

def configure_logging() -> None:
    """Configure loguru logger with appropriate format and level."""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )

def setup_output_directory() -> None:
    """Create the transcripts directory if it doesn't exist."""
    try:
        output_path = Path(TRANSCRIPTS_DIR)
        output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory ready: {output_path.absolute()}")
    except Exception as e:
        logger.error(f"Failed to create output directory: {e}")
        raise

def transcribe_audio(audio_path: str) -> Optional[Dict]:
    """Transcribe a single audio file using AssemblyAI."""
    try:
        logger.info(f"Transcribing {audio_path}")
        
        # Check if audio file exists
        if not Path(audio_path).exists():
            logger.error(f"Audio file not found: {audio_path}")
            return None
            
        # Configure transcription
        config = aai.TranscriptionConfig(
            speaker_labels=True,
            punctuate=True,
            format_text=True,
            boost_param=aai.BoostParam.HIGH,
            word_boost=["board", "meeting", "public", "school", "district"]
        )
        
        # Transcribe the file
        transcript = aai.Transcriber().transcribe(audio_path, config)
        
        if transcript.status == aai.TranscriptStatus.error:
            logger.error(f"Transcription failed: {transcript.error}")
            return None
            
        # Format result for our needs
        formatted_result = {
            "segments": [
                {
                    "start": utterance.start,
                    "end": utterance.end,
                    "text": utterance.text,
                    "confidence": utterance.confidence,
                    "speaker": utterance.speaker or "unknown"
                }
                for utterance in transcript.utterances
            ],
            "language": "en",
            "language_probability": 1.0,
            "metadata": {
                "audio_duration": transcript.audio_duration,
                "word_count": transcript.word_count,
                "speaker_count": len(set(u.speaker for u in transcript.utterances if u.speaker))
            }
        }
        
        logger.success(f"Successfully transcribed {audio_path}")
        return formatted_result
        
    except Exception as e:
        logger.error(f"Error transcribing {audio_path}: {e}")
        return None

def fetch_pending_transcriptions() -> List[Tuple[str, str, str]]:
    """Fetch videos that have audio downloaded but no transcript."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT video_id, title, local_audio_path
            FROM videos 
            WHERE audio_downloaded = 1 
            AND transcript_generated = 0
            AND local_audio_path IS NOT NULL
            ORDER BY record_date DESC
        ''')
        pending = cursor.fetchall()
        
        if not pending:
            logger.info("No pending transcriptions found")
        else:
            logger.info(f"Found {len(pending)} files pending transcription")
            
        return pending
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def save_transcript(video_id: str, transcript_data: Dict) -> Optional[str]:
    """Save transcript to JSON file and return the file path."""
    try:
        output_path = Path(TRANSCRIPTS_DIR) / f"{video_id}.json"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(transcript_data, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved transcript to {output_path}")
        return str(output_path)
        
    except Exception as e:
        logger.error(f"Error saving transcript for {video_id}: {e}")
        return None

def ensure_database_schema() -> None:
    """Ensure the database has all required columns."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Add transcript_path column if it doesn't exist
        cursor.execute("PRAGMA table_info(videos)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'transcript_path' not in columns:
            cursor.execute('''
                ALTER TABLE videos
                ADD COLUMN transcript_path TEXT
            ''')
            logger.info("Added transcript_path column to videos table")
            
        conn.commit()
        logger.debug("Database schema check completed")
        
    except sqlite3.Error as e:
        logger.error(f"Database schema error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def update_database(video_id: str, transcript_path: Optional[str], error_message: str = "") -> None:
    """Update the database with transcription results."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # First ensure the schema is correct
        ensure_database_schema()
        
        # Update the record
        cursor.execute('''
            UPDATE videos
            SET transcript_generated = ?,
                transcript_path = ?,
                error_message = ?
            WHERE video_id = ?
        ''', (1 if transcript_path else 0, transcript_path, error_message, video_id))
        
        conn.commit()
        logger.debug(f"Database updated for video {video_id}")
        
    except sqlite3.Error as e:
        logger.error(f"Database error while updating {video_id}: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def process_audio_batch(audio_files: List[Tuple[str, str, str]]) -> None:
    """Process a batch of audio files."""
    for video_id, title, audio_path in audio_files:
        try:
            logger.info(f"Processing: {title}")
            
            # Transcribe
            result = transcribe_audio(audio_path)
            if not result:
                update_database(video_id, None, "Transcription failed")
                continue
                
            # Save transcript
            transcript_path = save_transcript(video_id, result)
            if not transcript_path:
                update_database(video_id, None, "Failed to save transcript")
                continue
                
            # Update database
            update_database(video_id, transcript_path)
            
        except Exception as e:
            logger.error(f"Error processing {video_id}: {e}")
            update_database(video_id, None, str(e))

def main() -> None:
    """Main entry point for the transcription process."""
    configure_logging()
    logger.info("Starting transcription process using AssemblyAI")
    
    if not ASSEMBLY_API_KEY:
        logger.error("ASSEMBLY_API_KEY environment variable not set")
        sys.exit(1)
    
    try:
        # Configure AssemblyAI SDK
        aai.settings.api_key = ASSEMBLY_API_KEY
        
        # Setup
        setup_output_directory()
        ensure_database_schema()  # Ensure database schema before proceeding
        pending = fetch_pending_transcriptions()
        
        if not pending:
            logger.info("No pending transcriptions. Exiting.")
            return
            
        # Process files in batches
        batch_size = MAX_WORKERS
        for i in range(0, len(pending), batch_size):
            batch = pending[i:i + batch_size]
            process_audio_batch(batch)
            
        logger.success("Transcription process completed")
        
    except Exception as e:
        logger.exception("Fatal error during transcription process")
        sys.exit(1)

if __name__ == '__main__':
    main() 