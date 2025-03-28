import sqlite3
import sys
from pathlib import Path
from loguru import logger
import assemblyai as aai
import json
import os
from typing import Optional, List, Tuple, Dict
from datetime import datetime, timedelta

# Constants
DB_PATH = 'inventory.db'
TRANSCRIPTS_DIR = 'transcripts'
ASSEMBLY_API_KEY = os.getenv('ASSEMBLY_API_KEY')
MAX_WORKERS = 4  # Number of parallel processes
MIN_WAIT_TIME = timedelta(minutes=5)  # Minimum time to wait before checking status

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

def submit_transcription(audio_path: str) -> Optional[str]:
    """Submit an audio file for transcription and return the transcript ID."""
    try:
        logger.info(f"Submitting {audio_path} for transcription")
        
        # Check if audio file exists
        if not Path(audio_path).exists():
            logger.error(f"Audio file not found: {audio_path}")
            return None
            
        # Configure transcription with all features including summarization
        config = aai.TranscriptionConfig(
            speaker_labels=True,
            punctuate=True,
            format_text=True,
            boost_param="high",  # Use string instead of enum
            word_boost=["board", "meeting", "public", "school", "district"],
            summarization=True,
            summary_model=aai.SummarizationModel.informative,
            summary_type=aai.SummarizationType.bullets
        )
        
        # Submit for transcription
        transcript = aai.Transcriber().submit(audio_path, config)
        
        if transcript.status == aai.TranscriptStatus.error:
            logger.error(f"Submission failed: {transcript.error}")
            return None
            
        logger.success(f"Successfully submitted {audio_path} (ID: {transcript.id})")
        return transcript.id
        
    except Exception as e:
        logger.error(f"Error submitting {audio_path}: {e}")
        return None

def check_transcription_status(transcript_id: str) -> Optional[Dict]:
    """Check the status of a transcription and return the result if complete."""
    try:
        transcript = aai.Transcript.get_by_id(transcript_id)
        
        if transcript.status == aai.TranscriptStatus.error:
            logger.error(f"Transcription failed: {transcript.error}")
            return None
        elif transcript.status == aai.TranscriptStatus.completed:
            # Format complete result including all available data
            formatted_result = {
                "raw": {
                    "id": transcript.id,
                    "status": transcript.status,
                    "audio_url": transcript.audio_url,
                    "audio_duration": transcript.audio_duration,
                    "confidence": transcript.confidence,
                    "full_text": transcript.text,
                    "utterances": [
                        {
                            "start": utterance.start,
                            "end": utterance.end,
                            "text": utterance.text,
                            "confidence": utterance.confidence,
                            "speaker": utterance.speaker or "unknown",
                            "words": [
                                {
                                    "text": word.text,
                                    "start": word.start,
                                    "end": word.end,
                                    "confidence": word.confidence
                                }
                                for word in utterance.words
                            ] if hasattr(utterance, 'words') else []
                        }
                        for utterance in transcript.utterances
                    ],
                    "words": [
                        {
                            "text": word.text,
                            "start": word.start,
                            "end": word.end,
                            "confidence": word.confidence
                        }
                        for word in transcript.words
                    ] if hasattr(transcript, 'words') else [],
                    "sentences": [
                        {
                            "text": sentence.text,
                            "start": sentence.start,
                            "end": sentence.end,
                            "confidence": sentence.confidence
                        }
                        for sentence in transcript.sentences
                    ] if hasattr(transcript, 'sentences') else [],
                    "summary": transcript.summary if hasattr(transcript, 'summary') else None
                },
                "processed": {
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
                    "language": "en",  # Default to English since we're using English-specific settings
                    "language_probability": transcript.confidence,
                    "metadata": {
                        "audio_duration": transcript.audio_duration,
                        "word_count": len(transcript.words) if hasattr(transcript, 'words') else sum(len(utterance.text.split()) for utterance in transcript.utterances),
                        "speaker_count": len(set(u.speaker for u in transcript.utterances if u.speaker)),
                        "utterance_count": len(transcript.utterances),
                        "sentence_count": len(transcript.sentences) if hasattr(transcript, 'sentences') else None
                    },
                    "summary": transcript.summary if hasattr(transcript, 'summary') else None
                }
            }
            return formatted_result
        else:
            logger.debug(f"Transcription {transcript_id} still in progress: {transcript.status}")
            return None
            
    except Exception as e:
        logger.error(f"Error checking transcription {transcript_id}: {e}")
        return None

def ensure_database_schema() -> None:
    """Ensure the database has all required columns."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Add required columns if they don't exist
        cursor.execute("PRAGMA table_info(videos)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'transcript_id' not in columns:
            cursor.execute('''
                ALTER TABLE videos
                ADD COLUMN transcript_id TEXT
            ''')
            logger.info("Added transcript_id column to videos table")
            
        if 'transcript_submitted' not in columns:
            cursor.execute('''
                ALTER TABLE videos
                ADD COLUMN transcript_submitted INTEGER DEFAULT 0
            ''')
            logger.info("Added transcript_submitted column to videos table")
            
        if 'transcript_downloaded' not in columns:
            cursor.execute('''
                ALTER TABLE videos
                ADD COLUMN transcript_downloaded INTEGER DEFAULT 0
            ''')
            logger.info("Added transcript_downloaded column to videos table")
            
        if 'transcript_path' not in columns:
            cursor.execute('''
                ALTER TABLE videos
                ADD COLUMN transcript_path TEXT
            ''')
            logger.info("Added transcript_path column to videos table")
            
        if 'last_status_check' not in columns:
            cursor.execute('''
                ALTER TABLE videos
                ADD COLUMN last_status_check TIMESTAMP
            ''')
            logger.info("Added last_status_check column to videos table")
            
        conn.commit()
        logger.debug("Database schema check completed")
        
    except sqlite3.Error as e:
        logger.error(f"Database schema error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def fetch_pending_submissions() -> List[Tuple[str, str, str]]:
    """Fetch videos that have audio downloaded but no transcript submitted."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT video_id, title, local_audio_path
            FROM videos 
            WHERE audio_downloaded = 1 
            AND transcript_id IS NULL
            AND local_audio_path IS NOT NULL
            ORDER BY record_date DESC
        ''')
        pending = cursor.fetchall()
        
        if not pending:
            logger.info("No pending submissions found")
        else:
            logger.info(f"Found {len(pending)} files pending submission")
            
        return pending
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def fetch_pending_retrievals() -> List[Tuple[str, str, str]]:
    """Fetch videos that have transcriptions submitted but not downloaded."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT video_id, title, transcript_id
            FROM videos 
            WHERE transcript_id IS NOT NULL 
            AND transcript_downloaded = 0
            ORDER BY record_date DESC
        ''')
        pending = cursor.fetchall()
        
        if not pending:
            logger.info("No pending retrievals found")
        else:
            logger.info(f"Found {len(pending)} files pending retrieval")
            
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

def update_database(video_id: str, transcript_path: Optional[str] = None, 
                   transcript_id: Optional[str] = None, error_message: str = "",
                   transcript_submitted: bool = False, transcript_downloaded: bool = False) -> None:
    """Update the database with transcription results."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # First ensure the schema is correct
        ensure_database_schema()
        
        # Update the record
        cursor.execute('''
            UPDATE videos
            SET transcript_id = COALESCE(?, transcript_id),
                transcript_path = COALESCE(?, transcript_path),
                transcript_submitted = COALESCE(?, transcript_submitted),
                transcript_downloaded = COALESCE(?, transcript_downloaded),
                error_message = ?,
                last_status_check = CURRENT_TIMESTAMP
            WHERE video_id = ?
        ''', (transcript_id, transcript_path, transcript_submitted, transcript_downloaded, error_message, video_id))
        
        conn.commit()
        logger.debug(f"Database updated for video {video_id}")
        
    except sqlite3.Error as e:
        logger.error(f"Database error while updating {video_id}: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def process_submissions(audio_files: List[Tuple[str, str, str]]) -> None:
    """Process a batch of audio files for submission."""
    for video_id, title, audio_path in audio_files:
        try:
            logger.info(f"Submitting: {title}")
            
            # Submit for transcription
            transcript_id = submit_transcription(audio_path)
            if not transcript_id:
                update_database(video_id, error_message="Submission failed")
                continue
                
            # Update database with transcript ID and submission status
            update_database(video_id, transcript_id=transcript_id, transcript_submitted=True)
            
        except Exception as e:
            logger.error(f"Error processing {video_id}: {e}")
            update_database(video_id, error_message=str(e))

def process_retrievals(pending_retrievals: List[Tuple[str, str, str]]) -> None:
    """Process a batch of pending transcriptions for retrieval."""
    for video_id, title, transcript_id in pending_retrievals:
        try:
            logger.info(f"Checking status: {title}")
            
            # Check transcription status
            result = check_transcription_status(transcript_id)
            if not result:
                update_database(video_id, transcript_id=transcript_id)
                continue
                
            # Save transcript
            transcript_path = save_transcript(video_id, result)
            if not transcript_path:
                update_database(video_id, transcript_id=transcript_id, error_message="Failed to save transcript")
                continue
                
            # Update database with download status
            update_database(video_id, transcript_path=transcript_path, transcript_downloaded=True)
            
        except Exception as e:
            logger.error(f"Error processing {video_id}: {e}")
            update_database(video_id, transcript_id=transcript_id, error_message=str(e))

def verify_transcript_files() -> None:
    """Verify that all transcripts marked as downloaded in the database actually exist."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get all records marked as downloaded
        cursor.execute('''
            SELECT video_id, title, transcript_path
            FROM videos 
            WHERE transcript_downloaded = 1
            AND transcript_path IS NOT NULL
        ''')
        downloaded_records = cursor.fetchall()
        
        if not downloaded_records:
            logger.info("No downloaded transcripts found in database")
            return
            
        logger.info(f"Verifying {len(downloaded_records)} downloaded transcripts")
        
        # Check each transcript file
        for video_id, title, transcript_path in downloaded_records:
            if not Path(transcript_path).exists():
                logger.warning(f"Transcript file missing for {title} (ID: {video_id})")
                # Update database to reflect missing file
                cursor.execute('''
                    UPDATE videos
                    SET transcript_downloaded = 0,
                        transcript_path = NULL,
                        error_message = 'Transcript file missing'
                    WHERE video_id = ?
                ''', (video_id,))
        
        conn.commit()
        logger.info("Transcript file verification completed")
        
    except sqlite3.Error as e:
        logger.error(f"Database error during verification: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

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
        ensure_database_schema()
        
        # Verify existing transcript files
        verify_transcript_files()
        
        # Process new submissions
        pending_submissions = fetch_pending_submissions()
        if pending_submissions:
            logger.info("Processing new submissions...")
            batch_size = MAX_WORKERS
            for i in range(0, len(pending_submissions), batch_size):
                batch = pending_submissions[i:i + batch_size]
                process_submissions(batch)
        
        # Process pending retrievals
        pending_retrievals = fetch_pending_retrievals()
        if pending_retrievals:
            logger.info("Checking pending transcriptions...")
            batch_size = MAX_WORKERS
            for i in range(0, len(pending_retrievals), batch_size):
                batch = pending_retrievals[i:i + batch_size]
                process_retrievals(batch)
        
        if not pending_submissions and not pending_retrievals:
            logger.info("No pending tasks. Exiting.")
        else:
            logger.success("Processing completed")
        
    except Exception as e:
        logger.exception("Fatal error during transcription process")
        sys.exit(1)

if __name__ == '__main__':
    main() 