import sqlite3
import sys
from pathlib import Path
from loguru import logger
from faster_whisper import WhisperModel
from typing import Optional, List, Tuple, Dict
import json
import torch
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
import tempfile
import shutil
import whisperx
import numpy as np

# Constants
DB_PATH = 'inventory.db'
TRANSCRIPTS_DIR = 'transcripts'
MODEL_SIZE = "medium"  # Options: tiny, base, small, medium, large
CHUNK_SIZE = 1800  # 30 minutes in seconds
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
USE_CLOUD = os.getenv("USE_CLOUD", "false").lower() == "true"
CLOUD_PROVIDER = os.getenv("CLOUD_PROVIDER", "gcp")  # or "aws"
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

def setup_cloud_environment() -> None:
    """Set up cloud environment if needed."""
    if not USE_CLOUD:
        return
        
    try:
        if CLOUD_PROVIDER == "gcp":
            # Check if gcloud is installed
            subprocess.run(["gcloud", "--version"], check=True, capture_output=True)
            logger.info("GCP environment ready")
        elif CLOUD_PROVIDER == "aws":
            # Check if AWS CLI is installed
            subprocess.run(["aws", "--version"], check=True, capture_output=True)
            logger.info("AWS environment ready")
        else:
            raise ValueError(f"Unsupported cloud provider: {CLOUD_PROVIDER}")
    except subprocess.CalledProcessError:
        logger.error(f"Cloud provider CLI not found. Please install {'gcloud' if CLOUD_PROVIDER == 'gcp' else 'aws cli'}")
        raise

def optimize_audio(audio_path: str) -> str:
    """Optimize audio file for faster processing."""
    try:
        # Create a temporary directory for processing
        temp_dir = tempfile.mkdtemp()
        wav_path = os.path.join(temp_dir, "optimized.wav")
        
        # Use ffmpeg to optimize the audio
        # - Convert to mono WAV (Whisper's preferred format)
        # - Reduce sample rate to 16kHz
        # - Use high-quality resampling
        subprocess.run([
            "ffmpeg", "-i", audio_path,
            "-ac", "1",  # mono
            "-ar", "16000",  # 16kHz
            "-acodec", "pcm_s16le",  # 16-bit WAV
            "-y",  # overwrite output
            wav_path
        ], check=True, capture_output=True)
        
        logger.debug(f"Optimized audio saved to {wav_path}")
        return wav_path
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to optimize audio: {e.stderr.decode()}")
        raise
    except Exception as e:
        logger.error(f"Error during audio optimization: {e}")
        raise

def split_audio(audio_path: str) -> List[str]:
    """Split long audio files into chunks."""
    try:
        # Load audio using whisperx
        audio = whisperx.load_audio(audio_path)
        
        # Calculate number of chunks
        duration = len(audio) / 16000  # duration in seconds
        num_chunks = int(np.ceil(duration / CHUNK_SIZE))
        
        if num_chunks <= 1:
            return [audio_path]
            
        # Create chunks directory
        chunks_dir = os.path.join(os.path.dirname(audio_path), "chunks")
        os.makedirs(chunks_dir, exist_ok=True)
        
        chunk_paths = []
        for i in range(num_chunks):
            start = i * CHUNK_SIZE * 16000
            end = min((i + 1) * CHUNK_SIZE * 16000, len(audio))
            chunk = audio[start:end]
            
            chunk_path = os.path.join(chunks_dir, f"chunk_{i:03d}.wav")
            whisperx.save_audio(chunk, chunk_path)
            chunk_paths.append(chunk_path)
            
        logger.info(f"Split audio into {num_chunks} chunks")
        return chunk_paths
        
    except Exception as e:
        logger.error(f"Error splitting audio: {e}")
        raise

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

def transcribe_audio(model: WhisperModel, audio_path: str) -> Optional[Dict]:
    """Transcribe a single audio file using faster-whisper."""
    try:
        logger.info(f"Transcribing {audio_path}")
        
        # Check if audio file exists
        if not Path(audio_path).exists():
            logger.error(f"Audio file not found: {audio_path}")
            return None
            
        # Transcribe with faster-whisper
        segments, info = model.transcribe(
            audio_path,
            language="en",
            beam_size=5,
            vad_filter=True,
            vad_parameters=dict(min_silence_duration_ms=500)
        )
        
        # Convert segments to list for JSON serialization
        segments_list = [
            {
                "start": segment.start,
                "end": segment.end,
                "text": segment.text,
                "confidence": segment.confidence
            }
            for segment in segments
        ]
        
        result = {
            "segments": segments_list,
            "language": info.language,
            "language_probability": info.language_probability
        }
        
        logger.success(f"Successfully transcribed {audio_path}")
        return result
        
    except Exception as e:
        logger.error(f"Error transcribing {audio_path}: {e}")
        return None

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

def update_database(video_id: str, transcript_path: Optional[str], error_message: str = "") -> None:
    """Update the database with transcription results."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
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

def merge_transcripts(transcripts: List[Dict]) -> Dict:
    """Merge multiple transcript chunks into a single transcript."""
    if not transcripts:
        return {}
        
    # Sort segments by start time
    all_segments = []
    for transcript in transcripts:
        all_segments.extend(transcript.get("segments", []))
    
    all_segments.sort(key=lambda x: x["start"])
    
    # Merge overlapping segments
    merged_segments = []
    current_segment = all_segments[0] if all_segments else None
    
    for segment in all_segments[1:]:
        if current_segment and segment["start"] <= current_segment["end"]:
            # Merge overlapping segments
            current_segment["end"] = max(current_segment["end"], segment["end"])
            current_segment["text"] += " " + segment["text"]
            current_segment["confidence"] = max(current_segment["confidence"], segment["confidence"])
        else:
            merged_segments.append(current_segment)
            current_segment = segment
    
    if current_segment:
        merged_segments.append(current_segment)
    
    return {
        "segments": merged_segments,
        "language": transcripts[0].get("language"),
        "language_probability": transcripts[0].get("language_probability")
    }

def process_audio_batch(audio_files: List[Tuple[str, str, str]], model: WhisperModel) -> None:
    """Process a batch of audio files."""
    for video_id, title, audio_path in audio_files:
        try:
            logger.info(f"Processing: {title}")
            
            # Optimize audio
            optimized_path = optimize_audio(audio_path)
            
            # Split into chunks if needed
            chunk_paths = split_audio(optimized_path)
            
            # Transcribe each chunk
            transcripts = []
            for chunk_path in chunk_paths:
                result = transcribe_audio(model, chunk_path)
                if result:
                    transcripts.append(result)
            
            if not transcripts:
                update_database(video_id, None, "Transcription failed")
                continue
                
            # Merge transcripts if there were multiple chunks
            final_transcript = merge_transcripts(transcripts) if len(transcripts) > 1 else transcripts[0]
            
            # Save transcript
            transcript_path = save_transcript(video_id, final_transcript)
            if not transcript_path:
                update_database(video_id, None, "Failed to save transcript")
                continue
                
            # Update database
            update_database(video_id, transcript_path)
            
            # Clean up temporary files
            shutil.rmtree(os.path.dirname(optimized_path))
            
        except Exception as e:
            logger.error(f"Error processing {video_id}: {e}")
            update_database(video_id, None, str(e))

def main() -> None:
    """Main entry point for the transcription process."""
    configure_logging()
    logger.info(f"Starting transcription process using device: {DEVICE}")
    
    try:
        # Setup
        setup_output_directory()
        if USE_CLOUD:
            setup_cloud_environment()
            
        pending = fetch_pending_transcriptions()
        
        if not pending:
            logger.info("No pending transcriptions. Exiting.")
            return
            
        # Load Whisper model
        logger.info(f"Loading Whisper model: {MODEL_SIZE}")
        model = WhisperModel(
            MODEL_SIZE,
            device=DEVICE,
            compute_type="float16" if DEVICE == "cuda" else "int8"
        )
        
        # Process files in batches
        batch_size = MAX_WORKERS
        for i in range(0, len(pending), batch_size):
            batch = pending[i:i + batch_size]
            process_audio_batch(batch, model)
            
        logger.success("Transcription process completed")
        
    except Exception as e:
        logger.exception("Fatal error during transcription process")
        sys.exit(1)

if __name__ == '__main__':
    main() 