# Transcription Pipeline

This pipeline processes school board meeting videos from YouTube, downloading their audio and generating transcripts. It supports both cloud-based (AssemblyAI) and local (Whisper) transcription options.

## Overview

The pipeline consists of several steps:

1. **Create Inventory** (`create_inventory.py`): Extracts video metadata from YouTube channel
2. **Build Database** (`build_inventory_db.py`): Creates SQLite database with video metadata
3. **Test Database** (`test_inventory_db.py`): Verifies database integrity and contents
4. **Download Audio** (`download_audio.py`): Downloads audio from YouTube videos
5. **Generate Transcripts** (choose one):
   - `generate_transcripts_assembly.py`: Uses AssemblyAI's cloud service ($1.50/hr)
   - `generate_transcripts_whisper.py`: Uses local Whisper model (free but slower)

## Features

- Asynchronous processing of long audio files
- Automatic speaker diarization (AssemblyAI)
- Error handling and retry mechanisms
- Progress tracking and logging
- Database integration for tracking status
- Rate limiting to avoid API overuse
- Support for both cloud and local processing

## Prerequisites

- Python 3.10 or higher
- YouTube channel URL
- For AssemblyAI: API key
- For Whisper: GPU recommended (but not required)

## Installation

1. Install required packages:

```bash
pip install yt-dlp assemblyai loguru faster-whisper whisperx
```

2. Set up your AssemblyAI API key (if using cloud service):

```bash
export ASSEMBLY_API_KEY='your-api-key-here'
```

## Pipeline Steps

### 1. Create Inventory

Extract video metadata from the YouTube channel:

```bash
python transcription/create_inventory.py <channel_url> <output_file>
```

Example:

```bash
python transcription/create_inventory.py https://www.youtube.com/@ppsboardofeducation pps_yt_video_inventory.json
```

### 2. Build Database

Create SQLite database with video metadata:

```bash
python transcription/build_inventory_db.py <inventory_json_file>
```

Example:

```bash
python transcription/build_inventory_db.py pps_yt_video_inventory.json
```

### 3. Test Database

Verify database integrity and contents:

```bash
python transcription/test_inventory_db.py
```

This will show:

- Total number of records
- Date range coverage
- Records with/without dates
- Download and transcription status

### 4. Download Audio

Download audio files from YouTube:

```bash
python transcription/download_audio.py
```

This will:

- Find videos without downloaded audio
- Download audio in FLAC format
- Update the database with download status

### 5. Generate Transcripts

Choose one of two options:

#### Option A: AssemblyAI (Cloud Service)

```bash
python transcription/generate_transcripts_assembly.py
```

Features:

- Higher accuracy
- Speaker diarization
- Faster processing
- Cost: $1.50 per hour of audio

#### Option B: Whisper (Local)

```bash
python transcription/generate_transcripts_whisper.py
```

Features:

- Free to use
- Runs locally
- No API limits
- Slower processing
- GPU recommended

## Database Schema

The pipeline uses a SQLite database with the following columns in the `videos` table:

- `video_id`: YouTube video ID
- `title`: Video title
- `url`: YouTube video URL
- `record_date`: Extracted from title
- `audio_downloaded`: Boolean indicating if audio is downloaded
- `transcript_generated`: Boolean indicating if transcript exists
- `local_audio_path`: Path to downloaded audio file
- `transcript_path`: Path to generated transcript JSON
- `transcript_id`: AssemblyAI transcript ID (if using cloud service)
- `last_status_check`: Timestamp of last status check
- `download_attempts`: Number of download attempts
- `error_message`: Any error messages

## Output Format

Transcripts are saved as JSON files with the following structure:

```json
{
  "segments": [
    {
      "start": 0.0,
      "end": 5.2,
      "text": "Transcribed text here",
      "confidence": 0.95,
      "speaker": "Speaker A"  // Only with AssemblyAI
    }
  ],
  "language": "en",
  "language_probability": 1.0,
  "metadata": {
    "audio_duration": 7200.0,
    "word_count": 1500,
    "speaker_count": 5  // Only with AssemblyAI
  }
}
```

## Error Handling

The pipeline includes comprehensive error handling:

- Failed downloads are retried automatically
- Transcription errors are logged and tracked
- Database errors are caught and reported
- API rate limits are respected
- Invalid dates are handled gracefully

## Cost Considerations

### AssemblyAI Option

- $1.50 per hour of audio
- Features:
  - Speaker diarization
  - Higher accuracy
  - Faster processing
  - Automatic retries

### Whisper Option

- Free to use
- Features:
  - Local processing
  - No API limits
  - Slower processing
  - GPU recommended

## Troubleshooting

1. **API Key Issues**
   - Verify `ASSEMBLY_API_KEY` is set correctly
   - Check API key permissions in AssemblyAI dashboard

2. **Database Errors**
   - Ensure database has correct schema
   - Check file permissions on database file
   - Run test_inventory_db.py to verify integrity

3. **Transcription Failures**
   - Check error messages in logs
   - Verify audio file format and quality
   - Monitor AssemblyAI dashboard for status
   - For Whisper: Check GPU memory and CUDA setup

4. **Download Issues**
   - Check internet connectivity
   - Verify YouTube video availability
   - Check disk space
   - Review yt-dlp version

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
