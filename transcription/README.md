# Video Transcription Pipeline for PPS Board of Education

## Overview

This repository contains a pipeline for processing the PPS Board of Education YouTube channel. The pipeline currently includes scripts to:

- **Extract an Inventory:** Retrieve video metadata from the channel using yt-dlp's Python API.
- **Build a Local Database:** Convert the raw JSON inventory into an SQLite database with additional fields for tracking download and transcription status.
- **Download Audio:** Perform robust, parallel audio downloads (using yt-dlp's Python API) with retry logic and error logging.

The design is modular and can be extended to include transcription (e.g., via OpenAI's Whisper), speaker diarization, and summarization steps.

## Prerequisites

- **Python 3.10+**
- **yt-dlp:** Install with `pip install yt-dlp`
- **SQLite3:** Included in Python's standard library
- Recommended: A Unix-like environment (Pop!_OS, Ubuntu, etc.)

## Installation

1. **Clone the Repository:**

   ```bash
   git clone <repository_url>
   cd <repository_directory>
   ```

2. **Install Dependencies:**

   ```bash
   pip install yt-dlp
   ```

## Pipeline Steps

### 1. Build Inventory

Use the `build_inventory.py` script to extract a raw inventory of video metadata from the YouTube channel. This script leverages yt-dlp's Python API in "flat" mode (metadata only) and writes each entry as a JSON line.

**Example Command:**

```bash
python build_inventory.py https://www.youtube.com/@ppsboardofeducation pps_yt_video_inventory.json
```

This command creates a file named `pps_yt_video_inventory.json` containing the raw metadata.

### 2. Build Inventory Database

Convert the raw JSON inventory into a structured SQLite database with additional fields for tracking:

- **record_date:** Extracted from the video title (if available).
- **audio_downloaded:** Flag indicating if the audio was successfully downloaded.
- **transcript_generated:** Flag for transcription status.
- **local_audio_path:** Path where the audio file is stored.
- **download_attempts & error_message:** For robust error tracking.

**Example Command:**

```bash
python build_inventory_db.py pps_yt_video_inventory.json
```

This creates (or updates) an `inventory.db` file.

### 3. Download Audio

The `download_audio.py` script reads pending entries from the database and downloads the audio files in parallel using the yt-dlp Python API. It uses a `ThreadPoolExecutor` for parallelism, includes retry logic, and updates the database with the download status.

**Example Command:**

```bash
python download_audio.py
```

Audio files will be saved to the `audio_files/` directory. The database is updated with success flags, local file paths, and any error messages encountered.

## Example Pipeline Execution

Assuming you're working from the repository's root directory, execute the pipeline with the following commands:

1. **Extract Video Inventory:**

   ```bash
   python build_inventory.py https://www.youtube.com/@ppsboardofeducation pps_yt_video_inventory.json
   ```

2. **Build/Update the Database:**

   ```bash
   python build_inventory_db.py pps_yt_video_inventory.json
   ```

3. **Download Audio Tracks in Parallel:**

   ```bash
   python download_audio.py
   ```

## Future Enhancements

- **Transcription Integration:** Use a tool like OpenAI's Whisper to transcribe the audio files.
- **Speaker Diarization:** Incorporate speaker labeling with libraries such as pyannote.audio.
- **Summarization:** Develop routines to break down lengthy transcripts into manageable chunks for LLM processing.

## Troubleshooting

- **Dependency Issues:** Ensure that yt-dlp is correctly installed in your Python environment.
- **Database Errors:** Confirm that `inventory.db` is created and accessible.
- **Download Failures:** Check the console output and the `error_message` field in the database for troubleshooting download errors.

## License

This project is licensed under the [MIT License](LICENSE).

## Acknowledgments

- [yt-dlp](https://github.com/yt-dlp/yt-dlp) for its robust video downloading capabilities.
- The PPS Board of Education for providing the video content that inspired this project.
