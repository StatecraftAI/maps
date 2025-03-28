import yt_dlp
import json
import sys
from loguru import logger
from typing import Dict, List, Any

def configure_logging() -> None:
    """Configure loguru logger with appropriate format and level."""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )

def extract_playlist_data(channel_url: str, yt_dlp_options: Dict[str, Any]) -> Dict[str, Any]:
    """Extract playlist data from YouTube channel."""
    try:
        with yt_dlp.YoutubeDL(yt_dlp_options) as youtube_dl:
            playlist_data = youtube_dl.extract_info(channel_url, download=False)
        return playlist_data
    except yt_dlp.utils.DownloadError as download_error:
        logger.error(f"Failed to download playlist data: {download_error}")
        raise
    except Exception as error:
        logger.error(f"Unexpected error during playlist extraction: {error}")
        raise

def extract_video_records(playlist_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract video records from playlist data."""
    entries = playlist_data.get("entries")
    if not entries:
        logger.warning("No entries found in playlist data")
        return []

    first_entry = entries[0]
    if isinstance(first_entry, dict) and "entries" in first_entry:
        video_records = first_entry["entries"]
    else:
        video_records = entries

    return video_records

def write_video_records(video_records: List[Dict[str, Any]], output_file: str) -> None:
    """Write video records to the specified output file."""
    try:
        logger.info(f"Writing {len(video_records)} video records to {output_file}")
        with open(output_file, 'w', encoding='utf-8') as out_file:
            for record in video_records:
                out_file.write(json.dumps(record) + "\n")
        logger.info(f"Successfully wrote records to {output_file}")
    except IOError as io_error:
        logger.error(f"Failed to write to {output_file}: {io_error}")
        raise
    except Exception as generic_error:
        logger.error(f"Unexpected error while writing records: {generic_error}")
        raise

def main() -> None:
    """
    Extracts a flat playlist of videos from a YouTube channel and writes
    one JSON record per line to the specified output file.

    Usage:
        python build_inventory.py <channel_url> <output_file>
    """
    configure_logging()
    logger.info("Starting inventory creation")

    try:
        if len(sys.argv) < 3:
            logger.error("Usage: python build_inventory.py <channel_url> <output_file>")
            sys.exit(1)

        channel_url = sys.argv[1]
        output_file = sys.argv[2]

        yt_dlp_options = {
            'extract_flat': True,
            'skip_download': True,
            'quiet': True,
        }

        playlist_data = extract_playlist_data(channel_url, yt_dlp_options)
        video_records = extract_video_records(playlist_data)
        write_video_records(video_records, output_file)

        logger.info("Inventory creation completed successfully")

    except Exception as error:
        logger.exception("Fatal error during inventory creation")
        sys.exit(1)

if __name__ == '__main__':
    main()
