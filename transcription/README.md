# Portland Public Schools YouTube Transcription Pipeline

This project automates the process of downloading, transcribing, and summarizing [PPS board meeting](https://www.youtube.com/@ppsboardofeducation) videos from YouTube. It supports both [AssemblyAI](https://www.assemblyai.com/) and [OpenAI's Whisper](https://openai.com/index/whisper/) for transcription, with AssemblyAI providing high-quality transcription, speaker diarization, and summarization.

## Features

- **YouTube Audio Download**: Downloads audio from YouTube videos using yt-dlp
- **Database Management**: SQLite database to track video metadata and processing status
- **Dual Transcription Options**:
  - **AssemblyAI**:
    - Speaker diarization (identifies different speakers)
    - Word-level timing and confidence scores
    - Punctuation and text formatting
    - Word boosting for education-related terms
    - Automatic summarization with bullet points
  - **Whisper**:
    - Local processing with GPU acceleration
    - Cloud processing support (GCP/AWS)
    - Audio optimization and chunking
    - VAD filtering for better accuracy
- **Error Handling**: Comprehensive error catching and logging
- **Concurrent Processing**: Parallel processing of downloads and transcriptions
- **File Verification**: Safety checks to ensure database state matches actual files
- **Detailed Output**: JSON files containing:
  - Raw transcription data with word-level details
  - Processed segments with speaker labels
  - Metadata including word counts and confidence scores
  - Bullet-point summaries (AssemblyAI only)

## Project Structure

```shell
.
├── inventory.db           # SQLite database for tracking videos
├── transcription/        # Transcription pipeline scripts
│   ├── create_inventory.py      # Creates initial video inventory from YouTube
│   ├── build_inventory_db.py    # Builds SQLite database from inventory
│   ├── download_audio.py        # Downloads audio from YouTube
│   ├── generate_transcripts_assembly.py  # AssemblyAI transcription
│   ├── generate_transcripts_whisper.py   # Whisper transcription
│   └── generate_text_transcripts.py      # Creates text versions of transcripts
├── audio/               # Downloaded audio files
├── transcripts/         # JSON transcript files
└── text_transcripts/    # Plain text transcript files
```

## Database Schema

The `videos` table in `inventory.db` contains:

- `video_id`: Unique identifier
- `title`: Video title
- `url`: YouTube URL
- `record_date`: Meeting date
- `audio_downloaded`: Boolean for audio download status
- `local_audio_path`: Path to downloaded audio file
- `transcript_id`: AssemblyAI transcript ID (if using AssemblyAI)
- `transcript_submitted`: Boolean for submission status
- `transcript_downloaded`: Boolean for download status
- `transcript_path`: Path to JSON transcript file
- `text_transcript_path`: Path to text transcript file
- `text_transcript_generated`: Boolean for text transcript status
- `text_transcript_date`: Date text transcript was generated
- `last_status_check`: Timestamp of last status check
- `error_message`: Any error messages

## Pipeline Steps

1. **Create Inventory**:

   ```bash
   python transcription/create_inventory.py <channel_url> <output_file>
   ```

   Creates a JSON inventory of videos from a YouTube channel.

2. **Build Database**:

   ```bash
   python transcription/build_inventory_db.py <inventory_json_file>
   ```

   Builds SQLite database from the inventory file.

3. **Download Audio**:

   ```bash
   python transcription/download_audio.py
   ```

   Downloads audio files from YouTube using yt-dlp.

4. **Generate Transcripts** (Choose one):

   ```bash
   # Using AssemblyAI
   python transcription/generate_transcripts_assembly.py
   
   # Using Whisper
   python transcription/generate_transcripts_whisper.py
   ```

5. **Generate Text Transcripts**:

   ```bash
   python transcription/generate_text_transcripts.py
   ```

   Creates human-readable text versions of the transcripts.

## Requirements

- Python 3.8+
- yt-dlp
- assemblyai (for AssemblyAI transcription)
- faster-whisper (for Whisper transcription)
- loguru
- SQLite3
- ffmpeg (for audio processing)
- CUDA-capable GPU (optional, for local Whisper processing)

## Environment Variables

- `ASSEMBLY_API_KEY`: Your AssemblyAI API key
- `USE_CLOUD`: Set to "true" for cloud processing with Whisper
- `CLOUD_PROVIDER`: Set to "gcp" or "aws" for cloud processing

## Output Formats

### JSON Transcripts

Located in `transcripts/`, containing:

- Raw transcription data with word-level timing
- Speaker diarization (AssemblyAI)
- Confidence scores
- Meeting metadata
- Bullet-point summaries (AssemblyAI)

### Text Transcripts

Located in `text_transcripts/`, formatted as:

```
[00:00:00 - 00:00:30] Speaker A: Text of the utterance
[00:00:30 - 00:01:00] Speaker B: Text of the utterance
```

## Error Handling

The pipeline includes comprehensive error handling:

- Retries for failed downloads
- Database state verification
- File existence checks
- Detailed logging of all operations
- Graceful handling of API errors
- Automatic cleanup of temporary files

## Logging

All operations are logged using loguru with:

- Timestamps
- Log levels (INFO, ERROR, etc.)
- Module and function names
- Detailed error messages

## Cloud Setup for Whisper Processing

### Google Cloud Platform (GCP)

1. **Create a GCP Project**:

   ```bash
   gcloud projects create [PROJECT_ID]
   gcloud config set project [PROJECT_ID]
   ```

2. **Enable Required APIs**:

   ```bash
   gcloud services enable compute.googleapis.com
   gcloud services enable storage.googleapis.com
   ```

3. **Create a Compute Instance**:

   ```bash
   gcloud compute instances create whisper-instance \
     --machine-type=n1-standard-8 \
     --accelerator=type=nvidia-tesla-t4,count=1 \
     --zone=us-central1-a \
     --image-family=ubuntu-2004-lts \
     --image-project=ubuntu-os-cloud \
     --boot-disk-size=100GB \
     --maintenance-policy=TERMINATE
   ```

4. **Install NVIDIA Drivers and CUDA**:

   ```bash
   # SSH into the instance
   gcloud compute ssh whisper-instance --zone=us-central1-a

   # Install NVIDIA drivers
   curl -fsSL https://nvidia.github.io/nvidia-docker/gpgkey | sudo gpg --dearmor -o /usr/share/keyrings/nvidia-docker-keyring.gpg
   curl -fsSL https://nvidia.github.io/nvidia-docker/ubuntu20.04/nvidia-docker.list | sudo tee /etc/apt/sources.list.d/nvidia-docker.list
   sudo apt-get update
   sudo apt-get install -y nvidia-driver-535 nvidia-docker2
   sudo systemctl restart docker

   # Install CUDA
   wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-ubuntu2004.pin
   sudo mv cuda-ubuntu2004.pin /etc/apt/preferences.d/cuda-repository-pin-600
   wget https://developer.download.nvidia.com/compute/cuda/12.3.2/local_installers/cuda-repo-ubuntu2004-12-3-local_12.3.2-545.23.08-1_amd64.deb
   sudo dpkg -i cuda-repo-ubuntu2004-12-3-local_12.3.2-545.23.08-1_amd64.deb
   sudo cp /var/cuda-repo-ubuntu2004-12-3-local/7fa2af80.pub /etc/apt/trusted.gpg.d/
   sudo apt-get update
   sudo apt-get install -y cuda-12.3
   ```

5. **Set Environment Variables**:

   ```bash
   export USE_CLOUD=true
   export CLOUD_PROVIDER=gcp
   ```

### Amazon Web Services (AWS)

1. **Create an AWS Account and Configure AWS CLI**:

   ```bash
   aws configure
   # Enter your AWS Access Key ID, Secret Access Key, and default region
   ```

2. **Create an EC2 Instance**:

   ```bash
   # Create a key pair
   aws ec2 create-key-pair --key-name whisper-key --query 'KeyMaterial' --output text > whisper-key.pem
   chmod 400 whisper-key.pem

   # Launch an instance (using Deep Learning AMI)
   aws ec2 run-instances \
     --image-id ami-0c7217cdde317cfec \
     --instance-type g4dn.xlarge \
     --key-name whisper-key \
     --security-group-ids sg-xxxxxxxx \
     --subnet-id subnet-xxxxxxxx \
     --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=whisper-instance}]'
   ```

3. **Connect to the Instance**:

   ```bash
   ssh -i whisper-key.pem ubuntu@[INSTANCE_PUBLIC_IP]
   ```

4. **Install Dependencies**:

   ```bash
   # The Deep Learning AMI comes with CUDA and NVIDIA drivers pre-installed
   sudo apt-get update
   sudo apt-get install -y ffmpeg
   pip install faster-whisper
   ```

5. **Set Environment Variables**:

   ```bash
   export USE_CLOUD=true
   export CLOUD_PROVIDER=aws
   ```

### Cost Management Tips

- **GCP**:
  - Use preemptible instances for cost savings
  - Set up budget alerts
  - Use smaller GPU types (T4) for most transcriptions
  - Stop instances when not in use

- **AWS**:
  - Use spot instances for cost savings
  - Set up AWS Budgets
  - Choose appropriate instance types based on workload
  - Terminate instances when not in use

### Security Considerations

- Use IAM roles and service accounts
- Enable encryption at rest
- Use VPC security groups
- Regularly update security patches
- Follow the principle of least privilege