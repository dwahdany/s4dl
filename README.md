# Simple Storage Service pre-Signed Downloader (s4dl)

## Disclaimer

This script and the following documentation are mostly generated by Claude 3.5 Sonnet using Cursor IDE. SSL certificates are not verified during download.

## Description

A simple async downloader for S3 pre-signed URLs. Efficiently downloads files in parallel with proper error handling and progress tracking.

## Features

- Fast parallel downloads using asyncio
- Progress bar with ETA
- Handles SSL certificate issues
- Safe downloads using temporary files
- Skips existing files
- Simple command-line interface

## Installation

```bash
pip install s4dl
```

## Usage

Basic usage:

```bash
s4dl urls.txt [output_dir]
```

Where:
- `urls.txt`: File containing one pre-signed URL per line
- `output_dir`: Optional destination directory (default: current directory)

Example:

```bash
s4dl urls.txt ./downloads
```

## Error Handling

- Skips files that already exist
- Continues downloading on errors
- Uses temporary files to prevent partial downloads
- Logs errors for failed downloads

## Requirements

- Python ≥3.7
- aiohttp
- aiofiles
- tqdm
