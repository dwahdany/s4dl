import asyncio
import logging
import ssl
from pathlib import Path
from typing import List
from datetime import datetime

import aiofiles
import aiohttp
from tqdm.asyncio import tqdm
from . import presign
import typer

logger = logging.getLogger(__name__)

app = typer.Typer()

def setup_logging(debug: bool = False):
    """Configure logging level based on debug flag"""
    level = logging.DEBUG if debug else logging.WARNING
    logging.basicConfig(level=level)
    logger.setLevel(level)

@app.callback()
def callback(
    debug: bool = typer.Option(False, "--debug", help="Enable debug logging"),
):
    """S4DL - Simple S3 Download Utility"""
    setup_logging(debug)


async def download_file(
    session: aiohttp.ClientSession, url: str, dest_path: Path
) -> None:
    """Download a single file from a pre-signed URL."""
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")

    try:
        if dest_path.exists():
            logger.info(f"Skipping existing file: {dest_path}")
            return

        if tmp_path.exists():
            tmp_path.unlink()

        async with session.get(url.strip()) as response:
            if response.status != 200:
                logger.error(
                    f"Failed to download {url}: Status {response.status}"
                )
                return

            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(tmp_path, mode="wb") as f:
                async for chunk in response.content.iter_chunked(8192):
                    await f.write(chunk)

        tmp_path.rename(dest_path)

    except Exception as e:
        logger.error(f"Error downloading {url}: {str(e)}")
        if tmp_path.exists():
            tmp_path.unlink()


async def download_files(urls_file: str, output_dir: str = "./", max_concurrent: int = 10) -> None:
    """Download files from URLs listed in a text file.
    
    Args:
        urls_file: Path to file containing URLs to download
        output_dir: Directory to save downloaded files
        max_concurrent: Maximum number of concurrent downloads
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    with open(urls_file, "r") as f:
        urls = [line.strip() for line in f if line.strip()]

    if not urls:
        logger.warning("No URLs found in the input file")
        return

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Create semaphore to limit concurrent downloads
    semaphore = asyncio.Semaphore(max_concurrent)
    tasks: List[asyncio.Task] = []
    
    async def bounded_download(session: aiohttp.ClientSession, url: str, dest_path: Path) -> None:
        async with semaphore:
            await download_file(session, url, dest_path)

    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=max_concurrent)
    async with aiohttp.ClientSession(connector=connector) as session:
        for url in urls:
            filename = url.split("?")[0].split("/")[-1]
            dest_path = output_path / filename
            task = asyncio.create_task(bounded_download(session, url, dest_path))
            tasks.append(task)

        for task in tqdm.as_completed(
            tasks, total=len(tasks), desc="Downloading files"
        ):
            await task


@app.command()
def presign(
    s3_path: str = typer.Argument(..., help="Full S3 path (e.g., s3://bucket/prefix/)"),
    output: str = typer.Option(
        None,
        "-o",
        "--output",
        help="Output file (default: presigned_urls_<timestamp>.txt)",
    ),
    expiry: int = typer.Option(
        604800,
        "-e",
        "--expiry",
        help="URL expiry time in seconds (default: 604800)",
    ),
):
    """Generate presigned URLs for S3 objects with a specific prefix"""
    try:
        if output is None:
            output = f"presigned_urls_{datetime.now():%Y%m%d_%H%M%S}.txt"

        urls = asyncio.run(presign.presign_paths(s3_path, expiry))
        
        if not urls:
            typer.echo("No objects found")
            raise typer.Exit(0)
            
        with open(output, "w") as f:
            for url in urls:
                f.write(f"{url}\n")
                
        typer.echo(f"Presigned URLs have been written to {output}")
        typer.echo(f"URLs will expire in {expiry} seconds")
        
    except Exception as e:
        typer.echo(f"Error: {str(e)}", err=True)
        raise typer.Exit(1)


@app.command()
def download(
    urls_file: str = typer.Argument(..., help="Text file containing one URL per line"),
    output_dir: str = typer.Argument(
        "./",
        help="Directory to save downloaded files (default: current directory)",
    ),
    max_concurrent: int = typer.Option(
        10,
        "--max-concurrent",
        "-m",
        help="Maximum number of concurrent downloads (default: 10)",
    ),
):
    """Download files from pre-signed S3 URLs"""
    try:
        asyncio.run(download_files(urls_file, output_dir, max_concurrent))
    except Exception as e:
        typer.echo(f"Error: {str(e)}", err=True)
        raise typer.Exit(1)


def main():
    app()


if __name__ == "__main__":
    main()
