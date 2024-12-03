import asyncio
import logging
import ssl
from pathlib import Path

import aiofiles
import aiohttp
from tqdm.asyncio import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


async def download_files(urls_file: str, output_dir: str = "./") -> None:
    """Download files from URLs listed in a text file."""
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

    tasks = []
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    async with aiohttp.ClientSession(connector=connector) as session:
        for url in urls:
            filename = url.split("?")[0].split("/")[-1]
            dest_path = output_path / filename
            task = download_file(session, url, dest_path)
            tasks.append(task)

        for task in tqdm.as_completed(
            tasks, total=len(tasks), desc="Downloading files"
        ):
            await task


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Download files from pre-signed S3 URLs"
    )
    parser.add_argument(
        "urls_file",
        help="Text file containing one URL per line",
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        default="./",
        help="Directory to save downloaded files (default: current directory)",
    )

    args = parser.parse_args()
    asyncio.run(download_files(args.urls_file, args.output_dir))


if __name__ == "__main__":
    main()
