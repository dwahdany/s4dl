import asyncio
import json
import subprocess
import sys
from typing import List, Optional, Tuple
from urllib.parse import urlparse

from tqdm.asyncio import tqdm


def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    """Parse an S3 path into bucket and prefix"""
    parsed = urlparse(s3_path)
    if parsed.scheme != "s3":
        raise ValueError("Path must start with s3://")
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    return bucket, prefix


def list_objects(bucket: str, prefix: str) -> List[str]:
    """List all objects in the bucket with given prefix"""
    try:
        cmd = ["aws", "s3api", "list-objects-v2", "--bucket", bucket, "--prefix", prefix]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        objects = json.loads(result.stdout)
        return [obj["Key"] for obj in objects.get("Contents", [])]
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error listing objects: {e.stderr}")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Error parsing AWS response: {e}")


async def generate_presigned_url(
    bucket: str, object_key: str, expiry: int = 3600
) -> Optional[Tuple[str, str]]:
    """Generate a presigned URL for the given object"""
    try:
        cmd = [
            "aws",
            "s3",
            "presign",
            f"s3://{bucket}/{object_key}",
            "--expires-in",
            str(expiry),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            raise RuntimeError(f"Error generating presigned URL: {stderr.decode()}")

        return object_key, stdout.decode().strip()
    except Exception as e:
        raise RuntimeError(f"Error generating presigned URL: {str(e)}")


async def generate_all_urls(
    bucket: str, object_keys: List[str], expiry: int
) -> List[Tuple[str, str]]:
    """Generate presigned URLs for all objects concurrently with a limit on parallel tasks"""
    semaphore = asyncio.Semaphore(50)

    async def generate_with_semaphore(key: str) -> Optional[Tuple[str, str]]:
        async with semaphore:
            return await generate_presigned_url(bucket, key, expiry)

    tasks = [generate_with_semaphore(key) for key in object_keys]
    results = await tqdm.gather(*tasks, desc="Generating URLs", total=len(tasks))
    return [r for r in results if r is not None]


async def presign_paths(s3_path: str, expiry: int = 604800) -> List[str]:
    """Main function to generate presigned URLs for all objects under an S3 path"""
    bucket, prefix = parse_s3_path(s3_path)
    object_keys = list_objects(bucket, prefix)
    
    if not object_keys:
        return []
    
    results = await generate_all_urls(bucket, object_keys, expiry)
    return [url for _, url in results] 