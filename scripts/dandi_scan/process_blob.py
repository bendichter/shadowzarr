"""Generate the references for one DANDI blob and upload them to the bucket.

    python process_blob.py BLOB_ID

Writes the references to a temporary directory, uploads every file under
dandi/blobs/<BLOB_ID>/, and prints one line, RESULT followed by JSON. scan.py
imports process() into long-lived worker processes, which it replaces when a
blob runs over its time limit.

Uploads use R2's S3 API when R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY and
CLOUDFLARE_ACCOUNT_ID are set, and the Cloudflare REST API with
CLOUDFLARE_API_TOKEN otherwise.
"""

from __future__ import annotations

import functools
import json
import os
import sys
import tempfile
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

from shadowzarr import generate_rfs, write_rfs

BUCKET = os.environ.get("SHADOWZARR_BUCKET", "shadowzarr")
PREFIX = "dandi/blobs"
CACHE_CONTROL = "public, max-age=86400"


def blob_url(blob_id: str) -> str:
    return f"https://dandiarchive.s3.amazonaws.com/blobs/{blob_id[:3]}/{blob_id[3:6]}/{blob_id}"


def _content_type(path: str) -> str:
    return "application/json" if path.endswith(".json") else "application/octet-stream"


class S3Uploader:
    """Uploads through R2's S3 API, in parallel."""

    def __init__(self) -> None:
        import boto3

        account = os.environ["CLOUDFLARE_ACCOUNT_ID"]
        self.client = boto3.client(
            "s3",
            endpoint_url=f"https://{account}.r2.cloudflarestorage.com",
            aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
            region_name="auto",
        )

    def upload(self, files: list[tuple[str, Path]]) -> None:
        def put(item: tuple[str, Path]) -> None:
            key, path = item
            self.client.put_object(
                Bucket=BUCKET, Key=key, Body=path.read_bytes(),
                ContentType=_content_type(key), CacheControl=CACHE_CONTROL,
            )

        with ThreadPoolExecutor(16) as pool:
            list(pool.map(put, files))


class RestUploader:
    """Uploads through the Cloudflare REST API, one object at a time, waiting when rate limited."""

    def __init__(self) -> None:
        account = os.environ["CLOUDFLARE_ACCOUNT_ID"]
        self.base = f"https://api.cloudflare.com/client/v4/accounts/{account}/r2/buckets/{BUCKET}/objects/"
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {os.environ['CLOUDFLARE_API_TOKEN']}"

    def upload(self, files: list[tuple[str, Path]]) -> None:
        for key, path in files:
            for attempt in range(8):
                response = self.session.put(
                    self.base + key, data=path.read_bytes(),
                    headers={"Content-Type": _content_type(key), "Cache-Control": CACHE_CONTROL},
                )
                if response.status_code == 429 or response.status_code >= 500:
                    time.sleep(min(2**attempt, 60))
                    continue
                response.raise_for_status()
                break
            else:
                response.raise_for_status()


@functools.cache
def uploader():
    """One uploader per process, reused for every blob it handles."""
    if os.environ.get("R2_ACCESS_KEY_ID") and os.environ.get("R2_SECRET_ACCESS_KEY"):
        return S3Uploader()
    return RestUploader()


def process(blob_id: str) -> dict:
    result: dict = {"blob_id": blob_id}
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "refs"
        t0 = time.perf_counter()
        try:
            write_rfs(generate_rfs(blob_url(blob_id)), str(out))
        except Exception as error:
            result["error"] = f"generate: {type(error).__name__}: {error}"[:500]
            result["traceback"] = traceback.format_exc()[-2000:]
            return result
        result["generate_s"] = round(time.perf_counter() - t0, 1)
        files = sorted(p for p in out.rglob("*") if p.is_file())
        result["n_objects"] = len(files)
        result["refs_bytes"] = sum(p.stat().st_size for p in files)
        keys = [(f"{PREFIX}/{blob_id}/{p.relative_to(out).as_posix()}", p) for p in files]
        t1 = time.perf_counter()
        try:
            uploader().upload(keys)
        except Exception as error:
            result["error"] = f"upload: {type(error).__name__}: {error}"[:500]
            return result
        result["upload_s"] = round(time.perf_counter() - t1, 1)
    return result


if __name__ == "__main__":
    print("RESULT " + json.dumps(process(sys.argv[1])), flush=True)
    os._exit(0)  # skip teardown of h5py and open HTTP sessions
