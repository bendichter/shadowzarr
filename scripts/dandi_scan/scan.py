"""Generate and host shadowzarr references for every HDF5 NWB file on DANDI.

    python scan.py [--workers N] [--timeout SECONDS] [--dandisets 000003,000004] [--limit N] [--retry-failed]

Lists the .nwb assets of every public dandiset, in its draft and each
published version, and keeps one entry per blob, since a blob is the same file
wherever it appears. Each blob is processed by process_blob.py in its own
process, which uploads the references to dandi/blobs/<blob_id>/ in the bucket.

Progress is kept in scan.sqlite next to this script (or SHADOWZARR_SCAN_DB):
a blob that is done is not processed again, so a later run picks up only new
blobs, and one that failed is retried only with --retry-failed or once the
shadowzarr version changes.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sqlite3
import subprocess
import sys
import time
from importlib.metadata import version
from pathlib import Path

import requests

API = "https://api.dandiarchive.org/api"
HERE = Path(__file__).resolve().parent
DB = Path(os.environ.get("SHADOWZARR_SCAN_DB", HERE / "scan.sqlite"))
SHADOWZARR_VERSION = version("shadowzarr")

SCHEMA = """
CREATE TABLE IF NOT EXISTS blobs (
    blob_id TEXT PRIMARY KEY,
    size INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',  -- pending, done, failed, timeout
    shadowzarr_version TEXT,
    generate_s REAL,
    upload_s REAL,
    n_objects INTEGER,
    refs_bytes INTEGER,
    error TEXT,
    attempted_at TEXT
);
CREATE TABLE IF NOT EXISTS assets (
    dandiset TEXT,
    version TEXT,
    path TEXT,
    asset_id TEXT,
    blob_id TEXT,
    PRIMARY KEY (dandiset, version, asset_id)
);
CREATE INDEX IF NOT EXISTS assets_blob ON assets (blob_id);
"""


def get(url: str, params: dict | None = None) -> dict:
    for attempt in range(8):
        try:
            response = requests.get(url, params=params, timeout=60)
            if response.status_code < 500 and response.status_code != 429:
                response.raise_for_status()
                return response.json()
        except requests.ConnectionError:
            pass
        time.sleep(min(2**attempt, 60))
    response.raise_for_status()
    return response.json()


def paged(url: str, params: dict | None = None):
    page = get(url, params)
    while True:
        yield from page["results"]
        if not page.get("next"):
            return
        page = get(page["next"])


def dandisets(only: list[str] | None):
    if only:
        yield from only
        return
    params = {"page_size": 200, "ordering": "id", "embargoed": "false", "empty": "false", "draft": "true"}
    for dandiset in paged(f"{API}/dandisets/", params):
        yield dandiset["identifier"]


def list_assets(db: sqlite3.Connection, only: list[str] | None) -> None:
    """Record the .nwb assets of every version of every dandiset, and a pending row for each new blob."""
    for dandiset in dandisets(only):
        versions = [v["version"] for v in paged(f"{API}/dandisets/{dandiset}/versions/", {"page_size": 100})]
        for v in versions:
            params = {"glob": "*.nwb", "page_size": 1000, "metadata": "false", "order": "path"}
            rows = [
                (dandiset, v, a["path"], a["asset_id"], a["blob"], a["size"])
                for a in paged(f"{API}/dandisets/{dandiset}/versions/{v}/assets/", params)
                if a.get("blob") and a["path"].endswith(".nwb")
            ]
            db.executemany(
                "INSERT OR IGNORE INTO assets (dandiset, version, path, asset_id, blob_id) VALUES (?, ?, ?, ?, ?)",
                [r[:5] for r in rows],
            )
            db.executemany("INSERT OR IGNORE INTO blobs (blob_id, size) VALUES (?, ?)", [(r[4], r[5]) for r in rows])
            db.commit()
        n = db.execute("SELECT COUNT(*) FROM blobs").fetchone()[0]
        print(f"listed {dandiset} ({len(versions)} versions); {n} blobs so far", flush=True)


def run_one(blob_id: str, timeout: int) -> dict:
    try:
        proc = subprocess.run(
            [sys.executable, str(HERE / "process_blob.py"), blob_id],
            capture_output=True, text=True, timeout=timeout,
        )
        lines = [line for line in proc.stdout.splitlines() if line.startswith("RESULT ")]
        if lines:
            return json.loads(lines[-1][len("RESULT "):])
        return {"blob_id": blob_id, "error": f"crash: {proc.stderr[-500:]}"}
    except subprocess.TimeoutExpired:
        return {"blob_id": blob_id, "timeout": timeout}


def record(db: sqlite3.Connection, result: dict) -> str:
    status = "timeout" if "timeout" in result else ("failed" if "error" in result else "done")
    db.execute(
        """UPDATE blobs SET status=?, shadowzarr_version=?, generate_s=?, upload_s=?, n_objects=?, refs_bytes=?,
           error=?, attempted_at=datetime('now') WHERE blob_id=?""",
        (status, SHADOWZARR_VERSION, result.get("generate_s"), result.get("upload_s"), result.get("n_objects"),
         result.get("refs_bytes"), result.get("error") or (f"timed out after {result['timeout']} s" if status == "timeout" else None),
         result["blob_id"]),
    )
    db.commit()
    return status


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=3600, help="seconds allowed per blob")
    parser.add_argument("--dandisets", help="comma-separated dandiset IDs, instead of all of them")
    parser.add_argument("--limit", type=int, help="process at most this many blobs")
    parser.add_argument("--retry-failed", action="store_true", help="also retry blobs that failed or timed out")
    parser.add_argument("--skip-listing", action="store_true", help="process what is already listed")
    args = parser.parse_args()

    db = sqlite3.connect(DB)
    db.executescript(SCHEMA)
    only = args.dandisets.split(",") if args.dandisets else None
    if not args.skip_listing:
        list_assets(db, only)

    retry = "OR status IN ('failed', 'timeout')" if args.retry_failed else ""
    query = f"""SELECT blob_id FROM blobs
                WHERE status = 'pending' {retry}
                   OR (status != 'done' AND shadowzarr_version IS NOT ? )
                ORDER BY size"""
    todo = [row[0] for row in db.execute(query, (SHADOWZARR_VERSION,))]
    if only:
        wanted = {row[0] for row in db.execute(
            f"SELECT blob_id FROM assets WHERE dandiset IN ({','.join('?' * len(only))})", only)}
        todo = [b for b in todo if b in wanted]
    if args.limit:
        todo = todo[: args.limit]
    print(f"processing {len(todo)} blobs with {args.workers} workers, shadowzarr {SHADOWZARR_VERSION}", flush=True)

    counts: dict[str, int] = {}
    with concurrent.futures.ThreadPoolExecutor(args.workers) as pool:
        for result in pool.map(lambda b: run_one(b, args.timeout), todo):
            status = record(db, result)
            counts[status] = counts.get(status, 0) + 1
            detail = result.get("error", "")[:120] if status != "done" else f"{result['generate_s']} s, {result['n_objects']} objects"
            print(f"{status:7} {result['blob_id']} {detail}", flush=True)
    print("SCAN_DONE", counts, flush=True)


if __name__ == "__main__":
    main()
