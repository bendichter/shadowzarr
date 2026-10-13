"""Generate and host shadowzarr references for every HDF5 NWB file on DANDI.

    python scan.py [--workers N] [--timeout SECONDS] [--dandisets 000003,000004] [--limit N] [--retry-failed]

Lists the .nwb assets of every public dandiset, in its draft and each
published version, and keeps one entry per blob, since a blob is the same file
wherever it appears. Worker processes generate each blob's references and
upload them to dandi/blobs/<blob_id>/ in the bucket (process_blob.py). A worker
handles one blob after another, so the libraries are imported once, and it is
replaced when a blob runs over its time limit, when it crashes, and after
RECYCLE_AFTER blobs.

Progress is kept in scan.sqlite next to this script (or SHADOWZARR_SCAN_DB):
a blob that is done is not processed again, so a later run picks up only new
blobs, and one that failed is retried only with --retry-failed or once the
shadowzarr version changes.
"""

from __future__ import annotations

import argparse
import multiprocessing
import multiprocessing.connection
import os
import sqlite3
import sys
import time
from collections import deque
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


RECYCLE_AFTER = 200  # blobs a worker handles before it is replaced, so that memory cannot creep up


def _worker(conn) -> None:
    """Process blob IDs received on conn, one at a time, sending back each result."""
    # Progress bars and warnings would fill the log; a result carries any error
    os.environ["TQDM_DISABLE"] = "1"
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, 1)
    os.dup2(devnull, 2)
    sys.path.insert(0, str(HERE))
    import process_blob

    while (blob_id := conn.recv()) is not None:
        try:
            result = process_blob.process(blob_id)
        except Exception as error:  # process() catches its own errors; this is a last resort
            result = {"blob_id": blob_id, "error": f"crash: {type(error).__name__}: {error}"[:500]}
        conn.send(result)


class _Slot:
    """A worker process and the blob it is working on."""

    def __init__(self, context) -> None:
        self.conn, child = context.Pipe()
        self.process = context.Process(target=_worker, args=(child,), daemon=True)
        self.process.start()
        child.close()
        self.blob: str | None = None
        self.since = 0.0
        self.done = 0

    def assign(self, blob_id: str) -> None:
        self.blob, self.since = blob_id, time.monotonic()
        self.conn.send(blob_id)

    def stop(self, kill: bool = False) -> None:
        if kill:
            self.process.kill()
        else:
            try:
                self.conn.send(None)
            except (BrokenPipeError, OSError):
                pass
        self.process.join(10)
        if self.process.is_alive():
            self.process.kill()
            self.process.join()
        self.conn.close()


def run_all(todo: list[str], workers: int, timeout: int, on_result) -> None:
    """Process every blob in todo with long-lived workers, calling on_result with each result."""
    context = multiprocessing.get_context("spawn")
    pending = deque(todo)
    slots = [_Slot(context) for _ in range(min(workers, len(pending)))]
    while pending or any(slot.blob for slot in slots):
        for slot in slots:
            if slot.blob is None and pending:
                slot.assign(pending.popleft())
        busy = {slot.conn: slot for slot in slots if slot.blob}
        for conn in multiprocessing.connection.wait(list(busy), timeout=5):
            slot = busy[conn]
            try:
                result = conn.recv()
            except (EOFError, OSError):
                # The worker died, for instance killed for using too much memory
                result = {"blob_id": slot.blob, "error": f"crash: worker exited with code {slot.process.exitcode}"}
            on_result(result)
            slot.blob = None
            slot.done += 1
        now = time.monotonic()
        for i, slot in enumerate(slots):
            if slot.blob and now - slot.since > timeout:
                on_result({"blob_id": slot.blob, "timeout": timeout})
                slot.stop(kill=True)
                slots[i] = _Slot(context)
            elif slot.blob is None and (slot.done >= RECYCLE_AFTER or not slot.process.is_alive()):
                slot.stop()
                slots[i] = _Slot(context)
    for slot in slots:
        slot.stop()


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

    def on_result(result: dict) -> None:
        status = record(db, result)
        counts[status] = counts.get(status, 0) + 1
        if status == "done":
            detail = f"{result['generate_s']} s, {result['n_objects']} objects"
        else:
            detail = (result.get("error") or f"timed out after {result.get('timeout')} s")[:120]
        print(f"{status:7} {result['blob_id']} {detail}", flush=True)

    run_all(todo, args.workers, args.timeout, on_result)
    print("SCAN_DONE", counts, flush=True)


if __name__ == "__main__":
    main()
