# DANDI scan

Generates shadowzarr references for every HDF5 NWB file on [DANDI](https://dandiarchive.org) and uploads them to the `shadowzarr` R2 bucket, where they are served at `https://<domain>/dandi/blobs/<blob_id>/`, with `refs.json` at the top.

A blob is the content of one file, and appears in every dandiset version that holds that file. So references are made once per blob, and a client finds them from the blob ID in the asset's S3 URL, `https://dandiarchive.s3.amazonaws.com/blobs/<xxx>/<yyy>/<blob_id>`.

## Running

```bash
pip install -e ".[dev]" boto3
export CLOUDFLARE_ACCOUNT_ID=...
# R2's S3 API (bucket-scoped "Object Read & Write" token), for parallel uploads
export R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=...
python scripts/dandi_scan/scan.py --workers 8
```

Without `R2_ACCESS_KEY_ID`, uploads go through the Cloudflare REST API with `CLOUDFLARE_API_TOKEN`, which allows about 4 requests a second and suits small runs.

Useful options:

- `--dandisets 000003,000004` lists and processes only these.
- `--limit N` processes at most N blobs, smallest first.
- `--timeout SECONDS` is the time allowed per blob, 3600 by default. Most files take under 2 minutes, but a file with tens of thousands of objects can take over half an hour.
- `--retry-failed` retries blobs that failed or timed out.
- `--skip-listing` processes what is already listed without asking DANDI again.

## Progress

`scan.sqlite`, next to the script or at `SHADOWZARR_SCAN_DB`, records every blob and asset. A run lists DANDI again, adds new blobs as pending, and processes only those, so running it nightly keeps the bucket current. A blob that failed or timed out is retried with `--retry-failed`, or after the shadowzarr version changes.

```sql
SELECT status, COUNT(*) FROM blobs GROUP BY status;
SELECT a.dandiset, a.path, b.error FROM blobs b JOIN assets a USING (blob_id) WHERE b.status != 'done';
```
