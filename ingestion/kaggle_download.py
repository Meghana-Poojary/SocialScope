"""Download a Kaggle dataset with the official Kaggle API (no extra packages).

Credentials (the same ones the `kaggle` CLI uses), first match wins:
  1. environment variables KAGGLE_USERNAME and KAGGLE_KEY
  2. ~/.kaggle/kaggle.json   ({"username": "...", "key": "..."})
Create them at kaggle.com -> Settings -> API -> "Create New Token".

The download resumes and retries, so it survives unreliable connections.

    python ingestion/kaggle_download.py thoughtvector/customer-support-on-twitter
    python ingestion/kaggle_download.py <owner>/<dataset> --out data/kaggle/<name>
"""
from __future__ import annotations
import argparse
import base64
import http.client
import json
import os
import sys
import time
import urllib.parse
import zipfile
from pathlib import Path

API = "https://www.kaggle.com/api/v1/datasets/download/{slug}"


def credentials() -> tuple[str, str]:
    user, key = os.getenv("KAGGLE_USERNAME"), os.getenv("KAGGLE_KEY")
    if user and key:
        return user, key
    path = Path(os.getenv("KAGGLE_CONFIG_DIR", Path.home() / ".kaggle")) / "kaggle.json"
    if path.exists():
        cfg = json.loads(path.read_text())
        return cfg["username"], cfg["key"]
    sys.exit("No Kaggle credentials: set KAGGLE_USERNAME/KAGGLE_KEY or create ~/.kaggle/kaggle.json "
             "(kaggle.com -> Settings -> API -> Create New Token), or download the CSV manually "
             "into data/kaggle/.")


def _request(url: str, headers: dict, method: str = "GET"):
    u = urllib.parse.urlsplit(url)
    conn = http.client.HTTPSConnection(u.netloc, timeout=120)
    conn.request(method, u.path + (f"?{u.query}" if u.query else ""), headers=headers)
    return conn, conn.getresponse()


def resolve(slug: str, user: str, key: str) -> str:
    """Kaggle answers with a redirect to a signed storage URL. Follow it ourselves so
    the Kaggle auth header is NOT forwarded to the storage host (it would reject it)."""
    auth = base64.b64encode(f"{user}:{key}".encode()).decode()
    conn, resp = _request(API.format(slug=slug), {"Authorization": f"Basic {auth}", "User-Agent": "socialscope"})
    try:
        if resp.status in (301, 302, 303, 307, 308):
            return resp.getheader("Location")
        if resp.status == 401:
            sys.exit("Kaggle rejected the credentials (401). Re-create your API token.")
        if resp.status == 403:
            sys.exit("Kaggle returned 403: open the dataset page once in a browser and accept its terms.")
        if resp.status == 404:
            sys.exit(f"Dataset '{slug}' not found on Kaggle.")
        sys.exit(f"Unexpected Kaggle response {resp.status}: {resp.read(300)!r}")
    finally:
        conn.close()


def download(url: str, dest: Path, attempts: int = 30):
    """Resumable download (HTTP Range) with retries."""
    part = dest.with_suffix(dest.suffix + ".part")
    for i in range(1, attempts + 1):
        have = part.stat().st_size if part.exists() else 0
        headers = {"User-Agent": "socialscope"}
        if have:
            headers["Range"] = f"bytes={have}-"
        try:
            conn, resp = _request(url, headers)
            if resp.status == 416:              # already complete
                conn.close()
                break
            if resp.status not in (200, 206):
                raise OSError(f"HTTP {resp.status}")
            if resp.status == 200 and have:     # server ignored Range: start over
                have = 0
                part.unlink()
            total = int(resp.getheader("Content-Length") or 0) + have
            with open(part, "ab") as f:
                while True:
                    chunk = resp.read(1 << 20)
                    if not chunk:
                        break
                    f.write(chunk)
                    have += len(chunk)
                    if total:
                        print(f"\r  {have / 1e6:,.1f} / {total / 1e6:,.1f} MB", end="", flush=True)
            conn.close()
            if not total or have >= total:
                print()
                break
        except (OSError, http.client.HTTPException) as e:
            print(f"\n  attempt {i}/{attempts} interrupted ({e}); resuming in {min(60, 5 * i)}s")
            time.sleep(min(60, 5 * i))
    else:
        sys.exit("download kept failing - check the connection and run again (it resumes)")
    part.replace(dest)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("slug", help="<owner>/<dataset>, e.g. thoughtvector/customer-support-on-twitter")
    ap.add_argument("--out", help="folder to extract into (default data/kaggle/<dataset>)")
    a = ap.parse_args()

    out = Path(a.out or Path(__file__).resolve().parents[1] / "data" / "kaggle" / a.slug.split("/")[-1])
    out.mkdir(parents=True, exist_ok=True)
    zpath = out / "dataset.zip"
    if not zpath.exists():
        user, key = credentials()
        print(f"downloading {a.slug} ...")
        download(resolve(a.slug, user, key), zpath)
    print(f"extracting {zpath} ...")
    with zipfile.ZipFile(zpath) as z:
        z.extractall(out)
    for p in sorted(out.rglob("*.csv")):
        print(f"  {p}  ({p.stat().st_size / 1e6:,.1f} MB)")


if __name__ == "__main__":
    main()
