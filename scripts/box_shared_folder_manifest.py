"""
scripts/box_shared_folder_manifest.py - Generate Video Manifest from Box

This script enumerates .m4a video files from a Box shared folder and generates
a manifest file (videos.jsonl) that lists all videos with their IDs and media URLs.
It creates open shared links for each file so Azure Speech Service can access them.

Architecture Role:
- Pre-processing step before video ingestion
- Generates videos.jsonl input file for import_videos.py
- Handles Box API authentication and folder traversal
- Creates publicly accessible download URLs for Speech Service

Usage:
  python scripts/box_shared_folder_manifest.py

Output:
  - videos.jsonl: One JSON object per line with video_id and media_url

Configuration (via .env):
  - BOX_SHARED_FOLDER_URL: Box shared folder link
  - BOX_TOKEN or BOX_ACCESS_TOKEN/BOX_REFRESH_TOKEN: Box authentication
  - OUT_PATH: Output file path (default: videos.jsonl)
  - RECURSIVE: Whether to traverse subfolders (default: 1)
"""

import json
import os
import requests
from typing import Dict, Any, List, Optional

from box_auth import get_access_token

BOX_API = "https://api.box.com/2.0"

SHARED_FOLDER_URL = os.environ["BOX_SHARED_FOLDER_URL"]  # https://...box.com/s/<id>
print("BOX_SHARED_FOLDER_URL =", SHARED_FOLDER_URL)
OUT_PATH = os.environ.get("OUT_PATH", "videos.jsonl")
RECURSIVE = os.environ.get("RECURSIVE", "1") == "1"


def shared_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "BoxApi": f"shared_link={SHARED_FOLDER_URL}",
        "Content-Type": "application/json",
    }


def auth_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def resolve_shared_folder(token: str) -> Dict[str, Any]:
    url = f"{BOX_API}/shared_items"
    h = shared_headers(token)
    r = requests.get(url, headers=h, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code} {r.text} (headers sent: {h.get('BoxApi')})")
    return r.json()



def list_folder_items(token: str, folder_id: str, limit: int = 1000, offset: int = 0) -> Dict[str, Any]:
    url = f"{BOX_API}/folders/{folder_id}/items"
    params = {"limit": limit, "offset": offset}
    r = requests.get(url, headers=shared_headers(token), params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def ensure_open_shared_link_for_file(token: str, file_id: str) -> str:
    """
    Ensure file has an open shared link and return a direct-download URL.
    """
    url = f"{BOX_API}/files/{file_id}"
    payload = {"shared_link": {"access": "open"}}
    params = {"fields": "shared_link"}  # IMPORTANT: ask for shared_link back

    r = requests.put(url, headers=auth_headers(token), params=params, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()

    sl = data.get("shared_link") or {}
    # print("shared_link =", json.dumps(sl, indent=2))
    # Prefer direct static download URL
    dl = sl.get("download_url")
    if dl:
        return dl

    # Fallback: at least return the shared link (may require cookies)
    if sl.get("url"):
        return sl["url"]

    raise RuntimeError(f"No shared_link returned for file {file_id}: {data}")



def walk(token: str, folder_id: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    offset = 0
    limit = 1000
    while True:
        page = list_folder_items(token, folder_id, limit=limit, offset=offset)
        entries = page.get("entries", [])
        items.extend(entries)
        total = page.get("total_count", 0)
        offset += len(entries)
        if offset >= total or not entries:
            break
    return items


def main():
    token = get_access_token()

    shared_folder = resolve_shared_folder(token)
    if shared_folder.get("type") != "folder":
        raise RuntimeError(f"Shared link did not resolve to a folder: {shared_folder.get('type')}")
    root_id = shared_folder["id"]

    queue = [root_id]
    out_count = 0

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        while queue:
            fid = queue.pop(0)
            entries = walk(token, fid)

            for e in entries:
                et = e.get("type")
                name = (e.get("name") or "")
                lname = name.lower()

                if et == "folder" and RECURSIVE:
                    queue.append(e["id"])
                    continue

                if et != "file":
                    continue

                if not lname.endswith(".m4a"):
                    continue

                file_id = e["id"]
                video_id = f"vid_{file_id}"

                # Make a per-file open shared link (Speech can fetch without auth).
                # If your org disallows open links, this will fail — then you’ll need Blob staging.
                file_link = ensure_open_shared_link_for_file(token, file_id)

                # Encourage direct download behavior
                media_url = file_link # + ("?download=1" if "?" not in file_link else "&download=1")

                f.write(json.dumps({"video_id": video_id, "media_url": media_url}) + "\n")
                out_count += 1
                if out_count % 10 == 0:
                    print(f"Wrote {out_count} entries...")

    print(f"Done. Wrote {out_count} m4a entries to {OUT_PATH}")


if __name__ == "__main__":
    main()
