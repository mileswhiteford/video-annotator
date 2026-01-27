# Video Annotator – Batch Ingest + Index Pipeline (Box → Speech → Segments → Embeddings → Search)

This repo contains scripts to:

1. Enumerate `.m4a` files from a **Box shared folder** and generate a manifest (`videos.jsonl`)
2. Run each file through the Azure Functions pipeline:

   * Submit batch transcription (`TranscribeHttp`)
   * Write 30s segments JSON to Blob (`segments/<video_id>.json`)
   * Embed + index segments into Azure AI Search (`EmbedAndIndex`)
3. Query indexed segments (`SearchSegments`)

## Prerequisites

* Python 3.11+ recommended
* Azure Functions already deployed (or runnable locally)
* Box shared folder link that contains `.m4a` files
* Working Box API token:

  * EITHER a **Developer Token** (quick + expires)
  * OR OAuth tokens (`BOX_ACCESS_TOKEN` + `BOX_REFRESH_TOKEN` + client id/secret)

## Repo Layout (expected)

```
transcribe/
  scripts/
    box_auth.py
    box_shared_folder_manifest.py
  import_videos.py
  videos.jsonl            # generated
  requirements.txt
  .env                    # you create this (NOT committed)
```

## 1) Create a virtual environment + install deps

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If you don’t have a `requirements.txt` for scripts yet, minimally you’ll need:

```txt
requests
python-dotenv
```

(Box listing can be done via raw REST calls, so you may not need `boxsdk`.)

## 2) Create your `.env`

Create a `.env` file in the project root (same directory you run scripts from):

### Box settings

Set the shared folder URL:

```env
BOX_SHARED_FOLDER_URL=https://tulane.box.com/s/<shared-folder-token>
```

Choose **one** auth method:

#### Option A (fastest): Developer Token

```env
BOX_TOKEN=<your_box_developer_token>
```

#### Option B (durable): OAuth refresh tokens

```env
BOX_CLIENT_ID=<your_box_client_id>
BOX_CLIENT_SECRET=<your_box_client_secret>
BOX_ACCESS_TOKEN=<your_box_access_token>
BOX_REFRESH_TOKEN=<your_box_refresh_token>
```

> Note: refresh tokens can become invalid if rotated/revoked. If you see `invalid_grant`, re-run your OAuth login flow and update `.env`.

### Azure Function endpoints

These should be the **full function URLs**, including `?code=...`:

```env
TRANSCRIBE_URL=https://<yourapp>.azurewebsites.net/api/TranscribeHttp?code=...
EMBED_INDEX_URL=https://<yourapp>.azurewebsites.net/api/EmbedAndIndex?code=...
SEARCH_FN_URL=https://<yourapp>.azurewebsites.net/api/SearchSegments?code=...
```

### Optional runner settings

```env
SEGMENTS_CONTAINER=segments
POLL_SECONDS=15
MAX_ACTIVE=10
```

## 3) Generate the manifest from Box (`videos.jsonl`)

This script reads your Box shared folder and outputs `videos.jsonl` with one line per `.m4a`:

```jsonl
{"video_id":"vid_123","media_url":"https://..."}
{"video_id":"vid_456","media_url":"https://..."}
```

Run:

```bash
python scripts/box_shared_folder_manifest.py
```

### Sanity check one URL

Pick one entry from `videos.jsonl` and confirm it downloads:

```bash
python - <<'PY'
import json
with open("videos.jsonl","r") as f:
    print(json.loads(next(f)))
PY

curl -I -L "<media_url>"
```

You want `200 OK` (not HTML/404). If this fails, Speech won’t be able to fetch it either.

## 4) Run the pipeline import (`import_videos.py`)

This script:

* reads `videos.jsonl`
* submits transcription jobs via `TranscribeHttp`
* polls until each completes
* indexes segments via `EmbedAndIndex`

Run:

```bash
python import_videos.py
```

### Progress + resume

The importer writes a `pipeline_state.json` file as it runs. If the script stops, you can rerun it and it will resume from the saved state.

## 5) Verify search

Once a few videos are indexed, query your `SearchSegments` function:

```bash
curl -X POST "$SEARCH_FN_URL" \
  -H "Content-Type: application/json" \
  -d '{"q":"measles","mode":"hybrid","top":5,"k":40}'
```

If you get results, your segments are searchable.

## Troubleshooting

### Box links return 404

* Ensure the manifest script is producing **working `media_url`s**
* Validate with `curl -I -L "<media_url>"` (must end in 200)
* If a shared link works in browser but not via curl, it may rely on cookies/redirects. The manifest script should output a direct download URL.

### Importer submits jobs but never completes

* Speech batch jobs can take time; check your `TranscribeHttp` function logs / Application Insights
* Consider increasing `POLL_SECONDS` to reduce throttling
* Reduce `MAX_ACTIVE` if you see rate-limit behavior

### `EmbedAndIndex` fails with invalid document key

* Azure AI Search keys can’t contain `:` etc. If you use segment keys like `vid:0001`, replace `:` with `_` or `-`.

## Security notes

* **Do not commit** `.env`, `pipeline_state.json`, or any token/key material.
* Prefer query keys (read-only) for Search in front-end scenarios.
* For long-term automation, use a Box app auth method approved by your org (not developer token).

