# VANTAGE-AI: Video ANnotation, TAGging & Exploration

VANTAGE-AI helps researchers and analysts turn long-form video collections into searchable, label-aware evidence.

The app supports an end-to-end workflow:

1. Upload videos from YouTube or bulk-import links from CSV.
2. Define custom annotation labels in plain language (for example, "expresses skepticism of vaccines").
3. Run LLM-based labeling over segment-level transcripts.
4. Search passages by keyword and/or label to find relevant moments quickly.

## Core capabilities

- **Video ingestion**: Bring in content from multiple source link types.
- **Segment-level indexing**: Transcripts are split and indexed for granular retrieval.
- **Custom label taxonomy**: Teams define their own analytic labels without code changes.
- **LLM-assisted annotation**: Labels are evaluated against video segments with confidence and rationale fields.
- **Evidence-first retrieval**: Search results include passage text, timestamps, source links, and predicted labels.

## Repository overview

This repository combines ingestion scripts, backend function integrations, and the Streamlit UI:

- `ui/ui_search.py`: main Streamlit app entrypoint with multipage navigation.
- `ui/pages/`: upload, video management, label management, evaluation, and diagnostics pages.
- `import_videos.py`: batch importer for transcription and indexing pipeline runs.
- `scripts/box_shared_folder_manifest.py`: helper for generating `videos.jsonl` manifests from Box folders.

## Quick start

### 1) Install dependencies

From the project root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Configure environment variables

Create environment files (for example `ui/.env` and/or root `.env`) with your function and service endpoints:

```env
SEARCH_FN_URL=https://<your-function-app>.azurewebsites.net/api/SearchSegments?code=...
MANAGE_LABELS_URL=https://<your-function-app>.azurewebsites.net/api/ManageLabels?code=...
EVAL_LABELS_URL=https://<your-function-app>.azurewebsites.net/api/EvalLabels?code=...
```

You may also need storage, speech, and search credentials depending on which pages and pipeline actions you use.

### 3) Run the UI

From `ui/`:

```bash
python -m streamlit run ui_search.py
```

Then open the local Streamlit URL (typically `http://localhost:8501`).

## Typical usage flow in the UI

1. **Upload** videos or bulk CSV links.
2. **Manage Videos** to confirm content is available and indexed.
3. **Label Management** to create or revise analytic labels.
4. **Label Evaluation** to run LLM annotations for chosen labels.
5. **Search** to retrieve passages with keyword + label filters.

## Pipeline notes

If you are running ingestion outside the UI:

- Generate manifests with `scripts/box_shared_folder_manifest.py`.
- Run the importer with `import_videos.py` to trigger transcription and indexing.
- Query `SearchSegments` to validate indexed retrieval.

## Security

- Never commit secrets (`.env`, tokens, keys, state files).
- Prefer least-privilege keys for UI and query operations.
- Rotate credentials if they are accidentally exposed.

## More documentation

- UI setup and deployment details: `ui/README.md`
- Additional project docs: `docs/`

