"""
LabelSegments - Azure Function for AI-powered segment labeling

Labels all video segments using GPT-4o-mini based on the active label library.
Triggered by ManageLabels whenever labels are added, updated, or deleted.

Architecture Role:
- Reads label definitions from labels/label_library.json
- Reads all video segments from the segments container
- Calls Azure OpenAI GPT-4o-mini to classify each segment against unapplied labels only
- Merges new label assignments with existing ones in Azure AI Search
- Marks labels as applied and clears removed_labels in the library after completion

Input: HTTP POST (no body required)
Output: JSON with count of labeled segments and videos processed

Environment Variables:
  AZURE_STORAGE_ACCOUNT   - Storage account name
  AZURE_STORAGE_KEY       - Storage account key
  LABELS_CONTAINER        - Blob container for label library (default: "labels")
  SEGMENTS_CONTAINER      - Blob container for segments (default: "segments")
  PROXY_BASE_URL          - Azure OpenAI proxy base URL (e.g. https://<app>.azurewebsites.net/api/v1)
  FUNCTION_HOST_KEY       - Azure Function host key for the proxy
  SEARCH_ENDPOINT         - Azure AI Search endpoint
  SEARCH_ADMIN_KEY        - Azure AI Search admin key
  SEARCH_INDEX            - Search index name (default: "segments")
"""

import json
import logging
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Set, Tuple

import azure.functions as func
import requests
from azure.storage.blob import BlobServiceClient
from openai import OpenAI


LABEL_BATCH_SIZE = 20
INDEX_BATCH_SIZE = 500
GPT_WORKERS = 3
SEARCH_API_VERSION = "2024-05-01-preview"


def _blob_service() -> BlobServiceClient:
    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    key = os.environ["AZURE_STORAGE_KEY"]
    return BlobServiceClient(
        account_url=f"https://{account}.blob.core.windows.net",
        credential=key,
    )


def _read_label_json() -> Dict[str, Any]:
    service = _blob_service()
    container = os.environ.get("LABELS_CONTAINER", "labels")
    bc = service.get_blob_client(container=container, blob="label_library.json")
    data = bc.download_blob().readall()
    return json.loads(data)


def _write_label_json(library: Dict[str, Any]) -> None:
    service = _blob_service()
    container = os.environ.get("LABELS_CONTAINER", "labels")
    bc = service.get_blob_client(container=container, blob="label_library.json")
    bc.upload_blob(json.dumps(library, ensure_ascii=False, indent=2), overwrite=True)


def _list_segment_blobs() -> List[str]:
    """Return list of all .json blob names in the segments container."""
    service = _blob_service()
    container = os.environ.get("SEGMENTS_CONTAINER", "segments")
    cc = service.get_container_client(container)
    return [b.name for b in cc.list_blobs() if b.name.endswith(".json")]


def _read_json_blob(container: str, blob_name: str) -> Dict[str, Any]:
    service = _blob_service()
    bc = service.get_blob_client(container=container, blob=blob_name)
    data = bc.download_blob().readall()
    return json.loads(data)


def _fetch_existing_labels() -> Dict[str, Dict]:
    """Bulk-fetch segment_key -> {pred_labels, pred_label_details} from the search index."""
    endpoint = os.environ["SEARCH_ENDPOINT"].rstrip("/")
    admin_key = os.environ["SEARCH_ADMIN_KEY"]
    index_name = os.environ.get("SEARCH_INDEX", "segments")

    url = f"{endpoint}/indexes/{index_name}/docs/search?api-version={SEARCH_API_VERSION}"
    headers = {"Content-Type": "application/json", "api-key": admin_key}

    existing = {}
    skip = 0
    while True:
        body = {
            "search": "*",
            "select": "segment_key,pred_labels,pred_label_details",
            "top": 1000,
            "skip": skip,
        }
        r = requests.post(url, headers=headers, json=body, timeout=60)
        if not r.ok:
            logging.warning(f"Failed to fetch existing labels from index: {r.status_code}")
            break

        docs = r.json().get("value", [])
        if not docs:
            break

        for doc in docs:
            seg_key = doc.get("segment_key", "")
            pred_labels = doc.get("pred_labels") or []
            raw_details = doc.get("pred_label_details") or "[]"
            try:
                pred_details = json.loads(raw_details)
                if not isinstance(pred_details, list):
                    pred_details = []
            except Exception:
                pred_details = []
            existing[seg_key] = {"pred_labels": pred_labels, "pred_label_details": pred_details}

        skip += len(docs)
        if len(docs) < 1000:
            break

    return existing


def _call_gpt(label_defs: List[Dict], seg_inputs: List[Dict]) -> List[Dict]:
    """
    Call GPT-4o-mini via proxy to label a batch of segments against the given labels.
    Returns list of {segment_id, labels: [{name, rationale}]}.
    """
    client = OpenAI(
        base_url=os.environ["PROXY_BASE_URL"],
        api_key="unused",
        default_headers={"x-functions-key": os.environ["FUNCTION_HOST_KEY"]},
    )

    system_prompt = (
        "You are a content labeler. Given label definitions and video transcript segments, "
        "decide whether each label applies to each segment. "
        "Return a decision for EVERY label for EVERY segment — including labels that do not apply. "
        'Your response must be a JSON object with a "results" key containing an array.'
    )

    user_prompt = (
        f"Labels:\n{json.dumps(label_defs, ensure_ascii=False)}\n\n"
        f"Segments:\n{json.dumps(seg_inputs, ensure_ascii=False)}\n\n"
        "For each segment, return every label with:\n"
        '  - "applied": true if the segment mentions, discusses, or is clearly related to the label topic\n'
        '  - "applied": false if the label does not apply\n'
        '  - "rationale": a brief explanation of your decision either way\n\n'
        "Return:\n"
        '{"results": [\n'
        '  {"segment_id": "0000", "labels": [\n'
        '    {"name": "LabelName", "applied": true, "rationale": "explanation"},\n'
        '    {"name": "OtherLabel", "applied": false, "rationale": "explanation"}\n'
        "  ]}\n"
        "]}"
    )

    resp = client.responses.create(
        model="gpt-4o-mini",
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
        text={"format": {"type": "json_object"}},
    )

    parsed = json.loads(resp.output_text)
    return parsed.get("results", [])


def _validate_labels(gpt_labels: List[Dict], valid_names: Set[str]) -> List[Dict]:
    """Filter GPT-returned labels to only those present in the label library."""
    return [l for l in gpt_labels if isinstance(l, dict) and l.get("name", "") in valid_names]


def _process_batch(
    label_defs: List[Dict],
    valid_names: Set[str],
    strip_names: Set[str],
    existing_index: Dict[str, Dict],
    video_id: str,
    batch: List[Dict],
    blob_name: str,
    batch_idx: int,
) -> List[Dict[str, Any]]:
    """Call GPT for one batch of segments and return index-ready docs."""
    seg_inputs = [{"segment_id": s["segment_id"], "text": s["text"]} for s in batch]

    if label_defs:
        try:
            gpt_results = _call_gpt(label_defs, seg_inputs)
        except Exception as e:
            logging.warning(f"GPT batch failed for {blob_name} batch {batch_idx}: {e}")
            gpt_results = []
    else:
        gpt_results = []

    results_map = {r["segment_id"]: r for r in gpt_results}
    docs = []

    for s in batch:
        seg_id = s["segment_id"]
        segment_key = f"{video_id}_{seg_id}"

        existing = existing_index.get(segment_key, {"pred_labels": [], "pred_label_details": []})
        kept_labels = [n for n in existing["pred_labels"] if n not in strip_names]
        kept_details = [d for d in existing["pred_label_details"] if isinstance(d, dict) and d.get("name") not in strip_names]

        result = results_map.get(seg_id, {})
        validated = _validate_labels(result.get("labels", []), valid_names)

        docs.append({
            "@search.action": "mergeOrUpload",
            "segment_key": segment_key,
            "pred_labels": kept_labels + [l["name"] for l in validated if l.get("applied", True)],
            "pred_label_details": json.dumps(kept_details + validated, ensure_ascii=False),
        })

    return docs


def _index_documents(docs: List[Dict[str, Any]]) -> None:
    endpoint = os.environ["SEARCH_ENDPOINT"].rstrip("/")
    admin_key = os.environ["SEARCH_ADMIN_KEY"]
    index_name = os.environ.get("SEARCH_INDEX", "segments")

    url = f"{endpoint}/indexes/{index_name}/docs/index?api-version={SEARCH_API_VERSION}"
    headers = {
        "Content-Type": "application/json",
        "api-key": admin_key,
    }

    r = requests.post(url, headers=headers, json={"value": docs}, timeout=60)
    if not r.ok:
        raise RuntimeError(f"Search indexing failed: {r.status_code} {r.text}")

    failed = [v for v in r.json().get("value", []) if not v.get("succeeded", True)]
    if failed:
        raise RuntimeError(f"Search indexing had failures: {failed[:3]}")


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        library = _read_label_json()
        all_labels = library.get("labels", [])
        active_labels = [l for l in all_labels if l.get("is_active", True)]
        unapplied_labels = [l for l in active_labels if not l.get("applied", False)]
        removed_label_names = set(library.get("removed_labels", []))

        # Nothing to do
        if not unapplied_labels and not removed_label_names:
            return func.HttpResponse(
                json.dumps({"message": "All labels already applied, nothing to do."}),
                mimetype="application/json",
                status_code=200,
            )

        label_defs = [{"name": l["name"], "description": l["description"]} for l in unapplied_labels]
        valid_names = {l["name"] for l in unapplied_labels}
        # Names to strip from existing assignments (being re-processed or deleted)
        strip_names = {l["name"] for l in unapplied_labels} | removed_label_names

        # Fetch all existing label assignments from the index
        existing_index = _fetch_existing_labels()

        segments_container = os.environ.get("SEGMENTS_CONTAINER", "segments")
        blob_names = _list_segment_blobs()

        # Collect all batches across all blobs
        all_batches: List[Tuple] = []
        for blob_name in blob_names:
            payload = _read_json_blob(segments_container, blob_name)
            if not isinstance(payload, dict):
                continue
            video_id = payload.get("video_id")
            segments = [s for s in payload.get("segments", []) if (s.get("text") or "").strip()]
            if not video_id or not segments:
                continue
            for i in range(0, len(segments), LABEL_BATCH_SIZE):
                all_batches.append((video_id, segments[i: i + LABEL_BATCH_SIZE], blob_name, i))

        # Process all batches in parallel
        all_docs: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=GPT_WORKERS) as executor:
            futures = {
                executor.submit(
                    _process_batch,
                    label_defs, valid_names, strip_names, existing_index,
                    video_id, batch, blob_name, batch_idx,
                ): (blob_name, batch_idx)
                for video_id, batch, blob_name, batch_idx in all_batches
            }
            for future in as_completed(futures):
                try:
                    all_docs.extend(future.result())
                except Exception as e:
                    b_name, b_idx = futures[future]
                    logging.warning(f"Batch failed for {b_name} batch {b_idx}: {e}")

        total_labeled = len(all_docs)
        for i in range(0, len(all_docs), INDEX_BATCH_SIZE):
            _index_documents(all_docs[i: i + INDEX_BATCH_SIZE])

        # Mark all active labels as applied and clear removed_labels
        for l in all_labels:
            if l.get("is_active", True):
                l["applied"] = True
        library["removed_labels"] = []
        library["last_updated"] = datetime.now(timezone.utc).isoformat()
        _write_label_json(library)

        return func.HttpResponse(
            json.dumps({"labeled": total_labeled, "videos": len(blob_names)}),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.exception("LabelSegments failed")
        return func.HttpResponse(
            json.dumps({"error": str(e), "trace": traceback.format_exc()}),
            mimetype="application/json",
            status_code=500,
        )
