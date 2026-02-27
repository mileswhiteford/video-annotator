"""
LabelSegments - Azure Function for AI-powered segment labeling

Labels all video segments using GPT-4o-mini based on the active label library.
Triggered by ManageLabels whenever labels are added, updated, or deleted.

Architecture Role:
- Reads label definitions from labels/label_library.json
- Reads all video segments from the segments container
- Calls Azure OpenAI GPT-4o-mini to classify each segment against all labels
- Updates Azure AI Search with pred_labels and pred_label_details

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
from typing import Any, Dict, List

import azure.functions as func
import requests
from azure.storage.blob import BlobServiceClient
from openai import OpenAI


LABEL_BATCH_SIZE = 10
INDEX_BATCH_SIZE = 500
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


def _call_gpt(label_defs: List[Dict], seg_inputs: List[Dict]) -> List[Dict]:
    """
    Call GPT-4o-mini via proxy to label a batch of segments against all labels.
    Returns list of {segment_id, labels: [{name, rationale}]}.
    """
    client = OpenAI(
        base_url=os.environ["PROXY_BASE_URL"],
        api_key="unused",
        default_headers={"x-functions-key": os.environ["FUNCTION_HOST_KEY"]},
    )

    system_prompt = (
        "You are a content labeler. Given label definitions and video transcript segments, "
        "return which labels apply to each segment as a JSON object. "
        'Your response must be a JSON object with a "results" key containing an array.'
    )

    user_prompt = (
        f"Labels:\n{json.dumps(label_defs, ensure_ascii=False)}\n\n"
        f"Segments:\n{json.dumps(seg_inputs, ensure_ascii=False)}\n\n"
        "Return:\n"
        '{"results": [\n'
        '  {"segment_id": "0000", "labels": [{"name": "LabelName", "rationale": "explanation"}]}\n'
        "]}\n"
        "Use an empty labels array if no labels apply to a segment."
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


def _validate_labels(gpt_labels: List[Dict], valid_names: set) -> List[Dict]:
    """Filter GPT-returned labels to only those present in the label library."""
    return [l for l in gpt_labels if isinstance(l, dict) and l.get("name", "") in valid_names]


def _index_documents(docs: List[Dict[str, Any]]) -> None:
    endpoint = os.environ["SEARCH_ENDPOINT"].rstrip("/")
    key = os.environ["SEARCH_ADMIN_KEY"]
    index_name = os.environ.get("SEARCH_INDEX", "segments")

    url = f"{endpoint}/indexes/{index_name}/docs/index?api-version={SEARCH_API_VERSION}"
    headers = {
        "Content-Type": "application/json",
        "api-key": key,
    }

    r = requests.post(url, headers=headers, json={"value": docs}, timeout=60)
    if not r.ok:
        raise RuntimeError(f"Search indexing failed: {r.status_code} {r.text}")

    failed = [v for v in r.json().get("value", []) if not v.get("succeeded", True)]
    if failed:
        raise RuntimeError(f"Search indexing had failures: {failed[:3]}")


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Load active labels
        library = _read_label_json()
        labels = [l for l in library.get("labels", []) if l.get("is_active", True)]

        if not labels:
            return func.HttpResponse(
                json.dumps({"message": "No active labels, nothing to label."}),
                mimetype="application/json",
                status_code=200,
            )

        label_defs = [{"name": l["name"], "description": l["description"]} for l in labels]
        valid_names = {l["name"] for l in labels}

        segments_container = os.environ.get("SEGMENTS_CONTAINER", "segments")
        blob_names = _list_segment_blobs()
        total_labeled = 0

        for blob_name in blob_names:
            payload = _read_json_blob(segments_container, blob_name)
            video_id = payload.get("video_id")
            segments = [s for s in payload.get("segments", []) if (s.get("text") or "").strip()]

            if not video_id or not segments:
                continue

            docs: List[Dict[str, Any]] = []

            for i in range(0, len(segments), LABEL_BATCH_SIZE):
                batch = segments[i: i + LABEL_BATCH_SIZE]
                seg_inputs = [{"segment_id": s["segment_id"], "text": s["text"]} for s in batch]

                try:
                    gpt_results = _call_gpt(label_defs, seg_inputs)
                except Exception as e:
                    logging.warning(f"GPT batch failed for {blob_name} batch {i}: {e}")
                    gpt_results = []

                results_map = {r["segment_id"]: r for r in gpt_results}

                for s in batch:
                    seg_id = s["segment_id"]
                    result = results_map.get(seg_id, {})
                    raw_labels = result.get("labels", [])
                    validated = _validate_labels(raw_labels, valid_names)

                    docs.append({
                        "@search.action": "mergeOrUpload",
                        "segment_key": f"{video_id}_{seg_id}",
                        "pred_labels": [l["name"] for l in validated],
                        "pred_label_details": json.dumps(validated, ensure_ascii=False),
                    })
                    total_labeled += 1

            for i in range(0, len(docs), INDEX_BATCH_SIZE):
                _index_documents(docs[i: i + INDEX_BATCH_SIZE])

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
