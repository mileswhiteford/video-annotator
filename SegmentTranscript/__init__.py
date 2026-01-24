import json
import os
import math
import hashlib
from typing import Any, Dict, List

import azure.functions as func
from azure.storage.blob import BlobServiceClient

from shared.speech_batch import SpeechConfig, get_job, get_transcript_urls, download_and_normalize

SEGMENT_MS_DEFAULT = 30_000


def _speech_cfg() -> SpeechConfig:
    endpoint = os.environ.get("SPEECH_ENDPOINT")
    if not endpoint:
        region = os.environ.get("SPEECH_REGION", "eastus")
        endpoint = f"https://{region}.api.cognitive.microsoft.com"
    return SpeechConfig(
        key=os.environ["SPEECH_KEY"],
        endpoint=endpoint.rstrip("/"),
        api_version=os.environ.get("SPEECH_API_VERSION", "2025-10-15"),
    )


def _blob_client() -> BlobServiceClient:
    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    key = os.environ["AZURE_STORAGE_KEY"]
    return BlobServiceClient(
        account_url=f"https://{account}.blob.core.windows.net",
        credential=key,
    )


def _ensure_container(blob_service: BlobServiceClient, container: str) -> None:
    try:
        blob_service.get_container_client(container).create_container()
    except Exception:
        pass


def _stable_video_id(body: Dict[str, Any]) -> str:
    if body.get("video_id"):
        return str(body["video_id"])
    seed = body.get("job_url") or body.get("media_url") or json.dumps(body, sort_keys=True)
    h = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
    return f"vid_{h}"


def segment_utterances(utterances: List[Dict[str, Any]], segment_ms: int) -> List[Dict[str, Any]]:
    if not utterances:
        return []

    max_end = max(u.get("end_ms", 0) for u in utterances)
    num_segments = int(math.ceil(max_end / segment_ms))

    segments: List[Dict[str, Any]] = []
    u_idx = 0

    for i in range(num_segments):
        start = i * segment_ms
        end = start + segment_ms

        while u_idx < len(utterances) and utterances[u_idx].get("end_ms", 0) <= start:
            u_idx += 1

        j = u_idx
        texts = []
        while j < len(utterances):
            u = utterances[j]
            u_start = u.get("start_ms", 0)
            u_end = u.get("end_ms", 0)
            if u_start >= end:
                break
            if u_end > start and u_start < end:
                t = (u.get("text") or "").strip()
                if t:
                    texts.append(t)
            j += 1

        seg_text = " ".join(texts).strip()
        segments.append(
            {
                "segment_id": f"{i:04d}",
                "start_ms": start,
                "end_ms": end,
                "text": seg_text,
            }
        )

    return segments


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        segment_ms = int(body.get("segment_ms", SEGMENT_MS_DEFAULT))
        write_to_blob = bool(body.get("write_to_blob", True))

        normalized = body.get("result")

        if normalized is None:
            job_url = body.get("job_url") or req.params.get("job_url")
            if not job_url:
                return func.HttpResponse(
                    json.dumps({"error": "Send either 'result' (normalized transcript) or 'job_url'."}),
                    mimetype="application/json",
                    status_code=400,
                )

            cfg = _speech_cfg()
            job = get_job(cfg, job_url)
            status = job.get("status")
            if status != "Succeeded":
                return func.HttpResponse(
                    json.dumps({"error": f"Job not succeeded (status={status})", "status": status, "job": job}),
                    mimetype="application/json",
                    status_code=400,
                )

            urls = get_transcript_urls(cfg, job_url)
            normalized = download_and_normalize(urls, prefer_channel=0, dedupe=True)

        utterances = normalized.get("utterances") or []
        if not isinstance(utterances, list):
            return func.HttpResponse(
                json.dumps({"error": "'utterances' missing or invalid in normalized transcript."}),
                mimetype="application/json",
                status_code=400,
            )

        video_id = _stable_video_id(body)
        segments = segment_utterances(utterances, segment_ms)

        response: Dict[str, Any] = {
            "video_id": video_id,
            "segment_ms": segment_ms,
            "num_segments": len(segments),
            "segments": segments,
        }

        if write_to_blob:
            container = os.environ.get("SEGMENTS_CONTAINER", "segments")
            blob_name = f"{video_id}.json"
            blob_service = _blob_client()
            _ensure_container(blob_service, container)
            blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
            blob_client.upload_blob(json.dumps(response, ensure_ascii=False), overwrite=True)
            response["segments_blob"] = f"{container}/{blob_name}"

        return func.HttpResponse(
            json.dumps(response, ensure_ascii=False),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
