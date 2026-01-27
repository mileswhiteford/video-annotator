"""
shared/speech_batch.py - Azure Speech Service Batch Transcription Utilities

This module provides comprehensive utilities for working with Azure Speech Service
batch transcription API. It handles:
- Job submission and polling
- Transcript download and normalization
- Blob storage integration (SAS URL generation, file uploads)
- Channel filtering and deduplication
- Time conversion (100ns ticks to milliseconds)

Architecture Role:
- Core shared library for all transcription operations
- Used by TranscribeHttp and SegmentTranscript Azure Functions
- Provides normalized transcript format (utterances with timestamps)
- Handles complex Speech API response formats and edge cases

Key Functions:
  - submit_transcription_job: Submit batch transcription
  - get_job: Check transcription job status
  - get_transcript_urls: Get transcript file URLs from completed job
  - download_and_normalize: Download and normalize transcripts
  - normalize_transcript: Convert Speech API JSON to standard format
  - make_sas_url: Generate SAS URLs for blob access
"""

# shared/speech_batch.py - Simplified version
import os
import time
import json
import requests
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit, urlunsplit, parse_qs, urlencode


from azure.storage.blob import BlobServiceClient
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from azure.storage.blob import generate_container_sas, ContainerSasPermissions



# Constants
DEFAULT_POLL_SECONDS = 10
DEFAULT_TIMEOUT_MINUTES = 45
DEFAULT_SAS_HOURS_BLOB = 12
DEFAULT_SAS_HOURS_CONTAINER = 24
TICKS_PER_MS = 10_000  # 100ns ticks: 10,000 ticks = 1ms


# -----------------------------
# Config
# -----------------------------
@dataclass
class SpeechConfig:
    key: str
    endpoint: str
    api_version: str = "2025-10-15"


@dataclass
class StorageConfig:
    account: str
    key: str
    input_container: str = "speech-input"
    output_container: str = "speech-output"


def _get_env(name: str, default: Optional[str] = None) -> str:
    """Get required environment variable."""
    value = os.environ.get(name, default)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_speech_config() -> SpeechConfig:
    """Load speech service configuration."""
    return SpeechConfig(
        key=_get_env("SPEECH_KEY"),
        endpoint=_get_env("SPEECH_ENDPOINT").rstrip("/"),
        api_version=os.environ.get("SPEECH_API_VERSION", "v3.2"),
    )


def get_storage_config() -> StorageConfig:
    """Load storage configuration."""
    return StorageConfig(
        account=_get_env("AZURE_STORAGE_ACCOUNT"),
        key=_get_env("AZURE_STORAGE_KEY"),
        input_container=os.environ.get("INPUT_CONTAINER", "speech-input"),
        output_container=os.environ.get("OUTPUT_CONTAINER", "speech-output"),
    )


# -----------------------------
# Blob helpers
# -----------------------------
def create_blob_service(account: str, key: str) -> BlobServiceClient:
    """Create blob service client."""
    return BlobServiceClient(
        account_url=f"https://{account}.blob.core.windows.net",
        credential=key,
    )


def ensure_container(blob_service: BlobServiceClient, container: str) -> None:
    """Create container if it doesn't exist."""
    container_client = blob_service.get_container_client(container)
    try:
        container_client.create_container()
    except Exception:
        pass  # Container already exists


def upload_file_to_blob(
    blob_service: BlobServiceClient,
    container: str,
    blob_name: str,
    file_path: str,
) -> None:
    """Upload local file to blob storage."""
    ensure_container(blob_service, container)
    blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
    with open(file_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)


def make_sas_url(
    account: str,
    container: str,
    account_key: str,
    blob_name: Optional[str] = None,
    hours: int = 6,
) -> str:
    """Generate SAS URL for blob or container."""
    base_url = f"https://{account}.blob.core.windows.net/{container}"
    expiry = datetime.now(timezone.utc) + timedelta(hours=hours)
    
    if blob_name:
        # Blob SAS
        sas = generate_blob_sas(
            account_name=account,
            container_name=container,
            blob_name=blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=expiry,
        )
        return f"{base_url}/{blob_name}?{sas}"
    else:
        # Container SAS
        sas = generate_container_sas(
            account_name=account,
            container_name=container,
            account_key=account_key,
            permission=ContainerSasPermissions(read=True, write=True, list=True, add=True, create=True),
            expiry=expiry,
        )
        return f"{base_url}?{sas}"


# -----------------------------
# Speech API helpers
# -----------------------------
def _speech_headers(speech_key: str) -> Dict[str, str]:
    """Get headers for Speech API requests."""
    return {
        "Ocp-Apim-Subscription-Key": speech_key,
        "Content-Type": "application/json",
    }


def submit_transcription_job(
    config: SpeechConfig,
    content_urls: List[str],
    locale: str,
    display_name: str,
    *,
    word_level_timestamps: bool = True,
    ttl_hours: int = 24,
) -> str:
    """Submit batch transcription job. Returns job URL."""
    submit_url = f"{config.endpoint}/speechtotext/transcriptions:submit?api-version={config.api_version}"

    payload = {
        "displayName": display_name,
        "locale": locale,
        "contentUrls": content_urls,
        "properties": {
            "channels": [0],  # use mono to reduce duplicate outputs
            "wordLevelTimestampsEnabled": word_level_timestamps,
            "timeToLiveHours": ttl_hours,
        },
    }

    response = requests.post(
        submit_url,
        headers=_speech_headers(config.key),
        json=payload,
        timeout=30,
    )

    # Helpful debugging for 400s
    if not response.ok:
        raise RuntimeError(f"Speech submit failed: {response.status_code} {response.text}")

    job = response.json()
    if "self" not in job:
        raise RuntimeError(f"Unexpected response (missing 'self'): {job}")
    return job["self"]



def poll_transcription_job(
    config: SpeechConfig,
    job_url: str,
    poll_seconds: int = DEFAULT_POLL_SECONDS,
    timeout_minutes: int = DEFAULT_TIMEOUT_MINUTES,
) -> Dict[str, Any]:
    """Poll transcription job until completion. Returns final job status."""
    deadline = time.time() + (timeout_minutes * 60)
    
    while True:
        poll_url = _with_api_version(job_url, config.api_version)
        response = requests.get(poll_url, headers=_speech_headers(config.key), timeout=30)
        print(job_url)
        print(response)
        response.raise_for_status()
        job = response.json()
        print(job)
        status = job.get("status")
        if status in ("Succeeded", "Failed"):
            return job
        
        if time.time() > deadline:
            raise TimeoutError(f"Transcription job timed out: {job_url}")
        
        time.sleep(poll_seconds)



def _strip_query(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))

def get_transcript_urls(config: SpeechConfig, job_url: str) -> List[str]:
    """Get URLs of transcript files from completed job."""
    base_job_url = _strip_query(job_url).rstrip("/")
    files_url = f"{base_job_url}/files?api-version={config.api_version}"

    response = requests.get(files_url, headers=_speech_headers(config.key), timeout=30)
    response.raise_for_status()
    data = response.json()
    files = data.get("values") or data.get("files") or []

    urls: List[str] = []

    # Prefer explicit transcription kind (varies by API version; keep flexible)
    for f in files:
        links = f.get("links") or {}
        content_url = links.get("contentUrl") or links.get("contenturl")
        if not content_url:
            continue
        kind = (f.get("kind") or "").lower()
        if kind in {"transcription", "transcriptionresult", "transcriptionresultfile"}:
            urls.append(content_url)

    # Fallback: name heuristics
    if not urls:
        for f in files:
            links = f.get("links") or {}
            content_url = links.get("contentUrl") or links.get("contenturl")
            if not content_url:
                continue
            name = (f.get("name") or "").lower()
            if name.endswith(".json") and ("transcription" in name or "recognizedphrases" in name):
                urls.append(content_url)

    return urls


def download_json(url: str) -> Dict[str, Any]:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    try:
        return response.json()
    except ValueError:
        return json.loads(response.text)
        
def _with_api_version(url: str, api_version: str) -> str:
    """
    Ensure api-version is present in the URL query string (do not duplicate).
    """
    parts = urlsplit(url)
    qs = parse_qs(parts.query, keep_blank_values=True)

    # if already present, keep existing
    if "api-version" not in qs:
        qs["api-version"] = [api_version]

    new_query = urlencode(qs, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def get_job(config: SpeechConfig, job_url: str) -> Dict[str, Any]:
    """
    GET the transcription job resource.

    Accepts either:
    - job_url already containing ?api-version=...
    - job_url without api-version (we add it)
    """
    url = _with_api_version(job_url, config.api_version)
    resp = requests.get(url, headers=_speech_headers(config.key), timeout=30)
    resp.raise_for_status()
    return resp.json()

# -----------------------------
# Normalization
# -----------------------------
def ticks_to_ms(ticks: int) -> int:
    """Convert 100ns ticks to milliseconds."""
    return int(ticks // TICKS_PER_MS)


def _extract_text(phrase: Dict[str, Any]) -> str:
    """
    Prefer display text from nBest[0].display; fallback to other fields.
    """
    nbest = phrase.get("nBest") or phrase.get("NBest") or []
    if nbest:
        best = nbest[0] or {}
        # common fields seen in Speech output
        for k in ("display", "Display", "itn", "ITN", "lexical", "Lexical", "maskedITN", "MaskedITN"):
            v = best.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()

    # fallback fields
    for k in ("display", "Display", "text", "Text", "lexical", "Lexical"):
        v = phrase.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    return ""




def download_and_normalize(
    transcript_urls: List[str],
    *,
    prefer_channel: Optional[int] = 0,
    dedupe: bool = True,
) -> Dict[str, Any]:
    """
    Download transcript JSON from contentUrl(s), normalize using normalize_transcript(),
    and merge results.

    prefer_channel:
      - 0 (default): keep channel 0 if channel field exists
      - None: keep all channels

    dedupe:
      - True: remove exact duplicate utterances/words after merge
    """
    all_utterances: List[Dict[str, Any]] = []
    all_words: List[Dict[str, Any]] = []

    for url in transcript_urls:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        try:
            tj = resp.json()
        except ValueError:
            tj = json.loads(resp.text)

        # Optional channel filtering BEFORE normalization (best if present)
        if prefer_channel is not None:
            rp = tj.get("recognizedPhrases")
            if isinstance(rp, list) and rp and isinstance(rp[0], dict) and ("channel" in rp[0]):
                tj = dict(tj)  # shallow copy
                tj["recognizedPhrases"] = [
                    p for p in rp
                    if (p.get("channel") is None or p.get("channel") == prefer_channel)
                ]

        normalized = normalize_transcript(tj)
        all_utterances.extend(normalized.get("utterances") or [])
        all_words.extend(normalized.get("words") or [])

    # Sort merged results
    all_utterances.sort(key=lambda x: (x["start_ms"], x["end_ms"], x["text"]))
    all_words.sort(key=lambda x: (x["start_ms"], x["end_ms"], x["word"]))

    # De-duplicate (common with stereo channel duplication or multiple transcript files)
    if dedupe:
        seen_u = set()
        dedup_u: List[Dict[str, Any]] = []
        for u in all_utterances:
            key = (u["start_ms"], u["end_ms"], u["text"])
            if key in seen_u:
                continue
            seen_u.add(key)
            dedup_u.append(u)
        all_utterances = dedup_u

        if all_words:
            seen_w = set()
            dedup_w: List[Dict[str, Any]] = []
            for w in all_words:
                key = (w["start_ms"], w["end_ms"], w["word"])
                if key in seen_w:
                    continue
                seen_w.add(key)
                dedup_w.append(w)
            all_words = dedup_w

    result: Dict[str, Any] = {"utterances": all_utterances}
    if all_words:
        result["words"] = all_words
    return result


def normalize_transcript(transcription_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize Speech transcription JSON into:
      utterances: [{start_ms, end_ms, text}]
      words: [{start_ms, end_ms, word}] (if available)
    """
    utterances: List[Dict[str, Any]] = []
    words: List[Dict[str, Any]] = []
    
    for phrase in transcription_json.get("recognizedPhrases") or []:
        offset = phrase.get("offsetInTicks")
        duration = phrase.get("durationInTicks")
        if offset is None or duration is None:
            continue
        
        start_ms = ticks_to_ms(int(offset))
        end_ms = start_ms + ticks_to_ms(int(duration))
        
        # Extract text and words from nBest
        nbest = phrase.get("nBest") or []
        if nbest:
            best = nbest[0]
            text = (best.get("display") or best.get("lexical") or "").strip()
            
            # Extract word-level timestamps
            for word_info in best.get("words") or []:
                w_offset = word_info.get("offsetInTicks")
                w_dur = word_info.get("durationInTicks")
                w_text = word_info.get("word")
                if w_offset is not None and w_dur is not None and w_text:
                    w_start = ticks_to_ms(int(w_offset))
                    w_end = w_start + ticks_to_ms(int(w_dur))
                    words.append({"start_ms": w_start, "end_ms": w_end, "word": w_text})
        else:
            text = (phrase.get("display") or "").strip()
        
        if text:
            utterances.append({"start_ms": start_ms, "end_ms": end_ms, "text": text})
    
    # Sort once (items are usually already in order, but ensure consistency)
    utterances.sort(key=lambda x: x["start_ms"])
    words.sort(key=lambda x: x["start_ms"])
    
    result: Dict[str, Any] = {"utterances": utterances}
    if words:
        result["words"] = words
    return result


# -----------------------------
# Main entry point
# -----------------------------
def transcribe_and_normalize_from_local_file(
    *,
    local_media_path: str,
    locale: str = "en-US",
    display_name: Optional[str] = None,
    poll_seconds: int = DEFAULT_POLL_SECONDS,
    timeout_minutes: int = DEFAULT_TIMEOUT_MINUTES,
) -> Dict[str, Any]:
    """
    Transcribe local media file and return normalized transcript.
    
    Steps:
    1. Upload file to blob storage
    2. Submit transcription job
    3. Poll until completion
    4. Download and normalize results
    
    Returns: {locale, utterances, words?, source}
    """
    speech_config = get_speech_config()
    storage_config = get_storage_config()
    
    # Setup blob storage
    blob_service = create_blob_service(storage_config.account, storage_config.key)
    ensure_container(blob_service, storage_config.input_container)
    ensure_container(blob_service, storage_config.output_container)
    
    # Upload file
    blob_name = os.path.basename(local_media_path)
    upload_file_to_blob(blob_service, storage_config.input_container, blob_name, local_media_path)
    
    # Generate SAS URLs
    input_url = make_sas_url(
        storage_config.account,
        storage_config.input_container,
        storage_config.key,
        blob_name=blob_name,
        hours=DEFAULT_SAS_HOURS_BLOB,
    )
  
    # Submit transcription
    job_url = submit_transcription_job(
        config=speech_config,
        content_urls=[input_url],
        locale=locale,
        display_name=display_name or f"transcription-{blob_name}",
        word_level_timestamps=True,
        ttl_hours=24,
    )
    print('job url', job_url)
    
    # Poll until complete
    final_job = poll_transcription_job(
        config=speech_config,
        job_url=job_url,
        poll_seconds=poll_seconds,
        timeout_minutes=timeout_minutes,
    )
    
    if final_job.get("status") != "Succeeded":
        raise RuntimeError(f"Transcription failed: {final_job}")
    
    # Get transcript files
    transcript_urls = get_transcript_urls(speech_config, job_url)
    if not transcript_urls:
        raise RuntimeError(f"No transcript files found for job: {job_url}")
    

    normalized = download_and_normalize(transcript_urls, prefer_channel=0, dedupe=True)

    result = {
        "locale": locale,
        **normalized,
        "source": {
            "job_url": job_url,
            "transcript_files": transcript_urls,
            "input_blob": f"{storage_config.input_container}/{blob_name}",
        },
    }
    return result
    # # Download and merge transcripts
    # all_utterances: List[Dict[str, Any]] = []
    # all_words: List[Dict[str, Any]] = []
    
    # for url in transcript_urls:
    #     transcript_data = download_json(url)
    #     normalized = normalize_transcript(transcript_data)
    #     all_utterances.extend(normalized.get("utterances", []))
    #     all_words.extend(normalized.get("words", []))
    
    # # Final sort (in case multiple files)
    # all_utterances.sort(key=lambda x: x["start_ms"])
    # all_words.sort(key=lambda x: x["start_ms"])
    
    # result = {
    #     "locale": locale,
    #     "utterances": all_utterances,
    #     "source": {
    #         "job_url": job_url,
    #         "transcript_files": transcript_urls,
    #         "input_blob": f"{storage_config.input_container}/{blob_name}",
    #     },
    # }
    # if all_words:
    #     result["words"] = all_words
    # return result

