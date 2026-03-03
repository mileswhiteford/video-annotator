"""
ui_search.py - Main Streamlit entry point
Contains:
- Environment configuration
- Shared utility functions
- Sidebar navigation
- Search Segments page (default)
- Imports and calls the other three page modules
"""

import os
import requests
import streamlit as st
import json
import time
import re
import subprocess
import hashlib
import base64
import hmac
import uuid
import urllib.parse
import pandas as pd
import io
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple, List
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================

# Azure Function URLs
SEARCH_FN_URL = os.environ.get("SEARCH_FN_URL", "")

# Azure Speech Service (DIRECT)
SPEECH_KEY = os.environ.get("SPEECH_KEY")
SPEECH_REGION = os.environ.get("SPEECH_REGION", "eastus")
SPEECH_API_VERSION = os.environ.get("SPEECH_API_VERSION", "2024-11-15")

# Azure OpenAI
AZURE_OPENAI_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_KEY = os.environ.get("AZURE_OPENAI_KEY")
AZURE_OPENAI_DEPLOYMENT = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "text-embedding-3-small")

# Azure Cognitive Search
SEARCH_ENDPOINT = os.environ.get("SEARCH_ENDPOINT")
SEARCH_KEY = os.environ.get("SEARCH_KEY")
SEARCH_INDEX_NAME = os.environ.get("SEARCH_INDEX_NAME", "segments")

# Azure Storage
AZURE_STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT", "storagevideoannotator")
AZURE_STORAGE_KEY = os.environ.get("AZURE_STORAGE_KEY", "")
INPUT_CONTAINER = os.environ.get("INPUT_CONTAINER", "speech-input")
SEGMENTS_CONTAINER = os.environ.get("SEGMENTS_CONTAINER", "segments")

# Settings
DEFAULT_MODE = os.environ.get("DEFAULT_MODE", "hybrid")
DEFAULT_TOP = int(os.environ.get("DEFAULT_TOP", "10"))
DEFAULT_K = int(os.environ.get("DEFAULT_K", "40"))
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "15"))

# =============================================================================
# STREAMLIT SETUP
# =============================================================================

st.set_page_config(page_title="Video Annotation Platform", layout="wide")
st.title(" Video Annotation Platform")

# Initialize session state
session_state_defaults = {
    'yt_url_value': "",
    'batch_results': [],
    'batch_processing': False,
    'index_schema_cache': None,
    'stored_videos_cache': None,
    'url_fields_status': None,
    'debug_info': {},
    'video_to_delete': None,
    'delete_success': False,
    'videos_loaded': False
}

for key, value in session_state_defaults.items():
    if key not in st.session_state:
        st.session_state[key] = value

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def ms_to_ts(ms: int) -> str:
    """Convert milliseconds to timestamp string."""
    s = max(0, int(ms // 1000))
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"

def sanitize_id(id_string: str) -> str:
    """Sanitize ID for Azure Search (alphanumeric, hyphens, underscores only)."""
    if not id_string:
        id_string = "unknown"
    sanitized = re.sub(r'[^\w\-]', '_', str(id_string))
    sanitized = re.sub(r'_+', '_', sanitized)
    sanitized = sanitized.strip('_')
    if not sanitized:
        sanitized = "unknown"
    if sanitized.startswith('_') or sanitized.startswith('-'):
        sanitized = 'id' + sanitized
    if len(sanitized) > 1024:
        hash_suffix = hashlib.md5(sanitized.encode()).hexdigest()[:16]
        sanitized = sanitized[:1000] + "_" + hash_suffix
    return sanitized

def detect_url_type(url: str) -> str:
    """Detect if URL is YouTube, direct media, or unknown."""
    if not url:
        return "unknown"
    url_lower = str(url).lower().strip()
    youtube_patterns = [
        r'(?:https?:\/\/)?(?:www\.)?(?:youtube\.com|youtu\.be)',
        r'youtube\.com\/watch\?v=',
        r'youtu\.be\/',
        r'youtube\.com\/shorts\/'
    ]
    for pattern in youtube_patterns:
        if re.search(pattern, url_lower):
            return "youtube"
    media_extensions = ['.mp4', '.m4a', '.mp3', '.wav', '.mov', '.avi', '.mkv', '.webm']
    if any(url_lower.endswith(ext) for ext in media_extensions):
        return "direct"
    cloud_patterns = ['box.com', 'drive.google.com', 'dropbox.com', 'onedrive']
    if any(pattern in url_lower for pattern in cloud_patterns):
        return "direct"
    return "unknown"

def check_yt_dlp() -> bool:
    """Check if yt-dlp is installed."""
    try:
        result = subprocess.run(["which", "yt-dlp"], capture_output=True, text=True)
        return result.returncode == 0
    except:
        return False

def call_api(url: str, payload: dict) -> dict:
    """Make API call to search function."""
    try:
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"API call failed: {str(e)}")

# =============================================================================
# AZURE SEARCH SCHEMA FUNCTIONS
# =============================================================================

def debug_check_index_schema():
    """Check index schema and verify URL tracking fields."""
    if not SEARCH_ENDPOINT or not SEARCH_KEY or not SEARCH_INDEX_NAME:
        return "Search not configured - check SEARCH_ENDPOINT, SEARCH_KEY, and SEARCH_INDEX_NAME"
    url = f"{SEARCH_ENDPOINT}/indexes/{SEARCH_INDEX_NAME}?api-version=2024-07-01"
    headers = {"api-key": SEARCH_KEY}
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            schema = r.json()
            key_field = None
            fields_info = []
            url_fields = ['source_url', 'source_type', 'processed_at']
            found_url_fields = []
            for field in schema.get("fields", []):
                field_info = {
                    "name": field.get("name"),
                    "type": field.get("type"),
                    "key": field.get("key", False),
                    "retrievable": field.get("retrievable", False),
                    "filterable": field.get("filterable", False),
                    "sortable": field.get("sortable", False),
                    "facetable": field.get("facetable", False)
                }
                fields_info.append(field_info)
                if field.get("key", False):
                    key_field = field.get("name")
                if field.get("name") in url_fields:
                    found_url_fields.append(field.get("name"))
            return {
                "index_name": schema.get("name"),
                "key_field": key_field,
                "fields": fields_info,
                "found_url_fields": found_url_fields,
                "missing_url_fields": list(set(url_fields) - set(found_url_fields)),
                "has_all_url_fields": len(found_url_fields) == len(url_fields)
            }
        else:
            return f"Index check failed: HTTP {r.status_code} - {r.text[:500]}"
    except Exception as e:
        return f"Error checking index: {str(e)}"

def get_index_schema():
    """Get cached schema or fetch new one."""
    if st.session_state.index_schema_cache:
        return st.session_state.index_schema_cache
    schema_info = debug_check_index_schema()
    if isinstance(schema_info, dict):
        st.session_state.index_schema_cache = schema_info
        return schema_info
    else:
        raise RuntimeError(f"Cannot fetch index schema: {schema_info}")

def check_url_fields_status():
    """Check URL fields status with caching."""
    if st.session_state.url_fields_status:
        return st.session_state.url_fields_status
    try:
        schema = get_index_schema()
        if isinstance(schema, dict):
            result = {
                'fields_exist': schema.get('has_all_url_fields', False),
                'found_fields': schema.get('found_url_fields', []),
                'missing_fields': schema.get('missing_url_fields', []),
                'key_field': schema.get('key_field')
            }
            st.session_state.url_fields_status = result
            return result
    except:
        pass
    return {
        'fields_exist': False,
        'found_fields': [],
        'missing_fields': ['source_url', 'source_type', 'processed_at'],
        'key_field': None
    }

# =============================================================================
# AZURE SPEECH API FUNCTIONS
# =============================================================================

def submit_transcription_direct(video_id: str, media_url: str) -> Dict[str, Any]:
    """Submit transcription directly to Azure Speech API."""
    if not SPEECH_KEY:
        raise RuntimeError("SPEECH_KEY not configured")
    endpoint = f"https://{SPEECH_REGION}.api.cognitive.microsoft.com/speechtotext/transcriptions:submit?api-version={SPEECH_API_VERSION}"
    headers = {
        "Ocp-Apim-Subscription-Key": SPEECH_KEY,
        "Content-Type": "application/json"
    }
    payload = {
        "contentUrls": [media_url],
        "locale": "en-US",
        "displayName": f"transcription_{video_id}",
        "properties": {
            "diarizationEnabled": False,
            "wordLevelTimestampsEnabled": False,
            "punctuationMode": "DictatedAndAutomatic",
            "profanityFilterMode": "Masked",
            "timeToLiveHours": 24
        }
    }
    try:
        r = requests.post(endpoint, headers=headers, json=payload, timeout=60)
        r.raise_for_status()
        operation_url = r.headers.get("Location")
        if not operation_url:
            result = r.json()
            operation_url = result.get("self") or result.get("links", {}).get("self")
        if not operation_url:
            raise RuntimeError("No operation URL returned from Speech API")
        return {"operation_url": operation_url, "video_id": video_id}
    except requests.exceptions.HTTPError as e:
        error_msg = f"Speech API error {r.status_code}: {r.text}"
        if r.status_code == 401:
            error_msg = "Azure Speech API authentication failed. Check SPEECH_KEY."
        raise RuntimeError(error_msg)

def poll_transcription_operation(operation_url: str) -> Dict[str, Any]:
    """Poll transcription operation status."""
    if not SPEECH_KEY:
        raise RuntimeError("SPEECH_KEY not configured")
    headers = {"Ocp-Apim-Subscription-Key": SPEECH_KEY}
    try:
        poll_url = operation_url.replace("/transcriptions:submit/", "/transcriptions/")
        st.session_state['debug_poll_url'] = poll_url
        r = requests.get(poll_url, headers=headers, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to poll transcription: {str(e)}")

def get_transcription_from_result(result_data: Dict) -> Dict[str, Any]:
    """Get transcription JSON from result files."""
    if not SPEECH_KEY:
        raise RuntimeError("SPEECH_KEY not configured")
    headers = {"Ocp-Apim-Subscription-Key": SPEECH_KEY}
    try:
        links = result_data.get("links", {})
        files_url = links.get("files")
        if not files_url:
            if "combinedRecognizedPhrases" in result_data:
                return result_data
            raise RuntimeError("No files URL in result")
        r = requests.get(files_url, headers=headers, timeout=30)
        r.raise_for_status()
        files_data = r.json()
        for file in files_data.get("values", []):
            if file.get("kind") == "Transcription":
                content_url = file.get("links", {}).get("contentUrl")
                if content_url:
                    content_r = requests.get(content_url, timeout=60)
                    content_r.raise_for_status()
                    return content_r.json()
        raise RuntimeError("No transcription file found in results")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to get transcription result: {str(e)}")

# =============================================================================
# EMBEDDING AND INDEXING WITH URL TRACKING
# =============================================================================

def get_embeddings(texts: list) -> list:
    """Get embeddings from Azure OpenAI."""
    if not AZURE_OPENAI_ENDPOINT or not AZURE_OPENAI_KEY:
        raise RuntimeError("Azure OpenAI not configured")
    url = f"{AZURE_OPENAI_ENDPOINT}/openai/deployments/{AZURE_OPENAI_DEPLOYMENT}/embeddings?api-version=2024-02-01"
    headers = {
        "api-key": AZURE_OPENAI_KEY,
        "Content-Type": "application/json"
    }
    payload = {
        "input": texts,
        "model": "text-embedding-3-small"
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        r.raise_for_status()
        result = r.json()
        return [item["embedding"] for item in result["data"]]
    except Exception as e:
        raise RuntimeError(f"Embedding failed: {str(e)}")

def index_segments_direct(video_id: str, segments: list, source_url: str = None, source_type: str = None) -> Dict[str, Any]:
    """
    Index segments to Azure Cognitive Search with URL tracking.
    """
    if not SEARCH_ENDPOINT or not SEARCH_KEY:
        raise RuntimeError("Azure Search not configured")
    schema_info = get_index_schema()
    key_field = schema_info.get("key_field")
    available_fields = {f.get("name") for f in schema_info.get("fields", [])}
    if not key_field:
        raise RuntimeError("No key field found in index")
    url_fields_available = {
        'source_url': 'source_url' in available_fields,
        'source_type': 'source_type' in available_fields,
        'processed_at': 'processed_at' in available_fields
    }
    texts = [seg.get("text", "") for seg in segments]
    try:
        embeddings = get_embeddings(texts)
    except Exception as e:
        st.warning(f"Embedding failed, indexing without vectors: {e}")
        embeddings = [None] * len(segments)
    documents = []
    processed_timestamp = datetime.utcnow().isoformat() + "Z"
    for i, (seg, embedding) in enumerate(zip(segments, embeddings)):
        safe_video_id = sanitize_id(video_id)
        doc_id = f"{safe_video_id}_{i}"
        doc = {"@search.action": "upload", key_field: doc_id}
        field_mappings = {
            "video_id": safe_video_id,
            "segment_id": str(seg.get("segment_id", i)),
            "text": str(seg.get("text", "")),
            "start_ms": int(seg.get("start_ms", 0)),
            "end_ms": int(seg.get("end_ms", 0)),
            "pred_labels": seg.get("pred_labels", []) if seg.get("pred_labels") else []
        }
        if url_fields_available['source_url']:
            field_mappings["source_url"] = str(source_url) if source_url else ""
        if url_fields_available['source_type']:
            field_mappings["source_type"] = str(source_type) if source_type else "unknown"
        if url_fields_available['processed_at']:
            field_mappings["processed_at"] = processed_timestamp
        for field_name, value in field_mappings.items():
            if field_name in available_fields:
                doc[field_name] = value
        embedding_field = next((f for f in ["embedding", "embeddings", "vector", "vectors"] if f in available_fields), None)
        if embedding and embedding_field:
            try:
                doc[embedding_field] = [float(x) for x in embedding]
            except (ValueError, TypeError):
                pass
        documents.append(doc)
    url = f"{SEARCH_ENDPOINT}/indexes/{SEARCH_INDEX_NAME}/docs/index?api-version=2024-07-01"
    headers = {"api-key": SEARCH_KEY, "Content-Type": "application/json"}
    payload = {"value": documents}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        if r.status_code >= 400:
            error_detail = r.text
            try:
                error_detail = json.dumps(r.json(), indent=2)
            except:
                pass
            raise RuntimeError(f"Indexing failed: HTTP {r.status_code}\n{error_detail}")
        result = r.json()
        if r.status_code == 207:
            failed_docs = [item for item in result.get("value", []) if not item.get("status", False)]
            if failed_docs:
                st.warning(f"Partial indexing failure: {len(failed_docs)} documents failed")
        return {
            "indexed": len(documents),
            "video_id": video_id,
            "key_field_used": key_field,
            "source_url_stored": bool(source_url and url_fields_available['source_url']),
            "source_type_stored": bool(source_type and url_fields_available['source_type']),
            "url_fields_available": url_fields_available
        }
    except Exception as e:
        raise RuntimeError(f"Indexing failed: {str(e)}")

def process_transcription_to_segments(transcription_data: Dict, video_id: str) -> list:
    """Convert Azure Speech transcription to segments."""
    segments = []
    for i, phrase in enumerate(transcription_data.get("recognizedPhrases", [])):
        offset = phrase.get("offsetInTicks", 0) // 10000
        duration = phrase.get("durationInTicks", 0) // 10000
        nbest = phrase.get("nBest", [])
        text = nbest[0].get("display", "") if nbest else ""
        segments.append({
            "segment_id": i,
            "video_id": video_id,
            "text": text,
            "start_ms": offset,
            "end_ms": offset + duration,
            "pred_labels": []
        })
    return segments

# =============================================================================
# VIDEO RETRIEVAL AND MANAGEMENT (get_stored_videos, delete_video_by_id)
# =============================================================================

def get_stored_videos(video_id: str = None, source_type: str = None, 
                     include_missing: bool = True, limit: int = 1000) -> List[Dict]:
    """
    Retrieve videos from search index with URL data.
    FALLBACK METHOD: Does not use faceting (requires facetable field).
    Instead uses pagination to get all documents and deduplicates.
    """
    if not SEARCH_ENDPOINT or not SEARCH_KEY:
        return []
    url = f"{SEARCH_ENDPOINT}/indexes/{SEARCH_INDEX_NAME}/docs/search?api-version=2024-07-01"
    headers = {"api-key": SEARCH_KEY, "Content-Type": "application/json"}
    try:
        schema = get_index_schema()
        available_fields = {f['name'] for f in schema.get('fields', [])}
    except:
        available_fields = set()
    filters = []
    if video_id:
        escaped_id = video_id.replace("'", "''")
        filters.append(f"video_id eq '{escaped_id}'")
    if source_type and source_type != "All":
        escaped_type = source_type.replace("'", "''")
        filters.append(f"source_type eq '{escaped_type}'")
    filter_query = " and ".join(filters) if filters else None
    select_fields = ["video_id"]
    optional_fields = ["source_url", "source_type", "processed_at"]
    for field in optional_fields:
        if not available_fields or field in available_fields:
            select_fields.append(field)
    all_videos = {}
    skip = 0
    batch_size = 1000
    max_iterations = 100
    try:
        for iteration in range(max_iterations):
            payload = {
                "search": "*",
                "select": ",".join(select_fields),
                "top": batch_size,
                "skip": skip,
                "count": True
            }
            if filter_query:
                payload["filter"] = filter_query
            if "processed_at" in available_fields:
                payload["orderby"] = "processed_at desc"
            r = requests.post(url, headers=headers, json=payload, timeout=30)
            r.raise_for_status()
            data = r.json()
            docs = data.get("value", [])
            total_count = data.get("@odata.count", 0)
            if not docs:
                break
            for doc in docs:
                vid = doc.get('video_id')
                if vid and vid not in all_videos:
                    all_videos[vid] = {
                        'video_id': vid,
                        'source_type': doc.get('source_type') or 'unknown',
                        'source_url': doc.get('source_url', ''),
                        'processed_at': doc.get('processed_at', 'unknown')
                    }
            skip += len(docs)
            if skip >= total_count or len(docs) < batch_size:
                break
        videos = list(all_videos.values())[:limit]
        return videos
    except Exception as e:
        st.error(f"Failed to retrieve videos: {e}")
        import traceback
        st.error(traceback.format_exc())
        return []

def delete_video_by_id(video_id: str) -> bool:
    """Delete all segments for a video_id from the index."""
    if not SEARCH_ENDPOINT or not SEARCH_KEY:
        return False
    search_url = f"{SEARCH_ENDPOINT}/indexes/{SEARCH_INDEX_NAME}/docs/search?api-version=2024-07-01"
    headers = {"api-key": SEARCH_KEY, "Content-Type": "application/json"}
    escaped_id = video_id.replace("'", "''")
    payload = {
        "search": "*",
        "filter": f"video_id eq '{escaped_id}'",
        "select": "video_id",
        "top": 1000
    }
    try:
        r = requests.post(search_url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        docs = r.json().get("value", [])
        if not docs:
            st.warning(f"No documents found for video_id: {video_id}")
            return False
        schema = get_index_schema()
        key_field = schema.get('key_field', 'id')
        delete_docs = []
        for doc in docs:
            doc_key = doc.get(key_field) or doc.get('id')
            if doc_key:
                delete_docs.append({
                    "@search.action": "delete",
                    key_field: doc_key
                })
        if not delete_docs:
            st.warning("No valid documents to delete")
            return False
        delete_url = f"{SEARCH_ENDPOINT}/indexes/{SEARCH_INDEX_NAME}/docs/index?api-version=2024-07-01"
        r = requests.post(delete_url, headers=headers, json={"value": delete_docs}, timeout=60)
        r.raise_for_status()
        return True
    except Exception as e:
        st.error(f"Delete failed: {e}")
        import traceback
        st.error(traceback.format_exc())
        return False

# =============================================================================
# AZURE STORAGE FUNCTIONS
# =============================================================================

def generate_video_id(filename: str) -> str:
    """Generate safe video ID from filename."""
    clean_name = Path(filename).stem
    clean_name = re.sub(r'[^\w\s-]', '', clean_name)
    clean_name = re.sub(r'[-\s]+', '_', clean_name)
    hash_suffix = hashlib.md5(clean_name.encode()).hexdigest()[:8]
    return f"vid_{clean_name[:50]}_{hash_suffix}"

def test_sas_url(sas_url: str) -> Tuple[bool, str]:
    """Test if SAS URL is accessible."""
    try:
        r = requests.head(sas_url, timeout=10, allow_redirects=True)
        return (r.status_code == 200, f"HTTP {r.status_code}")
    except Exception as e:
        return (False, str(e))

def generate_sas_token_fixed(blob_name: str, expiry_hours: int = 24) -> Optional[str]:
    """Generate SAS token for blob access."""
    if not AZURE_STORAGE_KEY:
        return None
    try:
        expiry = datetime.now(timezone.utc) + timedelta(hours=expiry_hours)
        expiry_str = expiry.strftime('%Y-%m-%dT%H:%M:%SZ')
        account_key = base64.b64decode(AZURE_STORAGE_KEY)
        canonicalized_resource = f"/blob/{AZURE_STORAGE_ACCOUNT}/{INPUT_CONTAINER}/{blob_name}"
        string_to_sign = (
            f"r\n\n{expiry_str}\n{canonicalized_resource}\n\n\nhttps\n2020-12-06\nb\n\n\n\n\n\n\n"
        )
        signed_hmac = hmac.new(account_key, string_to_sign.encode('utf-8'), hashlib.sha256).digest()
        signature = base64.b64encode(signed_hmac).decode('utf-8')
        sas_params = {
            'sv': '2020-12-06',
            'sr': 'b',
            'sp': 'r',
            'se': expiry_str,
            'spr': 'https',
            'sig': signature
        }
        return '&'.join([f"{k}={urllib.parse.quote(v, safe='')}" for k, v in sas_params.items()])
    except Exception as e:
        st.error(f"SAS generation error: {e}")
        return None

def upload_to_azure_blob_fixed(file_bytes: bytes, blob_name: str) -> Tuple[Optional[str], Optional[str]]:
    """Upload to Azure Blob using REST API."""
    if not AZURE_STORAGE_KEY:
        return None, "Azure Storage key not configured"
    try:
        url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{INPUT_CONTAINER}/{blob_name}"
        date_str = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        content_length = len(file_bytes)
        string_to_sign = (
            f"PUT\n\n\n{content_length}\n\napplication/octet-stream\n\n\n\n\n\n\n"
            f"x-ms-blob-type:BlockBlob\nx-ms-date:{date_str}\nx-ms-version:2020-12-06\n"
            f"/{AZURE_STORAGE_ACCOUNT}/{INPUT_CONTAINER}/{blob_name}"
        )
        account_key = base64.b64decode(AZURE_STORAGE_KEY)
        signed_hmac = hmac.new(account_key, string_to_sign.encode('utf-8'), hashlib.sha256).digest()
        signature = base64.b64encode(signed_hmac).decode('utf-8')
        headers = {
            "x-ms-date": date_str,
            "x-ms-version": "2020-12-06",
            "x-ms-blob-type": "BlockBlob",
            "Content-Type": "application/octet-stream",
            "Content-Length": str(content_length),
            "Authorization": f"SharedKey {AZURE_STORAGE_ACCOUNT}:{signature}"
        }
        r = requests.put(url, data=file_bytes, headers=headers, timeout=300)
        if r.status_code not in [201, 200]:
            return None, f"Upload failed: HTTP {r.status_code}"
        sas_token = generate_sas_token_fixed(blob_name)
        if not sas_token:
            return None, "Failed to generate SAS token"
        sas_url = f"{url}?{sas_token}"
        is_valid, test_msg = test_sas_url(sas_url)
        if not is_valid:
            return None, f"SAS URL validation failed: {test_msg}"
        return sas_url, None
    except Exception as e:
        import traceback
        return None, f"Upload error: {str(e)}"

def upload_to_azure_blob_sdk(file_bytes: bytes, blob_name: str) -> Tuple[Optional[str], Optional[str]]:
    """Upload using Azure SDK (preferred method)."""
    try:
        from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
        connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={AZURE_STORAGE_ACCOUNT};"
            f"AccountKey={AZURE_STORAGE_KEY};"
            f"EndpointSuffix=core.windows.net"
        )
        blob_service = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service.get_container_client(INPUT_CONTAINER)
        try:
            container_client.create_container()
        except Exception:
            pass
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(file_bytes, overwrite=True)
        sas_token = generate_blob_sas(
            account_name=AZURE_STORAGE_ACCOUNT,
            container_name=INPUT_CONTAINER,
            blob_name=blob_name,
            account_key=AZURE_STORAGE_KEY,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(hours=24),
            protocol="https"
        )
        sas_url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{INPUT_CONTAINER}/{blob_name}?{sas_token}"
        is_valid, test_msg = test_sas_url(sas_url)
        if not is_valid:
            return None, f"SAS URL validation failed: {test_msg}"
        return sas_url, None
    except ImportError:
        return None, "azure-storage-blob not installed"
    except Exception as e:
        import traceback
        return None, f"SDK upload failed: {str(e)}"

def save_segments_to_blob(video_id: str, segments: list) -> str:
    """Save segments JSON to blob storage."""
    if not AZURE_STORAGE_KEY:
        raise RuntimeError("Azure Storage key not configured")
    blob_name = f"{video_id}_segments.json"
    url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{SEGMENTS_CONTAINER}/{blob_name}"
    json_bytes = json.dumps(segments, indent=2).encode('utf-8')
    content_length = len(json_bytes)
    date_str = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    string_to_sign = (
        f"PUT\n\n\n{content_length}\n\napplication/json\n\n\n\n\n\n\n"
        f"x-ms-blob-type:BlockBlob\nx-ms-date:{date_str}\nx-ms-version:2020-12-06\n"
        f"/{AZURE_STORAGE_ACCOUNT}/{SEGMENTS_CONTAINER}/{blob_name}"
    )
    account_key = base64.b64decode(AZURE_STORAGE_KEY)
    signed_hmac = hmac.new(account_key, string_to_sign.encode('utf-8'), hashlib.sha256).digest()
    signature = base64.b64encode(signed_hmac).decode('utf-8')
    headers = {
        "x-ms-date": date_str,
        "x-ms-version": "2020-12-06",
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "application/json",
        "Content-Length": str(content_length),
        "Authorization": f"SharedKey {AZURE_STORAGE_ACCOUNT}:{signature}"
    }
    r = requests.put(url, data=json_bytes, headers=headers, timeout=60)
    r.raise_for_status()
    return blob_name

def download_youtube_audio(youtube_url: str, output_path: str, 
                          progress_callback=None) -> Tuple[Optional[str], Optional[str]]:
    """Download audio from YouTube."""
    if not check_yt_dlp():
        return None, "yt-dlp not installed. Run: pip install yt-dlp"
    if not youtube_url or not youtube_url.strip():
        return None, "YouTube URL is empty"
    try:
        cmd = [
            "yt-dlp",
            "-f", "bestaudio[ext=m4a]/bestaudio",
            "--extract-audio",
            "--audio-format", "m4a",
            "--audio-quality", "0",
            "--no-check-certificate",
            "--no-warnings",
            "-o", output_path,
            youtube_url.strip()
        ]
        try:
            node_check = subprocess.run(["which", "node"], capture_output=True, text=True)
            if node_check.returncode != 0:
                cmd.extend(["--extractor-args", "youtube:player_client=web"])
        except:
            pass
        if progress_callback:
            progress_callback(15, "Downloading from YouTube...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            error_msg = result.stderr[:500]
            if "JavaScript runtime" in error_msg:
                error_msg += "\n\n💡 Tip: Install Node.js or run: pip install yt-dlp --upgrade"
            return None, f"yt-dlp failed: {error_msg}"
        if os.path.exists(output_path):
            return output_path, None
        base = output_path.rsplit('.', 1)[0]
        for ext in ['.m4a', '.mp3', '.webm', '.opus']:
            alt_path = base + ext
            if os.path.exists(alt_path):
                return alt_path, None
        return None, "Download completed but file not found"
    except subprocess.TimeoutExpired:
        return None, "Download timed out after 10 minutes"
    except Exception as e:
        return None, f"Error: {str(e)}"

# =============================================================================
# MAIN VIDEO PROCESSING (process_single_video)
# =============================================================================

def process_single_video(url: str, custom_id: Optional[str] = None,
                        source_type: str = "unknown",
                        progress_bar=None, status_text=None,
                        overall_progress: Tuple[int, int] = (0, 1)) -> Dict[str, Any]:
    """
    Process a single video: download (if needed), transcribe, segment, index.
    """
    result = {
        "url": url,
        "video_id": None,
        "status": "pending",
        "segments_count": 0,
        "error": None,
        "index_status": None,
        "source_url": url,
        "source_type": source_type,
        "url_stored": False
    }
    try:
        url_type = detect_url_type(url)
        if url_type == "unknown":
            result["status"] = "failed"
            result["error"] = "Unknown URL type. Must be YouTube or direct media URL."
            return result
        video_id = custom_id.strip() if custom_id else generate_video_id(f"batch_{url}")
        result["video_id"] = video_id
        current, total = overall_progress
        base_progress = int((current / total) * 100) if progress_bar else 0
        if status_text:
            status_text.text(f"[{current}/{total}] Processing: {video_id}")
        media_url = None
        if url_type == "youtube":
            if not check_yt_dlp():
                result["status"] = "failed"
                result["error"] = "yt-dlp not installed"
                return result
            import tempfile
            with tempfile.TemporaryDirectory() as tmpdir:
                if status_text:
                    status_text.text(f"[{current}/{total}] Downloading from YouTube...")
                output_path = f"{tmpdir}/youtube_{video_id}.m4a"
                downloaded_path, error = download_youtube_audio(url.strip(), output_path)
                if error:
                    result["status"] = "failed"
                    result["error"] = f"Download failed: {error}"
                    return result
                with open(downloaded_path, 'rb') as f:
                    file_bytes = f.read()
                blob_name = f"batch_youtube_{video_id}_{int(time.time())}.m4a"
                if status_text:
                    status_text.text(f"[{current}/{total}] Uploading to Azure...")
                sas_url, error = upload_to_azure_blob_sdk(file_bytes, blob_name)
                if error and ("not installed" in error or "SDK" in error):
                    sas_url, error = upload_to_azure_blob_fixed(file_bytes, blob_name)
                if error:
                    result["status"] = "failed"
                    result["error"] = f"Upload failed: {error}"
                    return result
                media_url = sas_url
        elif url_type == "direct":
            media_url = url.strip()
            if status_text:
                status_text.text(f"[{current}/{total}] Using direct URL...")
        if not media_url:
            result["status"] = "failed"
            result["error"] = "No media URL available"
            return result
        if status_text:
            status_text.text(f"[{current}/{total}] Submitting to Speech API...")
        submit_result = submit_transcription_direct(video_id, media_url)
        operation_url = submit_result.get("operation_url")
        if not operation_url:
            result["status"] = "failed"
            result["error"] = "No operation URL returned"
            return result
        max_polls = 120
        transcription_data = None
        for i in range(max_polls):
            time.sleep(POLL_SECONDS)
            poll_result = poll_transcription_operation(operation_url)
            status = poll_result.get("status", "unknown")
            if progress_bar:
                poll_progress = min(int((i / max_polls) * 20), 20)
                overall = base_progress + int((1 / total) * 80) + int((1 / total) * poll_progress)
                progress_bar.progress(min(overall, 99))
            if status.lower() == "succeeded":
                transcription_data = get_transcription_from_result(poll_result)
                break
            elif status.lower() == "failed":
                error_msg = poll_result.get("properties", {}).get("error", {}).get("message", "Unknown error")
                result["status"] = "failed"
                result["error"] = f"Transcription failed: {error_msg}"
                return result
        if not transcription_data:
            result["status"] = "failed"
            result["error"] = "Transcription timed out"
            return result
        if status_text:
            status_text.text(f"[{current}/{total}] Processing segments...")
        segments = process_transcription_to_segments(transcription_data, video_id)
        result["segments_count"] = len(segments)
        save_segments_to_blob(video_id, segments)
        try:
            index_result = index_segments_direct(
                video_id,
                segments,
                source_url=url,
                source_type=source_type
            )
            result["url_stored"] = index_result.get('source_url_stored', False)
            result["index_status"] = f"Indexed {index_result.get('indexed', 0)} documents"
            st.session_state['debug_info'][video_id] = {
                'url_fields_available': index_result.get('url_fields_available', {}),
                'source_url_stored': index_result.get('source_url_stored', False),
                'source_type_stored': index_result.get('source_type_stored', False)
            }
        except Exception as e:
            result["index_status"] = f"Indexing failed: {str(e)}"
        result["status"] = "success"
    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        import traceback
        result["error"] += f"\n{traceback.format_exc()}"
    return result

# =============================================================================
# IMPORT PAGE MODULES (after utilities are defined)
# =============================================================================

import upload_transcribe
import manage_videos
import system_diagnostics

# =============================================================================
# SIDEBAR NAVIGATION
# =============================================================================

with st.sidebar:
    st.header("Navigation")
    page = st.radio("Select Page", [
        "🔎 Search Segments", 
        "⬆️ Upload & Transcribe",
        "📚 Manage Videos",
        "⚙️ System Diagnostics"
    ])
    
    # Settings for search page
    if page == "🔎 Search Segments":
        st.header("Search Settings")
        mode = st.selectbox("Search Mode", ["keyword", "hybrid", "vector"], 
                          index=["keyword", "hybrid", "vector"].index(DEFAULT_MODE) if DEFAULT_MODE in ("keyword", "hybrid", "vector") else 1)
        top = st.slider("Results", 1, 50, DEFAULT_TOP)
        k = st.slider("Vector k", 5, 200, DEFAULT_K)
    
    # Quick actions
    st.markdown("---")
    if st.button("🔄 Refresh Schema Cache"):
        st.session_state.index_schema_cache = None
        st.session_state.url_fields_status = None
        st.success("Cache cleared! Navigate to System Diagnostics to refresh.")
    
    st.markdown("---")
    st.caption("Video Annotation Platform v2.1")
    st.caption("With URL Tracking")

# =============================================================================
# PAGE ROUTING
# =============================================================================

if page == "🔎 Search Segments":
    # -------------------------------------------------------------------------
    # SEARCH SEGMENTS PAGE (embedded)
    # -------------------------------------------------------------------------
    st.header("Search Indexed Video Segments")
    
    if not SEARCH_FN_URL:
        st.error("SEARCH_FN_URL not configured. Cannot search.")
        st.info("Please set SEARCH_FN_URL environment variable.")
    else:
        col1, col2 = st.columns([3, 1])
        with col1:
            q = st.text_input("Query", placeholder="e.g., measles vaccine side effects")
        with col2:
            video_id_filter = st.text_input("Filter by video_id (optional)")
        
        if st.button("Search", type="primary", disabled=(not q.strip())):
            try:
                payload = {"q": q.strip(), "mode": mode, "top": top}
                if mode in ("hybrid", "vector"):
                    payload["k"] = k
                if video_id_filter.strip():
                    payload["video_id"] = video_id_filter.strip()
                
                with st.spinner("Searching..."):
                    data = call_api(SEARCH_FN_URL, payload)
                
                hits = data.get("hits", [])
                st.caption(f"Found {data.get('count', 0)} total | Showing {len(hits)}")
                
                for i, h in enumerate(hits, start=1):
                    start_ms, end_ms = h.get("start_ms", 0), h.get("end_ms", 0)
                    vid, seg, score = h.get("video_id", ""), h.get("segment_id", ""), h.get("score")
                    
                    source_url = h.get('source_url', '')
                    source_type = h.get('source_type', '')
                    url_indicator = ""
                    
                    if source_url:
                        url_indicator = f" | 🔗 {source_type}: {source_url[:40]}..."
                    elif source_type and source_type != 'unknown':
                        url_indicator = f" | 📁 {source_type}"
                    
                    header = f"{i}. {vid} | {ms_to_ts(start_ms)}–{ms_to_ts(end_ms)}{url_indicator}"
                    if seg:
                        header += f" | seg={seg}"
                    if score is not None:
                        header += f" | score={score:.3f}" if isinstance(score, (int, float)) else f" | score={score}"
                    
                    with st.expander(header, expanded=(i <= 3)):
                        st.write(h.get("text", ""))
                        if h.get("pred_labels"):
                            st.caption(f"Labels: {', '.join(h['pred_labels'])}")
                        if source_url:
                            st.caption(f"**Source:** [{source_url}]({source_url})")
                            st.caption(f"**Type:** {source_type}")
            
            except Exception as e:
                st.error(f"Search failed: {e}")

elif page == "⬆️ Upload & Transcribe":
    upload_transcribe.show_upload_transcribe_page()

elif page == "📚 Manage Videos":
    manage_videos.show_manage_videos_page()

elif page == "⚙️ System Diagnostics":
    system_diagnostics.show_system_diagnostics_page()