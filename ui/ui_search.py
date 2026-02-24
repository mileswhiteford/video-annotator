"""
ui_search.py - Streamlit Web Interface for Video Segment Search & Upload

Features:
- Direct Azure Speech API integration (bypasses Azure Function)
- URL tracking for all processed videos (source_url, source_type, processed_at)
- Handles existing videos without URL data gracefully
- Batch processing with CSV upload
- Video management interface with filtering and deletion
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
    'debug_info': {}
}

for key, value in session_state_defaults.items():
    if key not in st.session_state:
        st.session_state[key] = value

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
    
    # Check URL field availability
    url_fields_available = {
        'source_url': 'source_url' in available_fields,
        'source_type': 'source_type' in available_fields,
        'processed_at': 'processed_at' in available_fields
    }
    
    # Generate embeddings
    texts = [seg.get("text", "") for seg in segments]
    try:
        embeddings = get_embeddings(texts)
    except Exception as e:
        st.warning(f"Embedding failed, indexing without vectors: {e}")
        embeddings = [None] * len(segments)
    
    # Prepare documents
    documents = []
    processed_timestamp = datetime.utcnow().isoformat() + "Z"
    
    for i, (seg, embedding) in enumerate(zip(segments, embeddings)):
        safe_video_id = sanitize_id(video_id)
        doc_id = f"{safe_video_id}_{i}"
        
        doc = {"@search.action": "upload", key_field: doc_id}
        
        # Core fields
        field_mappings = {
            "video_id": safe_video_id,
            "segment_id": str(seg.get("segment_id", i)),
            "text": str(seg.get("text", "")),
            "start_ms": int(seg.get("start_ms", 0)),
            "end_ms": int(seg.get("end_ms", 0)),
            "pred_labels": seg.get("pred_labels", []) if seg.get("pred_labels") else []
        }
        
        # URL tracking fields
        if url_fields_available['source_url']:
            field_mappings["source_url"] = str(source_url) if source_url else ""
        if url_fields_available['source_type']:
            field_mappings["source_type"] = str(source_type) if source_type else "unknown"
        if url_fields_available['processed_at']:
            field_mappings["processed_at"] = processed_timestamp
        
        # Only add existing fields
        for field_name, value in field_mappings.items():
            if field_name in available_fields:
                doc[field_name] = value
        
        # Handle embedding
        embedding_field = next((f for f in ["embedding", "embeddings", "vector", "vectors"] if f in available_fields), None)
        if embedding and embedding_field:
            try:
                doc[embedding_field] = [float(x) for x in embedding]
            except (ValueError, TypeError):
                pass
        
        documents.append(doc)
    
    # Upload to search index
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
        
        # Check for partial failures
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
# VIDEO RETRIEVAL AND MANAGEMENT
# =============================================================================

def get_stored_videos(video_id: str = None, source_type: str = None, 
                     include_missing: bool = True, limit: int = 1000) -> List[Dict]:
    """
    Retrieve videos from search index with URL data.
    """
    if not SEARCH_ENDPOINT or not SEARCH_KEY:
        return []
    
    url = f"{SEARCH_ENDPOINT}/indexes/{SEARCH_INDEX_NAME}/docs/search?api-version=2024-07-01"
    headers = {"api-key": SEARCH_KEY, "Content-Type": "application/json"}
    
    # Build filter
    filters = []
    if video_id:
        filters.append(f"video_id eq '{video_id}'")
    if source_type:
        filters.append(f"source_type eq '{source_type}'")
    
    filter_query = " and ".join(filters) if filters else None
    
    # Get available fields
    schema = get_index_schema()
    available_fields = {f['name'] for f in schema.get('fields', [])}
    
    # Build select
    select_fields = ["video_id"]
    for field in ["source_url", "source_type", "processed_at"]:
        if field in available_fields:
            select_fields.append(field)
    
    payload = {
        "search": "*",
        "select": ",".join(select_fields),
        "top": limit
    }
    
    if "processed_at" in available_fields:
        payload["orderby"] = "processed_at desc"
    if filter_query:
        payload["filter"] = filter_query
    
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        docs = r.json().get("value", [])
        
        # Deduplicate and normalize
        seen = set()
        unique_docs = []
        for doc in docs:
            vid = doc.get('video_id')
            if vid and vid not in seen:
                seen.add(vid)
                # Normalize missing values
                doc['source_type'] = doc.get('source_type') or 'unknown'
                doc['source_url'] = doc.get('source_url') or ''
                doc['processed_at'] = doc.get('processed_at') or 'unknown'
                unique_docs.append(doc)
        
        return unique_docs
        
    except Exception as e:
        st.error(f"Failed to retrieve videos: {e}")
        return []


def delete_video_by_id(video_id: str) -> bool:
    """Delete all segments for a video_id from the index."""
    if not SEARCH_ENDPOINT or not SEARCH_KEY:
        return False
    
    # Find all documents
    search_url = f"{SEARCH_ENDPOINT}/indexes/{SEARCH_INDEX_NAME}/docs/search?api-version=2024-07-01"
    headers = {"api-key": SEARCH_KEY, "Content-Type": "application/json"}
    
    payload = {
        "search": "*",
        "filter": f"video_id eq '{video_id}'",
        "select": "video_id",
        "top": 1000
    }
    
    try:
        r = requests.post(search_url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        docs = r.json().get("value", [])
        
        if not docs:
            return False
        
        # Delete documents
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
            return False
        
        delete_url = f"{SEARCH_ENDPOINT}/indexes/{SEARCH_INDEX_NAME}/docs/index?api-version=2024-07-01"
        r = requests.post(delete_url, headers=headers, json={"value": delete_docs}, timeout=60)
        r.raise_for_status()
        
        return True
        
    except Exception as e:
        st.error(f"Delete failed: {e}")
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
        
        # Handle missing Node.js
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
        
        # Find downloaded file
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
# MAIN VIDEO PROCESSING
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
        # Validate URL
        url_type = detect_url_type(url)
        if url_type == "unknown":
            result["status"] = "failed"
            result["error"] = "Unknown URL type. Must be YouTube or direct media URL."
            return result
        
        # Generate video ID
        video_id = custom_id.strip() if custom_id else generate_video_id(f"batch_{url}")
        result["video_id"] = video_id
        
        current, total = overall_progress
        base_progress = int((current / total) * 100) if progress_bar else 0
        
        if status_text:
            status_text.text(f"[{current}/{total}] Processing: {video_id}")
        
        media_url = None
        
        # Handle YouTube
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
                
                # Try SDK first, fallback to REST
                sas_url, error = upload_to_azure_blob_sdk(file_bytes, blob_name)
                if error and ("not installed" in error or "SDK" in error):
                    sas_url, error = upload_to_azure_blob_fixed(file_bytes, blob_name)
                
                if error:
                    result["status"] = "failed"
                    result["error"] = f"Upload failed: {error}"
                    return result
                
                media_url = sas_url
        
        # Handle Direct URL
        elif url_type == "direct":
            media_url = url.strip()
            if status_text:
                status_text.text(f"[{current}/{total}] Using direct URL...")
        
        if not media_url:
            result["status"] = "failed"
            result["error"] = "No media URL available"
            return result
        
        # Submit to Speech API
        if status_text:
            status_text.text(f"[{current}/{total}] Submitting to Speech API...")
        
        submit_result = submit_transcription_direct(video_id, media_url)
        operation_url = submit_result.get("operation_url")
        
        if not operation_url:
            result["status"] = "failed"
            result["error"] = "No operation URL returned"
            return result
        
        # Poll for completion
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
        
        # Process and index
        if status_text:
            status_text.text(f"[{current}/{total}] Processing segments...")
        
        segments = process_transcription_to_segments(transcription_data, video_id)
        result["segments_count"] = len(segments)
        
        # Save to blob
        save_segments_to_blob(video_id, segments)
        
        # Index with URL tracking
        try:
            index_result = index_segments_direct(
                video_id,
                segments,
                source_url=url,
                source_type=source_type
            )
            
            result["url_stored"] = index_result.get('source_url_stored', False)
            result["index_status"] = f"Indexed {index_result.get('indexed', 0)} documents"
            
            # Debug info
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
# PAGE 1: SEARCH SEGMENTS
# =============================================================================

if page == "🔎 Search Segments":
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
                    
                    # Show URL info if available
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


# =============================================================================
# PAGE 2: UPLOAD & TRANSCRIBE
# =============================================================================

elif page == "⬆️ Upload & Transcribe":
    st.header("Upload Video for Transcription")
    
    # Check URL fields status
    url_status = check_url_fields_status()
    
    if url_status['fields_exist']:
        st.success("✅ URL Tracking Enabled - Original source URLs will be stored")
    else:
        st.warning(f"""
        ⚠️ **Partial URL Tracking** - Missing fields: {', '.join(url_status['missing_fields'])}
        
        Videos will still be processed, but URL information will be limited.
        Add missing fields to your Azure Search index for full functionality.
        """)
    
    # Check Azure configuration
    azure_configured = bool(AZURE_STORAGE_KEY) and bool(SPEECH_KEY)
    if not azure_configured:
        st.error("⚠️ Azure Storage and Speech keys required. Check .env file.")
    
    # Source selection
    source_type = st.radio("Select Source", 
                          ["File Upload", "Direct URL", "YouTube", "📁 Batch CSV Upload"],
                          horizontal=True)
    
    media_url = None
    video_id = None
    file_bytes = None
    yt_url = None
    csv_df = None
    detected_source_type = "unknown"
    
    # --- File Upload ---
    if source_type == "File Upload":
        if not azure_configured:
            st.info("Please configure Azure Storage to enable file upload")
        else:
            uploaded_file = st.file_uploader(
                "Choose video/audio file",
                type=["mp4", "avi", "mov", "mkv", "m4a", "mp3", "wav"],
                accept_multiple_files=False
            )
            
            if uploaded_file:
                st.success(f"📁 {uploaded_file.name} ({uploaded_file.size / 1024 / 1024:.1f} MB)")
                file_bytes = uploaded_file.getvalue()
                video_id = generate_video_id(uploaded_file.name)
                detected_source_type = "upload"
                st.info("File ready for upload")
    
    # --- Direct URL ---
    elif source_type == "Direct URL":
        url_input = st.text_input("Media URL", placeholder="https://tulane.box.com/shared/static/ ...")
        
        if url_input.strip():
            media_url = url_input.strip()
            video_id = generate_video_id(url_input)
            detected_source_type = "direct"
            st.success("✅ URL validated")
    
    # --- YouTube ---
    elif source_type == "YouTube":
        yt_url = st.text_input(
            "YouTube URL",
            placeholder="https://youtube.com/watch?v= ...",
            value=st.session_state.yt_url_value,
            key="yt_url_input"
        )
        
        # Update session state
        if yt_url != st.session_state.yt_url_value:
            st.session_state.yt_url_value = yt_url
            try:
                st.rerun()
            except:
                pass
        
        # Check yt-dlp
        if not check_yt_dlp():
            st.warning("yt-dlp not installed")
            if st.button("Install yt-dlp"):
                with st.spinner("Installing..."):
                    subprocess.run(["pip", "install", "-q", "yt-dlp"])
                try:
                    st.rerun()
                except:
                    st.info("Please refresh the page")
        elif yt_url and yt_url.strip():
            video_id = generate_video_id(f"yt_{yt_url.strip()}")
            detected_source_type = "youtube"
            st.success("YouTube URL ready")
    
    # --- Batch CSV Upload ---
    elif source_type == "📁 Batch CSV Upload":
        st.subheader("📁 Batch Process Videos from CSV")
        
        csv_file = st.file_uploader(
            "Upload CSV file",
            type=["csv"],
            help="CSV must contain a column with video URLs"
        )
        
        if csv_file:
            try:
                # Read CSV with flexible parsing
                try:
                    csv_df = pd.read_csv(csv_file)
                except Exception:
                    csv_file.seek(0)
                    csv_df = pd.read_csv(csv_file, header=None)
                    csv_df.columns = [f"column_{i}" for i in range(len(csv_df.columns))]
                
                # Handle case where column names are URLs
                url_like_columns = []
                for col in csv_df.columns:
                    col_str = str(col).strip()
                    if detect_url_type(col_str) != "unknown":
                        url_like_columns.append(col)
                
                if url_like_columns and len(csv_df.columns) == 1:
                    url_col_name = csv_df.columns[0]
                    new_row = {url_col_name: url_col_name}
                    csv_df = pd.concat([pd.DataFrame([new_row]), csv_df], ignore_index=True)
                
                st.success(f"✅ Loaded CSV with {len(csv_df)} rows")
                
                # Column selection
                url_column = st.selectbox("Select column containing video URLs", options=csv_df.columns.tolist())
                
                id_column_options = ["Auto-generate"] + [c for c in csv_df.columns if c != url_column]
                id_column = st.selectbox("Select column for custom Video ID (optional)", options=id_column_options, index=0)
                
                # Extract and validate URLs
                urls_raw = csv_df[url_column].dropna().astype(str).tolist()
                urls_to_process = [u.strip() for u in urls_raw if u.strip()]
                
                # Preview
                with st.expander(f"Preview URLs ({len(urls_to_process)} found)"):
                    for i, url in enumerate(urls_to_process[:10], 1):
                        url_type = detect_url_type(url)
                        icon = "🎬" if url_type == "youtube" else "📄" if url_type == "direct" else "❓"
                        st.text(f"{i}. {icon} {url[:80]}...")
                
                # Validate
                valid_urls = []
                invalid_urls = []
                for url in urls_to_process:
                    url_type = detect_url_type(str(url))
                    if url_type in ["youtube", "direct"]:
                        valid_urls.append(url)
                    else:
                        invalid_urls.append(url)
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Total", len(urls_to_process))
                col2.metric("✅ Valid", len(valid_urls))
                col3.metric("❌ Invalid", len(invalid_urls))
                
                # Store in session state
                st.session_state['batch_urls'] = valid_urls
                st.session_state['batch_df'] = csv_df
                st.session_state['batch_url_column'] = url_column
                st.session_state['batch_id_column'] = id_column
                
            except Exception as e:
                st.error(f"Error reading CSV: {e}")
                import traceback
                st.error(traceback.format_exc())
    
    # Custom ID input
    custom_id = st.text_input("Custom Video ID (optional)")
    if custom_id.strip() and source_type != "📁 Batch CSV Upload":
        video_id = custom_id.strip()
    
    # Determine if we can process
    can_process = False
    if source_type == "File Upload":
        can_process = file_bytes is not None and azure_configured
    elif source_type == "Direct URL":
        can_process = media_url is not None and len(str(media_url).strip()) > 0
    elif source_type == "YouTube":
        yt_url_to_check = st.session_state.get('yt_url_value', '')
        can_process = len(str(yt_url_to_check).strip()) > 0 and check_yt_dlp()
    elif source_type == "📁 Batch CSV Upload":
        can_process = (st.session_state.get('batch_urls') and 
                      len(st.session_state.get('batch_urls', [])) > 0 and 
                      azure_configured and
                      not st.session_state.get('batch_processing', False))
    
    # Process button
    button_text = "🚀 Start Transcription"
    if source_type == "📁 Batch CSV Upload":
        count = len(st.session_state.get('batch_urls', []))
        button_text = f"🚀 Process {count} Videos from CSV"
    
    if st.button(button_text, type="primary", disabled=not can_process):
        
        # --- BATCH PROCESSING ---
        if source_type == "📁 Batch CSV Upload":
            st.session_state.batch_processing = True
            st.session_state.batch_results = []
            
            urls = st.session_state.get('batch_urls', [])
            csv_df = st.session_state.get('batch_df')
            url_column = st.session_state.get('batch_url_column')
            id_column = st.session_state.get('batch_id_column')
            
            total = len(urls)
            st.info(f"Starting batch processing of {total} videos...")
            
            # Progress UI
            overall_progress = st.progress(0)
            status_text = st.empty()
            results_container = st.container()
            
            results = []
            for idx, url in enumerate(urls, 1):
                # Get custom ID if specified
                custom_vid_id = None
                if id_column != "Auto-generate":
                    row = csv_df[csv_df[url_column] == url]
                    if not row.empty:
                        custom_vid_id = str(row[id_column].iloc[0])
                        custom_vid_id = re.sub(r'[^\w\s-]', '', custom_vid_id).strip().replace(' ', '_')[:50]
                
                # Detect source type
                url_type = detect_url_type(url)
                src_type = "youtube" if url_type == "youtube" else "direct"
                
                # Process
                result = process_single_video(
                    url=url,
                    custom_id=custom_vid_id,
                    source_type=src_type,
                    progress_bar=overall_progress,
                    status_text=status_text,
                    overall_progress=(idx, total)
                )
                
                results.append(result)
                st.session_state.batch_results = results
                
                # Update progress
                progress_pct = int((idx / total) * 100)
                overall_progress.progress(progress_pct)
                
                # Show result
                with results_container:
                    if result['status'] == 'success':
                        url_stored = "✅ URL saved" if result.get('url_stored') else "⚠️ URL not stored"
                        st.success(f"✅ [{idx}/{total}] {result['video_id']}: {result['segments_count']} segments ({url_stored})")
                    else:
                        error_msg = result.get('error', 'Unknown error')[:200]
                        st.error(f"❌ [{idx}/{total}] Failed: {error_msg}...")
                
                time.sleep(1)  # Rate limiting
            
            # Final summary
            overall_progress.progress(100)
            status_text.text("Batch processing complete!")
            
            successful = [r for r in results if r['status'] == 'success']
            failed = [r for r in results if r['status'] == 'failed']
            
            st.markdown("---")
            st.subheader("📊 Batch Processing Summary")
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Total", total)
            col2.metric("Successful", len(successful), f"{len(successful)/total*100:.1f}%" if total > 0 else "0%")
            col3.metric("Failed", len(failed), f"{len(failed)/total*100:.1f}%" if total > 0 else "0%")
            
            # Detailed results
            with st.expander("View Detailed Results"):
                results_df = pd.DataFrame([
                    {
                        'Video ID': r['video_id'],
                        'URL': r['url'][:50] + "..." if len(r['url']) > 50 else r['url'],
                        'Source Type': r.get('source_type', 'unknown'),
                        'Status': r['status'],
                        'Segments': r.get('segments_count', 0),
                        'URL Stored': r.get('url_stored', False),
                        'Indexing': r.get('index_status', 'N/A'),
                        'Error': (r.get('error', '')[:100] + '...') if r.get('error') else ''
                    }
                    for r in results
                ])
                st.dataframe(results_df)
                
                # Download results
                csv_buffer = io.StringIO()
                results_df.to_csv(csv_buffer, index=False)
                st.download_button("Download Results CSV", csv_buffer.getvalue(), "batch_results.csv", "text/csv")
            
            # Search hint
            if successful:
                st.info("💡 **Search processed videos:**")
                video_ids = [r['video_id'] for r in successful[:5]]
                st.code(f"video_id:({' OR '.join(video_ids)})")
            
            st.session_state.batch_processing = False
        
        # --- SINGLE VIDEO PROCESSING ---
        else:
            progress_bar = st.progress(0)
            status = st.empty()
            
            try:
                # Upload file if needed
                if source_type == "File Upload" and file_bytes:
                    progress_bar.progress(10)
                    status.text("Uploading to Azure Blob...")
                    
                    blob_name = f"upload_{video_id}_{int(time.time())}.m4a"
                    
                    sas_url, error = upload_to_azure_blob_sdk(file_bytes, blob_name)
                    if error and ("not installed" in error or "SDK" in error):
                        sas_url, error = upload_to_azure_blob_fixed(file_bytes, blob_name)
                    
                    if error:
                        raise Exception(error)
                    
                    media_url = sas_url
                    progress_bar.progress(50)
                
                # Download YouTube if needed
                elif source_type == "YouTube":
                    yt_url = st.session_state.get('yt_url_value', '')
                    
                    if not yt_url or not yt_url.strip():
                        raise Exception("YouTube URL is empty")
                    
                    import tempfile
                    with tempfile.TemporaryDirectory() as tmpdir:
                        progress_bar.progress(10)
                        status.text("Downloading from YouTube...")
                        
                        output_path = f"{tmpdir}/youtube_{video_id}.m4a"
                        downloaded_path, error = download_youtube_audio(yt_url.strip(), output_path)
                        
                        if error:
                            raise Exception(error)
                        
                        progress_bar.progress(50)
                        status.text("Uploading to Azure Blob...")
                        
                        with open(downloaded_path, 'rb') as f:
                            file_bytes = f.read()
                        
                        blob_name = f"youtube_{video_id}_{int(time.time())}.m4a"
                        
                        sas_url, error = upload_to_azure_blob_sdk(file_bytes, blob_name)
                        if error and ("not installed" in error):
                            sas_url, error = upload_to_azure_blob_fixed(file_bytes, blob_name)
                        
                        if error:
                            raise Exception(error)
                        
                        media_url = sas_url
                        progress_bar.progress(75)
                
                if not media_url:
                    raise Exception("No media URL available")
                
                # Transcribe
                status.text("Submitting to Azure Speech-to-Text...")
                result = submit_transcription_direct(video_id, media_url)
                operation_url = result.get("operation_url")
                
                if not operation_url:
                    raise Exception("No operation URL returned")
                
                # Poll
                max_polls = 120
                transcription_data = None
                
                for i in range(max_polls):
                    time.sleep(POLL_SECONDS)
                    poll_result = poll_transcription_operation(operation_url)
                    status_text = poll_result.get("status", "unknown")
                    
                    progress = min(75 + int((i / max_polls) * 20), 95)
                    progress_bar.progress(progress)
                    status.text(f"Transcribing... ({i * POLL_SECONDS // 60} min) - Status: {status_text}")
                    
                    if status_text.lower() == "succeeded":
                        transcription_data = get_transcription_from_result(poll_result)
                        break
                    elif status_text.lower() == "failed":
                        raise Exception(f"Transcription failed: {poll_result.get('properties', {}).get('error', {}).get('message', 'Unknown error')}")
                
                if not transcription_data:
                    raise Exception("Transcription timed out")
                
                # Process and index
                progress_bar.progress(98)
                status.text("Processing segments and indexing...")
                
                segments = process_transcription_to_segments(transcription_data, video_id)
                
                # Save to blob
                save_segments_to_blob(video_id, segments)
                
                # Index with URL tracking
                original_url = None
                if source_type == "YouTube":
                    original_url = st.session_state.get('yt_url_value', '')
                elif source_type == "Direct URL":
                    original_url = media_url
                elif source_type == "File Upload":
                    original_url = f"uploaded_file://{video_id}"
                
                index_result = index_segments_direct(
                    video_id,
                    segments,
                    source_url=original_url,
                    source_type=detected_source_type
                )
                
                url_stored_msg = "✅ Source URL stored" if index_result.get('source_url_stored') else "⚠️ URL storage not available"
                
                progress_bar.progress(100)
                status.text("Complete!")
                
                st.success(f"""
                ✅ **Transcription Complete!**
                - Video ID: {video_id}
                - Segments: {len(segments)}
                - Source Type: {detected_source_type}
                - Indexed: {index_result.get('indexed', 0)} documents
                - {url_stored_msg}
                """)
                
                if original_url:
                    st.info(f"**Original Source:** [{original_url}]({original_url})")
                
                st.code(f'Search: video_id:{video_id}')
                
                with st.expander("View first 5 segments"):
                    for seg in segments[:5]:
                        st.write(f"**{ms_to_ts(seg['start_ms'])} - {ms_to_ts(seg['end_ms'])}:** {seg['text'][:100]}...")
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                st.exception(e)


# =============================================================================
# PAGE 3: MANAGE VIDEOS
# =============================================================================

elif page == "📚 Manage Videos":
    st.header("📚 Manage Stored Videos")
    st.info("View, search, and manage all processed videos and their source URLs")
    
    if not SEARCH_ENDPOINT or not SEARCH_KEY:
        st.error("Azure Search not configured. Cannot retrieve video list.")
    else:
        # Check URL fields status
        url_status = check_url_fields_status()
        
        if url_status['fields_exist']:
            st.success("✅ URL tracking fields are configured")
        else:
            st.warning(f"⚠️ Missing URL fields: {', '.join(url_status['missing_fields'])}")
        
        # URL coverage analysis
        if st.button("📊 Analyze URL Data Coverage"):
            with st.spinner("Analyzing..."):
                all_videos = get_stored_videos(include_missing=True)
                
                with_urls = [v for v in all_videos if v.get('source_url') and v.get('source_type') not in ['', 'unknown']]
                without_urls = [v for v in all_videos if not v.get('source_url') or v.get('source_type') in ['', 'unknown']]
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Total Videos", len(all_videos))
                col2.metric("✅ With URL Data", len(with_urls), f"{len(with_urls)/len(all_videos)*100:.1f}%" if all_videos else "0%")
                col3.metric("⚠️ Missing URL Data", len(without_urls), f"{len(without_urls)/len(all_videos)*100:.1f}%" if all_videos else "0%")
                
                # By type breakdown
                st.subheader("Breakdown by Source Type")
                type_counts = {}
                for v in all_videos:
                    t = v.get('source_type') or 'unknown'
                    type_counts[t] = type_counts.get(t, 0) + 1
                
                cols = st.columns(len(type_counts) if type_counts else 1)
                for i, (stype, count) in enumerate(sorted(type_counts.items())):
                    icon = "🎬" if stype == "youtube" else "📄" if stype == "direct" else "📁" if stype == "upload" else "❓"
                    cols[i % len(cols)].metric(f"{icon} {stype}", count)
                
                if without_urls:
                    with st.expander(f"Videos without URL data ({len(without_urls)})"):
                        st.info("These were likely processed before URL tracking was enabled")
                        for v in without_urls[:20]:
                            st.text(f"• {v.get('video_id')}")
        
        st.markdown("---")
        
        # Filters
        st.subheader("Filter Videos")
        col1, col2 = st.columns(2)
        
        with col1:
            filter_video_id = st.text_input("Filter by Video ID (optional)")
        with col2:
            filter_options = ["All", "With URL Data Only", "Missing URL Data Only", "youtube", "direct", "upload", "unknown"]
            filter_source_type = st.selectbox("Filter by Source Type", options=filter_options, index=0)
        
        # Load videos
        if st.button("🔍 Load Videos", type="primary"):
            with st.spinner("Retrieving videos..."):
                
                # Handle special filters
                if filter_source_type == "Missing URL Data Only":
                    all_videos = get_stored_videos(include_missing=True)
                    videos = [v for v in all_videos if not v.get('source_url') or v.get('source_type') in ['', 'unknown']]
                    if filter_video_id.strip():
                        videos = [v for v in videos if filter_video_id.strip().lower() in v.get('video_id', '').lower()]
                elif filter_source_type == "With URL Data Only":
                    all_videos = get_stored_videos(include_missing=True)
                    videos = [v for v in all_videos if v.get('source_url') and v.get('source_type') not in ['', 'unknown']]
                    if filter_video_id.strip():
                        videos = [v for v in videos if filter_video_id.strip().lower() in v.get('video_id', '').lower()]
                else:
                    source_type = None if filter_source_type == "All" else filter_source_type
                    videos = get_stored_videos(
                        video_id=filter_video_id if filter_video_id.strip() else None,
                        source_type=source_type,
                        include_missing=True,
                        limit=1000
                    )
                
                st.session_state.stored_videos_cache = videos
                st.success(f"Found {len(videos)} videos")
        
        # Display videos
        if st.session_state.stored_videos_cache:
            videos = st.session_state.stored_videos_cache
            
            # Metrics
            st.markdown("---")
            cols = st.columns(4)
            
            type_counts = {}
            for v in videos:
                t = v.get('source_type') or 'unknown'
                type_counts[t] = type_counts.get(t, 0) + 1
            
            cols[0].metric("Total", len(videos))
            cols[1].metric("YouTube", type_counts.get('youtube', 0))
            cols[2].metric("Direct", type_counts.get('direct', 0))
            cols[3].metric("Upload", type_counts.get('upload', 0))
            
            # Group by type
            st.markdown("---")
            st.subheader("Video List")
            
            videos_by_type = {}
            for v in videos:
                stype = v.get('source_type') or 'unknown'
                if stype not in videos_by_type:
                    videos_by_type[stype] = []
                videos_by_type[stype].append(v)
            
            # Display by category
            for source_type in ['youtube', 'direct', 'upload', 'unknown']:
                if source_type not in videos_by_type:
                    continue
                
                type_videos = videos_by_type[source_type]
                icon = "🎬" if source_type == "youtube" else "📄" if source_type == "direct" else "📁" if source_type == "upload" else "❓"
                
                with st.expander(f"{icon} {source_type.upper()} ({len(type_videos)} videos)", expanded=(source_type == 'youtube')):
                    for i, video in enumerate(type_videos, 1):
                        vid = video.get('video_id', 'unknown')
                        src_url = video.get('source_url', '')
                        processed = video.get('processed_at', 'unknown')
                        
                        has_url = bool(src_url)
                        status_icon = "✅" if has_url else "⚠️"
                        
                        with st.container():
                            cols = st.columns([4, 1])
                            
                            with cols[0]:
                                st.write(f"**{status_icon} {i}. {vid}**")
                                st.caption(f"Processed: {processed}")
                                
                                if src_url:
                                    display_url = src_url[:80] + "..." if len(str(src_url)) > 80 else src_url
                                    st.code(display_url)
                                    if str(src_url).startswith('http'):
                                        st.markdown(f"[Open Source ↗]({src_url})")
                                else:
                                    st.warning("No source URL stored")
                            
                            with cols[1]:
                                if st.button(f"🗑️ Delete", key=f"del_{vid}_{i}_{source_type}"):
                                    if delete_video_by_id(vid):
                                        st.success(f"Deleted {vid}")
                                        st.session_state.stored_videos_cache = [
                                            v for v in videos if v.get('video_id') != vid
                                        ]
                                        try:
                                            st.rerun()
                                        except:
                                            pass
                            
                            st.markdown("---")
            
            # Export
            st.markdown("---")
            if st.button("📥 Export to CSV"):
                export_df = pd.DataFrame([
                    {
                        'video_id': v.get('video_id'),
                        'source_type': v.get('source_type') or 'unknown',
                        'source_url': v.get('source_url', ''),
                        'has_url_data': bool(v.get('source_url')),
                        'processed_at': v.get('processed_at', 'unknown')
                    }
                    for v in videos
                ])
                
                csv_buffer = io.StringIO()
                export_df.to_csv(csv_buffer, index=False)
                st.download_button("Download CSV", csv_buffer.getvalue(), "video_list.csv", "text/csv")


# =============================================================================
# PAGE 4: SYSTEM DIAGNOSTICS
# =============================================================================

elif page == "⚙️ System Diagnostics":
    st.header("⚙️ System Diagnostics")
    st.info("Check system configuration and troubleshoot issues")
    
    # Configuration status
    st.subheader("Configuration Status")
    
    config_checks = {
        "Azure Speech (SPEECH_KEY)": bool(SPEECH_KEY),
        "Azure OpenAI (AZURE_OPENAI_KEY)": bool(AZURE_OPENAI_KEY),
        "Azure Search (SEARCH_KEY)": bool(SEARCH_KEY),
        "Azure Storage (AZURE_STORAGE_KEY)": bool(AZURE_STORAGE_KEY),
        "Search Function (SEARCH_FN_URL)": bool(SEARCH_FN_URL),
        "yt-dlp installed": check_yt_dlp()
    }
    
    cols = st.columns(2)
    for i, (name, status) in enumerate(config_checks.items()):
        icon = "✅" if status else "❌"
        cols[i % 2].write(f"{icon} {name}: {'OK' if status else 'Not configured'}")
    
    # Index schema check
    st.markdown("---")
    st.subheader("Index Schema Check")
    
    if st.button("🔍 Check Index Schema"):
        with st.spinner("Fetching schema..."):
            schema = debug_check_index_schema()
            
            if isinstance(schema, dict):
                st.success(f"Index: {schema['index_name']}")
                st.write(f"Key Field: `{schema['key_field']}`")
                
                # URL fields status
                if schema.get('has_all_url_fields'):
                    st.success("✅ All URL tracking fields present")
                else:
                    st.warning(f"⚠️ Missing fields: {', '.join(schema.get('missing_url_fields', []))}")
                
                # Show all fields
                with st.expander("View all fields"):
                    for field in schema['fields']:
                        key = "🔑" if field['key'] else ""
                        url = "🔗" if 'url' in field['name'].lower() else ""
                        st.caption(f"{key}{url} `{field['name']}` ({field['type']})")
                
                st.session_state.index_schema_cache = schema
            else:
                st.error(f"Schema check failed: {schema}")
    
    # Debug info
    st.markdown("---")
    st.subheader("Debug Information")
    
    with st.expander("Session State"):
        st.json({
            k: str(v)[:100] + "..." if len(str(v)) > 100 else v 
            for k, v in st.session_state.items()
        })
    
    with st.expander("Recent Processing Debug"):
        if st.session_state.get('debug_info'):
            st.json(st.session_state['debug_info'])
        else:
            st.info("No debug info yet. Process a video first.")


# Footer
st.sidebar.markdown("---")
st.sidebar.caption("Video Annotation Platform v2.1")