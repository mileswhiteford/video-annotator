"""
ui_search.py - Streamlit Web Interface for Video Segment Search & Upload

This version calls Azure Speech API DIRECTLY, bypassing the Azure Function
that has the wrong API version hardcoded.
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

# Azure Function URLs (only Search uses these now)
SEARCH_FN_URL = os.environ.get("SEARCH_FN_URL", "")

# Azure Speech Service Configuration (DIRECT)
SPEECH_KEY = os.environ.get("SPEECH_KEY")
SPEECH_REGION = os.environ.get("SPEECH_REGION", "eastus")
SPEECH_API_VERSION = os.environ.get("SPEECH_API_VERSION", "2024-11-15")

# Azure OpenAI & Search for indexing
AZURE_OPENAI_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_KEY = os.environ.get("AZURE_OPENAI_KEY")
AZURE_OPENAI_DEPLOYMENT = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "text-embedding-3-small")

SEARCH_ENDPOINT = os.environ.get("SEARCH_ENDPOINT")
SEARCH_KEY = os.environ.get("SEARCH_KEY")
SEARCH_INDEX_NAME = os.environ.get("SEARCH_INDEX_NAME", "segments")

# Azure Storage Configuration
AZURE_STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT", "storagevideoannotator")
AZURE_STORAGE_KEY = os.environ.get("AZURE_STORAGE_KEY", "")
INPUT_CONTAINER = os.environ.get("INPUT_CONTAINER", "speech-input")
SEGMENTS_CONTAINER = os.environ.get("SEGMENTS_CONTAINER", "segments")

# Default settings
DEFAULT_MODE = os.environ.get("DEFAULT_MODE", "hybrid")
DEFAULT_TOP = int(os.environ.get("DEFAULT_TOP", "10"))
DEFAULT_K = int(os.environ.get("DEFAULT_K", "40"))
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "15"))
BATCH_MAX_WORKERS = int(os.environ.get("BATCH_MAX_WORKERS", "3"))  # Concurrent processing limit

st.set_page_config(page_title="Video Annotation Platform", layout="wide")
st.title(" Video Annotation Platform")

# Initialize session state
if 'yt_url_value' not in st.session_state:
    st.session_state.yt_url_value = ""
if 'batch_results' not in st.session_state:
    st.session_state.batch_results = []
if 'batch_processing' not in st.session_state:
    st.session_state.batch_processing = False
if 'index_schema_cache' not in st.session_state:
    st.session_state.index_schema_cache = None

# Sidebar
with st.sidebar:
    st.header("Navigation")
    page = st.radio("Select Page", ["🔎 Search Segments", "⬆️ Upload & Transcribe"])
    
    if page == "🔎 Search Segments":
        st.header("Settings")
        mode = st.selectbox("Search Mode", ["keyword", "hybrid", "vector"], 
                          index=["keyword", "hybrid", "vector"].index(DEFAULT_MODE) if DEFAULT_MODE in ("keyword", "hybrid", "vector") else 1)
        top = st.slider("Results", 1, 50, DEFAULT_TOP)
        k = st.slider("Vector k", 5, 200, DEFAULT_K)
    
    # Debug section
    st.markdown("---")
    if st.button("🔍 Debug Index Schema"):
        with st.spinner("Fetching index schema..."):
            schema_info = debug_check_index_schema()
            if isinstance(schema_info, dict):
                st.success(f"Index: {schema_info['index_name']}")
                st.write(f"**Key Field:** `{schema_info['key_field']}`")
                st.write("**Fields:**")
                for field in schema_info['fields']:
                    key_badge = "🔑 " if field['key'] else ""
                    st.caption(f"{key_badge}`{field['name']}` ({field['type']})")
                st.session_state.index_schema_cache = schema_info
            else:
                st.error(schema_info)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def ms_to_ts(ms: int) -> str:
    s = max(0, int(ms // 1000))
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def call_api(url: str, payload: dict, timeout: int = 60) -> dict:
    r = requests.post(url, json=payload, timeout=timeout, headers={"Content-Type": "application/json"})
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
    return r.json() if r.text else {}


def sanitize_id(id_string: str) -> str:
    """
    Sanitize ID to be valid for Azure Search (alphanumeric, hyphens, underscores only).
    Document key rules: Cannot start with underscore, max 1024 chars.
    """
    if not id_string:
        id_string = "unknown"
    
    # Replace invalid characters with underscore
    sanitized = re.sub(r'[^\w\-]', '_', str(id_string))
    
    # Ensure it doesn't start with underscore (invalid for Azure Search keys)
    if sanitized.startswith('_'):
        sanitized = 'id' + sanitized
    
    # Ensure it doesn't start with dash (also problematic)
    if sanitized.startswith('-'):
        sanitized = 'id' + sanitized
    
    # Limit length to 1024 characters (Azure Search limit)
    if len(sanitized) > 1024:
        # Use hash to ensure uniqueness while truncating
        hash_suffix = hashlib.md5(sanitized.encode()).hexdigest()[:16]
        sanitized = sanitized[:1000] + "_" + hash_suffix
    
    return sanitized


# =============================================================================
# AZURE SEARCH SCHEMA FUNCTIONS
# =============================================================================

def debug_check_index_schema():
    """Check if your index exists and verify the key field"""
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
            
            for field in schema.get("fields", []):
                field_info = {
                    "name": field.get("name"),
                    "type": field.get("type"),
                    "key": field.get("key", False),
                    "searchable": field.get("searchable", False),
                    "filterable": field.get("filterable", False),
                    "sortable": field.get("sortable", False),
                    "facetable": field.get("facetable", False),
                    "retrievable": field.get("retrievable", False)
                }
                fields_info.append(field_info)
                
                if field.get("key", False):
                    key_field = field.get("name")
            
            result = {
                "index_name": schema.get("name"),
                "key_field": key_field,
                "fields": fields_info
            }
            return result
        else:
            return f"Index check failed: HTTP {r.status_code} - {r.text[:500]}"
    except Exception as e:
        return f"Error checking index: {str(e)}"


def get_index_schema():
    """Get cached schema or fetch new one"""
    if st.session_state.index_schema_cache:
        return st.session_state.index_schema_cache
    
    schema_info = debug_check_index_schema()
    if isinstance(schema_info, dict):
        st.session_state.index_schema_cache = schema_info
        return schema_info
    else:
        raise RuntimeError(f"Cannot fetch index schema: {schema_info}")


# =============================================================================
# DIRECT AZURE SPEECH API FUNCTIONS (BYPASS AZURE FUNCTION)
# =============================================================================

def submit_transcription_direct(video_id: str, media_url: str) -> Dict[str, Any]:
    """
    Submit transcription directly to Azure Speech API.
    Bypasses the Azure Function with wrong API version.
    """
    if not SPEECH_KEY:
        raise RuntimeError("SPEECH_KEY not configured in environment")
    
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
        
        # Get operation URL from Location header (this is the operation status URL)
        operation_url = r.headers.get("Location")
        if not operation_url:
            result = r.json()
            operation_url = result.get("self") or result.get("links", {}).get("self")
        
        if not operation_url:
            raise RuntimeError("No operation URL returned from Speech API")
        
        return {"operation_url": operation_url, "video_id": video_id}
        
    except requests.exceptions.HTTPError as e:
        if r.status_code == 401:
            raise RuntimeError("Azure Speech API authentication failed. Check SPEECH_KEY.")
        elif r.status_code == 400:
            raise RuntimeError(f"Bad request: {r.text}")
        else:
            raise RuntimeError(f"Speech API error {r.status_code}: {r.text}")


def poll_transcription_operation(operation_url: str) -> Dict[str, Any]:
    """Poll transcription operation status directly from Azure Speech API."""
    if not SPEECH_KEY:
        raise RuntimeError("SPEECH_KEY not configured")
    
    headers = {
        "Ocp-Apim-Subscription-Key": SPEECH_KEY
    }
    
    try:
        # CRITICAL FIX: Azure returns operation URL with :submit but we need to poll
        # using the /transcriptions/{id} endpoint, not /transcriptions:submit/{id}
        # The operation_url looks like: .../transcriptions:submit/{id}?api-version=...
        # We need: .../transcriptions/{id}?api-version=...
        
        poll_url = operation_url.replace("/transcriptions:submit/", "/transcriptions/")
        
        # Debug info
        st.session_state['debug_poll_url'] = poll_url
        
        r = requests.get(poll_url, headers=headers, timeout=30)
        r.raise_for_status()
        return r.json()
        
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to poll transcription: {str(e)}")


def get_transcription_from_result(result_data: Dict) -> Dict[str, Any]:
    """Get the actual transcription JSON from the result files."""
    if not SPEECH_KEY:
        raise RuntimeError("SPEECH_KEY not configured")
    
    headers = {
        "Ocp-Apim-Subscription-Key": SPEECH_KEY
    }
    
    try:
        # Get the result files URL from the completed operation
        links = result_data.get("links", {})
        files_url = links.get("files")
        
        if not files_url:
            # Try to construct from result data or get content directly
            if "combinedRecognizedPhrases" in result_data:
                # Result might be embedded directly
                return result_data
            
            raise RuntimeError("No files URL in result")
        
        # Get list of files
        r = requests.get(files_url, headers=headers, timeout=30)
        r.raise_for_status()
        files_data = r.json()
        
        # Find the transcription JSON file
        for file in files_data.get("values", []):
            if file.get("kind") == "Transcription":
                content_url = file.get("links", {}).get("contentUrl")
                if content_url:
                    # Download the actual transcription content
                    content_r = requests.get(content_url, timeout=60)
                    content_r.raise_for_status()
                    return content_r.json()
        
        raise RuntimeError("No transcription file found in results")
        
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to get transcription result: {str(e)}")


# =============================================================================
# DIRECT EMBEDDING AND INDEXING (BYPASS AZURE FUNCTION)
# =============================================================================

def get_embeddings(texts: list) -> list:
    """Get embeddings directly from Azure OpenAI."""
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


def index_segments_direct(video_id: str, segments: list) -> Dict[str, Any]:
    """
    Index segments directly to Azure Cognitive Search.
    
    CRITICAL: Automatically detects the key field from index schema instead of assuming 'id'
    """
    if not SEARCH_ENDPOINT or not SEARCH_KEY:
        raise RuntimeError("Azure Search not configured")
    
    # Get the key field name from the index schema
    schema_info = get_index_schema()
    key_field = schema_info.get("key_field")
    
    if not key_field:
        available = [f.get("name") for f in schema_info.get("fields", [])]
        raise RuntimeError(f"No key field found in index. Available fields: {available}")
    
    # Get list of available fields to ensure we only send existing fields
    available_fields = {f.get("name") for f in schema_info.get("fields", [])}
    
    # Generate embeddings for all segments
    texts = [seg.get("text", "") for seg in segments]
    try:
        embeddings = get_embeddings(texts)
    except Exception as e:
        st.warning(f"Embedding failed, indexing without vectors: {e}")
        embeddings = [None] * len(segments)
    
    # Prepare search documents
    documents = []
    for i, (seg, embedding) in enumerate(zip(segments, embeddings)):
        safe_video_id = sanitize_id(video_id)
        doc_id = f"{safe_video_id}_{i}"
        
        # Build document dynamically based on what fields actually exist in the index
        doc = {
            "@search.action": "upload"
        }
        
        # Add the key field (whatever it's actually named in your index)
        doc[key_field] = doc_id
        
        # Map of our field names to potential index field names
        field_mappings = {
            "video_id": safe_video_id,
            "segment_id": str(seg.get("segment_id", i)),
            "text": str(seg.get("text", "")),
            "start_ms": int(seg.get("start_ms", 0)),
            "end_ms": int(seg.get("end_ms", 0)),
            "pred_labels": seg.get("pred_labels", []) if seg.get("pred_labels") else []
        }
        
        # Only add fields that exist in the index schema
        for field_name, value in field_mappings.items():
            if field_name in available_fields:
                doc[field_name] = value
        
        # Handle embedding field - check for common naming variations
        embedding_field = None
        for possible_name in ["embedding", "embeddings", "vector", "vectors"]:
            if possible_name in available_fields:
                embedding_field = possible_name
                break
        
        if embedding and isinstance(embedding, list) and len(embedding) > 0 and embedding_field:
            try:
                doc[embedding_field] = [float(x) for x in embedding]
            except (ValueError, TypeError):
                st.warning(f"Skipping embedding for segment {i} due to conversion error")
        
        documents.append(doc)
    
    # Upload to Azure Search
    url = f"{SEARCH_ENDPOINT}/indexes/{SEARCH_INDEX_NAME}/docs/index?api-version=2024-07-01"
    
    headers = {
        "api-key": SEARCH_KEY,
        "Content-Type": "application/json"
    }
    
    payload = {
        "value": documents
    }
    
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        
        if r.status_code >= 400:
            error_detail = ""
            try:
                error_json = r.json()
                error_detail = json.dumps(error_json, indent=2)
            except:
                error_detail = r.text
            
            raise RuntimeError(f"Indexing failed: HTTP {r.status_code}\nDetails: {error_detail}")
        
        result = r.json()
        
        # Check for partial failures (207 Multi-Status)
        if r.status_code == 207:
            failed_docs = [item for item in result.get("value", []) if not item.get("status", False)]
            if failed_docs:
                st.warning(f"Partial indexing failure: {len(failed_docs)} documents failed")
                for fail in failed_docs[:3]:
                    st.error(f"Doc {fail.get('key', 'unknown')}: {fail.get('errorMessage', 'Unknown error')}")
        
        return {
            "indexed": len(documents), 
            "video_id": video_id, 
            "key_field_used": key_field,
            "api_response": result
        }
        
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"HTTP Error: {str(e)}")
    except Exception as e:
        raise RuntimeError(f"Indexing failed: {str(e)}")


def process_transcription_to_segments(transcription_data: Dict, video_id: str) -> list:
    """
    Convert Azure Speech transcription JSON to segments format.
    """
    segments = []
    
    # Parse phrases/segments from transcription
    phrases = transcription_data.get("recognizedPhrases", [])
    
    for i, phrase in enumerate(phrases):
        # Extract timing
        offset = phrase.get("offsetInTicks", 0) // 10000  # Convert to ms
        duration = phrase.get("durationInTicks", 0) // 10000
        
        # Extract text
        nbest = phrase.get("nBest", [])
        if nbest:
            text = nbest[0].get("display", "")
        else:
            text = ""
        
        # Create segment
        segment = {
            "segment_id": i,
            "video_id": video_id,
            "text": text,
            "start_ms": offset,
            "end_ms": offset + duration,
            "pred_labels": []  # Could add label prediction here
        }
        
        segments.append(segment)
    
    return segments


# =============================================================================
# STORAGE FUNCTIONS - FIXED UPLOAD
# =============================================================================

def generate_video_id(filename: str) -> str:
    clean_name = Path(filename).stem
    clean_name = re.sub(r'[^\w\s-]', '', clean_name)
    clean_name = re.sub(r'[-\s]+', '_', clean_name)
    hash_suffix = hashlib.md5(clean_name.encode()).hexdigest()[:8]
    return f"vid_{clean_name[:50]}_{hash_suffix}"


def test_sas_url(sas_url: str) -> Tuple[bool, str]:
    """Test if SAS URL is accessible before sending to Speech API."""
    try:
        r = requests.head(sas_url, timeout=10, allow_redirects=True)
        if r.status_code == 200:
            return True, "SAS URL is accessible"
        else:
            return False, f"SAS URL returned HTTP {r.status_code}"
    except Exception as e:
        return False, f"SAS URL test failed: {str(e)}"


def upload_to_azure_blob_fixed(file_bytes: bytes, blob_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    FIXED upload to Azure Blob using REST API.
    Corrected string-to-sign format.
    """
    if not AZURE_STORAGE_KEY:
        return None, "Azure Storage key not configured"
    
    try:
        # Upload URL
        url = f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{INPUT_CONTAINER}/{blob_name}"
        
        # Create date header in the exact format Azure expects
        date_str = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        content_length = len(file_bytes)
        
        # ====================================================================
        # CRITICAL FIX: Correct string-to-sign format for Azure Blob Storage
        # Format: VERB\nContent-Encoding\nContent-Language\nContent-Length\n
        #         Content-MD5\nContent-Type\nDate\nIf-Modified-Since\nIf-Match\n
        #         If-None-Match\nIf-Unmodified-Since\nRange\n
        #         CanonicalizedHeaders\nCanonicalizedResource
        # ====================================================================
        string_to_sign = (
            f"PUT\n"                       # HTTP method
            f"\n"                          # Content-Encoding (empty)
            f"\n"                          # Content-Language (empty)
            f"{content_length}\n"          # Content-Length (REQUIRED - must be exact)
            f"\n"                          # Content-MD5 (empty)
            f"application/octet-stream\n"  # Content-Type (REQUIRED for PUT)
            f"\n"                          # Date (empty, using x-ms-date instead)
            f"\n"                          # If-Modified-Since (empty)
            f"\n"                          # If-Match (empty)
            f"\n"                          # If-None-Match (empty)
            f"\n"                          # If-Unmodified-Since (empty)
            f"\n"                          # Range (empty)
            f"x-ms-blob-type:BlockBlob\n"  # CanonicalizedHeaders (sorted alphabetically)
            f"x-ms-date:{date_str}\n"
            f"x-ms-version:2020-12-06\n"
            f"/{AZURE_STORAGE_ACCOUNT}/{INPUT_CONTAINER}/{blob_name}"  # CanonicalizedResource
        )
        
        # Sign with HMAC-SHA256
        account_key = base64.b64decode(AZURE_STORAGE_KEY)
        signed_hmac = hmac.new(account_key, string_to_sign.encode('utf-8'), hashlib.sha256).digest()
        signature = base64.b64encode(signed_hmac).decode('utf-8')
        
        # Build authorization header
        auth_header = f"SharedKey {AZURE_STORAGE_ACCOUNT}:{signature}"
        
        # Set headers - MUST match what was signed
        headers = {
            "x-ms-date": date_str,
            "x-ms-version": "2020-12-06",
            "x-ms-blob-type": "BlockBlob",
            "Content-Type": "application/octet-stream",
            "Content-Length": str(content_length),
            "Authorization": auth_header
        }
        
        # Upload
        r = requests.put(url, data=file_bytes, headers=headers, timeout=300)
        
        if r.status_code not in [201, 200]:
            return None, f"Upload failed: HTTP {r.status_code} - {r.text}"
        
        # Generate SAS token for reading
        sas_token = generate_sas_token_fixed(blob_name)
        if not sas_token:
            return None, "Failed to generate SAS token"
        
        sas_url = f"{url}?{sas_token}"
        
        # Test the SAS URL
        is_valid, test_msg = test_sas_url(sas_url)
        if not is_valid:
            return None, f"SAS URL validation failed: {test_msg}"
        
        return sas_url, None
        
    except Exception as e:
        import traceback
        return None, f"Upload error: {str(e)}\n{traceback.format_exc()}"


def generate_sas_token_fixed(blob_name: str, expiry_hours: int = 24) -> str:
    """
    FIXED SAS token generation for Azure Blob - Service SAS format.
    """
    if not AZURE_STORAGE_KEY:
        return None
    
    try:
        # Set expiry in UTC
        expiry = datetime.now(timezone.utc) + timedelta(hours=expiry_hours)
        expiry_str = expiry.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Decode account key
        account_key = base64.b64decode(AZURE_STORAGE_KEY)
        
        # ====================================================================
        # CRITICAL FIX: Service SAS string-to-sign format
        # Reference: https://docs.microsoft.com/en-us/rest/api/storageservices/create-service-sas  
        # Format for Blob service SAS:
        # StringToSign = signedPermissions + "\n" +
        #                signedStart + "\n" +
        #                signedExpiry + "\n" +
        #                canonicalizedResource + "\n" +
        #                signedIdentifier + "\n" +
        #                signedIP + "\n" +
        #                signedProtocol + "\n" +
        #                signedVersion + "\n" +
        #                signedResource + "\n" +
        #                signedSnapshotTime + "\n" +
        #                signedEncryptionScope + "\n" +
        #                signedCacheControl + "\n" +
        #                signedContentDisposition + "\n" +
        #                signedContentEncoding + "\n" +
        #                signedContentLanguage + "\n" +
        #                signedContentType
        # ====================================================================
        
        # Canonicalized resource for service SAS: /blob/{account}/{container}/{blob}
        canonicalized_resource = f"/blob/{AZURE_STORAGE_ACCOUNT}/{INPUT_CONTAINER}/{blob_name}"
        
        # Build string to sign for Service SAS
        string_to_sign = (
            f"r\n"                           # signed permissions (read)
            f"\n"                            # signed start (empty)
            f"{expiry_str}\n"                # signed expiry
            f"{canonicalized_resource}\n"    # canonicalized resource
            f"\n"                            # signed identifier (empty)
            f"\n"                            # signed IP (empty)
            f"https\n"                       # signed protocol
            f"2020-12-06\n"                  # signed version
            f"b\n"                           # signed resource (b = blob)
            f"\n"                            # signed snapshot time (empty)
            f"\n"                            # signed encryption scope (empty)
            f"\n"                            # signed cache control (empty)
            f"\n"                            # signed content disposition (empty)
            f"\n"                            # signed content encoding (empty)
            f"\n"                            # signed content language (empty)
            f""                              # signed content type (empty, no newline at end)
        )
        
        # Sign with HMAC-SHA256
        signed_hmac = hmac.new(account_key, string_to_sign.encode('utf-8'), hashlib.sha256).digest()
        signature = base64.b64encode(signed_hmac).decode('utf-8')
        
        # Build query parameters - Order matters for some clients
        sas_params = {
            'sv': '2020-12-06',             # signed version
            'sr': 'b',                      # signed resource (blob)
            'sp': 'r',                      # signed permissions (read)
            'se': expiry_str,               # signed expiry
            'spr': 'https',                 # signed protocol
            'sig': signature                # signature
        }
        
        # URL encode the signature and other values
        sas_token = '&'.join([f"{k}={urllib.parse.quote(v, safe='')}" for k, v in sas_params.items()])
        return sas_token
        
    except Exception as e:
        st.error(f"SAS generation error: {e}")
        import traceback
        st.error(traceback.format_exc())
        return None


def upload_to_azure_blob_sdk(file_bytes: bytes, blob_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Upload using Azure SDK (more reliable, requires azure-storage-blob package).
    """
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
        
        # Ensure container exists
        try:
            container_client.create_container()
        except Exception:
            pass
        
        # Upload blob
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(file_bytes, overwrite=True)
        
        # Generate SAS token
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
        
        # Test the SAS URL
        is_valid, test_msg = test_sas_url(sas_url)
        if not is_valid:
            return None, f"SAS URL validation failed: {test_msg}"
        
        return sas_url, None
        
    except ImportError:
        return None, "azure-storage-blob not installed"
    except Exception as e:
        import traceback
        return None, f"SDK upload failed: {str(e)}\n{traceback.format_exc()}"


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
        f"PUT\n"
        f"\n"
        f"\n"
        f"{content_length}\n"
        f"\n"
        f"application/json\n"
        f"\n"
        f"\n"
        f"\n"
        f"\n"
        f"\n"
        f"\n"
        f"x-ms-blob-type:BlockBlob\n"
        f"x-ms-date:{date_str}\n"
        f"x-ms-version:2020-12-06\n"
        f"/{AZURE_STORAGE_ACCOUNT}/{SEGMENTS_CONTAINER}/{blob_name}"
    )
    
    account_key = base64.b64decode(AZURE_STORAGE_KEY)
    signed_hmac = hmac.new(account_key, string_to_sign.encode('utf-8'), hashlib.sha256).digest()
    signature = base64.b64encode(signed_hmac).decode('utf-8')
    auth_header = f"SharedKey {AZURE_STORAGE_ACCOUNT}:{signature}"
    
    headers = {
        "x-ms-date": date_str,
        "x-ms-version": "2020-12-06",
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": "application/json",
        "Content-Length": str(content_length),
        "Authorization": auth_header
    }
    
    r = requests.put(url, data=json_bytes, headers=headers, timeout=60)
    r.raise_for_status()
    
    return blob_name


def check_yt_dlp() -> bool:
    try:
        result = subprocess.run(["which", "yt-dlp"], capture_output=True, text=True)
        return result.returncode == 0
    except:
        return False


def download_youtube_audio(youtube_url: str, output_path: str, progress_callback=None) -> Tuple[Optional[str], Optional[str]]:
    """Download YouTube audio to specific path."""
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
            "--no-check-certificate",  # Added for compatibility
            "--no-warnings",           # Reduce noise
            "-o", output_path,
            youtube_url.strip()
        ]
        
        # Try to use Node.js runtime if available, otherwise let yt-dlp handle it
        # This fixes the "No supported JavaScript runtime" error
        try:
            node_check = subprocess.run(["which", "node"], capture_output=True, text=True)
            if node_check.returncode != 0:
                # No node.js, try to use legacy format that doesn't require JS
                cmd.extend(["--extractor-args", "youtube:player_client=web"])
        except:
            pass
        
        if progress_callback:
            progress_callback(15, "Downloading from YouTube...")
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        
        if result.returncode != 0:
            error_msg = result.stderr[:500]
            # Provide helpful error message for JS runtime issues
            if "JavaScript runtime" in error_msg:
                error_msg += "\n\n💡 Tip: Install Node.js or run: pip install yt-dlp --upgrade"
            return None, f"yt-dlp failed: {error_msg}"
        
        # Find the actual file
        if os.path.exists(output_path):
            return output_path, None
        
        # Try alternative extensions
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


def detect_url_type(url: str) -> str:
    """Detect if URL is YouTube, direct media, or unknown."""
    if not url:
        return "unknown"
    
    url_lower = str(url).lower().strip()
    
    # YouTube patterns
    youtube_patterns = [
        r'(?:https?:\/\/)?(?:www\.)?(?:youtube\.com|youtu\.be)',
        r'(?:https?:\/\/)?(?:www\.)?youtube\.com\/watch\?v=',
        r'(?:https?:\/\/)?(?:www\.)?youtu\.be\/',
        r'youtube\.com\/shorts\/'
    ]
    
    for pattern in youtube_patterns:
        if re.search(pattern, url_lower):
            return "youtube"
    
    # Direct media patterns
    media_extensions = ['.mp4', '.m4a', '.mp3', '.wav', '.mov', '.avi', '.mkv', '.webm']
    if any(url_lower.endswith(ext) for ext in media_extensions):
        return "direct"
    
    # Box.com, Google Drive, Dropbox, etc. - treat as direct
    cloud_patterns = ['box.com', 'drive.google.com', 'dropbox.com', 'onedrive']
    if any(pattern in url_lower for pattern in cloud_patterns):
        return "direct"
    
    return "unknown"


def process_single_video(url: str, custom_id: Optional[str] = None, 
                        progress_bar=None, status_text=None, 
                        overall_progress: Tuple[int, int] = (0, 1)) -> Dict[str, Any]:
    """
    Process a single video URL (YouTube or Direct).
    Returns result dict with status and metadata.
    """
    result = {
        "url": url,
        "video_id": None,
        "status": "pending",
        "segments_count": 0,
        "error": None,
        "index_status": None
    }
    
    try:
        # Detect URL type
        url_type = detect_url_type(url)
        
        if url_type == "unknown":
            result["status"] = "failed"
            result["error"] = "Unknown URL type. Must be YouTube or direct media URL."
            return result
        
        # Generate video ID
        if custom_id:
            video_id = custom_id.strip()
        else:
            video_id = generate_video_id(f"batch_{url}")
        
        result["video_id"] = video_id
        
        # Update progress
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
                
                # Read and upload
                with open(downloaded_path, 'rb') as f:
                    file_bytes = f.read()
                
                blob_name = f"batch_youtube_{video_id}_{int(time.time())}.m4a"
                
                if status_text:
                    status_text.text(f"[{current}/{total}] Uploading to Azure...")
                
                # Try SDK first
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
        
        # Submit transcription
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
            
            # Update progress during polling
            if progress_bar:
                poll_progress = min(int((i / max_polls) * 20), 20)  # 20% of progress for polling
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
        
        # Process segments
        if status_text:
            status_text.text(f"[{current}/{total}] Processing segments...")
        
        segments = process_transcription_to_segments(transcription_data, video_id)
        result["segments_count"] = len(segments)
        
        # Save to blob
        save_segments_to_blob(video_id, segments)
        
        # Index to search
        try:
            index_result = index_segments_direct(video_id, segments)
            result["index_status"] = f"Indexed {index_result.get('indexed', 0)} documents (key: {index_result.get('key_field_used', 'unknown')})"
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
# PAGE 1: SEARCH
# =============================================================================

if page == "🔎 Search Segments":
    st.header("Search Indexed Video Segments")
    
    if not SEARCH_FN_URL:
        st.error("SEARCH_FN_URL not configured. Cannot search.")
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
                    
                    header = f"{i}. {vid} | {ms_to_ts(start_ms)}–{ms_to_ts(end_ms)}"
                    if seg:
                        header += f" | seg={seg}"
                    if score is not None:
                        header += f" | score={score:.3f}" if isinstance(score, (int, float)) else f" | score={score}"
                    
                    with st.expander(header, expanded=(i <= 3)):
                        st.write(h.get("text", ""))
                        if h.get("pred_labels"):
                            st.caption(f"Labels: {', '.join(h['pred_labels'])}")
            except Exception as e:
                st.error(f"Search failed: {e}")


# =============================================================================
# PAGE 2: UPLOAD (DIRECT API VERSION)
# =============================================================================

elif page == "⬆️ Upload & Transcribe":
    st.header("Upload Video for Transcription")
    st.info(" Using direct Azure Speech API (bypassing Azure Function)")
    
    # Check Azure config
    azure_configured = bool(AZURE_STORAGE_KEY) and bool(SPEECH_KEY)
    if not azure_configured:
        st.error("⚠️ Azure Storage and Speech keys required. Check .env file.")
    
    source_type = st.radio("Select Source", 
                          ["File Upload", "Direct URL", "YouTube", "📁 Batch CSV Upload"],
                          horizontal=True)
    
    media_url = None
    video_id = None
    file_bytes = None
    yt_url = None  # Initialize to None
    csv_df = None
    
    # -------------------------------------------------------------------------
    # FILE UPLOAD
    # -------------------------------------------------------------------------
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
                st.info("File ready for upload to Azure")
    
    # -------------------------------------------------------------------------
    # DIRECT URL
    # -------------------------------------------------------------------------
    elif source_type == "Direct URL":
        url_input = st.text_input("Media URL", placeholder="https://tulane.box.com/shared/static/...")
        
        if url_input.strip():
            media_url = url_input.strip()
            video_id = generate_video_id(url_input)
            st.success("✅ URL validated")
    
    # -------------------------------------------------------------------------
    # YOUTUBE - FIXED with session state
    # -------------------------------------------------------------------------
    elif source_type == "YouTube":
        # Use session state to persist the URL
        yt_url = st.text_input(
            "YouTube URL", 
            placeholder="https://youtube.com/watch?v=...",
            value=st.session_state.yt_url_value,
            key="yt_url_input"
        )
        
        # Update session state when URL changes - FIXED: removed experimental_rerun
        if yt_url != st.session_state.yt_url_value:
            st.session_state.yt_url_value = yt_url
            # Use st.rerun() instead of st.experimental_rerun() for newer Streamlit versions
            try:
                st.rerun()
            except AttributeError:
                # Fallback for older versions
                try:
                    st.experimental_rerun()
                except AttributeError:
                    pass  # If neither exists, just continue without rerun
        
        if not check_yt_dlp():
            st.warning("yt-dlp not installed")
            if st.button("Install yt-dlp"):
                with st.spinner("Installing..."):
                    subprocess.run(["pip", "install", "-q", "yt-dlp"])
                # FIXED: Use st.rerun() instead of experimental_rerun
                try:
                    st.rerun()
                except AttributeError:
                    try:
                        st.experimental_rerun()
                    except AttributeError:
                        st.info("Please refresh the page manually")
        elif yt_url and yt_url.strip():
            video_id = generate_video_id(f"yt_{yt_url.strip()}")
            st.success("YouTube URL ready")
    
    # -------------------------------------------------------------------------
    # BATCH CSV UPLOAD - NEW FEATURE
    # -------------------------------------------------------------------------
    elif source_type == "📁 Batch CSV Upload":
        st.subheader("📁 Batch Process Videos from CSV")
        
        csv_file = st.file_uploader(
            "Upload CSV file",
            type=["csv"],
            help="CSV must contain a column with video URLs (YouTube or direct links)"
        )
        
        if csv_file:
            try:
                # Read CSV - handle various formats
                # Try to detect if URLs are in header or rows
                content = csv_file.read().decode('utf-8')
                csv_file.seek(0)  # Reset pointer
                
                # First attempt: standard read
                try:
                    csv_df = pd.read_csv(csv_file)
                except Exception:
                    # Second attempt: maybe single column with no header
                    csv_file.seek(0)
                    csv_df = pd.read_csv(csv_file, header=None)
                    csv_df.columns = [f"column_{i}" for i in range(len(csv_df.columns))]
                
                # Check if column names look like URLs (common issue)
                url_like_columns = []
                for col in csv_df.columns:
                    col_str = str(col).strip()
                    if detect_url_type(col_str) != "unknown" or col_str.startswith('http'):
                        url_like_columns.append(col)
                
                # If column names look like URLs, treat them as data
                if url_like_columns and len(csv_df.columns) == 1:
                    # The column name is actually a URL, convert to data
                    url_col_name = csv_df.columns[0]
                    new_row = {url_col_name: url_col_name}
                    csv_df = pd.concat([pd.DataFrame([new_row]), csv_df], ignore_index=True)
                
                st.success(f"✅ Loaded CSV with {len(csv_df)} rows and {len(csv_df.columns)} columns")
                
                # Show available columns
                st.write("**Available columns:**", list(csv_df.columns))
                
                # Let user select the URL column
                url_column = st.selectbox(
                    "Select column containing video URLs",
                    options=csv_df.columns.tolist(),
                    help="Choose the column that contains YouTube or direct media URLs"
                )
                
                # Optional: Select custom ID column
                id_column_options = ["Auto-generate"] + [c for c in csv_df.columns if c != url_column]
                id_column = st.selectbox(
                    "Select column for custom Video ID (optional)",
                    options=id_column_options,
                    index=0,
                    help="Optional: Choose a column to use as custom video ID (e.g., title, ID field)"
                )
                
                # Extract and validate URLs
                urls_raw = csv_df[url_column].dropna().astype(str).tolist()
                
                # Clean URLs (remove whitespace)
                urls_to_process = [u.strip() for u in urls_raw if u.strip()]
                
                # Preview
                with st.expander(f"Preview URLs to process ({len(urls_to_process)} found)"):
                    for i, url in enumerate(urls_to_process[:10], 1):
                        url_type = detect_url_type(url)
                        icon = "🎬" if url_type == "youtube" else "📄" if url_type == "direct" else "❓"
                        st.text(f"{i}. {icon} {url[:80]}...")
                    if len(urls_to_process) > 10:
                        st.caption(f"... and {len(urls_to_process) - 10} more")
                
                # Validate URLs
                valid_urls = []
                invalid_urls = []
                
                for url in urls_to_process:
                    url_type = detect_url_type(str(url))
                    if url_type in ["youtube", "direct"]:
                        valid_urls.append(url)
                    else:
                        invalid_urls.append(url)
                
                col1, col2, col3 = st.columns(3)
                col1.metric("Total URLs", len(urls_to_process))
                col2.metric("✅ Valid", len(valid_urls), f"{len(valid_urls)/len(urls_to_process)*100:.1f}%" if urls_to_process else "0%")
                col3.metric("❌ Invalid", len(invalid_urls))
                
                if invalid_urls:
                    with st.expander(f"Show {len(invalid_urls)} invalid URLs"):
                        for url in invalid_urls[:10]:
                            st.text(f"❌ {url[:100]}...")
                
                # Store in session state for processing
                st.session_state['batch_urls'] = valid_urls
                st.session_state['batch_df'] = csv_df
                st.session_state['batch_url_column'] = url_column
                st.session_state['batch_id_column'] = id_column
                
            except Exception as e:
                st.error(f"Error reading CSV: {e}")
                import traceback
                st.error(traceback.format_exc())
    
    # Custom ID (for single uploads)
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
        yt_url_to_check = st.session_state.get('yt_url_value', '') or (yt_url if yt_url else '')
        can_process = len(str(yt_url_to_check).strip()) > 0 and check_yt_dlp()
    elif source_type == "📁 Batch CSV Upload":
        can_process = (st.session_state.get('batch_urls') and 
                      len(st.session_state.get('batch_urls', [])) > 0 and 
                      azure_configured and
                      not st.session_state.get('batch_processing', False))
    
    # Process button
    button_text = " Start Transcription"
    if source_type == "📁 Batch CSV Upload":
        count = len(st.session_state.get('batch_urls', []))
        button_text = f" Process {count} Videos from CSV"
    
    if st.button(button_text, type="primary", disabled=not can_process):
        
        # ---------------------------------------------------------------------
        # BATCH PROCESSING
        # ---------------------------------------------------------------------
        if source_type == "📁 Batch CSV Upload":
            st.session_state.batch_processing = True
            st.session_state.batch_results = []
            
            urls = st.session_state.get('batch_urls', [])
            csv_df = st.session_state.get('batch_df')
            url_column = st.session_state.get('batch_url_column')
            id_column = st.session_state.get('batch_id_column')
            
            total = len(urls)
            
            st.info(f"Starting batch processing of {total} videos...")
            
            # Create progress containers
            overall_progress = st.progress(0)
            status_text = st.empty()
            results_container = st.container()
            
            # Process each URL
            results = []
            for idx, url in enumerate(urls, 1):
                # Get custom ID if specified
                custom_vid_id = None
                if id_column != "Auto-generate":
                    # Find the row with this URL and get the ID
                    row = csv_df[csv_df[url_column] == url]
                    if not row.empty:
                        custom_vid_id = str(row[id_column].iloc[0])
                        # Sanitize ID
                        custom_vid_id = re.sub(r'[^\w\s-]', '', custom_vid_id).strip().replace(' ', '_')[:50]
                
                # Process video
                result = process_single_video(
                    url=url,
                    custom_id=custom_vid_id,
                    progress_bar=overall_progress,
                    status_text=status_text,
                    overall_progress=(idx, total)
                )
                
                results.append(result)
                st.session_state.batch_results = results
                
                # Update progress
                progress_pct = int((idx / total) * 100)
                overall_progress.progress(progress_pct)
                
                # Show result in container
                with results_container:
                    if result['status'] == 'success':
                        st.success(f"✅ [{idx}/{total}] {result['video_id']}: {result['segments_count']} segments")
                    else:
                        error_msg = result.get('error', 'Unknown error')
                        # Truncate long error messages
                        if len(error_msg) > 200:
                            error_msg = error_msg[:200] + "..."
                        st.error(f"❌ [{idx}/{total}] Failed: {error_msg}")
                
                # Small delay to prevent rate limiting
                time.sleep(1)
            
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
            
            # Detailed results table
            with st.expander("View Detailed Results"):
                results_df = pd.DataFrame([
                    {
                        'Video ID': r['video_id'],
                        'URL': r['url'][:50] + "..." if len(r['url']) > 50 else r['url'],
                        'Status': r['status'],
                        'Segments': r.get('segments_count', 0),
                        'Indexing': r.get('index_status', 'N/A'),
                        'Error': (r.get('error', '')[:100] + '...') if r.get('error') else ''
                    }
                    for r in results
                ])
                st.dataframe(results_df)
                
                # Download results as CSV
                csv_buffer = io.StringIO()
                results_df.to_csv(csv_buffer, index=False)
                st.download_button(
                    "Download Results CSV",
                    csv_buffer.getvalue(),
                    "batch_processing_results.csv",
                    "text/csv"
                )
            
            # Search hint
            if successful:
                st.info("💡 **Search processed videos using:**")
                video_ids = [r['video_id'] for r in successful[:5]]
                st.code(f"video_id:({' OR '.join(video_ids)})")
            
            st.session_state.batch_processing = False
            
        else:
            # -----------------------------------------------------------------
            # SINGLE VIDEO PROCESSING (Original logic)
            # -----------------------------------------------------------------
            progress_bar = st.progress(0)
            status = st.empty()
            
            try:
                # -------------------------------------------------------------
                # HANDLE FILE UPLOAD (Direct to Azure)
                # -------------------------------------------------------------
                if source_type == "File Upload" and file_bytes:
                    progress_bar.progress(10)
                    status.text("Uploading to Azure Blob...")
                    
                    blob_name = f"upload_{video_id}_{int(time.time())}.m4a"
                    
                    # Try SDK method first, fallback to fixed REST method
                    sas_url = None
                    error = None
                    
                    try:
                        sas_url, error = upload_to_azure_blob_sdk(file_bytes, blob_name)
                    except Exception as e:
                        error = str(e)
                    
                    if error and ("not installed" in error or "SDK" in error):
                        st.info("Using REST API for upload...")
                        sas_url, error = upload_to_azure_blob_fixed(file_bytes, blob_name)
                    
                    if error:
                        raise Exception(error)
                    
                    if not sas_url:
                        raise Exception("Failed to generate SAS URL")
                    
                    media_url = sas_url
                    progress_bar.progress(50)
                    status.text("Upload complete, starting transcription...")
                
                # -------------------------------------------------------------
                # HANDLE YOUTUBE (Download then Upload)
                # -------------------------------------------------------------
                elif source_type == "YouTube":
                    # Get URL from session state
                    yt_url = st.session_state.get('yt_url_value', '')
                    
                    if not yt_url or not yt_url.strip():
                        raise Exception("YouTube URL is empty. Please enter a valid YouTube URL.")
                    
                    import tempfile
                    with tempfile.TemporaryDirectory() as tmpdir:
                        progress_bar.progress(10)
                        status.text("Downloading from YouTube...")
                        
                        output_path = f"{tmpdir}/youtube_{video_id}.m4a"
                        downloaded_path, error = download_youtube_audio(
                            yt_url.strip(), 
                            output_path,
                            lambda p, m: (progress_bar.progress(p), status.text(m))
                        )
                        
                        if error:
                            raise Exception(error)
                        
                        progress_bar.progress(50)
                        status.text("Uploading to Azure Blob...")
                        
                        # Read file and upload
                        with open(downloaded_path, 'rb') as f:
                            file_bytes = f.read()
                        
                        blob_name = f"youtube_{video_id}_{int(time.time())}.m4a"
                        
                        # Try SDK first, fallback to fixed REST
                        sas_url = None
                        error = None
                        
                        try:
                            sas_url, error = upload_to_azure_blob_sdk(file_bytes, blob_name)
                        except Exception as e:
                            error = str(e)
                        
                        if error and ("not installed" in error or "SDK" in error):
                            st.info("Using REST API for upload...")
                            sas_url, error = upload_to_azure_blob_fixed(file_bytes, blob_name)
                        
                        if error:
                            raise Exception(error)
                        
                        if not sas_url:
                            raise Exception("Failed to generate SAS URL")
                        
                        media_url = sas_url
                        progress_bar.progress(75)
                        status.text("Processing with Azure Speech...")
                
                # -------------------------------------------------------------
                # TRANSCRIBE (All paths lead here)
                # -------------------------------------------------------------
                if not media_url:
                    raise Exception("No media URL available")
                
                # Submit directly to Azure Speech API
                status.text("Submitting to Azure Speech-to-Text...")
                result = submit_transcription_direct(video_id, media_url)
                operation_url = result.get("operation_url")
                
                if not operation_url:
                    raise Exception("No operation URL returned")
                
                # Debug info
                st.info(f"Debug: Operation URL received")
                
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
                        status.text("Transcription complete, retrieving results...")
                        transcription_data = get_transcription_from_result(poll_result)
                        break
                        
                    elif status_text.lower() == "failed":
                        error_msg = poll_result.get("properties", {}).get("error", {}).get("message", "Unknown error")
                        raise Exception(f"Transcription failed: {error_msg}")
                
                if not transcription_data:
                    raise Exception("Transcription timed out")
                
                # -------------------------------------------------------------
                # PROCESS & INDEX (DIRECT)
                # -------------------------------------------------------------
                progress_bar.progress(98)
                status.text("Processing segments and indexing...")
                
                # Convert to segments
                segments = process_transcription_to_segments(transcription_data, video_id)
                
                # Save to blob
                blob_name = save_segments_to_blob(video_id, segments)
                
                # Index to search
                try:
                    index_result = index_segments_direct(video_id, segments)
                    index_msg = f"Indexed: {index_result.get('indexed', 0)} documents (key field: {index_result.get('key_field_used', 'unknown')})"
                except Exception as e:
                    index_msg = f"Indexing failed: {str(e)}"
                
                progress_bar.progress(100)
                status.text("Complete!")
                
                st.success(f"""
                ✅ **Transcription Complete!**
                - Video ID: {video_id}
                - Segments: {len(segments)}
                - {index_msg}
                """)
                st.code(f'Search: video_id:{video_id}')
                
                # Show sample segments
                with st.expander("View first 5 segments"):
                    for seg in segments[:5]:
                        st.write(f"**{ms_to_ts(seg['start_ms'])} - {ms_to_ts(seg['end_ms'])}:** {seg['text'][:100]}...")
                    
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                st.exception(e)
                
                # Debug info
                if 'debug_poll_url' in st.session_state:
                    st.error(f"Debug - Poll URL used: {st.session_state['debug_poll_url']}")


# Footer
st.sidebar.markdown("---")
st.sidebar.caption("Video Annotation Platform v1.0 - Direct API Mode")