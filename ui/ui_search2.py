"""
ui_search.py - Streamlit Web Interface for Video Segment Search & Upload

This Streamlit application provides:
- Search indexed video segments (keyword, vector, hybrid)
- Upload new videos for transcription and indexing
- View processing status and results

Architecture:
- Frontend for video annotation system
- Calls SearchSegments, TranscribeHttp, and EmbedAndIndex Azure Functions
- Supports both search and ingest workflows
"""

import os
import requests
import streamlit as st
import tempfile
import json
import time
from typing import Optional, Dict, Any
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Azure Function URLs
SEARCH_FN_URL = os.environ["SEARCH_FN_URL"]
TRANSCRIBE_URL = os.environ.get("TRANSCRIBE_URL", "")
EMBED_INDEX_URL = os.environ.get("EMBED_INDEX_URL", "")

# Default settings
DEFAULT_MODE = os.environ.get("DEFAULT_MODE", "hybrid")
DEFAULT_TOP = int(os.environ.get("DEFAULT_TOP", "10"))
DEFAULT_K = int(os.environ.get("DEFAULT_K", "40"))
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "15"))

st.set_page_config(page_title="Video Annotation Platform", layout="wide")
st.title("🎬 Video Annotation Platform")

# Sidebar navigation
with st.sidebar:
    st.header("Navigation")
    page = st.radio("Select Page", ["🔎 Search Segments", "⬆️ Upload & Transcribe"])
    
    st.header("Settings")
    if page == "🔎 Search Segments":
        mode = st.selectbox(
            "Search Mode",
            ["keyword", "hybrid", "vector"],
            index=["keyword", "hybrid", "vector"].index(DEFAULT_MODE)
            if DEFAULT_MODE in ("keyword", "hybrid", "vector")
            else 1,
        )
        top = st.slider("Results", 1, 50, DEFAULT_TOP)
        k = st.slider("Vector k", 5, 200, DEFAULT_K)
        st.caption("Tip: keep k ~ 4×top for hybrid")


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def ms_to_ts(ms: int) -> str:
    """Convert milliseconds to timestamp."""
    s = max(0, int(ms // 1000))
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def call_api(url: str, payload: dict, timeout: int = 60) -> dict:
    """Generic API call with error handling."""
    if not url:
        raise RuntimeError("API URL not configured")
    
    r = requests.post(
        url,
        json=payload,
        timeout=timeout,
        headers={"Content-Type": "application/json"},
    )
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
    return r.json() if r.text else {}


def submit_transcription(video_id: str, media_url: str) -> Dict[str, Any]:
    """Submit video for transcription."""
    payload = {
        "video_id": video_id,
        "media_url": media_url,
        "language": "en-US"
    }
    return call_api(TRANSCRIBE_URL, payload, timeout=60)


def poll_transcription(job_url: str) -> Dict[str, Any]:
    """Poll transcription job status."""
    r = requests.get(job_url, timeout=30)
    r.raise_for_status()
    return r.json()


def embed_and_index(video_id: str, transcript_data: Dict[str, Any]) -> Dict[str, Any]:
    """Send transcript for embedding and indexing."""
    payload = {
        "video_id": video_id,
        "transcript": transcript_data
    }
    return call_api(EMBED_INDEX_URL, payload, timeout=60)


def process_video_pipeline(video_id: str, media_url: str, progress_bar=None, status_text=None):
    """
    Complete pipeline: transcribe -> poll -> embed/index
    Returns final status
    """
    # Step 1: Submit transcription
    if status_text:
        status_text.text("Submitting to Azure Speech-to-Text...")
    if progress_bar:
        progress_bar.progress(10)
    
    try:
        result = submit_transcription(video_id, media_url)
        job_url = result.get("job_url")
        
        if not job_url:
            return {"status": "failed", "error": "No job URL returned"}
        
        # Step 2: Poll for completion
        if status_text:
            status_text.text("Transcribing audio (this may take several minutes)...")
        
        max_polls = 120  # 30 minutes max
        for i in range(max_polls):
            time.sleep(POLL_SECONDS)
            
            poll_result = poll_transcription(job_url)
            status = poll_result.get("status", "unknown").lower()
            
            if progress_bar:
                progress = min(10 + int((i / max_polls) * 70), 80)
                progress_bar.progress(progress)
            
            if status == "succeeded":
                if status_text:
                    status_text.text("Transcription complete! Indexing segments...")
                if progress_bar:
                    progress_bar.progress(85)
                
                # Step 3: Embed and index
                transcript_data = poll_result.get("transcript", {})
                index_result = embed_and_index(video_id, transcript_data)
                
                if progress_bar:
                    progress_bar.progress(100)
                if status_text:
                    status_text.text("✅ Complete! Video is now searchable.")
                
                return {
                    "status": "completed",
                    "video_id": video_id,
                    "segments_indexed": index_result.get("indexed", 0),
                    "job_url": job_url
                }
                
            elif status == "failed":
                error = poll_result.get("error", "Unknown error")
                return {"status": "failed", "error": error}
            
            # Still running, continue polling
            if status_text and i % 4 == 0:  # Update every minute
                status_text.text(f"Transcribing... ({i * POLL_SECONDS // 60} minutes elapsed)")
        
        # Timeout
        return {"status": "timeout", "error": "Transcription timed out after 30 minutes"}
        
    except Exception as e:
        return {"status": "error", "error": str(e)}


def generate_video_id(filename: str) -> str:
    """Generate unique video ID from filename."""
    import hashlib
    clean_name = Path(filename).stem
    hash_suffix = hashlib.md5(clean_name.encode()).hexdigest()[:8]
    return f"vid_{clean_name[:50]}_{hash_suffix}"


# =============================================================================
# PAGE 1: SEARCH SEGMENTS
# =============================================================================

if page == "🔎 Search Segments":
    st.header("Search Indexed Video Segments")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        q = st.text_input("Query", value="", placeholder="e.g., measles vaccine side effects")
    with col2:
        video_id_filter = st.text_input("Filter by video_id (optional)", value="")
    
    go = st.button("Search", type="primary", disabled=(not q.strip()))
    
    if go:
        payload = {"q": q.strip(), "mode": mode, "top": top}
        if mode in ("hybrid", "vector"):
            payload["k"] = k
        if video_id_filter.strip():
            payload["video_id"] = video_id_filter.strip()
        
        try:
            with st.spinner("Searching..."):
                data = call_api(SEARCH_FN_URL, payload)
        except Exception as e:
            st.error(f"Search failed: {e}")
            st.stop()
        
        hits = data.get("hits", [])
        total_count = data.get("count", 0)
        
        st.caption(f"Found {total_count} total segments | Showing top {len(hits)}")
        
        if not hits:
            st.info("No results found. Try a different query or upload videos first.")
        
        for i, h in enumerate(hits, start=1):
            start_ms = h.get("start_ms", 0)
            end_ms = h.get("end_ms", 0)
            vid = h.get("video_id", "")
            seg = h.get("segment_id", "")
            score = h.get("score", None)
            
            header = f"{i}. {vid}  |  {ms_to_ts(start_ms)}–{ms_to_ts(end_ms)}"
            if seg:
                header += f"  |  seg={seg}"
            if score is not None:
                header += f"  |  score={score:.3f}" if isinstance(score, (float, int)) else f"  |  score={score}"
            
            with st.expander(header, expanded=(i <= 3)):
                st.write(h.get("text", ""))
                
                # Show annotations if present
                labels = h.get("pred_labels") or []
                conf = h.get("pred_confidence")
                rationale = h.get("pred_rationale")
                
                if labels or conf is not None or rationale:
                    st.subheader("Annotations")
                    cols = st.columns(3)
                    if labels:
                        cols[0].metric("Labels", ", ".join(labels))
                    if conf is not None:
                        cols[1].metric("Confidence", f"{conf:.2f}" if isinstance(conf, float) else conf)
                    if rationale:
                        cols[2].metric("Rationale", rationale[:100] + "..." if len(str(rationale)) > 100 else rationale)


# =============================================================================
# PAGE 2: UPLOAD & TRANSCRIBE
# =============================================================================

elif page == "⬆️ Upload & Transcribe":
    st.header("Upload Video for Transcription")
    
    st.markdown("""
    Upload a video file to:
    1. Extract audio and transcribe using Azure Speech-to-Text
    2. Segment the transcript into searchable chunks
    3. Create vector embeddings and index for semantic search
    
    Supported formats: MP4, AVI, MOV, MKV, M4A, MP3, WAV
    """)
    
    # File uploader
    uploaded_file = st.file_uploader(
        "Choose a video or audio file",
        type=["mp4", "avi", "mov", "mkv", "m4a", "mp3", "wav"],
        accept_multiple_files=False
    )
    
    # Or provide URL
    st.markdown("**OR** provide a media URL:")
    media_url_input = st.text_input(
        "Media URL (e.g., Box shared link, Azure Blob URL)",
        placeholder="https://..."
    )
    
    # Video ID input (optional)
    custom_video_id = st.text_input(
        "Custom Video ID (optional)",
        placeholder="my_video_001",
        help="Leave blank to auto-generate from filename"
    )
    
    # Process button
    process_clicked = st.button(
        "🚀 Start Transcription",
        type="primary",
        disabled=(not uploaded_file and not media_url_input.strip())
    )
    
    if process_clicked:
        # Determine video ID and media URL
        if uploaded_file:
            # Save uploaded file temporarily
            video_id = custom_video_id or generate_video_id(uploaded_file.name)
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uploaded_file.name).suffix) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            
            st.info(f"📁 File saved temporarily: {tmp_path}")
            st.warning("⚠️ Direct file upload requires Azure Blob storage integration. Please use 'Media URL' option with a publicly accessible URL (Box, Azure Blob, etc.) for now.")
            
            # For now, instruct user to use URL option
            st.error("Please use the 'Media URL' option instead. Upload your file to Box or Azure Blob first, then paste the direct download URL.")
            
        elif media_url_input.strip():
            video_id = custom_video_id or generate_video_id(media_url_input)
            media_url = media_url_input.strip()
            
            st.success(f"🎬 Processing: {video_id}")
            st.info(f"URL: {media_url[:80]}...")
            
            # Progress tracking
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Run pipeline
            result = process_video_pipeline(video_id, media_url, progress_bar, status_text)
            
            # Display results
            if result["status"] == "completed":
                st.success(f"""
                ✅ **Transcription Complete!**
                
                - **Video ID**: {result['video_id']}
                - **Segments Indexed**: {result['segments_indexed']}
                - **Status**: Ready for search
                
                Go to the **Search** page to query this video's content.
                """)
                
                # Show sample query
                st.code(f'Query: "video_id:{video_id}" to see all segments from this video', language="text")
                
            elif result["status"] == "failed":
                st.error(f"❌ **Processing Failed**\n\nError: {result.get('error', 'Unknown error')}")
                
            elif result["status"] == "timeout":
                st.warning(f"⏱️ **Processing Timeout**\n\nThe transcription is taking longer than expected. Check pipeline_state.json for status.")
                
            else:
                st.error(f"⚠️ **Unexpected Error**: {result.get('error', 'Unknown')}")


# =============================================================================
# FOOTER
# =============================================================================

st.sidebar.markdown("---")
st.sidebar.caption("""
**Video Annotation Platform v1.0**

- Search: Query indexed segments
- Upload: Add new videos via URL
- Azure Speech-to-Text powered
""")