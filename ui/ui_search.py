"""
ui_search.py - Main Streamlit entry point
Uses multipage navigation (pages folder) and shared utilities from utils.py

This Streamlit application provides a user-friendly web interface for searching
indexed video segments. Users can:
- Enter text queries to search across all indexed segments
- Choose search mode (keyword, vector, or hybrid)
- Filter results by video_id and adjust result count
- View segment text with timestamps and relevance scores
Architecture Role:
- Frontend user interface for the video annotation system
- Deployed as Azure Container App (video-annotator-ui)
- Calls SearchSegments Azure Function for all search operations
- Displays results with formatted timestamps and metadata
Deployment:
  - Local: python -m streamlit run ui_search.py
  - Azure: Deployed as Container App (see ui/README.md)
Configuration (via .env or Container App env vars):
  - SEARCH_FN_URL: SearchSegments function endpoint
  - DEFAULT_MODE: Default search mode (hybrid/keyword/vector)
  - DEFAULT_TOP: Default number of results
  - DEFAULT_K: Default vector recall depth
"""

import os
import requests
import streamlit as st
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple, List
from dotenv import load_dotenv

# Import shared utilities (must be in same directory)
from utils import ms_to_ts, SEARCH_FN_URL, get_stored_videos, build_video_link

load_dotenv()

st.set_page_config(page_title="Video Segment Search", layout="wide")

# =============================================================================
# SESSION STATE INITIALIZATION
# =============================================================================
defaults = {
    'yt_url_value': "",
    'batch_results': [],
    'batch_processing': False,
    'index_schema_cache': None,
    'stored_videos_cache': None,
    'url_fields_status': None,
    'debug_info': {},
    'video_to_delete': None,
    'delete_success': False,
    'videos_loaded': False,
    'debug_poll_url': None,
    # Cache for video metadata (source_url etc.)
    'video_metadata_cache': {},
    # Flag to indicate metadata has been loaded
    'metadata_loaded': False
}
for key, value in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = value


# -----------------------------------------------------------------------------
# Metadata caching (automatically loaded)
# -----------------------------------------------------------------------------
@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_all_video_metadata() -> Dict[str, Dict]:
    """Fetch all videos with their source_url and return a dict keyed by video_id."""
    try:
        videos = get_stored_videos(limit=10000)  # Adjust limit as needed
        metadata = {}
        for v in videos:
            vid = v.get('video_id')
            if vid:
                metadata[vid] = v
        return metadata
    except Exception as e:
        st.error(f"Failed to load video metadata: {e}")
        return {}


# -----------------------------------------------------------------------------
# Search-specific functions
# -----------------------------------------------------------------------------
def call_search_api(payload: dict) -> dict:
    r = requests.post(
        SEARCH_FN_URL,
        json=payload,
        timeout=60,
        headers={"Content-Type": "application/json"},
    )
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
    return r.json() if r.text else {}


def render_search_page() -> None:
    st.title("🔎 Search indexed video segments")

    # --- Automatically load metadata on first run ---
    if not st.session_state.get('metadata_loaded'):
        with st.spinner("Loading video metadata..."):
            st.session_state['video_metadata_cache'] = load_all_video_metadata()
            st.session_state['metadata_loaded'] = True

    with st.sidebar:
        st.header("Settings")
        mode = st.selectbox(
            "Mode",
            ["keyword", "hybrid", "vector"],
            index=1
        )
        top = st.slider("Top", 1, 50, 10)
        k = st.slider("Vector k (hybrid/vector)", 5, 200, 40)
        video_id_filter = st.text_input("Filter by video_id (optional)", value="")

        # Display cache status and refresh button
        cache_size = len(st.session_state['video_metadata_cache'])
        st.caption(f"📦 {cache_size} videos in metadata cache")
        if st.button("🔄 Refresh cache"):
            st.cache_data.clear()
            st.session_state['video_metadata_cache'] = load_all_video_metadata()
            st.session_state['metadata_loaded'] = True
            st.rerun()

        st.caption("Tip: keep k ~ 4×top for hybrid.")

    q = st.text_input("Query", value="", placeholder="e.g., measles misinformation")
    go = st.button("Search", type="primary", disabled=(not q.strip()))

    if go:
        payload = {"q": q.strip(), "mode": mode, "top": top}
        if mode in ("hybrid", "vector"):
            payload["k"] = k
        if video_id_filter.strip():
            payload["video_id"] = video_id_filter.strip()

        try:
            with st.spinner("Searching..."):
                data = call_search_api(payload)
        except Exception as e:
            st.error(f"Search failed: {e}")
            st.stop()

        hits = data.get("hits", [])
        st.caption(f"Count: {data.get('count')} | Returned: {len(hits)}")

        # Track statistics for summary
        source_stats = {"from_search": 0, "from_cache": 0, "missing": 0}
        type_counts = {}

        for i, h in enumerate(hits, start=1):
            start_ms = h.get("start_ms", 0)
            end_ms = h.get("end_ms", 0)
            vid = h.get("video_id", "")
            seg = h.get("segment_id", "")
            score = h.get("score", None)

            # Get source_url: first from search result, then from cache
            source_url = h.get("source_url")
            source_origin = "search"
            if not source_url and vid in st.session_state['video_metadata_cache']:
                source_url = st.session_state['video_metadata_cache'][vid].get("source_url")
                source_origin = "cache"

            # Build link using utility function
            video_link, link_type, supports_time = build_video_link(
                vid, start_ms, source_url, h.get("source_type")
            )

            # Update statistics
            if source_url:
                if source_origin == "search":
                    source_stats["from_search"] += 1
                else:
                    source_stats["from_cache"] += 1
            else:
                source_stats["missing"] += 1
            type_counts[link_type] = type_counts.get(link_type, 0) + 1

            start_sec = int(start_ms // 1000) if start_ms else 0
            display_vid = vid if len(vid) < 30 else f"{vid[:27]}..."
            header = f"{i}. {display_vid}  |  {ms_to_ts(start_ms)}–{ms_to_ts(end_ms)}"
            if seg:
                header += f"  |  seg={seg}"
            if score is not None:
                header += f"  |  score={score:.3f}" if isinstance(score, (int, float)) else f"  |  score={score}"

            with st.expander(header, expanded=(i <= 3)):
                # Link row
                c1, c2, c3 = st.columns([3, 2, 1])

                with c1:
                    if video_link == "#":
                        st.error("❌ Link unavailable")
                        st.caption("No source URL found")
                    else:
                        time_text = f"at {ms_to_ts(start_ms)}" if supports_time else ""
                        st.markdown(f"**[▶️ Play {time_text}]({video_link})**", unsafe_allow_html=True)
                        st.caption(f"Type: *{link_type}*")

                with c2:
                    if source_url:
                        display_url = source_url[:30] + "..." if len(source_url) > 30 else source_url
                        st.code(display_url, language=None)
                        st.caption(f"✓ From {source_origin}")
                    else:
                        st.caption("❌ No source URL")

                with c3:
                    if supports_time:
                        st.metric("Start", f"{start_sec}s")
                    else:
                        st.metric("Seek", "N/A")

                st.divider()
                st.write(h.get("text", ""))

                # Labels and metadata
                labels = h.get("pred_labels") or []
                conf = h.get("pred_confidence")
                rationale = h.get("pred_rationale")
                if labels or conf is not None or rationale:
                    st.divider()
                    if labels:
                        st.write("**Labels:**", ", ".join(labels))
                    if conf is not None:
                        st.write("**Confidence:**", conf)
                    if rationale:
                        st.write("**Rationale:**", rationale)

                # Debug expander
                with st.expander("🔧 Debug"):
                    st.json({
                        "video_id": vid,
                        "source_url": source_url,
                        "source_origin": source_origin,
                        "final_link": video_link,
                        "link_type": link_type,
                        "supports_time": supports_time
                    })

        # Summary footer
        if hits:
            st.divider()
            col1, col2, col3 = st.columns(3)
            with col1:
                st.caption(f"From search: {source_stats['from_search']}")
            with col2:
                st.caption(f"From cache: {source_stats['from_cache']}")
            with col3:
                if source_stats['missing'] > 0:
                    st.caption(f"⚠ Missing: {source_stats['missing']}")
                else:
                    st.caption("✅ All have source URLs")

            # Show type breakdown
            type_summary = ", ".join(f"{k}: {v}" for k, v in type_counts.items())
            st.caption(f"Types: {type_summary}")


# -----------------------------------------------------------------------------
# Multipage navigation
# -----------------------------------------------------------------------------
pg_search = st.Page(render_search_page, title="Search", icon="🔎", default=True)
pg_labels = st.Page("pages/1_Label_Management.py", title="Label Management", icon="🏷️")
pg_upload = st.Page("pages/2_Upload.py", title="Upload", icon="⬆️")
pg_manage = st.Page("pages/3_Manage_Videos.py", title="Manage Videos", icon="📚")
pg_diag   = st.Page("pages/4_System_Diagnostics.py", title="System Diagnostics", icon="⚙️")

st.navigation([pg_search, pg_labels, pg_upload, pg_manage, pg_diag]).run()