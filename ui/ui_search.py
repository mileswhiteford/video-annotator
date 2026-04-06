"""
ui_search.py - Main Streamlit entry point
Uses multipage navigation (pages folder) and shared utilities from utils.py
"""

import os
import requests
import streamlit as st
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple, List
from dotenv import load_dotenv

# Import shared utilities (must be in same directory)
from utils import ms_to_ts, SEARCH_FN_URL

load_dotenv()

st.set_page_config(page_title="Video Segment Search", layout="wide")

# =============================================================================
# SESSION STATE INITIALIZATION
# =============================================================================
# All session variables used across pages must be initialized here
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
    'debug_poll_url': None
}
for key, value in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = value

# -----------------------------------------------------------------------------
# Search‑specific functions
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
                header += f"  |  score={score:.3f}" if isinstance(score, (int, float)) else f"  |  score={score}"

            with st.expander(header, expanded=(i <= 3)):
                st.write(h.get("text", ""))
                labels = h.get("pred_labels") or []
                conf = h.get("pred_confidence")
                rationale = h.get("pred_rationale")
                if labels or conf is not None or rationale:
                    if labels:
                        st.write("**Labels:**", ", ".join(labels))
                    if conf is not None:
                        st.write("**Confidence:**", conf)
                    if rationale:
                        st.write("**Rationale:**", rationale)


# -----------------------------------------------------------------------------
# Multipage navigation
# -----------------------------------------------------------------------------
pg_search = st.Page(render_search_page, title="Search", icon="🔎", default=True)
pg_labels = st.Page("pages/1_Label_Management.py", title="Label Management", icon="🏷️")

pg_upload = st.Page("pages/2_Upload.py", title="Upload", icon="⬆️")
pg_manage = st.Page("pages/3_Manage_Videos.py", title="Manage Videos", icon="📚")
pg_diag   = st.Page("pages/4_System_Diagnostics.py", title="System Diagnostics", icon="⚙️")

st.navigation([pg_search, pg_labels, pg_upload, pg_manage, pg_diag]).run()
=======
pg_eval = st.Page("pages/5_Label_Evaluation.py", title="Label Evaluation", icon="📊")

st.navigation([pg_search, pg_labels, pg_eval]).run()

