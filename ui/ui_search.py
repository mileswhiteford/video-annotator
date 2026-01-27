"""
ui_search.py - Streamlit Web Interface for Video Segment Search

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
from dotenv import load_dotenv

# Load .env locally (Container Apps/App Service will use real env vars)
load_dotenv()

SEARCH_FN_URL = os.environ["SEARCH_FN_URL"]
DEFAULT_MODE = os.environ.get("DEFAULT_MODE", "hybrid")
DEFAULT_TOP = int(os.environ.get("DEFAULT_TOP", "10"))
DEFAULT_K = int(os.environ.get("DEFAULT_K", "40"))

st.set_page_config(page_title="Video Segment Search", layout="wide")
st.title("🔎 Search indexed video segments")

with st.sidebar:
    st.header("Search settings")
    mode = st.selectbox(
        "Mode",
        ["keyword", "hybrid", "vector"],
        index=["keyword", "hybrid", "vector"].index(DEFAULT_MODE)
        if DEFAULT_MODE in ("keyword", "hybrid", "vector")
        else 1,
    )
    top = st.slider("Top", 1, 50, DEFAULT_TOP)
    k = st.slider("Vector k (hybrid/vector)", 5, 200, DEFAULT_K)
    video_id_filter = st.text_input("Filter by video_id (optional)", value="")
    st.caption("Tip: keep k ~ 4×top for hybrid.")

q = st.text_input("Query", value="", placeholder="e.g., measles misinformation")
go = st.button("Search", type="primary", disabled=(not q.strip()))


def ms_to_ts(ms: int) -> str:
    s = max(0, int(ms // 1000))
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


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
                st.subheader("Annotations")
                if labels:
                    st.write("**Labels:**", ", ".join(labels))
                if conf is not None:
                    st.write("**Confidence:**", conf)
                if rationale:
                    st.write("**Rationale:**", rationale)
