"""
1_Label_Management.py - Streamlit Page for Label Management

Provides UI for managing label definitions:
- View all existing labels
- Add new labels with name and description
- Edit existing labels
- Deactivate labels

Calls ManageLabels Azure Function for all operations.

Configuration (via .env):
  - MANAGE_LABELS_URL: ManageLabels function endpoint
"""

import os
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

MANAGE_LABELS_URL = os.environ.get("MANAGE_LABELS_URL", "")

st.set_page_config(page_title="Label Management", layout="wide")
st.title("Label Library Management")

if not MANAGE_LABELS_URL:
    st.error("MANAGE_LABELS_URL must be configured in .env")
    st.stop()


def call_labels_api(method: str = "GET", payload: dict = None) -> dict:
    """Call ManageLabels Azure Function."""
    try:
        r = requests.request(
            method,
            MANAGE_LABELS_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=60,
        )
        if r.status_code >= 400:
            error_msg = r.json().get("error", r.text) if r.text else f"HTTP {r.status_code}"
            st.error(f"API Error: {error_msg}")
            return {}
        return r.json() if r.text else {}
    except requests.exceptions.RequestException as e:
        st.error(f"Connection error: {e}")
        return {}


# --- Labeling status banner ---
_status_library = call_labels_api("GET")
_pending = any(not l.get("applied", True) for l in _status_library.get("labels", []))
if _pending:
    st.warning("Labeling in progress — search results will update once complete. Refresh to check status.")

# --- Tab layout ---
tab_view, tab_add, tab_edit = st.tabs(["View Labels", "Add Label", "Edit Label"])

# --- TAB 1: View Labels ---
with tab_view:
    if st.button("Refresh"):
        st.rerun()

    library = call_labels_api("GET")

    if library and "labels" in library:
        st.caption(f"**Last Updated:** {library.get('last_updated', 'N/A')}")

        if not library["labels"]:
            st.info("No labels defined yet. Add labels in the 'Add Label' tab.")
        else:
            st.write(f"**Total Labels:** {len(library['labels'])}")

            for i, label in enumerate(library["labels"], 1):
                with st.expander(f"{i}. {label['name']}", expanded=False):
                    st.write(f"**Description:** {label['description']}")
                    st.write(f"**Label ID:** `{label['label_id']}`")
                    st.write(f"**Created:** {label['created_at']}")
                    st.write(f"**Updated:** {label['updated_at']}")

# --- TAB 2: Add Label ---
with tab_add:
    with st.form("add_label_form", clear_on_submit=True):
        new_name = st.text_input("Label Name", placeholder="e.g., Misinformation")
        new_desc = st.text_area(
            "Description",
            placeholder="Describe what this label represents...",
            help="This description will be used by AI to understand when to apply this label.",
        )
        add_submit = st.form_submit_button("Add Label", type="primary")

        if add_submit:
            if not new_name or not new_desc:
                st.error("Both name and description are required.")
            else:
                result = call_labels_api("POST", {"name": new_name, "description": new_desc})
                if result and "label_id" in result:
                    st.success(f"Label '{result['name']}' added!")
                    st.info("Labeling queued — updated labels will appear in search results shortly.")
                    st.rerun()

# --- TAB 3: Edit Label ---
with tab_edit:
    library = call_labels_api("GET")

    if library and library.get("labels"):
        label_options = {l["name"]: l for l in library["labels"]}
        selected_name = st.selectbox("Select Label", options=list(label_options.keys()))

        if selected_name:
            label = label_options[selected_name]

            with st.form("edit_label_form"):
                edit_name = st.text_input("Label Name", value=label["name"])
                edit_desc = st.text_area("Description", value=label["description"])

                col_update, col_delete = st.columns(2)
                with col_update:
                    update_submit = st.form_submit_button("Update", type="primary", use_container_width=True)
                with col_delete:
                    delete_submit = st.form_submit_button("Deactivate", use_container_width=True)

                if update_submit:
                    result = call_labels_api("PUT", {
                        "label_id": label["label_id"],
                        "name": edit_name,
                        "description": edit_desc,
                    })
                    if result and "label_id" in result:
                        st.success("Label updated!")
                        st.info("Labeling queued — updated labels will appear in search results shortly.")
                        st.rerun()

                if delete_submit:
                    result = call_labels_api("DELETE", {"label_id": label["label_id"]})
                    if result and result.get("success"):
                        st.success("Label deactivated!")
                        st.info("Labeling queued — updated labels will appear in search results shortly.")
                        st.rerun()
    else:
        st.info("No labels available to edit. Add a label first.")
