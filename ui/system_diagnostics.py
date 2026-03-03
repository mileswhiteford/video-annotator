"""
system_diagnostics.py - System Diagnostics page for the Video Annotation Platform
"""

import streamlit as st

import ui_search

def show_system_diagnostics_page():
    """Display the System Diagnostics page."""
    st.header("⚙️ System Diagnostics")
    st.info("Check system configuration and troubleshoot issues")
    
    # Configuration status
    st.subheader("Configuration Status")
    
    config_checks = {
        "Azure Speech (SPEECH_KEY)": bool(ui_search.SPEECH_KEY),
        "Azure OpenAI (AZURE_OPENAI_KEY)": bool(ui_search.AZURE_OPENAI_KEY),
        "Azure Search (SEARCH_KEY)": bool(ui_search.SEARCH_KEY),
        "Azure Storage (AZURE_STORAGE_KEY)": bool(ui_search.AZURE_STORAGE_KEY),
        "Search Function (SEARCH_FN_URL)": bool(ui_search.SEARCH_FN_URL),
        "yt-dlp installed": ui_search.check_yt_dlp()
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
            schema = ui_search.debug_check_index_schema()
            
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
                        facet = "📊" if field.get('facetable') else ""
                        st.caption(f"{key}{url}{facet} `{field['name']}` ({field['type']}) - facetable: {field.get('facetable', False)}")
                
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