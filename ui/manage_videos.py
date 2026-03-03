"""
manage_videos.py - Manage Videos page for the Video Annotation Platform
"""

import streamlit as st
import pandas as pd
import io
import time

import ui_search

def show_manage_videos_page():
    """Display the Manage Videos page."""
    st.header("📚 Manage Stored Videos")
    st.info("View, search, and manage all processed videos and their source URLs")
    
    if not ui_search.SEARCH_ENDPOINT or not ui_search.SEARCH_KEY:
        st.error("Azure Search not configured. Cannot retrieve video list.")
    else:
        # Check URL fields status
        url_status = ui_search.check_url_fields_status()
        
        if url_status['fields_exist']:
            st.success("✅ URL tracking fields are configured")
        else:
            st.warning(f"⚠️ Missing URL fields: {', '.join(url_status['missing_fields'])}")
        
        # URL coverage analysis
        if st.button("📊 Analyze URL Data Coverage"):
            with st.spinner("Analyzing..."):
                all_videos = ui_search.get_stored_videos(include_missing=True)
                
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
        
        # Load videos button
        load_clicked = st.button("🔍 Load Videos", type="primary")
        
        # Handle deletion using session state
        if st.session_state.video_to_delete:
            vid_to_delete = st.session_state.video_to_delete
            
            with st.spinner(f"Deleting {vid_to_delete}..."):
                success = ui_search.delete_video_by_id(vid_to_delete)
            
            if success:
                # Remove from cache immediately
                if st.session_state.stored_videos_cache:
                    st.session_state.stored_videos_cache = [
                        v for v in st.session_state.stored_videos_cache 
                        if v.get('video_id') != vid_to_delete
                    ]
                st.success(f"✅ Deleted {vid_to_delete}")
                st.session_state.delete_success = True
            else:
                st.error(f"❌ Failed to delete {vid_to_delete}")
            
            # Clear the trigger
            st.session_state.video_to_delete = None
            time.sleep(0.5)
            st.rerun()
        
        # Load videos if button clicked
        if load_clicked:
            with st.spinner("Retrieving videos..."):
                
                # Handle special filters
                if filter_source_type == "Missing URL Data Only":
                    all_videos = ui_search.get_stored_videos(include_missing=True)
                    videos = [v for v in all_videos if not v.get('source_url') or v.get('source_type') in ['', 'unknown']]
                    if filter_video_id.strip():
                        videos = [v for v in videos if filter_video_id.strip().lower() in v.get('video_id', '').lower()]
                elif filter_source_type == "With URL Data Only":
                    all_videos = ui_search.get_stored_videos(include_missing=True)
                    videos = [v for v in all_videos if v.get('source_url') and v.get('source_type') not in ['', 'unknown']]
                    if filter_video_id.strip():
                        videos = [v for v in videos if filter_video_id.strip().lower() in v.get('video_id', '').lower()]
                else:
                    source_type = None if filter_source_type == "All" else filter_source_type
                    videos = ui_search.get_stored_videos(
                        video_id=filter_video_id if filter_video_id.strip() else None,
                        source_type=source_type,
                        include_missing=True,
                        limit=1000
                    )
                
                st.session_state.stored_videos_cache = videos
                st.session_state.videos_loaded = True
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
                                # Capture current vid value for callback
                                st.button(
                                    f"🗑️ Delete", 
                                    key=f"del_{vid}_{i}_{source_type}",
                                    on_click=lambda v=vid: setattr(st.session_state, 'video_to_delete', v)
                                )
                            
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