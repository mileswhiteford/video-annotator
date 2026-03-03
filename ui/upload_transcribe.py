"""
upload_transcribe.py - Upload & Transcribe page for the Video Annotation Platform
"""

import streamlit as st
import time
import pandas as pd
import io
import tempfile
from typing import Tuple, Optional

# Import shared utilities from ui_search (must be in same directory)
import ui_search

def show_upload_transcribe_page():
    """Display the Upload & Transcribe page."""
    st.header("Upload Video for Transcription")
    
    # Check URL fields status
    url_status = ui_search.check_url_fields_status()
    
    if url_status['fields_exist']:
        st.success("✅ URL Tracking Enabled - Original source URLs will be stored")
    else:
        st.warning(f"""
        ⚠️ **Partial URL Tracking** - Missing fields: {', '.join(url_status['missing_fields'])}
        
        Videos will still be processed, but URL information will be limited.
        Add missing fields to your Azure Search index for full functionality.
        """)
    
    # Check Azure configuration
    azure_configured = bool(ui_search.AZURE_STORAGE_KEY) and bool(ui_search.SPEECH_KEY)
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
                video_id = ui_search.generate_video_id(uploaded_file.name)
                detected_source_type = "upload"
                st.info("File ready for upload")
    
    # --- Direct URL ---
    elif source_type == "Direct URL":
        url_input = st.text_input("Media URL", placeholder="https://tulane.box.com/shared/static/    ...")
        
        if url_input.strip():
            media_url = url_input.strip()
            video_id = ui_search.generate_video_id(url_input)
            detected_source_type = "direct"
            st.success("✅ URL validated")
    
    # --- YouTube ---
    elif source_type == "YouTube":
        yt_url = st.text_input(
            "YouTube URL",
            placeholder="https://youtube.com/watch?v=    ...",
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
        if not ui_search.check_yt_dlp():
            st.warning("yt-dlp not installed")
            if st.button("Install yt-dlp"):
                with st.spinner("Installing..."):
                    import subprocess
                    subprocess.run(["pip", "install", "-q", "yt-dlp"])
                try:
                    st.rerun()
                except:
                    st.info("Please refresh the page")
        elif yt_url and yt_url.strip():
            video_id = ui_search.generate_video_id(f"yt_{yt_url.strip()}")
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
                    if ui_search.detect_url_type(col_str) != "unknown":
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
                        url_type = ui_search.detect_url_type(url)
                        icon = "🎬" if url_type == "youtube" else "📄" if url_type == "direct" else "❓"
                        st.text(f"{i}. {icon} {url[:80]}...")
                
                # Validate
                valid_urls = []
                invalid_urls = []
                for url in urls_to_process:
                    url_type = ui_search.detect_url_type(str(url))
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
        can_process = len(str(yt_url_to_check).strip()) > 0 and ui_search.check_yt_dlp()
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
                url_type = ui_search.detect_url_type(url)
                src_type = "youtube" if url_type == "youtube" else "direct"
                
                # Process
                result = ui_search.process_single_video(
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
                    
                    sas_url, error = ui_search.upload_to_azure_blob_sdk(file_bytes, blob_name)
                    if error and ("not installed" in error or "SDK" in error):
                        sas_url, error = ui_search.upload_to_azure_blob_fixed(file_bytes, blob_name)
                    
                    if error:
                        raise Exception(error)
                    
                    media_url = sas_url
                    progress_bar.progress(50)
                
                # Download YouTube if needed
                elif source_type == "YouTube":
                    yt_url = st.session_state.get('yt_url_value', '')
                    
                    if not yt_url or not yt_url.strip():
                        raise Exception("YouTube URL is empty")
                    
                    with tempfile.TemporaryDirectory() as tmpdir:
                        progress_bar.progress(10)
                        status.text("Downloading from YouTube...")
                        
                        output_path = f"{tmpdir}/youtube_{video_id}.m4a"
                        downloaded_path, error = ui_search.download_youtube_audio(yt_url.strip(), output_path)
                        
                        if error:
                            raise Exception(error)
                        
                        progress_bar.progress(50)
                        status.text("Uploading to Azure Blob...")
                        
                        with open(downloaded_path, 'rb') as f:
                            file_bytes = f.read()
                        
                        blob_name = f"youtube_{video_id}_{int(time.time())}.m4a"
                        
                        sas_url, error = ui_search.upload_to_azure_blob_sdk(file_bytes, blob_name)
                        if error and ("not installed" in error):
                            sas_url, error = ui_search.upload_to_azure_blob_fixed(file_bytes, blob_name)
                        
                        if error:
                            raise Exception(error)
                        
                        media_url = sas_url
                        progress_bar.progress(75)
                
                if not media_url:
                    raise Exception("No media URL available")
                
                # Transcribe
                status.text("Submitting to Azure Speech-to-Text...")
                result = ui_search.submit_transcription_direct(video_id, media_url)
                operation_url = result.get("operation_url")
                
                if not operation_url:
                    raise Exception("No operation URL returned")
                
                # Poll
                max_polls = 120
                transcription_data = None
                
                for i in range(max_polls):
                    time.sleep(ui_search.POLL_SECONDS)
                    poll_result = ui_search.poll_transcription_operation(operation_url)
                    status_text = poll_result.get("status", "unknown")
                    
                    progress = min(75 + int((i / max_polls) * 20), 95)
                    progress_bar.progress(progress)
                    status.text(f"Transcribing... ({i * ui_search.POLL_SECONDS // 60} min) - Status: {status_text}")
                    
                    if status_text.lower() == "succeeded":
                        transcription_data = ui_search.get_transcription_from_result(poll_result)
                        break
                    elif status_text.lower() == "failed":
                        raise Exception(f"Transcription failed: {poll_result.get('properties', {}).get('error', {}).get('message', 'Unknown error')}")
                
                if not transcription_data:
                    raise Exception("Transcription timed out")
                
                # Process and index
                progress_bar.progress(98)
                status.text("Processing segments and indexing...")
                
                segments = ui_search.process_transcription_to_segments(transcription_data, video_id)
                
                # Save to blob
                ui_search.save_segments_to_blob(video_id, segments)
                
                # Index with URL tracking
                original_url = None
                if source_type == "YouTube":
                    original_url = st.session_state.get('yt_url_value', '')
                elif source_type == "Direct URL":
                    original_url = media_url
                elif source_type == "File Upload":
                    original_url = f"uploaded_file://{video_id}"
                
                index_result = ui_search.index_segments_direct(
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
                        st.write(f"**{ui_search.ms_to_ts(seg['start_ms'])} - {ui_search.ms_to_ts(seg['end_ms'])}:** {seg['text'][:100]}...")
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                st.exception(e)