# Video Annotator - Data Flow Diagram

```mermaid
flowchart TD
    %% Input Sources
    Box[Box Shared Folder<br/>.m4a video files]
    
    %% Manifest Generation
    BoxScript[box_shared_folder_manifest.py]
    Manifest[videos.jsonl<br/>video_id, media_url]
    
    %% Import Pipeline
    Import[import_videos.py<br/>Orchestrator]
    State[pipeline_state.json<br/>Progress tracking]
    
    %% Transcription Function
    TranscribeFn[TranscribeHttp Function]
    SpeechAPI[Azure Speech Service<br/>Batch Transcription]
    Transcript[Transcript JSON<br/>utterances with timestamps]
    
    %% Segmentation
    Segments[30-second Segments<br/>segment_id, start_ms, end_ms, text]
    
    %% Storage
    BlobStorage[(Azure Blob Storage<br/>segments/container)]
    SegmentsBlob[segments/vid_xxx.json]
    
    %% Embedding & Indexing
    EmbedFn[EmbedAndIndex Function]
    OpenAI[Azure OpenAI<br/>Embeddings API]
    Embeddings[Vector Embeddings<br/>per segment]
    SearchIndex[(Azure AI Search<br/>segments index)]
    
    %% Search & UI
    SearchFn[SearchSegments Function]
    StreamlitUI[Streamlit UI<br/>ui_search.py]
    User[User]
    
    %% Data Flow - Ingestion
    Box -->|List .m4a files| BoxScript
    BoxScript -->|Generate manifest| Manifest
    Manifest -->|Read videos| Import
    Import -->|Track progress| State
    
    %% Transcription Flow
    Import -->|POST media_url| TranscribeFn
    TranscribeFn -->|Submit batch job| SpeechAPI
    SpeechAPI -->|Poll job status| TranscribeFn
    SpeechAPI -->|Return transcript| Transcript
    TranscribeFn -->|Normalize + segment| Segments
    
    %% Storage Flow
    TranscribeFn -->|Write segments| BlobStorage
    BlobStorage -->|segments/vid_xxx.json| SegmentsBlob
    
    %% Embedding & Indexing Flow
    Import -->|POST segments_blob| EmbedFn
    EmbedFn -->|Read segments| SegmentsBlob
    SegmentsBlob -->|Extract text| EmbedFn
    EmbedFn -->|Batch embed texts| OpenAI
    OpenAI -->|Return vectors| Embeddings
    EmbedFn -->|Index documents| SearchIndex
    Embeddings -->|Include in docs| SearchIndex
    
    %% Search Flow
    User -->|Enter query| StreamlitUI
    StreamlitUI -->|POST search request| SearchFn
    SearchFn -->|Embed query| OpenAI
    OpenAI -->|Query vector| SearchFn
    SearchFn -->|Hybrid/Vector/Keyword search| SearchIndex
    SearchIndex -->|Return results| SearchFn
    SearchFn -->|Format results| StreamlitUI
    StreamlitUI -->|Display segments| User
    
    %% Styling
    classDef inputClass fill:#e1f5ff,stroke:#01579b,stroke-width:2px
    classDef functionClass fill:#fff3e0,stroke:#e65100,stroke-width:2px
    classDef storageClass fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    classDef apiClass fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px
    classDef uiClass fill:#fce4ec,stroke:#880e4f,stroke-width:2px
    
    class Box,Manifest inputClass
    class TranscribeFn,SegmentFn,EmbedFn,SearchFn,Import,BoxScript functionClass
    class BlobStorage,SegmentsBlob,SearchIndex,State storageClass
    class SpeechAPI,OpenAI apiClass
    class StreamlitUI,User uiClass
```

## Key Components

### Ingestion Pipeline
1. **Box Shared Folder** → Contains source `.m4a` video files
2. **box_shared_folder_manifest.py** → Enumerates videos and generates `videos.jsonl`
3. **import_videos.py** → Orchestrates the entire pipeline, tracks progress in `pipeline_state.json`

### Transcription & Segmentation
4. **TranscribeHttp Function** → Submits batch transcription jobs to Azure Speech Service
5. **Azure Speech Service** → Processes audio and returns transcripts with word-level timestamps
6. **Segmentation** → Splits transcripts into 30-second segments (handled by `shared/segmenter.py`)
7. **Blob Storage** → Stores segment JSON files (`segments/vid_xxx.json`)

### Embedding & Indexing
8. **EmbedAndIndex Function** → Reads segments from Blob, generates embeddings, and indexes to Azure AI Search
9. **Azure OpenAI** → Generates vector embeddings for segment text
10. **Azure AI Search** → Stores indexed segments with embeddings for hybrid search

### Search & UI
11. **SearchSegments Function** → Handles search queries (keyword, vector, or hybrid mode)
12. **Streamlit UI** → Provides web interface for users to search indexed segments

## Data Formats

- **videos.jsonl**: `{"video_id": "vid_123", "media_url": "https://..."}`
- **segments JSON**: `{"video_id": "...", "segments": [{"segment_id": "0000", "start_ms": 0, "end_ms": 30000, "text": "..."}]}`
- **Search Index**: Documents with `segment_key`, `video_id`, `segment_id`, `text`, `embedding`, `start_ms`, `end_ms`
