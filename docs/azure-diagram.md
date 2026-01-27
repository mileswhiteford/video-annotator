# Video Annotator - Azure Infrastructure Diagram

```mermaid
graph TB
    subgraph External["External Services"]
        Box[Box Shared Folder<br/>Source videos .m4a]
    end
    
    subgraph Local["Local Development"]
        BoxScript[box_shared_folder_manifest.py<br/>Generate manifest]
        ImportScript[import_videos.py<br/>Pipeline orchestrator]
    end
    
    subgraph FunctionApp["Azure Function App<br/>video-annotator-function"]
        TranscribeFn[TranscribeHttp<br/>HTTP Trigger<br/>POST /api/TranscribeHttp]
        SegmentFn[SegmentTranscript<br/>HTTP Trigger<br/>POST /api/SegmentTranscript]
        EmbedFn[EmbedAndIndex<br/>HTTP Trigger<br/>POST /api/EmbedAndIndex]
        SearchFn[SearchSegments<br/>HTTP Trigger<br/>POST /api/SearchSegments]
    end
    
    subgraph Storage["Azure Storage Account"]
        BlobStorage[Blob Storage<br/>segments container<br/>segments/vid_xxx.json]
    end
    
    subgraph CognitiveServices["Azure Cognitive Services"]
        SpeechAPI[Azure Speech Service<br/>Batch Transcription API<br/>SPEECH_KEY, SPEECH_REGION]
    end
    
    subgraph AI["Azure AI Services"]
        OpenAI[Azure OpenAI<br/>Embeddings API<br/>EMBEDDINGS_ENDPOINT<br/>EMBEDDINGS_DEPLOYMENT]
        AISearch[Azure AI Search<br/>segments index<br/>SEARCH_ENDPOINT<br/>SEARCH_INDEX]
    end
    
    subgraph ContainerApp["Azure Container Apps"]
        StreamlitApp[Streamlit UI<br/>Container App<br/>video-annotator-ui]
    end
    
    subgraph Monitoring["Monitoring & Logging"]
        AppInsights[Application Insights<br/>Function telemetry]
    end
    
    %% External to Local
    Box -->|List files| BoxScript
    BoxScript -->|videos.jsonl| ImportScript
    
    %% Local to Function App
    ImportScript -->|POST media_url| TranscribeFn
    ImportScript -->|POST segments_blob| EmbedFn
    
    %% Function App Internal Flow
    TranscribeFn -->|Submit job| SpeechAPI
    SpeechAPI -->|Poll status| TranscribeFn
    SpeechAPI -->|Return transcript| TranscribeFn
    TranscribeFn -->|Write segments| BlobStorage
    
    %% Embedding Flow
    EmbedFn -->|Read segments| BlobStorage
    EmbedFn -->|Batch embed| OpenAI
    OpenAI -->|Return vectors| EmbedFn
    EmbedFn -->|Index documents| AISearch
    
    %% Search Flow
    StreamlitApp -->|POST search query| SearchFn
    SearchFn -->|Embed query| OpenAI
    OpenAI -->|Query vector| SearchFn
    SearchFn -->|Hybrid/Vector/Keyword| AISearch
    AISearch -->|Return results| SearchFn
    SearchFn -->|JSON response| StreamlitApp
    
    %% Monitoring
    FunctionApp -->|Telemetry| AppInsights
    
    %% Styling
    classDef externalClass fill:#ffebee,stroke:#c62828,stroke-width:2px
    classDef localClass fill:#e3f2fd,stroke:#1565c0,stroke-width:2px
    classDef functionClass fill:#fff3e0,stroke:#ef6c00,stroke-width:3px
    classDef storageClass fill:#f3e5f5,stroke:#6a1b9a,stroke-width:2px
    classDef cognitiveClass fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px
    classDef aiClass fill:#e0f2f1,stroke:#00695c,stroke-width:2px
    classDef containerClass fill:#fce4ec,stroke:#c2185b,stroke-width:2px
    classDef monitorClass fill:#fff9c4,stroke:#f57f17,stroke-width:2px
    
    class Box externalClass
    class BoxScript,ImportScript localClass
    class TranscribeFn,SegmentFn,EmbedFn,SearchFn,FunctionApp functionClass
    class BlobStorage,Storage storageClass
    class SpeechAPI,CognitiveServices cognitiveClass
    class OpenAI,AISearch,AI aiClass
    class StreamlitApp,ContainerApp containerClass
    class AppInsights,Monitoring monitorClass
```

## Azure Services Overview

### Azure Function App
**Resource**: `video-annotator-function`  
**Runtime**: Python 3.11+  
**Functions**:
- **TranscribeHttp** - HTTP trigger, handles batch transcription submission and polling
- **SegmentTranscript** - HTTP trigger, segments transcripts into 30-second clips
- **EmbedAndIndex** - HTTP trigger, generates embeddings and indexes segments
- **SearchSegments** - HTTP trigger, performs hybrid/vector/keyword search

**Configuration** (App Settings):
- `SPEECH_KEY`, `SPEECH_REGION`, `SPEECH_ENDPOINT`, `SPEECH_API_VERSION`
- `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`
- `SEGMENTS_CONTAINER` (default: "segments")
- `EMBEDDINGS_ENDPOINT`, `EMBEDDINGS_KEY`, `EMBEDDINGS_DEPLOYMENT`, `EMBEDDINGS_API_VERSION`
- `SEARCH_ENDPOINT`, `SEARCH_ADMIN_KEY`, `SEARCH_QUERY_KEY`, `SEARCH_INDEX` (default: "segments")

### Azure Storage Account
**Service**: Blob Storage  
**Container**: `segments`  
**Blob Format**: `segments/vid_xxx.json`  
**Content**: JSON files containing video segments with transcript text, timestamps, and metadata

### Azure Speech Service
**Service**: Cognitive Services - Speech  
**API**: Batch Transcription API  
**Features**: 
- Word-level timestamps
- Multi-channel support (uses channel 0)
- TTL: 24 hours for transcription jobs

### Azure OpenAI
**Service**: Azure OpenAI Service  
**API**: Embeddings API  
**Deployment**: Custom embedding model deployment  
**Usage**: Generates vector embeddings for segment text and search queries

### Azure AI Search
**Service**: Azure Cognitive Search  
**Index**: `segments`  
**Features**:
- Hybrid search (keyword + vector)
- Vector search support
- Keyword search support
- Document fields: `segment_key`, `video_id`, `segment_id`, `start_ms`, `end_ms`, `text`, `embedding`

### Azure Container Apps
**Resource**: `video-annotator-ui`  
**Runtime**: Python Streamlit  
**Configuration**:
- Environment variables: `SEARCH_FN_URL`, `DEFAULT_MODE`, `DEFAULT_TOP`, `DEFAULT_K`
- Scaling: 0-1 replicas (scale to zero enabled)

### Application Insights
**Service**: Monitoring and Telemetry  
**Usage**: Function App logging, performance monitoring, error tracking

## Authentication & Security

- **Function App**: Function-level auth keys (`?code=...` in URLs)
- **Storage**: Storage account key for read/write access
- **Speech Service**: API key authentication
- **OpenAI**: API key authentication
- **AI Search**: Admin key (indexing) and Query key (search)
- **Box API**: Developer token or OAuth tokens (stored locally in `.env`)

## Deployment

### Function App Deployment
```bash
func azure functionapp publish video-annotator-function
```

### Container App Deployment
```bash
az containerapp up --name video-annotator-ui --resource-group video-annotator-robot
```

## Resource Group
**Name**: `video-annotator-robot`  
**Location**: `eastus` (default)
