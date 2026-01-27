"""
shared/util.py - Utility Functions for Azure Blob Storage

This module provides helper functions for working with Azure Blob Storage,
specifically for writing JSON data to blob containers.

Architecture Role:
- Shared utility used by Azure Functions (TranscribeHttp, SegmentTranscript)
- Handles blob uploads with automatic container creation
- Provides consistent JSON serialization and blob path formatting

Functions:
  - write_json_blob: Upload JSON payload to blob storage, returns path
"""

import json
import os
from azure.storage.blob import BlobServiceClient

def write_json_blob(container: str, blob_name: str, payload: dict) -> str:
    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    key = os.environ["AZURE_STORAGE_KEY"]
    service = BlobServiceClient(
        account_url=f"https://{account}.blob.core.windows.net",
        credential=key,
    )
    # create container if needed
    try:
        service.get_container_client(container).create_container()
    except Exception:
        pass

    bc = service.get_blob_client(container=container, blob=blob_name)
    bc.upload_blob(json.dumps(payload, ensure_ascii=False), overwrite=True)
    return f"{container}/{blob_name}"
