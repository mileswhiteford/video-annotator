"""
scripts/make_sas.py - Generate SAS URL for Blob Storage Testing

This utility script generates a Shared Access Signature (SAS) URL for a blob
in Azure Storage. Useful for testing transcription with local audio files
or generating URLs for manual transcription job submission.

Architecture Role:
- Development/testing utility
- Helps create accessible URLs for Azure Speech Service
- Used when testing transcription with files already in blob storage

Usage:
  python scripts/make_sas.py

Configuration (via local.settings.json or .env):
  - AZURE_STORAGE_ACCOUNT: Storage account name
  - AZURE_STORAGE_KEY: Storage account key
  - INPUT_CONTAINER: Container name (default: speech-input)

Note: Modify blob_name variable in script to target specific file
"""

# scripts/make_sas.py
import os
from shared.speech_batch import make_sas_url
from test_transcribe import load_local_settings

if __name__ == "__main__":
    load_local_settings()
    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    key = os.environ["AZURE_STORAGE_KEY"]
    container = os.environ.get("INPUT_CONTAINER", "speech-input")

    blob_name = "measles_short.m4a"  # <-- change this

    print(make_sas_url(
        account=account,
        container=container,
        account_key=key,
        blob_name=blob_name,
        hours=12,
    ))
