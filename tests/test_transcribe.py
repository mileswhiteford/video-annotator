"""
tests/test_transcribe.py - Local Transcription Testing Script

This test script provides a simple way to test the transcription pipeline
locally with a sample audio file. It:
1. Loads configuration from local.settings.json
2. Uploads local audio file to blob storage
3. Submits transcription job and polls until completion
4. Downloads and normalizes transcript
5. Prints sample utterances

Architecture Role:
- Development/testing utility
- Validates transcription pipeline without Azure Functions
- Useful for debugging Speech Service integration
- Tests the full transcribe_and_normalize_from_local_file workflow

Usage:
  python tests/test_transcribe.py

Configuration (via local.settings.json):
  - SPEECH_KEY, SPEECH_REGION/SPEECH_ENDPOINT
  - AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY
  - INPUT_CONTAINER, OUTPUT_CONTAINER

Note: Requires local audio file (default: measles_short.m4a)
"""

import json
import  os
from shared.speech_batch import transcribe_and_normalize_from_local_file


def load_local_settings(path="local.settings.json"):
    if not os.path.exists(path):
        return
    data = json.load(open(path, "r", encoding="utf-8"))
    values = data.get("Values", {})
    for k, v in values.items():
        # don't overwrite real env vars if already set
        os.environ.setdefault(k, v)

if __name__ == "__main__":
    load_local_settings()

    out = transcribe_and_normalize_from_local_file(
        local_media_path="measles_short.m4a",
        locale="en-US",
    )

    print(json.dumps(out["utterances"][:5], indent=2))
    print(f"\nTotal utterances: {len(out['utterances'])}")

