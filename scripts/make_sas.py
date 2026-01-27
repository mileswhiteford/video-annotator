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
