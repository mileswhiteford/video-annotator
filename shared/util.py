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
