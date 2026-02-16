"""
ManageLabels - Azure Function for Label Library Management

This Azure Function manages label definitions for AI-powered segment labeling:
1. Stores label definitions in Azure Blob Storage (labels/label_library.json)
2. Supports CRUD operations (Create, Read, Update, Delete)
3. Validates label uniqueness and format

Architecture Role:
- Provides label management for the annotation system
- Accessed by UI for label CRUD operations
- Read by future function for AI labeling

Input: HTTP request (GET/POST/PUT/DELETE)
Output: JSON with label data or operation status
"""

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import azure.functions as func
from azure.storage.blob import BlobServiceClient


def _blob_service() -> BlobServiceClient:
    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    key = os.environ["AZURE_STORAGE_KEY"]
    return BlobServiceClient(
        account_url=f"https://{account}.blob.core.windows.net",
        credential=key,
    )


def _read_label_json() -> Dict[str, Any]:
    try:
        service = _blob_service()
        container = os.environ.get("LABELS_CONTAINER", "labels")
        bc = service.get_blob_client(container=container, blob="label_library.json")
        data = bc.download_blob().readall()
        return json.loads(data)
    except Exception:
        # Return empty library if not found
        return {
            "labels": [],
            "last_updated": datetime.now(timezone.utc).isoformat()
        }


def _write_label_json(library: Dict[str, Any]) -> str:
    service = _blob_service()
    container = os.environ.get("LABELS_CONTAINER", "labels")

    # Create container if needed
    try:
        service.get_container_client(container).create_container()
    except Exception:
        pass

    bc = service.get_blob_client(container=container, blob="label_library.json")
    bc.upload_blob(json.dumps(library, ensure_ascii=False, indent=2), overwrite=True)
    return f"{container}/label_library.json"


def _validate_label_name(name: str, library: Dict[str, Any], exclude_id: Optional[str] = None) -> bool:
    """Check if label name is unique among active labels"""
    for label in library.get("labels", []):
        if label["label_id"] == exclude_id:
            continue
        if label["name"].lower() == name.lower() and label.get("is_active", True):
            return False
    return True


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        method = req.method.upper()

        if method == "GET":
            library = _read_label_json()
            # Filter to active labels only (unless query param says otherwise)
            include_inactive = req.params.get("include_inactive", "false").lower() == "true"
            if not include_inactive:
                library["labels"] = [l for l in library["labels"] if l.get("is_active", True)]

            return func.HttpResponse(
                json.dumps(library, ensure_ascii=False),
                mimetype="application/json",
                status_code=200,
            )

        # POST/PUT/DELETE: Modify labels
        body = req.get_json()
        library = _read_label_json()

        # POST: Add new label
        if method == "POST":
            name = body.get("name", "").strip()
            description = body.get("description", "").strip()

            if not name:
                return func.HttpResponse(
                    json.dumps({"error": "Label 'name' is required"}),
                    mimetype="application/json",
                    status_code=400,
                )

            if not _validate_label_name(name, library):
                return func.HttpResponse(
                    json.dumps({"error": f"Label '{name}' already exists"}),
                    mimetype="application/json",
                    status_code=400,
                )

            now = datetime.now(timezone.utc).isoformat()
            new_label = {
                "label_id": str(uuid.uuid4()),
                "name": name,
                "description": description,
                "created_at": now,
                "updated_at": now,
                "is_active": True,
            }

            library["labels"].append(new_label)
            library["last_updated"] = now
            _write_label_json(library)

            return func.HttpResponse(
                json.dumps(new_label, ensure_ascii=False),
                mimetype="application/json",
                status_code=201,
            )

        # PUT: Update existing label
        elif method == "PUT":
            label_id = body.get("label_id")
            if not label_id:
                return func.HttpResponse(
                    json.dumps({"error": "'label_id' is required for update"}),
                    mimetype="application/json",
                    status_code=400,
                )

            label = next((l for l in library["labels"] if l["label_id"] == label_id), None)
            if not label:
                return func.HttpResponse(
                    json.dumps({"error": f"Label '{label_id}' not found"}),
                    mimetype="application/json",
                    status_code=404,
                )

            # Update fields
            if "name" in body:
                new_name = body["name"].strip()
                if new_name and not _validate_label_name(new_name, library, exclude_id=label_id):
                    return func.HttpResponse(
                        json.dumps({"error": f"Label '{new_name}' already exists"}),
                        mimetype="application/json",
                        status_code=400,
                    )
                label["name"] = new_name

            if "description" in body:
                label["description"] = body["description"].strip()

            if "is_active" in body:
                label["is_active"] = bool(body["is_active"])

            label["updated_at"] = datetime.now(timezone.utc).isoformat()
            library["last_updated"] = label["updated_at"]
            _write_label_json(library)

            return func.HttpResponse(
                json.dumps(label, ensure_ascii=False),
                mimetype="application/json",
                status_code=200,
            )

        # DELETE: Soft delete label
        elif method == "DELETE":
            label_id = body.get("label_id")
            if not label_id:
                return func.HttpResponse(
                    json.dumps({"error": "'label_id' is required for delete"}),
                    mimetype="application/json",
                    status_code=400,
                )

            label = next((l for l in library["labels"] if l["label_id"] == label_id), None)
            if not label:
                return func.HttpResponse(
                    json.dumps({"error": f"Label '{label_id}' not found"}),
                    mimetype="application/json",
                    status_code=404,
                )

            label["is_active"] = False
            label["updated_at"] = datetime.now(timezone.utc).isoformat()
            library["last_updated"] = label["updated_at"]
            _write_label_json(library)

            return func.HttpResponse(
                json.dumps({"success": True, "message": "Label deactivated"}),
                mimetype="application/json",
                status_code=200,
            )

        else:
            return func.HttpResponse(
                json.dumps({"error": f"Unsupported HTTP method '{method}'"}),
                mimetype="application/json",
                status_code=405,
            )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
