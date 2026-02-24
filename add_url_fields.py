#!/usr/bin/env python3
"""
Add URL tracking fields to Azure Search index
"""

import os
import requests
import sys

# Load from .env file
env_path = os.path.join(os.path.dirname(__file__), "ui", ".env")
env_vars = {}

if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                env_vars[key] = value

SEARCH_ENDPOINT = env_vars.get("SEARCH_ENDPOINT")
SEARCH_KEY = env_vars.get("SEARCH_KEY")
SEARCH_INDEX_NAME = env_vars.get("SEARCH_INDEX_NAME", "segments")

print(f"Endpoint: {SEARCH_ENDPOINT}")
print(f"Index: {SEARCH_INDEX_NAME}")
print(f"Key: {'*' * 10}{SEARCH_KEY[-4:] if SEARCH_KEY else 'NOT FOUND'}")
print()

if not SEARCH_ENDPOINT or not SEARCH_KEY:
    print("ERROR: Missing SEARCH_ENDPOINT or SEARCH_KEY in .env")
    sys.exit(1)

API_VERSION = "2024-07-01"

def get_index():
    url = f"{SEARCH_ENDPOINT}/indexes/{SEARCH_INDEX_NAME}?api-version={API_VERSION}"
    headers = {"api-key": SEARCH_KEY}
    
    print(f"Fetching index: {url}")
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to get index: {response.status_code}")
        print(response.text)
        return None

def update_index(index_def):
    url = f"{SEARCH_ENDPOINT}/indexes/{SEARCH_INDEX_NAME}?api-version={API_VERSION}"
    headers = {
        "Content-Type": "application/json",
        "api-key": SEARCH_KEY
    }
    
    response = requests.put(url, headers=headers, json=index_def)
    
    if response.status_code in [200, 201]:
        print("✅ Index updated successfully!")
        return True
    else:
        print(f"❌ Failed to update: {response.status_code}")
        print(response.text)
        return False

def main():
    print("Fetching current index...")
    index = get_index()
    if not index:
        sys.exit(1)
    
    existing_fields = {f["name"] for f in index.get("fields", [])}
    print(f"Existing fields: {existing_fields}")
    print()
    
    new_fields = [
        {
            "name": "source_url",
            "type": "Edm.String",
            "searchable": False,
            "filterable": True,
            "retrievable": True,
            "sortable": False,
            "facetable": False,
            "key": False
        },
        {
            "name": "source_type",
            "type": "Edm.String",
            "searchable": False,
            "filterable": True,
            "retrievable": True,
            "sortable": False,
            "facetable": True,
            "key": False
        },
        {
            "name": "processed_at",
            "type": "Edm.DateTimeOffset",
            "searchable": False,
            "filterable": True,
            "retrievable": True,
            "sortable": True,
            "facetable": False,
            "key": False
        }
    ]
    
    added = 0
    for field in new_fields:
        if field["name"] in existing_fields:
            print(f"⚠️  Already exists: {field['name']}")
        else:
            print(f"➕ Adding: {field['name']}")
            index["fields"].append(field)
            added += 1
    
    if added == 0:
        print("\n✅ All fields already present!")
        return
    
    print(f"\n💾 Saving with {added} new fields...")
    if update_index(index):
        print("\n🎉 SUCCESS! URL tracking fields added.")
        print("\nNext steps:")
        print("1. Restart your Streamlit app")
        print("2. Go to 'System Diagnostics' page")
        print("3. Click 'Check Index Schema' to verify")

if __name__ == "__main__":
    main()
