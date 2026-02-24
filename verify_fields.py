#!/usr/bin/env python3
"""Verify URL fields were added to the index"""

import os
import requests

# Load .env
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

API_VERSION = "2024-07-01"

def check_index():
    url = f"{SEARCH_ENDPOINT}/indexes/{SEARCH_INDEX_NAME}?api-version={API_VERSION}"
    headers = {"api-key": SEARCH_KEY}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        index = response.json()
        fields = {f["name"]: f["type"] for f in index.get("fields", [])}
        
        print("✅ Successfully connected to index!")
        print(f"\nTotal fields: {len(fields)}")
        print(f"\nChecking URL tracking fields:")
        
        url_fields = {
            "source_url": "Edm.String",
            "source_type": "Edm.String", 
            "processed_at": "Edm.DateTimeOffset"
        }
        
        all_present = True
        for field, expected_type in url_fields.items():
            if field in fields:
                print(f"  ✅ {field}: {fields[field]}")
            else:
                print(f"  ❌ {field}: MISSING")
                all_present = False
        
        if all_present:
            print("\n🎉 SUCCESS! All URL tracking fields are present!")
            print("\nYou can now:")
            print("1. Restart your Streamlit app")
            print("2. Process new videos - URLs will be stored automatically")
        else:
            print("\n⚠️  Some fields are missing. Run the add script again.")
            
        return all_present
    else:
        print(f"❌ Failed to get index: {response.status_code}")
        print(response.text)
        return False

if __name__ == "__main__":
    check_index()
