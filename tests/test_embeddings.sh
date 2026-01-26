curl -sS -X POST \
  "https://shared-openai-resource.cognitiveservices.azure.com/openai/deployments/text-embedding-3-small/embeddings?api-version=2024-10-21" \
  -H "Content-Type: application/json" \
  -H "api-key: <KEY>" \
  -d '{"input":["hello world"]}' | python -c 'import sys,json; d=json.load(sys.stdin); print(len(d["data"][0]["embedding"]))'