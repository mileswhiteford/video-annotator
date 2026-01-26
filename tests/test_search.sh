curl -X POST "$SEARCH_ENDPOINT/indexes/segments/docs/search?api-version=2024-05-01-preview" \
  -H "Content-Type: application/json" \
  -H "api-key: $SEARCH_ADMIN_KEY" \
  -d '{
    "search": "vaccine",
    "top": 5
  }'
