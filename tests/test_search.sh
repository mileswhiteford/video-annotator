# hybrid (keyword + vector search)
curl -X POST "http://localhost:7071/api/SearchSegments" \
  -H "Content-Type: application/json" \
  -d '{"q":"measles misinformation","mode":"hybrid","top":5,"k":40}'

# vector only
curl -X POST "http://localhost:7071/api/SearchSegments" \
  -H "Content-Type: application/json" \
  -d '{"q":"childhood vaccination risks","mode":"vector","top":5,"k":40}'

# hybrid + filter by video_id
curl -X POST "http://localhost:7071/api/SearchSegments" \
  -H "Content-Type: application/json" \
  -d '{"q":"measles","mode":"hybrid","top":5,"video_id":"vid_130099394193495225"}'
