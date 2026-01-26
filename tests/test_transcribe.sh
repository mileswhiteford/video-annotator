curl -X POST "https://video-annotator-function-ftdggpcvfyb0ehee.eastus-01.azurewebsites.net/api/TranscribeHttp?code=<INSERT>" \
  -H "Content-Type: application/json" \
  -d '{
    "media_url": "https://storagevideoannotator.blob.core.windows.net/speech-input/measles_short.m4a...",
    "locale": "en-US",
    "display_name": "mvp-test",
    "auto_segment": true,
    "segment_ms": 30000,
    "video_id": "test_001"
  }'

