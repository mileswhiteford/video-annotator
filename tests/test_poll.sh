curl -X POST "https://video-annotator-function-ftdggpcvfyb0ehee.eastus-01.azurewebsites.net/api/TranscribeHttp?code=<INSERT_KEY>" \
  -H "Content-Type: application/json" \
  -d '{ "job_url": "https://eastus.api.cognitive.microsoft.com/speechtotext/transcriptions/..." }'
