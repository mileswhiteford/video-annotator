RG="video-annotator-robot"
APP="video-annotator-ui"

az containerapp up \
  --name "$APP" \
  --resource-group "$RG" \
  --source .

